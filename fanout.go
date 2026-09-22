package river

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"strings"
	"time"

	"github.com/riverqueue/river/rivertype"
)

// fanOutSpecVersion is the current version of the serialized fan-out spec
// stored under the reserved `river:fanout` metadata key.
const fanOutSpecVersion = 1

// FanOutChild describes a single child job to be inserted when the parent job
// is completed with JobCompleteTx.
type FanOutChild struct {
	// Args are the arguments of the child job to insert. A worker for the
	// args' kind must be registered on the client that completes the parent
	// job so that the args can be decoded at fan-out time.
	Args JobArgs

	// Opts are insertion options for the child job. If nil or if individual
	// fields are left at their zero values, the child inherits the parent's
	// priority and any whitelisted metadata keys, with all other fields
	// defaulting as they would for a normal insert. UniqueOpts and nested
	// FanOut are not supported on fan-out children.
	Opts *InsertOpts
}

// FanOutOpts describes a set of child jobs to insert atomically when a job is
// completed using JobCompleteTx. The spec is serialized into the job's
// metadata under the reserved `river:fanout` key at insert time so that it
// stays durable alongside the job itself.
type FanOutOpts struct {
	// Children are the child jobs to insert when the parent completes. At
	// least one child is required.
	Children []FanOutChild

	// MetadataKeys is a whitelist of metadata keys to copy from the parent
	// job's metadata onto each fanned-out child. Keys not present on the
	// parent are skipped, and keys prefixed with `river:` are reserved and
	// may not be listed. A child's own metadata takes precedence over
	// inherited values for the same key.
	MetadataKeys []string
}

// encodeIntoMetadata validates the fan-out options and returns the given
// metadata JSON with the serialized fan-out spec merged in under the reserved
// `river:fanout` key.
func (o *FanOutOpts) encodeIntoMetadata(metadata []byte) ([]byte, error) {
	if err := o.validate(); err != nil {
		return nil, err
	}

	spec, err := o.marshalSpec()
	if err != nil {
		return nil, err
	}

	var metadataMap map[string]json.RawMessage
	if err := json.Unmarshal(metadata, &metadataMap); err != nil {
		return nil, fmt.Errorf("error unmarshaling metadata: %w", err)
	}
	if metadataMap == nil {
		metadataMap = make(map[string]json.RawMessage)
	}
	if _, ok := metadataMap[rivertype.MetadataKeyFanOut]; ok {
		return nil, errors.New("metadata key `river:fanout` is reserved for fan-out and must not be set directly")
	}
	metadataMap[rivertype.MetadataKeyFanOut] = json.RawMessage(spec)

	return json.Marshal(metadataMap)
}

// marshalSpec serializes the fan-out options into their stored representation.
func (o *FanOutOpts) marshalSpec() ([]byte, error) {
	spec := fanOutSpecJSON{
		Children:     make([]fanOutChildJSON, 0, len(o.Children)),
		MetadataKeys: o.MetadataKeys,
		Version:      fanOutSpecVersion,
	}
	for _, child := range o.Children {
		encodedArgs, err := json.Marshal(child.Args)
		if err != nil {
			return nil, fmt.Errorf("error marshaling fan-out child args to JSON: %w", err)
		}

		childSpec := fanOutChildJSON{
			Args: encodedArgs,
			Kind: child.Args.Kind(),
		}
		if child.Opts != nil {
			childSpec.Opts = &fanOutChildOptsJSON{
				MaxAttempts: child.Opts.MaxAttempts,
				Metadata:    child.Opts.Metadata,
				Pending:     child.Opts.Pending,
				Priority:    child.Opts.Priority,
				Queue:       child.Opts.Queue,
				ScheduledAt: child.Opts.ScheduledAt,
				Tags:        child.Opts.Tags,
			}
		}
		spec.Children = append(spec.Children, childSpec)
	}

	return json.Marshal(spec)
}

// validate checks the fan-out options for errors at insert time.
func (o *FanOutOpts) validate() error {
	if len(o.Children) == 0 {
		return errors.New("fan-out requires at least one child")
	}
	for _, key := range o.MetadataKeys {
		if key == "" {
			return errors.New("fan-out metadata keys must not be empty")
		}
		if strings.HasPrefix(key, "river:") {
			return fmt.Errorf("fan-out metadata key %q uses reserved prefix `river:`", key)
		}
	}
	for _, child := range o.Children {
		if child.Args == nil {
			return errors.New("fan-out child args must not be nil")
		}
		if child.Opts == nil {
			continue
		}
		if child.Opts.FanOut != nil {
			return errors.New("fan-out children cannot themselves fan out (nesting is not supported)")
		}
		if !child.Opts.UniqueOpts.isEmpty() {
			return errors.New("fan-out children do not support UniqueOpts")
		}
	}
	return nil
}

// fanOutChildOptsJSON is the serialized form of a fan-out child's insert options.
type fanOutChildOptsJSON struct {
	MaxAttempts int       `json:"max_attempts,omitempty"`
	Metadata    []byte    `json:"metadata,omitempty"`
	Pending     bool      `json:"pending,omitempty"`
	Priority    int       `json:"priority,omitempty"`
	Queue       string    `json:"queue,omitempty"`
	ScheduledAt time.Time `json:"scheduled_at,omitzero"`
	Tags        []string  `json:"tags,omitempty"`
}

// fanOutChildJSON is the serialized form of a single fan-out child.
type fanOutChildJSON struct {
	Args json.RawMessage      `json:"args"`
	Kind string               `json:"kind"`
	Opts *fanOutChildOptsJSON `json:"opts,omitempty"`
}

// fanOutSpecJSON is the serialized form of a fan-out spec as stored under the
// reserved `river:fanout` metadata key.
type fanOutSpecJSON struct {
	Children     []fanOutChildJSON `json:"children"`
	MetadataKeys []string          `json:"metadata_keys,omitempty"`
	Version      int               `json:"version"`
}

// insertFanOutChildren inserts any fan-out children specified on the given
// parent job as part of transaction tx. It's a no-op for jobs without a
// fan-out spec in their metadata. Any error is returned so that the caller
// can roll back the transaction, leaving the parent job uncompleted rather
// than silently dropping children.
func (c *Client[TTx]) insertFanOutChildren(ctx context.Context, tx TTx, parentJob *rivertype.JobRow) error {
	spec, hasSpec, err := fanOutSpecFromMetadata(parentJob.Metadata)
	if err != nil {
		return err
	}
	if !hasSpec {
		return nil
	}

	var parentMetadata map[string]json.RawMessage
	if err := json.Unmarshal(parentJob.Metadata, &parentMetadata); err != nil {
		return fmt.Errorf("error unmarshaling parent job metadata: %w", err)
	}

	params := make([]InsertManyParams, 0, len(spec.Children))
	for _, child := range spec.Children {
		args, err := c.decodeFanOutChildArgs(child.Kind, child.Args)
		if err != nil {
			return err
		}

		opts := &InsertOpts{}
		if child.Opts != nil {
			opts.MaxAttempts = child.Opts.MaxAttempts
			opts.Pending = child.Opts.Pending
			opts.Queue = child.Opts.Queue
			opts.ScheduledAt = child.Opts.ScheduledAt
			opts.Tags = child.Opts.Tags
			opts.Priority = child.Opts.Priority
			opts.Metadata = child.Opts.Metadata
		}

		// Children inherit the parent's priority unless they specify one.
		if opts.Priority == 0 {
			opts.Priority = parentJob.Priority
		}

		metadata, err := fanOutChildMetadata(parentMetadata, spec.MetadataKeys, opts.Metadata)
		if err != nil {
			return err
		}
		opts.Metadata = metadata

		params = append(params, InsertManyParams{Args: args, InsertOpts: opts})
	}

	if _, err := c.InsertManyTx(ctx, tx, params); err != nil {
		return fmt.Errorf("error inserting fan-out children for job %d: %w", parentJob.ID, err)
	}

	return nil
}

// decodeFanOutChildArgs decodes serialized fan-out child args into a concrete
// JobArgs using the client's worker registry.
func (c *Client[TTx]) decodeFanOutChildArgs(kind string, encodedArgs json.RawMessage) (JobArgs, error) {
	if c.config.Workers == nil {
		return nil, fmt.Errorf("fan-out child kind %q cannot be decoded: client has no registered workers", kind)
	}
	workerInfo, ok := c.config.Workers.workersMap[kind]
	if !ok {
		return nil, fmt.Errorf("fan-out child kind %q cannot be decoded: no worker registered for kind", kind)
	}

	argsType := reflect.TypeOf(workerInfo.jobArgs)
	argsPtr := reflect.New(argsType)
	isPointerArgs := argsType.Kind() == reflect.Pointer
	if isPointerArgs {
		argsPtr = reflect.New(argsType.Elem())
	}
	if err := json.Unmarshal(encodedArgs, argsPtr.Interface()); err != nil {
		return nil, fmt.Errorf("error unmarshaling fan-out child args for kind %q: %w", kind, err)
	}

	if isPointerArgs {
		return argsPtr.Interface().(JobArgs), nil //nolint:forcetypeassert
	}
	return argsPtr.Elem().Interface().(JobArgs), nil //nolint:forcetypeassert
}

// fanOutChildMetadata builds the metadata for a fan-out child by copying the
// whitelisted keys from the parent's metadata and overlaying the child's own
// metadata, which takes precedence on key conflicts.
func fanOutChildMetadata(parentMetadata map[string]json.RawMessage, metadataKeys []string, childMetadata []byte) ([]byte, error) {
	merged := make(map[string]json.RawMessage, len(metadataKeys))
	for _, key := range metadataKeys {
		if value, ok := parentMetadata[key]; ok {
			merged[key] = value
		}
	}

	if len(childMetadata) > 0 {
		var childMap map[string]json.RawMessage
		if err := json.Unmarshal(childMetadata, &childMap); err != nil {
			return nil, fmt.Errorf("error unmarshaling fan-out child metadata: %w", err)
		}
		maps.Copy(merged, childMap)
	}

	if len(merged) == 0 {
		return nil, nil
	}
	return json.Marshal(merged)
}

// fanOutSpecFromMetadata extracts a fan-out spec from job metadata. The
// boolean return is false if no spec is present.
func fanOutSpecFromMetadata(metadata []byte) (*fanOutSpecJSON, bool, error) {
	var metadataMap map[string]json.RawMessage
	if err := json.Unmarshal(metadata, &metadataMap); err != nil {
		return nil, false, fmt.Errorf("error unmarshaling metadata: %w", err)
	}

	rawSpec, ok := metadataMap[rivertype.MetadataKeyFanOut]
	if !ok {
		return nil, false, nil
	}

	var spec fanOutSpecJSON
	if err := json.Unmarshal(rawSpec, &spec); err != nil {
		return nil, false, fmt.Errorf("error unmarshaling fan-out spec: %w", err)
	}
	if spec.Version != fanOutSpecVersion {
		return nil, false, fmt.Errorf("unsupported fan-out spec version %d", spec.Version)
	}
	return &spec, true, nil
}
