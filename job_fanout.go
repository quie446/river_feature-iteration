package river

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivertype"
)

// FanOutMetadataParentKey is the metadata key under which the parent job's ID
// is recorded on every child job inserted by a fan-out. It uses the reserved
// `river:` prefix so it cannot collide with user-set metadata.
const FanOutMetadataParentKey = "river:fanout_parent_id"

// FanOutChild describes a single child job to be inserted as part of a
// fan-out when a parent job completes.
type FanOutChild struct {
	// Args are the arguments of the child job to insert.
	Args JobArgs

	// InsertOpts are insertion options for the child job. They take
	// precedence over inherited values: if InsertOpts specifies a non-zero
	// Priority, it is used instead of the parent's priority. Any Metadata
	// set here is merged on top of the inherited metadata, with keys set
	// here winning on conflict.
	InsertOpts *InsertOpts
}

// FanOutOpts configures a cascading fan-out of child jobs performed
// atomically with the completion of a parent job via
// JobCompleteWithFanOutTx.
type FanOutOpts struct {
	// Children are the child jobs to insert when the parent completes. At
	// least one child is required.
	Children []FanOutChild

	// InheritPriority controls whether children inherit the parent job's
	// priority. When true (the default when the option is set), a child
	// whose InsertOpts does not specify a priority uses the parent's
	// priority. When false, children without an explicit priority fall back
	// to the normal insertion defaults.
	InheritPriority bool

	// MetadataKeys is a whitelist of metadata keys copied from the parent
	// job's metadata onto each child. Only the listed keys are inherited;
	// all other parent metadata is left behind. An empty or nil list means
	// no parent metadata is inherited.
	MetadataKeys []string
}

// JobCompleteWithFanOutTx marks the given job as completed and inserts a set
// of child jobs, all as part of transaction tx. If any part of the operation
// fails, an error is returned and the caller is expected to roll back tx, in
// which case neither the completion nor any of the child insertions take
// effect. The parent never reports success while its children were only
// partially inserted.
//
// Child jobs inherit fields from the parent according to opts:
//
//   - Priority: inherited when opts.InheritPriority is set and the child
//     doesn't specify its own.
//   - Metadata: only the whitelisted keys in opts.MetadataKeys are copied
//     from the parent's metadata. In addition, each child records the
//     parent's job ID under the reserved metadata key
//     FanOutMetadataParentKey so parent and children can be correlated
//     later (e.g. when inspecting failures).
//
// Like JobCompleteTx, this function must be called from within a River
// worker so that the client can be found in context:
//
//	completedJob, children, err := river.JobCompleteWithFanOutTx[*riverpgxv5.Driver](ctx, tx, job, &river.FanOutOpts{
//		InheritPriority: true,
//		MetadataKeys:    []string{"tenant_id", "trace_id"},
//		Children: []river.FanOutChild{
//			{Args: ChildArgs{...}},
//		},
//	})
//
// Passing a nil opts behaves exactly like JobCompleteTx and inserts no
// children.
//
// Returns the updated, completed parent job along with the insert results
// for each child, in the same order as opts.Children.
func JobCompleteWithFanOutTx[TDriver riverdriver.Driver[TTx], TTx any, TArgs JobArgs](ctx context.Context, tx TTx, job *Job[TArgs], opts *FanOutOpts) (*Job[TArgs], []*rivertype.JobInsertResult, error) {
	if opts == nil || len(opts.Children) == 0 {
		completedJob, err := JobCompleteTx[TDriver](ctx, tx, job)
		if err != nil {
			return nil, nil, err
		}
		return completedJob, nil, nil
	}

	client := ClientFromContext[TTx](ctx)
	if client == nil {
		return nil, nil, errors.New("client not found in context, can only work within a River worker")
	}

	insertParams, err := fanOutInsertParams(job.JobRow, opts)
	if err != nil {
		return nil, nil, err
	}

	completedJob, err := JobCompleteTx[TDriver](ctx, tx, job)
	if err != nil {
		return nil, nil, err
	}

	results, err := client.InsertManyTx(ctx, tx, insertParams)
	if err != nil {
		return nil, nil, fmt.Errorf("error inserting fan-out children for job %d (roll back the transaction to avoid a partial fan-out): %w", job.ID, err)
	}

	return completedJob, results, nil
}

// fanOutInsertParams builds the insert parameters for each fan-out child,
// applying priority and metadata inheritance from the parent job row.
func fanOutInsertParams(parent *rivertype.JobRow, opts *FanOutOpts) ([]InsertManyParams, error) {
	var parentMetadata map[string]json.RawMessage
	if len(opts.MetadataKeys) > 0 {
		if err := json.Unmarshal(parent.Metadata, &parentMetadata); err != nil {
			return nil, fmt.Errorf("error unmarshaling parent job %d metadata for fan-out: %w", parent.ID, err)
		}
	}

	parentIDJSON, err := json.Marshal(parent.ID)
	if err != nil {
		return nil, err
	}

	params := make([]InsertManyParams, len(opts.Children))
	for i, child := range opts.Children {
		if child.Args == nil {
			return nil, fmt.Errorf("fan-out child %d: args must not be nil", i)
		}

		insertOpts := &InsertOpts{}
		if child.InsertOpts != nil {
			*insertOpts = *child.InsertOpts
		}

		if opts.InheritPriority && insertOpts.Priority == 0 {
			insertOpts.Priority = parent.Priority
		}

		metadata, err := fanOutChildMetadata(parentMetadata, opts.MetadataKeys, parentIDJSON, insertOpts.Metadata)
		if err != nil {
			return nil, fmt.Errorf("fan-out child %d: %w", i, err)
		}
		insertOpts.Metadata = metadata

		params[i] = InsertManyParams{Args: child.Args, InsertOpts: insertOpts}
	}

	return params, nil
}

// fanOutChildMetadata merges inherited parent metadata (whitelisted keys
// only), the parent linkage key, and any metadata explicitly set on the
// child, in increasing order of precedence.
func fanOutChildMetadata(parentMetadata map[string]json.RawMessage, metadataKeys []string, parentIDJSON, childMetadata []byte) ([]byte, error) {
	merged := make(map[string]json.RawMessage, len(metadataKeys)+1)

	for _, key := range metadataKeys {
		if value, ok := parentMetadata[key]; ok {
			merged[key] = value
		}
	}

	merged[FanOutMetadataParentKey] = parentIDJSON

	if len(childMetadata) > 0 {
		var childMap map[string]json.RawMessage
		if err := json.Unmarshal(childMetadata, &childMap); err != nil {
			return nil, fmt.Errorf("error unmarshaling child metadata: %w", err)
		}
		maps.Copy(merged, childMap)
	}

	metadata, err := json.Marshal(merged)
	if err != nil {
		return nil, err
	}

	return metadata, nil
}
