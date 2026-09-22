package river

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/testfactory"
	"github.com/riverqueue/river/rivertype"
)

type fanOutTestChildArgs struct {
	Message string `json:"message"`
}

func (fanOutTestChildArgs) Kind() string { return "fan_out_test_child" }

type fanOutTestChildWorker struct {
	WorkerDefaults[fanOutTestChildArgs]
}

func (w *fanOutTestChildWorker) Work(ctx context.Context, job *Job[fanOutTestChildArgs]) error {
	return nil
}

type fanOutTestOrphanArgs struct{}

func (fanOutTestOrphanArgs) Kind() string { return "fan_out_test_orphan" }

type fanOutTestParentArgs struct{}

func (fanOutTestParentArgs) Kind() string { return "fan_out_test_parent" }

type fanOutTestParentWorker struct {
	WorkerDefaults[fanOutTestParentArgs]
}

func (w *fanOutTestParentWorker) Work(ctx context.Context, job *Job[fanOutTestParentArgs]) error {
	return nil
}

func TestJobCompleteTxFanOut(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		client *Client[pgx.Tx]
		exec   riverdriver.Executor
		tx     pgx.Tx
	}

	setup := func(ctx context.Context, t *testing.T) (context.Context, *testBundle) {
		t.Helper()

		tx := riverdbtest.TestTxPgx(ctx, t)
		workers := NewWorkers()
		AddWorker(workers, &fanOutTestChildWorker{})
		AddWorker(workers, &fanOutTestParentWorker{})
		client, err := NewClient(riverpgxv5.New(nil), &Config{
			Logger:  riversharedtest.Logger(t),
			Workers: workers,
		})
		require.NoError(t, err)
		ctx = context.WithValue(ctx, rivercommon.ContextKeyClient{}, client)

		return ctx, &testBundle{
			client: client,
			exec:   riverpgxv5.New(nil).UnwrapExecutor(tx),
			tx:     tx,
		}
	}

	// insertRunningParent inserts a running parent job carrying the given
	// fan-out options serialized into its metadata, simulating a job that was
	// inserted with InsertOpts.FanOut and subsequently picked up by a worker.
	insertRunningParent := func(ctx context.Context, t *testing.T, exec riverdriver.Executor, fanOut *FanOutOpts, metadata []byte, priority int) *rivertype.JobRow {
		t.Helper()

		if metadata == nil {
			metadata = []byte("{}")
		}
		if fanOut != nil {
			var err error
			metadata, err = fanOut.encodeIntoMetadata(metadata)
			require.NoError(t, err)
		}

		return testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
			Kind:     new("fan_out_test_parent"),
			Metadata: metadata,
			Priority: new(priority),
			State:    new(rivertype.JobStateRunning),
		})
	}

	getChildren := func(ctx context.Context, t *testing.T, exec riverdriver.Executor) []*rivertype.JobRow {
		t.Helper()

		jobs, err := exec.JobGetByKindMany(ctx, &riverdriver.JobGetByKindManyParams{
			Kind: []string{"fan_out_test_child"},
		})
		require.NoError(t, err)
		return jobs
	}

	t.Run("CompleteFansOutChildrenAtomically", func(t *testing.T) {
		t.Parallel()

		ctx, bundle := setup(ctx, t)

		fanOut := &FanOutOpts{
			Children: []FanOutChild{
				{Args: fanOutTestChildArgs{Message: "hello"}},
				{Args: fanOutTestChildArgs{Message: "world"}, Opts: &InsertOpts{
					Metadata: []byte(`{"child_key":"c"}`),
					Priority: 3,
					Queue:    "other",
				}},
			},
			MetadataKeys: []string{"trace_id"},
		}
		parent := insertRunningParent(ctx, t, bundle.exec, fanOut, []byte(`{"trace_id":"t1","internal":"secret"}`), 1)

		completedJob, err := JobCompleteTx[*riverpgxv5.Driver](ctx, bundle.tx, &Job[fanOutTestParentArgs]{JobRow: parent})
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateCompleted, completedJob.State)

		children := getChildren(ctx, t, bundle.exec)
		require.Len(t, children, 2)

		childByMessage := make(map[string]*rivertype.JobRow, len(children))
		for _, child := range children {
			var args fanOutTestChildArgs
			require.NoError(t, json.Unmarshal(child.EncodedArgs, &args))
			childByMessage[args.Message] = child
		}

		// Child with no opts inherits the parent's priority and whitelisted
		// metadata only.
		child1 := childByMessage["hello"]
		require.NotNil(t, child1)
		require.Equal(t, rivertype.JobStateAvailable, child1.State)
		require.Equal(t, 1, child1.Priority)
		require.Equal(t, rivercommon.QueueDefault, child1.Queue)
		require.JSONEq(t, `{"trace_id":"t1"}`, string(child1.Metadata))

		// Child with explicit opts keeps its own priority/queue, inherits
		// whitelisted metadata, and its own metadata takes precedence.
		child2 := childByMessage["world"]
		require.NotNil(t, child2)
		require.Equal(t, 3, child2.Priority)
		require.Equal(t, "other", child2.Queue)
		require.JSONEq(t, `{"trace_id":"t1","child_key":"c"}`, string(child2.Metadata))
	})

	t.Run("CompleteErrorOnUnregisteredChildKindLeavesParentRunning", func(t *testing.T) {
		t.Parallel()

		ctx, bundle := setup(ctx, t)

		fanOut := &FanOutOpts{
			Children: []FanOutChild{
				{Args: fanOutTestChildArgs{Message: "hello"}},
				{Args: fanOutTestOrphanArgs{}},
			},
		}
		parent := insertRunningParent(ctx, t, bundle.exec, fanOut, nil, 2)

		// Use a nested transaction (savepoint) so its rollback can be
		// observed from the outer test transaction.
		nestedTx, err := bundle.tx.Begin(ctx)
		require.NoError(t, err)

		_, err = JobCompleteTx[*riverpgxv5.Driver](ctx, nestedTx, &Job[fanOutTestParentArgs]{JobRow: parent})
		require.ErrorContains(t, err, `fan-out child kind "fan_out_test_orphan" cannot be decoded`)
		require.NoError(t, nestedTx.Rollback(ctx))

		// After rollback the parent is not completed and no children were
		// inserted, so the job can be retried without a partial fan-out.
		updatedJob, err := bundle.exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: parent.ID})
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateRunning, updatedJob.State)
		require.Empty(t, getChildren(ctx, t, bundle.exec))
	})

	t.Run("DisabledLeavesInsertAndCompleteUnchanged", func(t *testing.T) {
		t.Parallel()

		ctx, bundle := setup(ctx, t)

		insertRes, err := bundle.client.InsertTx(ctx, bundle.tx, fanOutTestParentArgs{}, nil)
		require.NoError(t, err)
		require.JSONEq(t, `{}`, string(insertRes.Job.Metadata))

		parent := insertRunningParent(ctx, t, bundle.exec, nil, []byte(`{"foo":"bar"}`), 2)

		completedJob, err := JobCompleteTx[*riverpgxv5.Driver](ctx, bundle.tx, &Job[fanOutTestParentArgs]{JobRow: parent})
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateCompleted, completedJob.State)
		require.Empty(t, getChildren(ctx, t, bundle.exec))
	})

	t.Run("InsertStoresSpecInMetadata", func(t *testing.T) {
		t.Parallel()

		ctx, bundle := setup(ctx, t)

		insertRes, err := bundle.client.InsertTx(ctx, bundle.tx, fanOutTestParentArgs{}, &InsertOpts{
			FanOut: &FanOutOpts{
				Children:     []FanOutChild{{Args: fanOutTestChildArgs{Message: "hello"}}},
				MetadataKeys: []string{"trace_id"},
			},
			Metadata: []byte(`{"trace_id":"t1"}`),
		})
		require.NoError(t, err)

		var metadata map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(insertRes.Job.Metadata, &metadata))
		require.Contains(t, metadata, rivertype.MetadataKeyFanOut)
		require.JSONEq(t, `"t1"`, string(metadata["trace_id"]))

		spec, hasSpec, err := fanOutSpecFromMetadata(insertRes.Job.Metadata)
		require.NoError(t, err)
		require.True(t, hasSpec)
		require.Equal(t, []string{"trace_id"}, spec.MetadataKeys)
		require.Len(t, spec.Children, 1)
		require.Equal(t, "fan_out_test_child", spec.Children[0].Kind)
	})

	t.Run("InsertValidationErrors", func(t *testing.T) {
		t.Parallel()

		testCases := []struct {
			name      string
			fanOut    *FanOutOpts
			metadata  []byte
			expectErr string
		}{
			{
				name:      "NoChildren",
				fanOut:    &FanOutOpts{},
				expectErr: "fan-out requires at least one child",
			},
			{
				name: "NilChildArgs",
				fanOut: &FanOutOpts{
					Children: []FanOutChild{{Args: nil}},
				},
				expectErr: "fan-out child args must not be nil",
			},
			{
				name: "ReservedMetadataKey",
				fanOut: &FanOutOpts{
					Children:     []FanOutChild{{Args: fanOutTestChildArgs{}}},
					MetadataKeys: []string{"river:foo"},
				},
				expectErr: "reserved prefix",
			},
			{
				name: "NestedFanOut",
				fanOut: &FanOutOpts{
					Children: []FanOutChild{{Args: fanOutTestChildArgs{}, Opts: &InsertOpts{
						FanOut: &FanOutOpts{Children: []FanOutChild{{Args: fanOutTestChildArgs{}}}},
					}}},
				},
				expectErr: "nesting is not supported",
			},
			{
				name: "UniqueOptsUnsupported",
				fanOut: &FanOutOpts{
					Children: []FanOutChild{{Args: fanOutTestChildArgs{}, Opts: &InsertOpts{
						UniqueOpts: UniqueOpts{ByArgs: true},
					}}},
				},
				expectErr: "do not support UniqueOpts",
			},
			{
				name: "UserMetadataContainsReservedKey",
				fanOut: &FanOutOpts{
					Children: []FanOutChild{{Args: fanOutTestChildArgs{}}},
				},
				metadata:  []byte(`{"river:fanout":{}}`),
				expectErr: "must not be set directly",
			},
		}

		for _, testCase := range testCases {
			t.Run(testCase.name, func(t *testing.T) {
				t.Parallel()

				ctx, bundle := setup(ctx, t)

				_, err := bundle.client.InsertTx(ctx, bundle.tx, fanOutTestParentArgs{}, &InsertOpts{
					FanOut:   testCase.fanOut,
					Metadata: testCase.metadata,
				})
				require.ErrorContains(t, err, testCase.expectErr)
			})
		}
	})
}
