package river

import (
	"bytes"
	"context"
	"encoding/json"
	"strconv"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/testfactory"
	"github.com/riverqueue/river/rivershared/util/testutil"
	"github.com/riverqueue/river/rivertype"
)

func TestJobCompleteWithFanOutTx(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type JobArgs struct {
		testutil.JobArgsReflectKind[JobArgs]
	}

	type ChildArgs struct {
		testutil.JobArgsReflectKind[ChildArgs]
	}

	type testBundle struct {
		client *Client[pgx.Tx]
		exec   riverdriver.Executor
		tx     pgx.Tx
	}

	setup := func(ctx context.Context, t *testing.T) (context.Context, *testBundle) {
		t.Helper()

		tx := riverdbtest.TestTxPgx(ctx, t)
		client, err := NewClient(riverpgxv5.New(nil), &Config{
			Logger:              riversharedtest.Logger(t),
			SkipUnknownJobCheck: true,
		})
		require.NoError(t, err)
		ctx = context.WithValue(ctx, rivercommon.ContextKeyClient{}, client)

		return ctx, &testBundle{
			client: client,
			exec:   riverpgxv5.New(nil).UnwrapExecutor(tx),
			tx:     tx,
		}
	}

	metadataMap := func(t *testing.T, metadata []byte) map[string]any {
		t.Helper()

		var decoded map[string]any
		decoder := json.NewDecoder(bytes.NewReader(metadata))
		decoder.UseNumber()
		require.NoError(t, decoder.Decode(&decoded))
		return decoded
	}

	t.Run("ChildInsertOptsOverrideInheritedValues", func(t *testing.T) {
		t.Parallel()

		ctx, bundle := setup(ctx, t)

		job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{
			Metadata: []byte(`{"tenant_id": "t1"}`),
			Priority: new(1),
			State:    new(rivertype.JobStateRunning),
		})

		_, results, err := JobCompleteWithFanOutTx[*riverpgxv5.Driver](ctx, bundle.tx, &Job[JobArgs]{JobRow: job}, &FanOutOpts{
			InheritPriority: true,
			MetadataKeys:    []string{"tenant_id"},
			Children: []FanOutChild{
				{
					Args: ChildArgs{},
					InsertOpts: &InsertOpts{
						Metadata: []byte(`{"tenant_id": "overridden", "extra": true}`),
						Priority: 4,
					},
				},
			},
		})
		require.NoError(t, err)
		require.Len(t, results, 1)

		child, err := bundle.exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: results[0].Job.ID})
		require.NoError(t, err)
		require.Equal(t, 4, child.Priority)

		metadata := metadataMap(t, child.Metadata)
		require.Equal(t, "overridden", metadata["tenant_id"])
		require.Equal(t, true, metadata["extra"])
		require.Equal(t, json.Number(strconv.FormatInt(job.ID, 10)), metadata[FanOutMetadataParentKey])
	})

	t.Run("ChildrenInheritPriorityAndWhitelistedMetadata", func(t *testing.T) {
		t.Parallel()

		ctx, bundle := setup(ctx, t)

		job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{
			Metadata: []byte(`{"secret": "s3", "tenant_id": "t1", "trace_id": "tr1"}`),
			Priority: new(1),
			State:    new(rivertype.JobStateRunning),
		})

		completedJob, results, err := JobCompleteWithFanOutTx[*riverpgxv5.Driver](ctx, bundle.tx, &Job[JobArgs]{JobRow: job}, &FanOutOpts{
			InheritPriority: true,
			MetadataKeys:    []string{"tenant_id", "trace_id"},
			Children: []FanOutChild{
				{Args: ChildArgs{}},
				{Args: ChildArgs{}},
			},
		})
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateCompleted, completedJob.State)
		require.Len(t, results, 2)

		for _, result := range results {
			child, err := bundle.exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: result.Job.ID})
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateAvailable, child.State)
			require.Equal(t, 1, child.Priority)

			metadata := metadataMap(t, child.Metadata)
			require.Equal(t, "t1", metadata["tenant_id"])
			require.Equal(t, "tr1", metadata["trace_id"])
			require.NotContains(t, metadata, "secret")
			require.Equal(t, json.Number(strconv.FormatInt(job.ID, 10)), metadata[FanOutMetadataParentKey])
		}
	})

	t.Run("ErrorWhenChildInsertFailsLeavesParentUncompleted", func(t *testing.T) {
		t.Parallel()

		tx := riverdbtest.TestTxPgx(ctx, t)

		// Client only knows about the no-op job kind, so inserting the child
		// fails mid-fan-out.
		workers := NewWorkers()
		AddWorker(workers, &noOpWorker{})

		client, err := NewClient(riverpgxv5.New(nil), &Config{
			Logger:  riversharedtest.Logger(t),
			Workers: workers,
		})
		require.NoError(t, err)
		ctx := context.WithValue(ctx, rivercommon.ContextKeyClient{}, client)

		exec := riverpgxv5.New(nil).UnwrapExecutor(tx)

		job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
			Kind:  new(noOpArgs{}.Kind()),
			State: new(rivertype.JobStateRunning),
		})

		// Run the fan-out in a nested transaction (savepoint) so it can be
		// rolled back without discarding the parent job fixture.
		nestedTx, err := tx.Begin(ctx)
		require.NoError(t, err)

		_, _, err = JobCompleteWithFanOutTx[*riverpgxv5.Driver](ctx, nestedTx, &Job[JobArgs]{JobRow: job}, &FanOutOpts{
			Children: []FanOutChild{{Args: ChildArgs{}}},
		})
		require.Error(t, err)
		require.ErrorAs(t, err, new(*UnknownJobKindError))

		// Roll back the transaction as a caller would on error, then verify
		// that neither the completion nor any child insert took effect.
		require.NoError(t, nestedTx.Rollback(ctx))

		parent, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID})
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateRunning, parent.State)

		children, err := exec.JobList(ctx, &riverdriver.JobListParams{
			Max:           10,
			NamedArgs:     map[string]any{"kind": (ChildArgs{}).Kind()},
			OrderByClause: "id",
			WhereClause:   "kind = @kind",
		})
		require.NoError(t, err)
		require.Empty(t, children)
	})

	t.Run("NoFanOutWhenOptsNil", func(t *testing.T) {
		t.Parallel()

		ctx, bundle := setup(ctx, t)

		job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{
			State: new(rivertype.JobStateRunning),
		})

		completedJob, results, err := JobCompleteWithFanOutTx[*riverpgxv5.Driver](ctx, bundle.tx, &Job[JobArgs]{JobRow: job}, nil)
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateCompleted, completedJob.State)
		require.Empty(t, results)

		children, err := bundle.exec.JobList(ctx, &riverdriver.JobListParams{
			Max:           10,
			NamedArgs:     map[string]any{"kind": (ChildArgs{}).Kind()},
			OrderByClause: "id",
			WhereClause:   "kind = @kind",
		})
		require.NoError(t, err)
		require.Empty(t, children)
	})
}
