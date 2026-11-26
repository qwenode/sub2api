package repository

import (
	"context"
	"encoding/json"
	"regexp"
	"strings"
	"testing"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
	"github.com/DATA-DOG/go-sqlmock"
	dbent "github.com/Wei-Shaw/sub2api/ent"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestUpdateExtraCodexDisplaySnapshotsAvoidSchedulerOutbox(t *testing.T) {
	for _, key := range []string{"codex_credits_snapshot", "codex_referral_snapshot"} {
		for _, tc := range []struct {
			name             string
			value            any
			schedulingChange bool
		}{
			{"refresh", map[string]any{"fetched_at": 1770000000}, false},
			{"invalidate", nil, false},
			{"mixed scheduling update", nil, true},
		} {
			t.Run(key+"/"+tc.name, func(t *testing.T) {
				db, mock, err := sqlmock.New()
				require.NoError(t, err)
				client := dbent.NewClient(dbent.Driver(entsql.OpenDB(dialect.Postgres, db)))
				t.Cleanup(func() { _ = client.Close() })
				repo := newAccountRepositoryWithSQL(client, db, nil)
				updates := map[string]any{key: tc.value}
				if tc.schedulingChange {
					updates["openai_compact_enabled"] = true
					mock.ExpectBegin()
				}
				payload, err := json.Marshal(updates)
				require.NoError(t, err)
				expression := "COALESCE(extra, '{}'::jsonb) || $1::jsonb"
				if key == "codex_credits_snapshot" {
					expression = codexCreditsExtraMergeSQL()
				}
				mock.ExpectExec(regexp.QuoteMeta("UPDATE accounts SET extra = "+expression+", updated_at = NOW() WHERE id = $2 AND deleted_at IS NULL")).
					WithArgs(string(payload), int64(27)).WillReturnResult(sqlmock.NewResult(0, 1))
				if tc.schedulingChange {
					mock.ExpectExec(regexp.QuoteMeta("INSERT INTO scheduler_outbox")).
						WithArgs(service.SchedulerOutboxEventAccountChanged, int64(27), nil, nil, sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(1, 1))
					mock.ExpectCommit()
				}

				require.NoError(t, repo.UpdateExtra(context.Background(), 27, updates))
				require.NoError(t, mock.ExpectationsWereMet())
			})
		}
	}
}

func TestCodexCreditsUpdateExtraAtomicObservationGuard(t *testing.T) {
	for _, tc := range []struct {
		name     string
		observed int64
		credits  any
	}{
		{"older positive", 1000, map[string]any{"has_credits": true, "balance": "1"}},
		{"newer zero", 3000, map[string]any{"has_credits": false, "balance": "0"}},
		{"same time unknown", 2000, nil},
		{"same time positive", 2000, map[string]any{"has_credits": true, "balance": "1"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			matcher := sqlmock.QueryMatcherFunc(func(_, query string) error {
				require.True(t, strings.HasPrefix(query, "UPDATE accounts SET extra ="))
				require.Contains(t, query, "COALESCE(extra, '{}'::jsonb) || ($1::jsonb - 'codex_credits_snapshot')")
				require.Contains(t, query, "::numeric * 1000")
				require.Contains(t, query, "ELSE 0 END) > (CASE WHEN (extra -> 'codex_credits_snapshot')")
				require.Contains(t, query, "ELSE 0 END) = (CASE WHEN (extra -> 'codex_credits_snapshot')")
				require.Contains(t, query, "AND (NOT COALESCE")
				require.Contains(t, query, "OR COALESCE((((extra -> 'codex_credits_snapshot') -> 'credits')")
				require.Contains(t, query, "ELSE '{}'::jsonb END")
				return nil
			})
			db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(matcher))
			require.NoError(t, err)
			client := dbent.NewClient(dbent.Driver(entsql.OpenDB(dialect.Postgres, db)))
			t.Cleanup(func() { _ = client.Close() })
			repo := newAccountRepositoryWithSQL(client, db, nil)
			updates := map[string]any{"codex_credits_snapshot": map[string]any{"observed_at_ms": tc.observed, "fetched_at": 2, "credits": tc.credits}, "codex_5h_used_percent": 100.0}
			payload, err := json.Marshal(updates)
			require.NoError(t, err)
			mock.ExpectExec("atomic credits merge").WithArgs(string(payload), int64(1065)).WillReturnResult(sqlmock.NewResult(0, 1))
			require.NoError(t, repo.UpdateExtra(context.Background(), 1065, updates))
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
