//go:build unit

package repository

import (
	"context"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestCodexCreditsCacheCrossInstance(t *testing.T) {
	ctx := context.Background()
	writer := newSchedulerCacheUnit(t)
	reader := NewSchedulerCache(writer.rdb).(*schedulerCache)
	now := time.Now()
	account := service.Account{ID: 1065, Platform: service.PlatformOpenAI, Type: service.AccountTypeOAuth, Status: service.StatusActive, Schedulable: true,
		Extra: map[string]any{"codex_7d_used_percent": 100.0, "codex_7d_reset_at": now.Add(72 * time.Hour).Format(time.RFC3339), "codex_usage_updated_at": now.Format(time.RFC3339)}}
	bucket := service.SchedulerBucket{GroupID: 1, Platform: service.PlatformOpenAI, Mode: service.SchedulerModeSingle}
	token, err := writer.CaptureBucketWriteToken(ctx, bucket)
	require.NoError(t, err)
	require.NoError(t, writer.SetSnapshot(ctx, bucket, token, []service.Account{account}))
	old, hit, err := reader.GetSnapshot(ctx, bucket)
	require.NoError(t, err)
	require.True(t, hit)
	require.Len(t, old, 1)
	require.True(t, service.EvaluateAccountSchedulingThreshold(old[0], map[string]int{service.PlatformOpenAI: 95}, now).ShouldPause)
	previousRevision := memoRevision(t, writer, schedulerAccountMetaRevisionKeyForPlatform(service.PlatformOpenAI))
	account.Extra["codex_credits_snapshot"] = map[string]any{"credits": map[string]any{"has_credits": true, "unlimited": false, "balance": "2997.3994180000"}, "fetched_at": now.Unix(), "observed_at_ms": now.UnixMilli()}
	account.Extra["codex_credits_refresh_state"] = map[string]any{"attempted_at": now.Unix(), "retry_after": now.Add(time.Minute).Unix()}
	require.NoError(t, writer.SetAccount(ctx, &account))
	require.NotEqual(t, previousRevision, memoRevision(t, writer, schedulerAccountMetaRevisionKeyForPlatform(service.PlatformOpenAI)))
	fresh, hit, err := reader.GetSnapshot(ctx, bucket)
	require.NoError(t, err)
	require.True(t, hit)
	require.Len(t, fresh, 1)
	require.Contains(t, fresh[0].Extra, "codex_credits_refresh_state")
	require.False(t, service.EvaluateAccountSchedulingThreshold(fresh[0], map[string]int{service.PlatformOpenAI: 95}, now).ShouldPause)
	full, err := reader.GetAccount(ctx, account.ID)
	require.NoError(t, err)
	require.NotNil(t, full)
	require.Equal(t, full.Extra["codex_credits_snapshot"], fresh[0].Extra["codex_credits_snapshot"])
	require.NotContains(t, old[0].Extra, "codex_credits_snapshot")
	require.False(t, shouldEnqueueSchedulerOutboxForExtraUpdates(map[string]any{"codex_credits_snapshot": account.Extra["codex_credits_snapshot"], "codex_credits_refresh_state": account.Extra["codex_credits_refresh_state"]}))
}
