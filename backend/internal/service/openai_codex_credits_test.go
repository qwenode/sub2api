//go:build unit

package service

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/pagination"
	"github.com/stretchr/testify/require"
)

func codexCreditsTestAccount(now time.Time) *Account {
	return &Account{ID: 1065, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Status: StatusActive, Schedulable: true,
		Extra: map[string]any{
			"codex_5h_used_percent": 100.0, "codex_7d_used_percent": 100.0,
			"codex_5h_reset_at": now.Add(time.Hour).Format(time.RFC3339), "codex_7d_reset_at": now.Add(72 * time.Hour).Format(time.RFC3339),
			"codex_usage_updated_at": now.Format(time.RFC3339), "auto_pause_5h_threshold": 0.95, "auto_pause_7d_threshold": 0.95,
			openaiQuotaCreditsKey: newOpenAICreditsSnapshot(&OpenAICredits{HasCredits: true}, now.Unix(), now.UnixMilli()),
		}}
}

func TestCodexCreditsStateMatrix(t *testing.T) {
	now := time.Now()
	for _, tc := range []struct {
		name, raw string
		want      OpenAICodexCreditsState
	}{
		{"positive", `{"has_credits":true,"balance":"2997.3994180000"}`, OpenAICodexCreditsAvailable},
		{"tiny", `{"has_credits":true,"balance":"0.0000000000000000000000000000000000001"}`, OpenAICodexCreditsAvailable},
		{"zero", `{"has_credits":true,"balance":"0.0000"}`, OpenAICodexCreditsUnavailable},
		{"negative", `{"has_credits":true,"balance":"-0.0001"}`, OpenAICodexCreditsUnavailable},
		{"hidden", `{"has_credits":true}`, OpenAICodexCreditsAvailable},
		{"hidden null", `{"has_credits":true,"balance":null}`, OpenAICodexCreditsAvailable},
		{"unlimited wins", `{"has_credits":false,"unlimited":true,"balance":"-2"}`, OpenAICodexCreditsAvailable},
		{"false wins", `{"has_credits":false,"balance":"100"}`, OpenAICodexCreditsUnavailable},
		{"malformed", `{"has_credits":true,"balance":"bad"}`, OpenAICodexCreditsUnknown},
		{"wrong type", `{"has_credits":true,"balance":12}`, OpenAICodexCreditsUnknown},
		{"missing flag", `{"balance":"12"}`, OpenAICodexCreditsUnknown},
		{"empty", `{}`, OpenAICodexCreditsUnknown},
		{"null", `null`, OpenAICodexCreditsUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var credits *OpenAICredits
			require.NoError(t, json.Unmarshal([]byte(tc.raw), &credits))
			account := codexCreditsTestAccount(now)
			account.Extra[openaiQuotaCreditsKey] = newOpenAICreditsSnapshot(credits, now.Unix(), now.UnixMilli())
			require.Equal(t, tc.want, openAICodexCreditsStateForAccount(account, now))
			paused, _ := shouldAutoPauseOpenAIAccountByQuota(context.Background(), account)
			require.Equal(t, tc.want != OpenAICodexCreditsAvailable, paused)
		})
	}
}

func TestCodexCreditsObservationPriorityAndFreshness(t *testing.T) {
	now := time.Now().Truncate(time.Millisecond)
	for _, tc := range []struct {
		name              string
		observed, fetched int64
		fresh             bool
	}{
		{"new", now.UnixMilli(), now.Unix(), true}, {"legacy", 0, now.Unix(), true},
		{"expired", now.Add(-10 * time.Minute).UnixMilli(), now.Unix(), false},
		{"future", now.Add(time.Second).UnixMilli(), now.Unix(), false},
		{"zero", 0, 0, false}, {"negative", -1, now.Unix(), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := codexCreditsTestAccount(now)
			a.Extra[openaiQuotaCreditsKey] = newOpenAICreditsSnapshot(&OpenAICredits{HasCredits: true}, tc.fetched, tc.observed)
			require.Equal(t, tc.fresh, openAICodexCreditsStateForAccount(a, now) == OpenAICodexCreditsAvailable)
		})
	}
	a := codexCreditsTestAccount(now)
	for _, value := range []string{"false", "invalid", ""} {
		h := http.Header{}
		h.Set("x-codex-credits-has-credits", value)
		require.NotEqual(t, OpenAICodexCreditsAvailable, openAICodexCreditsStateForResponse(a, h, now))
		u := buildCodexUsageExtraUpdates(ParseCodexRateLimitHeaders(h), now)
		require.Contains(t, u, openaiQuotaCreditsKey)
		require.NotContains(t, u, "codex_usage_updated_at")
	}
	h := http.Header{}
	h.Set("x-codex-primary-used-percent", "100")
	u := buildCodexUsageExtraUpdates(ParseCodexRateLimitHeaders(h), now)
	require.NotContains(t, u, openaiQuotaCreditsKey)
	require.Equal(t, OpenAICodexCreditsAvailable, openAICodexCreditsStateForResponse(a, h, now))
	throttle := newAccountWriteThrottle(30 * time.Second)
	require.True(t, throttle.Allow(a.ID, now))
	positive := newOpenAICreditsSnapshot(&OpenAICredits{HasCredits: true}, now.Unix(), now.UnixMilli())
	require.True(t, throttle.Allow(a.ID, now, &positive))
	require.False(t, throttle.Allow(a.ID, now.Add(time.Second), &positive))
	negative := newOpenAICreditsSnapshot(&OpenAICredits{}, now.Unix(), now.Add(time.Second).UnixMilli())
	require.True(t, throttle.Allow(a.ID, now.Add(time.Second), &negative))
}

func TestCodexCreditsCardPriority(t *testing.T) {
	for _, window := range []string{"5h", "7d"} {
		for _, tc := range []struct {
			status string
			count  int
			stale  bool
			pause  bool
		}{
			{"", 0, false, true}, {OpenAIAutoResetStatusAvailable, 1, false, true},
			{OpenAIAutoResetStatusChecking, 0, false, true}, {OpenAIAutoResetStatusResetting, 0, false, true},
			{OpenAIAutoResetStatusFailed, 0, false, true}, {OpenAIAutoResetStatusNoCredit, 0, false, false},
			{OpenAIAutoResetStatusNoCredit, 0, true, true},
		} {
			t.Run(fmt.Sprintf("%s/%s/%t", window, tc.status, tc.stale), func(t *testing.T) {
				now := time.Now()
				a := codexCreditsTestAccount(now)
				a.Extra["codex_5h_used_percent"], a.Extra["codex_7d_used_percent"] = 0.0, 0.0
				a.Extra["codex_"+window+"_used_percent"] = 100.0
				a.Extra[OpenAIAutoResetCreditEnabledExtraKey] = true
				checked := now
				if tc.stale {
					checked = checked.Add(-11 * time.Minute)
				}
				a.Extra[OpenAIAutoResetCreditStateExtraKey] = OpenAIAutoResetCreditState{Status: tc.status, AvailableCount: tc.count, CheckedAt: checked.Format(time.RFC3339)}
				paused, _ := shouldAutoPauseOpenAIAccountByQuota(context.Background(), a)
				require.Equal(t, tc.pause, paused)
				a.Extra["codex_"+window+"_used_percent"] = 96.0
				paused, _ = shouldAutoPauseOpenAIAccountByQuota(context.Background(), a)
				require.False(t, paused, "credits can continue to the reset-card threshold")
			})
		}
	}
}

func TestCodexCreditsPlatformThreshold(t *testing.T) {
	accountSchedulingThresholdsCache.Store(&cachedAccountSchedulingThresholds{})
	settings := newMockSettingRepo()
	settings.data[SettingKeyAccountSchedulingThresholds] = `{"openai":95}`
	for _, status := range []OpenAICodexCreditsState{OpenAICodexCreditsAvailable, OpenAICodexCreditsUnavailable, OpenAICodexCreditsUnknown} {
		t.Run(string(status), func(t *testing.T) {
			now := time.Now()
			a := codexCreditsTestAccount(now)
			if status == OpenAICodexCreditsUnknown {
				delete(a.Extra, openaiQuotaCreditsKey)
			}
			if status == OpenAICodexCreditsUnavailable {
				a.Extra[openaiQuotaCreditsKey] = newOpenAICreditsSnapshot(&OpenAICredits{}, now.Unix(), now.UnixMilli())
			}
			repo := &rateLimitAccountRepoStub{}
			rl := NewRateLimitService(repo, nil, &config.Config{}, nil, nil)
			rl.SetSettingService(NewSettingService(settings, &config.Config{}))
			require.Equal(t, status != OpenAICodexCreditsAvailable, rl.ApplyAccountSchedulingThreshold(context.Background(), a))
			if status == OpenAICodexCreditsUnavailable {
				require.Equal(t, 1, repo.tempCalls)
			} else {
				require.Zero(t, repo.tempCalls)
			}
			a.Credentials = map[string]any{accountSchedulingThresholdCredentialKey: 80}
			require.Equal(t, status != OpenAICodexCreditsAvailable, EvaluateAccountSchedulingThreshold(a, nil, now).ShouldPause)
		})
	}
}

type codexCreditsScanRepo struct{ autoResetTestAccountRepo }

func (r *codexCreditsScanRepo) ListWithFilters(context.Context, pagination.PaginationParams, string, string, string, string, int64, string) ([]Account, *pagination.PaginationResult, error) {
	a, _ := r.GetByID(context.Background(), r.account.ID)
	return []Account{*a}, nil, nil
}

func TestCodexCreditsReadOnlyRefresh(t *testing.T) {
	now := time.Now()
	a := codexCreditsTestAccount(now)
	delete(a.Extra, openaiQuotaCreditsKey)
	until := now.Add(48 * time.Hour)
	a.RateLimitResetAt, a.TempUnschedulableUntil = &until, &until
	repo := &codexCreditsScanRepo{autoResetTestAccountRepo: autoResetTestAccountRepo{account: a}}
	quota := &autoResetTestQuota{usage: &OpenAIQuotaUsage{FetchedAt: now.Unix(), CreditsObservedAtMs: now.UnixMilli(), Credits: &OpenAICredits{HasCredits: true}}}
	svc := NewOpenAIQuotaAutoResetService(repo, quota, nil, nil, nil, nil, nil)
	defer svc.Stop()
	svc.scanEnabledAccounts(context.Background())
	require.Len(t, svc.queue, 1)
	require.NoError(t, svc.evaluateAccount(context.Background(), a.ID))
	require.Equal(t, int32(1), quota.queryCalls.Load())
	require.Zero(t, quota.resetCalls.Load())
	fresh, _ := repo.GetByID(context.Background(), a.ID)
	require.Equal(t, OpenAICodexCreditsAvailable, openAICodexCreditsStateForAccount(fresh, time.Now()))
	require.Equal(t, &until, fresh.RateLimitResetAt)
	require.Equal(t, &until, fresh.TempUnschedulableUntil)
	require.NotContains(t, fresh.Extra, OpenAIAutoResetCreditStateExtraKey)
	require.NoError(t, svc.evaluateAccount(context.Background(), a.ID))
	require.Equal(t, int32(1), quota.queryCalls.Load())
	svc.Stop()
	require.ErrorIs(t, svc.evaluateAccount(context.Background(), a.ID), context.Canceled)
	require.Equal(t, int32(1), quota.queryCalls.Load())
}

func TestCodexCreditsRefreshBackoffAndHealthyAccounts(t *testing.T) {
	now := time.Now()
	a := codexCreditsTestAccount(now)
	old := newOpenAICreditsSnapshot(&OpenAICredits{HasCredits: true}, now.Add(-time.Hour).Unix(), now.Add(-time.Hour).UnixMilli())
	a.Extra[openaiQuotaCreditsKey] = old
	repo := &autoResetTestAccountRepo{account: a}
	quota := &autoResetTestQuota{queryErr: context.DeadlineExceeded}
	svc := NewOpenAIQuotaAutoResetService(repo, quota, nil, nil, nil, nil, nil)
	defer svc.Stop()
	require.Error(t, svc.evaluateAccount(context.Background(), a.ID))
	require.NoError(t, svc.evaluateAccount(context.Background(), a.ID))
	require.Equal(t, int32(1), quota.queryCalls.Load())
	fresh, _ := repo.GetByID(context.Background(), a.ID)
	require.Equal(t, old, fresh.Extra[openaiQuotaCreditsKey])
	require.NotContains(t, fresh.Extra, OpenAIAutoResetCreditStateExtraKey)
	a.Extra["codex_5h_used_percent"], a.Extra["codex_7d_used_percent"] = 1.0, 1.0
	require.False(t, svc.creditsRefreshCandidate(a, now))
	a.Extra["codex_5h_used_percent"] = 96.0
	require.True(t, svc.creditsRefreshCandidate(a, now))
	delete(a.Extra, openAICodexCreditsRefreshStateKey)
	quota.queryErr = nil
	quota.usage = &OpenAIQuotaUsage{FetchedAt: now.Unix(), CreditsObservedAtMs: now.UnixMilli()}
	require.NoError(t, svc.evaluateAccount(context.Background(), a.ID))
	fresh, _ = repo.GetByID(context.Background(), a.ID)
	require.Equal(t, OpenAICodexCreditsUnknown, openAICodexCreditsStateForAccount(fresh, now))
	require.True(t, openAICodexCreditsSnapshotFresh(openAICodexCreditsSnapshotFromExtra(fresh.Extra), now))
	bounded := NewOpenAIQuotaAutoResetService(repo, quota, nil, nil, nil, nil, nil)
	defer bounded.Stop()
	bounded.queue = make(chan int64, 1)
	bounded.Notify(1)
	bounded.Notify(1)
	bounded.Notify(2)
	require.Len(t, bounded.queue, 1)
	_, exists := bounded.pending.Load(int64(2))
	require.False(t, exists)
}

func codexCreditsQuotaHeaders() http.Header {
	h := http.Header{}
	h.Set("x-codex-primary-used-percent", "93")
	h.Set("x-codex-primary-window-minutes", "300")
	h.Set("x-codex-primary-reset-after-seconds", "10085")
	h.Set("x-codex-secondary-used-percent", "100")
	h.Set("x-codex-secondary-window-minutes", "10080")
	h.Set("x-codex-secondary-reset-after-seconds", "312384")
	return h
}

func TestCodexCredits429CooldownAndFailover(t *testing.T) {
	for _, mode := range []string{"current positive", "snapshot positive", "unknown", "expired", "current negative", "invalid current", "disabled", "retry after"} {
		t.Run(mode, func(t *testing.T) {
			repo := &oauth429RateLimitRepo{}
			rl := NewRateLimitService(repo, nil, &config.Config{}, nil, nil)
			svc := &OpenAIGatewayService{rateLimitService: rl}
			rl.SetAccountRuntimeBlocker(svc)
			a := codexCreditsTestAccount(time.Now())
			h := codexCreditsQuotaHeaders()
			switch mode {
			case "current positive":
				h.Set("x-codex-credits-has-credits", "True")
				h.Set("x-codex-credits-balance", "2997.3994180000")
			case "unknown":
				delete(a.Extra, openaiQuotaCreditsKey)
			case "expired":
				a.Extra[openaiQuotaCreditsKey] = newOpenAICreditsSnapshot(&OpenAICredits{HasCredits: true}, time.Now().Add(-time.Hour).Unix(), 0)
			case "current negative":
				h.Set("x-codex-credits-has-credits", "False")
			case "invalid current":
				h.Set("x-codex-credits-has-credits", "invalid")
			case "disabled":
				settings := newMockSettingRepo()
				settings.data[SettingKeyRateLimit429CooldownSettings] = `{"enabled":false,"cooldown_seconds":12}`
				rl.SetSettingService(NewSettingService(settings, &config.Config{}))
			case "retry after":
				h.Set("Retry-After", "90")
			}
			svc.handleOpenAIAccountUpstreamError(context.Background(), a, http.StatusTooManyRequests, h, nil)
			require.False(t, svc.ShouldRetryOpenAIOAuth429(a, h, nil))
			err := svc.newOpenAIAccountFailoverError(a, 429, h, nil, "quota exhausted", false, false)
			require.False(t, err.RetryableOnSameAccount)
			if mode == "disabled" {
				require.Zero(t, repo.setRateLimitedCalls)
				require.False(t, svc.isOpenAIAccountRuntimeBlocked(a))
				return
			}
			require.Equal(t, 1, repo.setRateLimitedCalls)
			value, ok := svc.openaiAccountRuntimeBlockUntil.Load(a.ID)
			require.True(t, ok)
			require.WithinDuration(t, repo.lastRateLimitedUntil, value.(time.Time), time.Millisecond)
			if mode == "current negative" {
				require.Greater(t, time.Until(repo.lastRateLimitedUntil), 72*time.Hour)
			} else if mode == "retry after" {
				require.InDelta(t, 90, time.Until(repo.lastRateLimitedUntil).Seconds(), 1)
			} else {
				require.Less(t, time.Until(repo.lastRateLimitedUntil), time.Minute)
			}
		})
	}
}

func TestCodexCreditsSemanticErrorsIgnoreOldHeaders(t *testing.T) {
	for _, transport := range []string{"sse", "ws"} {
		t.Run(transport, func(t *testing.T) {
			repo := &oauth429RateLimitRepo{}
			rl := NewRateLimitService(repo, nil, &config.Config{}, nil, nil)
			svc := &OpenAIGatewayService{rateLimitService: rl}
			rl.SetAccountRuntimeBlocker(svc)
			a := codexCreditsTestAccount(time.Now())
			a.Extra[openaiQuotaCreditsKey] = newOpenAICreditsSnapshot(&OpenAICredits{}, time.Now().Unix(), 0)
			h := codexCreditsQuotaHeaders()
			h.Set("x-codex-credits-has-credits", "true")
			body := []byte(`{"type":"error","error":{"type":"usage_limit_reached","code":"rate_limit_exceeded","resets_in_seconds":312384,"message":"quota exhausted"}}`)
			if transport == "ws" {
				svc.persistOpenAIWSRateLimitSignal(context.Background(), a, h, body, "rate_limit_exceeded", "usage_limit_reached", "quota exhausted", "gpt-5.3-codex")
			} else {
				svc.handleOpenAIStreamTerminalAccountSideEffects(nil, a, body, "quota exhausted", h)
			}
			require.Equal(t, 1, repo.setRateLimitedCalls)
			require.Greater(t, time.Until(repo.lastRateLimitedUntil), 72*time.Hour, "successful headers must not renew a positive balance")
		})
	}
}

func TestCodexCreditsScopeAndOtherLimits(t *testing.T) {
	now := time.Now()
	for _, mode := range []string{"apikey", "setup", "grok", "shadow"} {
		a := codexCreditsTestAccount(now)
		switch mode {
		case "apikey":
			a.Type = AccountTypeAPIKey
		case "setup":
			a.Type = AccountTypeSetupToken
		case "grok":
			a.Platform = PlatformGrok
		case "shadow":
			id := int64(1)
			a.ParentAccountID = &id
		}
		require.NotEqual(t, OpenAICodexCreditsAvailable, openAICodexCreditsStateForAccount(a, now))
		disposition, _ := resolveOpenAICredits429Decision(a, codexCreditsQuotaHeaders(), nil)
		require.Equal(t, openAIOAuth429Quota7d, disposition)
	}
	a := codexCreditsTestAccount(now)
	svc := &OpenAIGatewayService{}
	scheduler := &defaultOpenAIAccountScheduler{service: svc}
	ok, _ := scheduler.isAccountRequestCompatibleReason(context.Background(), a, OpenAIAccountScheduleRequest{})
	require.True(t, ok)
	future := now.Add(time.Hour)
	a.RateLimitResetAt = &future
	svc.BlockAccountScheduling(a, now.Add(time.Hour), "existing")
	ok, reason := scheduler.isAccountRequestCompatibleReason(context.Background(), a, OpenAIAccountScheduleRequest{})
	require.False(t, ok)
	require.Equal(t, "runtime_blocked", reason)
	require.False(t, a.IsSchedulable())
	a.RateLimitResetAt = nil
	a.Schedulable = false
	require.False(t, a.IsSchedulable())
}

func TestCodexCreditsPersistWithoutResetCardDetails(t *testing.T) {
	now := time.Now()
	a := codexCreditsTestAccount(now)
	delete(a.Extra, openaiQuotaCreditsKey)
	repo := &autoResetTestAccountRepo{account: a}
	quota := &autoResetTestQuota{}
	svc := NewOpenAIQuotaAutoResetService(repo, quota, nil, nil, nil, nil, nil)
	defer svc.Stop()
	usage := &OpenAIQuotaUsage{FetchedAt: now.Unix(), CreditsObservedAtMs: now.UnixMilli(), Credits: &OpenAICredits{HasCredits: true}}
	require.NoError(t, svc.persistFreshUsage(context.Background(), a.ID, usage, now))
	fresh, _ := repo.GetByID(context.Background(), a.ID)
	require.Equal(t, OpenAICodexCreditsAvailable, openAICodexCreditsStateForAccount(fresh, now))
	direct := &OpenAIQuotaService{accountRepo: repo}
	require.Error(t, direct.CachePostResetSnapshot(context.Background(), a.ID, usage))
	fresh, _ = repo.GetByID(context.Background(), a.ID)
	require.Equal(t, OpenAICodexCreditsAvailable, openAICodexCreditsStateForAccount(fresh, now))
	require.Zero(t, quota.resetCalls.Load())
}

func TestCodexCredits429RefreshSurvivesBackoff(t *testing.T) {
	now := time.Now()
	a := codexCreditsTestAccount(now)
	a.Extra["codex_5h_used_percent"], a.Extra["codex_7d_used_percent"] = 1.0, 1.0
	a.Extra[openAICodexCreditsRefreshStateKey] = openAICodexCreditsRefreshState{AttemptedAt: now.Unix(), RetryAfter: now.Add(time.Minute).Unix()}
	repo := &codexCreditsScanRepo{autoResetTestAccountRepo: autoResetTestAccountRepo{account: a}}
	quota := &autoResetTestQuota{usage: &OpenAIQuotaUsage{FetchedAt: now.Unix(), CreditsObservedAtMs: now.UnixMilli()}}
	svc := NewOpenAIQuotaAutoResetService(repo, quota, nil, nil, nil, nil, nil)
	defer svc.Stop()
	svc.creditsRequested.Store(a.ID, now.UnixNano())
	require.NoError(t, svc.evaluateAccount(context.Background(), a.ID))
	require.Zero(t, quota.queryCalls.Load())
	svc.scanEnabledAccounts(context.Background())
	require.Len(t, svc.queue, 1)
	delete(a.Extra, openAICodexCreditsRefreshStateKey)
	require.NoError(t, svc.evaluateAccount(context.Background(), a.ID))
	require.Equal(t, int32(1), quota.queryCalls.Load())
	_, pending := svc.creditsRequested.Load(a.ID)
	require.False(t, pending)
	require.Zero(t, quota.resetCalls.Load())
}

type codexCreditsExtendingRepo struct {
	oauth429RateLimitRepo
	storedUntil time.Time
	extendCalls int
}

func (r *codexCreditsExtendingRepo) SetRateLimitedIfLater(_ context.Context, _ int64, until time.Time) error {
	r.extendCalls++
	if until.After(r.storedUntil) {
		r.storedUntil = until
	}
	return nil
}

func TestCodexCredits429PreservesConcurrentFreeze(t *testing.T) {
	now := time.Now()
	oldUntil := now.Add(72 * time.Hour)
	repo := &codexCreditsExtendingRepo{storedUntil: oldUntil}
	rl := NewRateLimitService(repo, nil, &config.Config{}, nil, nil)
	svc := &OpenAIGatewayService{rateLimitService: rl}
	rl.SetAccountRuntimeBlocker(svc)
	a := codexCreditsTestAccount(now)
	svc.BlockAccountScheduling(a, oldUntil, "existing")
	svc.handleOpenAIAccountUpstreamError(context.Background(), a, 429, codexCreditsQuotaHeaders(), nil)
	require.Equal(t, 1, repo.extendCalls)
	require.Zero(t, repo.setRateLimitedCalls)
	require.Equal(t, oldUntil, repo.storedUntil)
	local, ok := svc.openaiAccountRuntimeBlockUntil.Load(a.ID)
	require.True(t, ok)
	require.Equal(t, oldUntil, local)
}
