package service

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/shopspring/decimal"
)

type OpenAICodexCreditsState string

const (
	OpenAICodexCreditsAvailable   OpenAICodexCreditsState = "available"
	OpenAICodexCreditsUnavailable OpenAICodexCreditsState = "unavailable"
	OpenAICodexCreditsUnknown     OpenAICodexCreditsState = "unknown"
)

const openAICodexCreditsSnapshotTTL = openAIProbeCacheTTL

func IsOpenAICodexCreditsAccount(account *Account) bool {
	return account != nil && account.Platform == PlatformOpenAI && account.Type == AccountTypeOAuth && !account.IsShadow()
}

func OpenAICodexCreditsStateOf(credits *OpenAICredits) OpenAICodexCreditsState {
	if credits == nil {
		return OpenAICodexCreditsUnknown
	}
	if credits.Unlimited {
		return OpenAICodexCreditsAvailable
	}
	if credits.invalid || (credits.decoded && !credits.hasCreditsPresent) {
		return OpenAICodexCreditsUnknown
	}
	if !credits.HasCredits {
		return OpenAICodexCreditsUnavailable
	}
	if credits.Balance == nil {
		return OpenAICodexCreditsAvailable
	}
	balance, err := decimal.NewFromString(*credits.Balance)
	if err != nil {
		return OpenAICodexCreditsUnknown
	}
	if balance.GreaterThan(decimal.Zero) {
		return OpenAICodexCreditsAvailable
	}
	return OpenAICodexCreditsUnavailable
}

func openAICodexCreditsSnapshotObservedAtMs(snapshot openAICreditsSnapshot) int64 {
	if snapshot.observedAtPresent || snapshot.ObservedAtMs != 0 {
		return snapshot.ObservedAtMs
	}
	if snapshot.FetchedAt > 0 {
		return snapshot.FetchedAt * 1000
	}
	return 0
}

func (s *openAICreditsSnapshot) UnmarshalJSON(data []byte) error {
	type snapshotAlias openAICreditsSnapshot
	var decoded snapshotAlias
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	*s = openAICreditsSnapshot(decoded)
	_, s.observedAtPresent = fields["observed_at_ms"]
	return nil
}

func (c *OpenAICredits) UnmarshalJSON(data []byte) error {
	*c = OpenAICredits{decoded: true}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil || fields == nil {
		c.invalid = true
		return nil
	}
	if raw, ok := fields["has_credits"]; ok {
		c.hasCreditsPresent = string(raw) != "null"
		if err := json.Unmarshal(raw, &c.HasCredits); err != nil {
			c.invalid = true
		}
	}
	if raw, ok := fields["unlimited"]; ok {
		if err := json.Unmarshal(raw, &c.Unlimited); err != nil {
			c.invalid = true
		}
	}
	if raw, ok := fields["balance"]; ok {
		if err := json.Unmarshal(raw, &c.Balance); err != nil {
			c.invalid = true
		}
	}
	return nil
}

func newOpenAICreditsSnapshot(credits *OpenAICredits, fetchedAt, observedAtMs int64) openAICreditsSnapshot {
	if OpenAICodexCreditsStateOf(credits) == OpenAICodexCreditsUnknown {
		credits = nil
	}
	return openAICreditsSnapshot{Credits: credits, FetchedAt: fetchedAt, ObservedAtMs: observedAtMs}
}

func openAICodexCreditsSnapshotFromExtra(extra map[string]any) *openAICreditsSnapshot {
	raw, ok := extra[openaiQuotaCreditsKey]
	if !ok || raw == nil {
		return nil
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		return nil
	}
	var snapshot openAICreditsSnapshot
	if err := json.Unmarshal(encoded, &snapshot); err != nil {
		return nil
	}
	return &snapshot
}

func openAICodexCreditsStateForAccount(account *Account, now time.Time) OpenAICodexCreditsState {
	if !IsOpenAICodexCreditsAccount(account) {
		return OpenAICodexCreditsUnknown
	}
	return openAICodexCreditsStateFromSnapshot(openAICodexCreditsSnapshotFromExtra(account.Extra), now)
}

func openAICodexCreditsStateForResponse(account *Account, headers http.Header, now time.Time) OpenAICodexCreditsState {
	if !IsOpenAICodexCreditsAccount(account) {
		return OpenAICodexCreditsUnknown
	}
	if snapshot := ParseCodexRateLimitHeaders(headers); snapshot != nil && snapshot.CreditsPresent {
		return OpenAICodexCreditsStateOf(snapshot.Credits)
	}
	return openAICodexCreditsStateForAccount(account, now)
}

func resolveOpenAICredits429Decision(account *Account, headers http.Header, body []byte) (openAIOAuth429Disposition, *time.Time) {
	disposition, resetAt := classifyOpenAIOAuth429(headers, body)
	if disposition == openAIOAuth429Transient || !IsOpenAICodexCreditsAccount(account) || isOpenAIImageRateLimitError(http.StatusTooManyRequests, body) {
		return disposition, resetAt
	}
	if openAICodexCreditsStateForResponse(account, headers, time.Now()) == OpenAICodexCreditsUnavailable {
		return disposition, resetAt
	}
	return openAIOAuth429CreditsShortCooldown, nil
}

type openAICredits429ContextKey struct{}

type openAICredits429Cooldown struct {
	accountID int64
	until     *time.Time
}

func (s *RateLimitService) codexCredits429Cooldown(ctx context.Context, account *Account, headers http.Header) *time.Time {
	if value, ok := ctx.Value(openAICredits429ContextKey{}).(openAICredits429Cooldown); ok && value.accountID == account.ID {
		return value.until
	}
	cooldown, enabled := time.Duration(defaultRateLimit429CooldownSeconds)*time.Second, true
	if s != nil {
		cooldown, enabled = s.get429FallbackCooldown(ctx, account)
	}
	if !enabled || cooldown <= 0 {
		return nil
	}
	now := time.Now()
	until := now.Add(cooldown)
	if retryAt := parseRetryAfterResetTime(headers, now); retryAt != nil && retryAt.After(until) {
		until = *retryAt
	}
	if account.RateLimitResetAt != nil && account.RateLimitResetAt.After(until) {
		until = *account.RateLimitResetAt
	}
	return &until
}

func openAICodexCreditsSnapshotFresh(snapshot *openAICreditsSnapshot, now time.Time) bool {
	if snapshot == nil {
		return false
	}
	observedAtMs := openAICodexCreditsSnapshotObservedAtMs(*snapshot)
	if observedAtMs <= 0 || now.IsZero() {
		return false
	}
	observedAt := time.UnixMilli(observedAtMs)
	if observedAt.After(now) {
		return false
	}
	return now.Sub(observedAt) < openAICodexCreditsSnapshotTTL
}

func openAICodexCreditsStateFromSnapshot(snapshot *openAICreditsSnapshot, now time.Time) OpenAICodexCreditsState {
	if snapshot == nil || !openAICodexCreditsSnapshotFresh(snapshot, now) {
		return OpenAICodexCreditsUnknown
	}
	return OpenAICodexCreditsStateOf(snapshot.Credits)
}
