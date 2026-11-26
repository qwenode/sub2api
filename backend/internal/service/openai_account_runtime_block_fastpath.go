package service

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	openAIAccountStateUpdateTimeout       = 5 * time.Second
	openAIOAuth429FallbackCooldown        = 5 * time.Second
	openAIOAuth429RetryWindow             = 2 * time.Minute
	openAIOAuth429RetryDelay              = 500 * time.Millisecond
	openAIOAuth429MaxRetryDelay           = 8 * time.Second
	openAIOAuth429MaxAccountAttempts      = 3
	openAIStopSchedulingBridgeCooldown    = 2 * time.Minute
	openAIOAuth429StormWindow             = 10 * time.Second
	openAIOAuth429StormMaxAccountSwitches = 1
)

type OpenAIOAuth429FailoverState struct {
	grokOAuth429FollowupPending bool
}

type openAIOAuth429Disposition uint8

const (
	openAIOAuth429Transient openAIOAuth429Disposition = iota
	openAIOAuth429Quota5h
	openAIOAuth429Quota7d
	openAIOAuth429QuotaReset
	openAIOAuth429CreditsShortCooldown
)

func classifyOpenAIOAuth429(headers http.Header, responseBody []byte) (openAIOAuth429Disposition, *time.Time) {
	if snapshot := ParseCodexRateLimitHeaders(headers); snapshot != nil {
		if normalized := snapshot.Normalize(); normalized != nil {
			if normalized.Used7dPercent != nil && *normalized.Used7dPercent >= 100 {
				if normalized.Reset7dSeconds != nil {
					now := time.Now()
					resetAt := now.Add(time.Duration(*normalized.Reset7dSeconds) * time.Second)
					return openAIOAuth429Quota7d, &resetAt
				}
				return openAIOAuth429Quota7d, nil
			}
			if normalized.Used5hPercent != nil && *normalized.Used5hPercent >= 100 {
				if normalized.Reset5hSeconds != nil {
					now := time.Now()
					resetAt := now.Add(time.Duration(*normalized.Reset5hSeconds) * time.Second)
					return openAIOAuth429Quota5h, &resetAt
				}
				return openAIOAuth429Quota5h, nil
			}
		}
	}
	if resetAt := calculateOpenAI429ResetTime(headers); resetAt != nil {
		return openAIOAuth429QuotaReset, resetAt
	}
	if resetUnix := parseOpenAIRateLimitResetTime(responseBody); resetUnix != nil {
		resetAt := time.Unix(*resetUnix, 0)
		return openAIOAuth429QuotaReset, &resetAt
	}
	return openAIOAuth429Transient, nil
}

func openAIAccountStateContext(ctx context.Context) (context.Context, context.CancelFunc) {
	base := context.Background()
	if ctx != nil {
		base = context.WithoutCancel(ctx)
	}
	return context.WithTimeout(base, openAIAccountStateUpdateTimeout)
}

func isOpenAIOAuthAccount(account *Account) bool {
	return account != nil && account.IsOpenAIOAuthLike()
}

func isGrokOAuthAccount(account *Account) bool {
	return account != nil && account.Platform == PlatformGrok && account.Type == AccountTypeOAuth
}

func isOpenAIAccount(account *Account) bool {
	return account != nil && (account.Platform == PlatformOpenAI || account.Platform == PlatformGrok)
}

func (s *OpenAIGatewayService) handleOpenAIAccountUpstreamError(ctx context.Context, account *Account, statusCode int, headers http.Header, responseBody []byte, canonicalModel ...string) bool {
	if account != nil && account.Platform == PlatformGrok && isGrokContentPolicyRejection(statusCode, responseBody) {
		return false
	}

	if s != nil {
		scheduleOllamaCloudUsageActivity(s.deferredService, account)
		scheduleOpenCodeGoUsageActivity(s.deferredService, account)
	}

	if account != nil && account.Platform == PlatformOpenAI && isOpenAIRequestScopedCapacityShed("", responseBody) {

		model := firstRequestedModel(canonicalModel)
		if s != nil && s.rateLimitService != nil && model != "" && account.IsTempUnschedulableEnabled() {
			ruleBody := openAICapacityShedRuleBody(responseBody)
			matches := matchTempUnschedulableRules(account, statusCode, ruleBody)
			if len(matches) > 0 && matches[0].rule.Scope == TempUnschedScopeModel {
				stateCtx, cancel := openAIAccountStateContext(ctx)
				defer cancel()
				return s.rateLimitService.HandleTempUnschedulable(stateCtx, account, statusCode, ruleBody, model)
			}
		}
		return false
	}
	stateCtx, cancel := openAIAccountStateContext(ctx)
	defer cancel()
	if account != nil && account.Platform == PlatformOpenAI && isOpenAIHTTPUpstreamAccessStateError(statusCode, "", responseBody) {
		message := "OpenAI upstream account or workspace is unavailable"
		if upstreamMsg := strings.TrimSpace(extractUpstreamErrorMessage(responseBody)); upstreamMsg != "" {
			message = upstreamMsg
		}
		if s != nil && s.rateLimitService != nil {
			s.rateLimitService.handleAuthError(stateCtx, account, message)
		}
		if s != nil {
			s.BlockAccountScheduling(account, time.Time{}, "openai_access_state")
		}
		return true
	}

	if account != nil && account.Platform == PlatformOpenAI && isOpenAIContextWindowError("", responseBody) {
		return false
	}

	if isOpenAIImageRateLimitError(statusCode, responseBody) {
		if s != nil && s.rateLimitService != nil {
			_ = s.rateLimitService.HandleOpenAIImageRateLimit(stateCtx, account, statusCode, headers, responseBody)
		}
		return false
	}

	if isOpenAIImagesSelfBuiltRequest(ctx) && isOpenAIImageCapabilityLossError(statusCode, responseBody) {
		if s != nil && s.rateLimitService != nil {
			_ = s.rateLimitService.HandleOpenAIImageCapabilityLoss(stateCtx, account, statusCode, responseBody)
		}
		return false
	}

	if s == nil || account == nil {
		return false
	}

	if s.rateLimitService != nil {
		s.rateLimitService.maybeHandleOpenAITeamLinkedError(stateCtx, account, statusCode, responseBody)
	}
	stateCtx = withTempUnschedulableModel(stateCtx, canonicalModel)
	if s.rateLimitService != nil && len(canonicalModel) > 0 && s.rateLimitService.HandleUpstreamModelNotFound(stateCtx, account, canonicalModel[0], statusCode, responseBody) {
		return true
	}

	if s.rateLimitService != nil && len(canonicalModel) > 0 && strings.TrimSpace(canonicalModel[0]) != "" &&
		tempUnschedRulesApply(account, statusCode, responseBody, canonicalModel[0]) &&
		s.rateLimitService.HandleTempUnschedulable(stateCtx, account, statusCode, responseBody, canonicalModel[0]) {
		return true
	}
	if statusCode == http.StatusTooManyRequests && s.rateLimitService != nil && len(canonicalModel) > 0 &&
		s.rateLimitService.HandleOpenAICodexSparkRateLimit(stateCtx, account, canonicalModel[0], statusCode, headers, responseBody) {
		return false
	}
	if statusCode == http.StatusTooManyRequests {
		if disposition, _ := resolveOpenAICredits429Decision(account, headers, responseBody); disposition == openAIOAuth429CreditsShortCooldown {
			until := s.rateLimitService.codexCredits429Cooldown(stateCtx, account, headers)
			stateCtx = context.WithValue(stateCtx, openAICredits429ContextKey{}, openAICredits429Cooldown{accountID: account.ID, until: until})
		}
		s.markOpenAIOAuth429RateLimited(stateCtx, account, headers, responseBody)
	}
	if s.rateLimitService == nil {
		return false
	}
	shouldDisable := s.rateLimitService.HandleUpstreamError(stateCtx, account, statusCode, headers, responseBody)
	tempModel := tempUnschedulableModel(stateCtx, nil)
	modelTempMatched := tempModel != "" && tempUnschedRulesApply(account, statusCode, responseBody, tempModel) &&
		len(matchTempUnschedulableRules(account, statusCode, responseBody)) > 0
	if shouldDisable && !modelTempMatched {
		s.BlockAccountScheduling(account, time.Time{}, "upstream_disable")
	}

	poolModeRetryable := account.IsPoolMode() && account.IsPoolModeRetryableStatus(statusCode)
	if !shouldDisable && account.Platform == PlatformOpenAI && account.Type == AccountTypeAPIKey &&
		shouldCooldownOpenAITransientUpstreamError(statusCode, responseBody) && !poolModeRetryable {
		model := ""
		if len(canonicalModel) > 0 {
			model = canonicalModel[0]
		}
		decision := s.recordOpenAIAccountModelTransientFailure(account, model, time.Now())
		if decision.FailureStreak > 0 {
			slog.Warn("openai_model_transient_state",
				"account_id", account.ID,
				"model", openAIAccountModelTransientModel(model),
				"failure_streak", decision.FailureStreak,
				"cooldown_ms", decision.Cooldown.Milliseconds(),
				"block_scope", "account_model",
			)
		}
	}
	return shouldDisable
}

func shouldCooldownOpenAITransientUpstreamError(statusCode int, responseBody []byte) bool {
	switch statusCode {
	case http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout, 520, 521, 522, 523, 524:
		return true
	case http.StatusBadRequest:
		return isOpenAITransientProcessingError(statusCode, "", responseBody)
	default:
		return false
	}
}

func (s *OpenAIGatewayService) markOpenAIOAuth429RateLimited(ctx context.Context, account *Account, headers http.Header, responseBody []byte) {
	if s == nil || !isOpenAIOAuthAccount(account) {
		return
	}

	if account.IsShadow() {
		return
	}
	s.recordOpenAIOAuth429()
	disposition, resetAt := resolveOpenAICredits429Decision(account, headers, responseBody)
	if disposition == openAIOAuth429CreditsShortCooldown {
		notifyOpenAICodexCredits429(account.ID)
		if until := s.rateLimitService.codexCredits429Cooldown(ctx, account, headers); until != nil {
			s.BlockAccountScheduling(account, *until, "credits_short_cooldown")
		}
		s.openaiOAuth429RetryStartedAt.Delete(account.ID)
		return
	}
	if disposition == openAIOAuth429Transient && s.openAIOAuth429RetryWindowActive(account) {
		return
	}

	now := time.Now()
	cooldownUntil := now.Add(openAIOAuth429FallbackCooldown)
	if resetAt != nil && resetAt.After(now) {
		cooldownUntil = *resetAt
	} else if s.rateLimitService != nil {
		cooldown, ok := s.rateLimitService.get429FallbackCooldown(ctx, account)
		if !ok || cooldown <= 0 {
			s.openaiOAuth429RetryStartedAt.Delete(account.ID)
			return
		}
		cooldownUntil = now.Add(cooldown)
	}
	s.BlockAccountScheduling(account, cooldownUntil, "429")
	s.openaiOAuth429RetryStartedAt.Delete(account.ID)
}

func (s *OpenAIGatewayService) shouldRetryOpenAIOAuth429OnSameAccount(account *Account, statusCode int, shouldDisable bool) bool {
	return s.shouldRetryOpenAIOAuth429OnSameAccountWithResponse(account, statusCode, shouldDisable, nil, nil)
}

func (s *OpenAIGatewayService) shouldRetryOpenAIOAuth429OnSameAccountWithResponse(account *Account, statusCode int, shouldDisable bool, headers http.Header, responseBody []byte) bool {
	if shouldDisable || statusCode != http.StatusTooManyRequests || !isOpenAIOAuthAccount(account) || account.IsShadow() {
		return false
	}
	disposition, _ := resolveOpenAICredits429Decision(account, headers, responseBody)
	if disposition != openAIOAuth429Transient {
		return false
	}

	if s.isOpenAIAccountRuntimeBlocked(account) {
		return false
	}
	return s.openAIOAuth429RetryWindowActive(account)
}

func (s *OpenAIGatewayService) ShouldRetryOpenAIOAuth429(account *Account, headers http.Header, responseBody []byte) bool {
	if s == nil || !isOpenAIOAuthAccount(account) || account.IsShadow() || s.isOpenAIAccountRuntimeBlocked(account) {
		return false
	}
	disposition, _ := resolveOpenAICredits429Decision(account, headers, responseBody)
	if disposition != openAIOAuth429Transient {
		return false
	}
	return s.openAIOAuth429RetryWindowActive(account)
}

func (s *OpenAIGatewayService) openAIOAuth429RetryWindowActive(account *Account) bool {
	if s == nil || !isOpenAIOAuthAccount(account) || account.IsShadow() {
		return false
	}
	now := time.Now()
	value, _ := s.openaiOAuth429RetryStartedAt.LoadOrStore(account.ID, now)
	startedAt, ok := value.(time.Time)
	if !ok {
		s.openaiOAuth429RetryStartedAt.Store(account.ID, now)
		startedAt = now
	}
	return now.Before(startedAt.Add(openAIOAuth429RetryWindow))
}

func (s *OpenAIGatewayService) openAIOAuth429RetryDeadline(account *Account) time.Time {
	if s == nil || !isOpenAIOAuthAccount(account) || account.IsShadow() {
		return time.Time{}
	}
	value, ok := s.openaiOAuth429RetryStartedAt.Load(account.ID)
	if !ok {
		return time.Time{}
	}
	startedAt, ok := value.(time.Time)
	if !ok {
		return time.Time{}
	}
	return startedAt.Add(openAIOAuth429RetryWindow)
}

func openAIOAuth429SameAccountRetryDelay(headers http.Header, deadline time.Time) time.Duration {
	delay := openAIOAuth429RetryDelay
	now := time.Now()
	if resetAt := parseRetryAfterResetTime(headers, now); resetAt != nil && resetAt.After(now) {
		delay = resetAt.Sub(now)
	}
	if delay > openAIOAuth429MaxRetryDelay {
		delay = openAIOAuth429MaxRetryDelay
	}
	if remaining := time.Until(deadline); !deadline.IsZero() && delay > remaining {
		delay = remaining
	}
	if delay < 0 {
		return 0
	}
	return delay
}

func (s *OpenAIGatewayService) BlockAccountScheduling(account *Account, until time.Time, reason string) {
	if s == nil || !isOpenAIAccount(account) {
		return
	}
	mu := s.openAIAccountRuntimeBlockLock(account.ID)
	mu.Lock()
	defer mu.Unlock()
	_, _ = s.blockAccountSchedulingLocked(account, until, reason)
}

func (s *OpenAIGatewayService) openAIAccountRuntimeBlockLock(accountID int64) *sync.Mutex {
	actual, _ := s.openaiAccountRuntimeBlockLocks.LoadOrStore(accountID, &sync.Mutex{})
	mu, ok := actual.(*sync.Mutex)
	if !ok {
		mu = &sync.Mutex{}
		s.openaiAccountRuntimeBlockLocks.Store(accountID, mu)
	}
	return mu
}

func (s *OpenAIGatewayService) blockAccountSchedulingLocked(account *Account, until time.Time, _ string) (uint64, bool) {
	generation := s.openaiAccountRuntimeBlockSequence.Add(1)
	s.openaiAccountRuntimeBlockGeneration.Store(account.ID, generation)
	now := time.Now()
	blockUntil := until
	if blockUntil.IsZero() || !blockUntil.After(now) {
		blockUntil = now.Add(openAIStopSchedulingBridgeCooldown)
	}

	for {
		current, loaded := s.openaiAccountRuntimeBlockUntil.Load(account.ID)
		if !loaded {
			actual, stored := s.openaiAccountRuntimeBlockUntil.LoadOrStore(account.ID, blockUntil)
			if !stored {
				return generation, true
			}
			current = actual
		}

		currentUntil, ok := current.(time.Time)
		if !ok || currentUntil.IsZero() {
			if s.openaiAccountRuntimeBlockUntil.CompareAndSwap(account.ID, current, blockUntil) {
				return generation, true
			}
			continue
		}
		if !blockUntil.After(currentUntil) {
			return generation, false
		}
		if s.openaiAccountRuntimeBlockUntil.CompareAndSwap(account.ID, current, blockUntil) {
			return generation, true
		}
	}
}

func (s *OpenAIGatewayService) ClearAccountSchedulingBlock(accountID int64) {
	if s == nil || accountID <= 0 {
		return
	}
	mu := s.openAIAccountRuntimeBlockLock(accountID)
	mu.Lock()
	defer mu.Unlock()
	s.openaiAccountRuntimeBlockUntil.Delete(accountID)
	s.openaiOAuth429RetryStartedAt.Delete(accountID)
	s.openaiAccountRuntimeBlockGeneration.Store(accountID, s.openaiAccountRuntimeBlockSequence.Add(1))
}

func (s *OpenAIGatewayService) isOpenAIAccountRuntimeBlocked(account *Account) bool {
	if s == nil || !isOpenAIAccount(account) {
		return false
	}
	mu := s.openAIAccountRuntimeBlockLock(account.ID)
	mu.Lock()
	defer mu.Unlock()
	value, ok := s.openaiAccountRuntimeBlockUntil.Load(account.ID)
	if !ok {
		return false
	}
	cooldownUntil, ok := value.(time.Time)
	if !ok || cooldownUntil.IsZero() {
		s.openaiAccountRuntimeBlockUntil.Delete(account.ID)
		s.openaiAccountRuntimeBlockGeneration.Store(account.ID, s.openaiAccountRuntimeBlockSequence.Add(1))
		return false
	}
	if time.Now().Before(cooldownUntil) {
		return true
	}
	s.openaiAccountRuntimeBlockUntil.Delete(account.ID)
	s.openaiAccountRuntimeBlockGeneration.Store(account.ID, s.openaiAccountRuntimeBlockSequence.Add(1))
	return false
}

func (s *OpenAIGatewayService) getOpenAIAccountModelTransientState() *openAIAccountModelTransientState {
	if s == nil {
		return nil
	}
	s.openaiModelTransientOnce.Do(func() {
		if s.openaiModelTransient == nil {
			s.openaiModelTransient = newOpenAIAccountModelTransientState(openAIModelTransientDefaultMax)
		}
	})
	return s.openaiModelTransient
}

func canonicalOpenAIAccountSchedulingModel(account *Account, requestedModel string) string {
	model := strings.TrimSpace(requestedModel)
	if account == nil || model == "" {
		return model
	}
	if account.IsOpenAI() {
		return resolveOpenAIAccountUpstreamModelForRequest(account, model, false)
	}
	if mapped := strings.TrimSpace(account.GetMappedModel(model)); mapped != "" {
		return mapped
	}
	return model
}

func openAIAccountModelTransientModel(canonicalModel string) string {
	return normalizeOpenAIAccountModelTransientModel(canonicalModel)
}

func (s *OpenAIGatewayService) recordOpenAIAccountModelTransientFailure(account *Account, canonicalModel string, now time.Time) openAIAccountModelTransientDecision {
	if s == nil || account == nil {
		return openAIAccountModelTransientDecision{}
	}
	state := s.getOpenAIAccountModelTransientState()
	if state == nil {
		return openAIAccountModelTransientDecision{}
	}
	return state.recordFailure(account.ID, openAIAccountModelTransientModel(canonicalModel), now)
}

func (s *OpenAIGatewayService) clearOpenAIAccountModelTransientState(accountID int64, model string) {
	state := s.getOpenAIAccountModelTransientState()
	if state == nil {
		return
	}
	state.recordSuccess(accountID, model)
}

func (s *OpenAIGatewayService) isOpenAIAccountModelRuntimeBlocked(account *Account, requestedModel string) bool {
	if s == nil || account == nil {
		return false
	}
	state := s.getOpenAIAccountModelTransientState()
	if state == nil {
		return false
	}
	canonicalModel := canonicalOpenAIAccountSchedulingModel(account, requestedModel)
	return state.isBlocked(account.ID, openAIAccountModelTransientModel(canonicalModel), time.Now())
}

func accountPersistedSchedulingCooldownActive(account *Account) bool {
	if account == nil {
		return false
	}
	now := time.Now()
	if account.TempUnschedulableUntil != nil && now.Before(*account.TempUnschedulableUntil) {
		return true
	}
	if account.RateLimitResetAt != nil && now.Before(*account.RateLimitResetAt) {
		return true
	}
	if account.OverloadUntil != nil && now.Before(*account.OverloadUntil) {
		return true
	}
	return false
}

type openAIAccountRuntimeBlockSnapshot struct {
	until      time.Time
	generation uint64
	blocked    bool
}

func (s *OpenAIGatewayService) peekOpenAIAccountRuntimeBlock(account *Account) openAIAccountRuntimeBlockSnapshot {
	if s == nil || !isOpenAIAccount(account) {
		return openAIAccountRuntimeBlockSnapshot{}
	}
	mu := s.openAIAccountRuntimeBlockLock(account.ID)
	mu.Lock()
	defer mu.Unlock()
	value, ok := s.openaiAccountRuntimeBlockUntil.Load(account.ID)
	if !ok {
		return openAIAccountRuntimeBlockSnapshot{}
	}
	until, isTime := value.(time.Time)
	if !isTime || until.IsZero() || !time.Now().Before(until) {
		s.openaiAccountRuntimeBlockUntil.Delete(account.ID)
		s.openaiOAuth429RetryStartedAt.Delete(account.ID)
		s.openaiAccountRuntimeBlockGeneration.Store(account.ID, s.openaiAccountRuntimeBlockSequence.Add(1))
		return openAIAccountRuntimeBlockSnapshot{}
	}
	generation, _ := s.openaiAccountRuntimeBlockGeneration.Load(account.ID)
	gen, _ := generation.(uint64)
	return openAIAccountRuntimeBlockSnapshot{until: until, generation: gen, blocked: true}
}

func (s *OpenAIGatewayService) clearOpenAIAccountRuntimeBlockIfUnchanged(accountID int64, snapshot openAIAccountRuntimeBlockSnapshot) {
	if s == nil || accountID <= 0 || !snapshot.blocked {
		return
	}
	mu := s.openAIAccountRuntimeBlockLock(accountID)
	mu.Lock()
	defer mu.Unlock()
	generation, ok := s.openaiAccountRuntimeBlockGeneration.Load(accountID)
	if !ok || generation != snapshot.generation {
		return
	}
	current, ok := s.openaiAccountRuntimeBlockUntil.Load(accountID)
	currentUntil, isTime := current.(time.Time)
	if !ok || !isTime || !currentUntil.Equal(snapshot.until) {
		return
	}
	s.openaiAccountRuntimeBlockUntil.Delete(accountID)
	s.openaiOAuth429RetryStartedAt.Delete(accountID)
	s.openaiAccountRuntimeBlockGeneration.Store(accountID, s.openaiAccountRuntimeBlockSequence.Add(1))
}

func (s *OpenAIGatewayService) isOpenAIAccountRequestRuntimeBlocked(account *Account, requestedModel string) bool {
	if s == nil {
		return false
	}
	snapshot := s.peekOpenAIAccountRuntimeBlock(account)
	if snapshot.blocked {
		if accountPersistedSchedulingCooldownActive(account) {
			return true
		}
		s.clearOpenAIAccountRuntimeBlockIfUnchanged(account.ID, snapshot)
	}
	return s.isOpenAIAccountModelRuntimeBlocked(account, requestedModel)
}

func (s *OpenAIGatewayService) recordOpenAIOAuth429() {
	if s == nil {
		return
	}
	now := time.Now()
	windowStart := s.openaiOAuth429WindowStartUnixNano.Load()
	if windowStart == 0 || now.Sub(time.Unix(0, windowStart)) >= openAIOAuth429StormWindow {
		if s.openaiOAuth429WindowStartUnixNano.CompareAndSwap(windowStart, now.UnixNano()) {
			s.openaiOAuth429WindowCount.Store(1)
			return
		}
	}
	s.openaiOAuth429WindowCount.Add(1)
}

func (s *OpenAIGatewayService) ShouldStopOpenAIOAuth429Failover(account *Account, statusCode int, failedSwitches int, state *OpenAIOAuth429FailoverState) bool {
	if failedSwitches < openAIOAuth429StormMaxAccountSwitches {
		return false
	}
	if state != nil && state.grokOAuth429FollowupPending {

		return true
	}
	if isGrokOAuthAccount(account) {
		if state == nil {

			return statusCode == http.StatusTooManyRequests && failedSwitches >= 2
		}
		if statusCode == http.StatusTooManyRequests {
			state.grokOAuth429FollowupPending = true
		}
		return false
	}
	if statusCode != http.StatusTooManyRequests || !isOpenAIOAuthAccount(account) {
		return false
	}

	return failedSwitches >= openAIOAuth429MaxAccountAttempts
}
