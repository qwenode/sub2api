package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/adminaccess"
	httppool "github.com/Wei-Shaw/sub2api/internal/pkg/httpclient"
	openaipkg "github.com/Wei-Shaw/sub2api/internal/pkg/openai"
	"github.com/Wei-Shaw/sub2api/internal/pkg/pagination"
	"github.com/Wei-Shaw/sub2api/internal/pkg/timezone"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"github.com/Wei-Shaw/sub2api/internal/pkg/usagestats"
	"github.com/Wei-Shaw/sub2api/internal/pkg/xai"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/singleflight"
)

type UsageLogRepository interface {
	Create(ctx context.Context, log *UsageLog) (inserted bool, err error)
	GetByID(ctx context.Context, id int64) (*UsageLog, error)
	Delete(ctx context.Context, id int64) error

	ListByUser(ctx context.Context, userID int64, params pagination.PaginationParams) ([]UsageLog, *pagination.PaginationResult, error)
	ListByAPIKey(ctx context.Context, apiKeyID int64, params pagination.PaginationParams) ([]UsageLog, *pagination.PaginationResult, error)
	ListByAccount(ctx context.Context, accountID int64, params pagination.PaginationParams) ([]UsageLog, *pagination.PaginationResult, error)

	ListByUserAndTimeRange(ctx context.Context, userID int64, startTime, endTime time.Time) ([]UsageLog, *pagination.PaginationResult, error)
	ListByAPIKeyAndTimeRange(ctx context.Context, apiKeyID int64, startTime, endTime time.Time) ([]UsageLog, *pagination.PaginationResult, error)
	ListByAccountAndTimeRange(ctx context.Context, accountID int64, startTime, endTime time.Time) ([]UsageLog, *pagination.PaginationResult, error)
	ListByModelAndTimeRange(ctx context.Context, modelName string, startTime, endTime time.Time) ([]UsageLog, *pagination.PaginationResult, error)

	GetAccountWindowStats(ctx context.Context, accountID int64, startTime time.Time) (*usagestats.AccountStats, error)
	GetAccountTodayStats(ctx context.Context, accountID int64) (*usagestats.AccountStats, error)

	GetDashboardStats(ctx context.Context) (*usagestats.DashboardStats, error)
	GetUsageTrendWithFilters(ctx context.Context, startTime, endTime time.Time, granularity string, userID, apiKeyID, accountID, groupID int64, model string, requestType *int16, stream *bool, billingType *int8) ([]usagestats.TrendDataPoint, error)
	GetModelStatsWithFilters(ctx context.Context, startTime, endTime time.Time, userID, apiKeyID, accountID, groupID int64, requestType *int16, stream *bool, billingType *int8) ([]usagestats.ModelStat, error)
	GetEndpointStatsWithFilters(ctx context.Context, startTime, endTime time.Time, userID, apiKeyID, accountID, groupID int64, model string, requestType *int16, stream *bool, billingType *int8) ([]usagestats.EndpointStat, error)
	GetUpstreamEndpointStatsWithFilters(ctx context.Context, startTime, endTime time.Time, userID, apiKeyID, accountID, groupID int64, model string, requestType *int16, stream *bool, billingType *int8) ([]usagestats.EndpointStat, error)
	GetGroupStatsWithFilters(ctx context.Context, startTime, endTime time.Time, userID, apiKeyID, accountID, groupID int64, requestType *int16, stream *bool, billingType *int8) ([]usagestats.GroupStat, error)
	GetUserBreakdownStats(ctx context.Context, startTime, endTime time.Time, dim usagestats.UserBreakdownDimension, limit int) ([]usagestats.UserBreakdownItem, error)
	GetAllGroupUsageSummary(ctx context.Context, todayStart time.Time) ([]usagestats.GroupUsageSummary, error)
	GetAPIKeyUsageTrend(ctx context.Context, startTime, endTime time.Time, granularity string, limit int) ([]usagestats.APIKeyUsageTrendPoint, error)
	GetUserUsageTrend(ctx context.Context, startTime, endTime time.Time, granularity string, limit int, metric string) ([]usagestats.UserUsageTrendPoint, error)
	GetUserSpendingRanking(ctx context.Context, startTime, endTime time.Time, limit int) (*usagestats.UserSpendingRankingResponse, error)
	GetBatchUserUsageStats(ctx context.Context, userIDs []int64, startTime, endTime time.Time) (map[int64]*usagestats.BatchUserUsageStats, error)
	GetBatchAPIKeyUsageStats(ctx context.Context, apiKeyIDs []int64, startTime, endTime time.Time) (map[int64]*usagestats.BatchAPIKeyUsageStats, error)

	GetUserDashboardStats(ctx context.Context, userID int64) (*usagestats.UserDashboardStats, error)
	GetAPIKeyDashboardStats(ctx context.Context, apiKeyID int64) (*usagestats.UserDashboardStats, error)
	GetUserUsageTrendByUserID(ctx context.Context, userID int64, startTime, endTime time.Time, granularity string) ([]usagestats.TrendDataPoint, error)
	GetUserModelStats(ctx context.Context, userID int64, startTime, endTime time.Time) ([]usagestats.ModelStat, error)

	ListWithFilters(ctx context.Context, params pagination.PaginationParams, filters usagestats.UsageLogFilters) ([]UsageLog, *pagination.PaginationResult, error)
	GetGlobalStats(ctx context.Context, startTime, endTime time.Time) (*usagestats.UsageStats, error)
	GetStatsWithFilters(ctx context.Context, filters usagestats.UsageLogFilters) (*usagestats.UsageStats, error)

	GetAccountUsageStats(ctx context.Context, accountID int64, startTime, endTime time.Time) (*usagestats.AccountUsageStatsResponse, error)

	GetUserStatsAggregated(ctx context.Context, userID int64, startTime, endTime time.Time) (*usagestats.UsageStats, error)
	GetAPIKeyStatsAggregated(ctx context.Context, apiKeyID int64, startTime, endTime time.Time) (*usagestats.UsageStats, error)
	GetAccountStatsAggregated(ctx context.Context, accountID int64, startTime, endTime time.Time) (*usagestats.UsageStats, error)
	GetModelStatsAggregated(ctx context.Context, modelName string, startTime, endTime time.Time) (*usagestats.UsageStats, error)
	GetDailyStatsAggregated(ctx context.Context, userID int64, startTime, endTime time.Time) ([]map[string]any, error)
}

type accountWindowStatsBatchReader interface {
	GetAccountWindowStatsBatch(ctx context.Context, accountIDs []int64, startTime time.Time) (map[int64]*usagestats.AccountStats, error)
}

type apiUsageCache struct {
	response  *ClaudeUsageResponse
	err       error
	timestamp time.Time
}

type windowStatsCache struct {
	stats     *WindowStats
	timestamp time.Time
	startTime time.Time
}

type windowStatsCacheKey struct {
	accountID int64
	duration  time.Duration
}

type antigravityUsageCache struct {
	usageInfo *UsageInfo
	timestamp time.Time
}

const (
	apiCacheTTL         = 3 * time.Minute
	apiErrorCacheTTL    = 1 * time.Minute
	antigravityErrorTTL = 1 * time.Minute
	apiQueryMaxJitter   = 800 * time.Millisecond
	windowStatsCacheTTL = 1 * time.Minute
	openAIProbeCacheTTL = 10 * time.Minute
	grokProbeRetryTTL   = 1 * time.Minute
	grokFreeQuotaWindow = 24 * time.Hour
)

type UsageCache struct {
	apiCache          sync.Map
	windowStatsCache  sync.Map
	antigravityCache  sync.Map
	apiFlight         singleflight.Group
	antigravityFlight singleflight.Group
	openAIProbeCache  sync.Map
	grokProbeCache    sync.Map
}

func NewUsageCache() *UsageCache {
	return &UsageCache{}
}

type WindowStats struct {
	Requests     int64   `json:"requests"`
	Tokens       int64   `json:"tokens"`
	Cost         float64 `json:"cost"`
	StandardCost float64 `json:"standard_cost"`
	UserCost     float64 `json:"user_cost"`
}

type UsageProgress struct {
	Utilization        float64      `json:"utilization"`
	UtilizationUnknown bool         `json:"utilization_unknown,omitempty"`
	ResetsAt           *time.Time   `json:"resets_at"`
	RemainingSeconds   int          `json:"remaining_seconds"`
	WindowStats        *WindowStats `json:"window_stats,omitempty"`
	UsedRequests       int64        `json:"used_requests,omitempty"`
	LimitRequests      int64        `json:"limit_requests,omitempty"`
}

type AntigravityQuotaGroup struct {
	Key         string                    `json:"key"`
	Name        string                    `json:"name"`
	Description string                    `json:"description,omitempty"`
	Windows     []*AntigravityQuotaWindow `json:"windows"`
}

type AntigravityQuotaWindow struct {
	Window      string     `json:"window"`
	Utilization float64    `json:"utilization"`
	ResetsAt    *time.Time `json:"resets_at"`
}

type AntigravityModelQuota struct {
	Utilization int    `json:"utilization"`
	ResetTime   string `json:"reset_time"`
}

type AntigravityModelDetail struct {
	DisplayName        string          `json:"display_name,omitempty"`
	SupportsImages     *bool           `json:"supports_images,omitempty"`
	SupportsThinking   *bool           `json:"supports_thinking,omitempty"`
	ThinkingBudget     *int            `json:"thinking_budget,omitempty"`
	Recommended        *bool           `json:"recommended,omitempty"`
	MaxTokens          *int            `json:"max_tokens,omitempty"`
	MaxOutputTokens    *int            `json:"max_output_tokens,omitempty"`
	SupportedMimeTypes map[string]bool `json:"supported_mime_types,omitempty"`
}

type AICredit struct {
	CreditType     string  `json:"credit_type,omitempty"`
	Amount         float64 `json:"amount,omitempty"`
	MinimumBalance float64 `json:"minimum_balance,omitempty"`
}

type UsageInfo struct {
	Source             string         `json:"source,omitempty"`
	UpdatedAt          *time.Time     `json:"updated_at,omitempty"`
	FiveHour           *UsageProgress `json:"five_hour"`
	SevenDay           *UsageProgress `json:"seven_day,omitempty"`
	SevenDaySonnet     *UsageProgress `json:"seven_day_sonnet,omitempty"`
	SevenDayFable      *UsageProgress `json:"seven_day_fable,omitempty"`
	GeminiSharedDaily  *UsageProgress `json:"gemini_shared_daily,omitempty"`
	GeminiProDaily     *UsageProgress `json:"gemini_pro_daily,omitempty"`
	GeminiFlashDaily   *UsageProgress `json:"gemini_flash_daily,omitempty"`
	GeminiSharedMinute *UsageProgress `json:"gemini_shared_minute,omitempty"`
	GeminiProMinute    *UsageProgress `json:"gemini_pro_minute,omitempty"`
	GeminiFlashMinute  *UsageProgress `json:"gemini_flash_minute,omitempty"`

	AntigravityQuota map[string]*AntigravityModelQuota `json:"antigravity_quota,omitempty"`

	AntigravityQuotaGroups []*AntigravityQuotaGroup `json:"antigravity_quota_groups,omitempty"`

	GrokRequestQuota       *xai.QuotaWindow `json:"grok_request_quota,omitempty"`
	GrokTokenQuota         *xai.QuotaWindow `json:"grok_token_quota,omitempty"`
	GrokRetryAfterSeconds  *int             `json:"grok_retry_after_seconds,omitempty"`
	GrokEntitlementStatus  string           `json:"grok_entitlement_status,omitempty"`
	GrokQuotaSnapshotState string           `json:"grok_quota_snapshot_state,omitempty"`
	GrokLastQuotaProbeAt   string           `json:"grok_last_quota_probe_at,omitempty"`
	GrokLastHeadersSeenAt  string           `json:"grok_last_headers_seen_at,omitempty"`
	GrokLastStatusCode     int              `json:"grok_last_status_code,omitempty"`
	GrokFreeTokenLimit     int64            `json:"grok_free_token_limit,omitempty"`
	GrokLocalUsage         *WindowStats     `json:"grok_local_usage,omitempty"`
	GrokLocalUsage24h      *WindowStats     `json:"grok_local_usage_24h,omitempty"`
	GrokLocalUsage7d       *WindowStats     `json:"grok_local_usage_7d,omitempty"`
	GrokLocalUsageMonthly  *WindowStats     `json:"grok_local_usage_monthly,omitempty"`

	ThirtyDay   *UsageProgress      `json:"thirty_day,omitempty"`
	GrokBilling *xai.BillingSummary `json:"grok_billing,omitempty"`

	SubscriptionTier    string `json:"subscription_tier,omitempty"`
	SubscriptionTierRaw string `json:"subscription_tier_raw,omitempty"`

	AntigravityQuotaDetails map[string]*AntigravityModelDetail `json:"antigravity_quota_details,omitempty"`

	AICredits []AICredit `json:"ai_credits,omitempty"`

	ModelForwardingRules map[string]string `json:"model_forwarding_rules,omitempty"`

	IsForbidden     bool   `json:"is_forbidden,omitempty"`
	ForbiddenReason string `json:"forbidden_reason,omitempty"`
	ForbiddenType   string `json:"forbidden_type,omitempty"`
	ValidationURL   string `json:"validation_url,omitempty"`

	NeedsVerify bool `json:"needs_verify,omitempty"`
	IsBanned    bool `json:"is_banned,omitempty"`
	NeedsReauth bool `json:"needs_reauth,omitempty"`

	ErrorCode string `json:"error_code,omitempty"`

	Error string `json:"error,omitempty"`
}

type ClaudeUsageWindow struct {
	Utilization float64 `json:"utilization"`
	ResetsAt    string  `json:"resets_at"`
}

type ClaudeUsageResponse struct {
	FiveHour struct {
		Utilization float64 `json:"utilization"`
		ResetsAt    string  `json:"resets_at"`
	} `json:"five_hour"`
	SevenDay struct {
		Utilization float64 `json:"utilization"`
		ResetsAt    string  `json:"resets_at"`
	} `json:"seven_day"`
	SevenDaySonnet struct {
		Utilization float64 `json:"utilization"`
		ResetsAt    string  `json:"resets_at"`
	} `json:"seven_day_sonnet"`

	SevenDayOverageIncluded ClaudeUsageWindow `json:"seven_day_overage_included"`
	Limits                  []struct {
		Kind     string   `json:"kind"`
		Group    string   `json:"group"`
		Percent  *float64 `json:"percent"`
		ResetsAt string   `json:"resets_at"`
		Scope    struct {
			Model struct {
				DisplayName string `json:"display_name"`
			} `json:"model"`
			Surface json.RawMessage `json:"surface"`
		} `json:"scope"`
	} `json:"limits"`
}

type ClaudeUsageFetchOptions struct {
	AccessToken string
	ProxyURL    string
	AccountID   int64
	TLSProfile  *tlsfingerprint.Profile
	Fingerprint *Fingerprint
}

type ClaudeUsageFetcher interface {
	FetchUsage(ctx context.Context, accessToken, proxyURL string) (*ClaudeUsageResponse, error)

	FetchUsageWithOptions(ctx context.Context, opts *ClaudeUsageFetchOptions) (*ClaudeUsageResponse, error)
}

type AccountUsageService struct {
	accountRepo             AccountRepository
	usageLogRepo            UsageLogRepository
	usageFetcher            ClaudeUsageFetcher
	geminiQuotaService      *GeminiQuotaService
	antigravityQuotaFetcher *AntigravityQuotaFetcher
	grokQuotaFetcher        *GrokQuotaFetcher
	grokQuotaService        *GrokQuotaService
	openAIQuotaService      *OpenAIQuotaService
	cache                   *UsageCache
	identityCache           IdentityCache
	tlsFPProfileService     *TLSFingerprintProfileService
	agentIdentityTaskMu     sync.Mutex
	agentIdentityWS         agentIdentityWSConnectionInvalidator
}

func NewAccountUsageService(
	accountRepo AccountRepository,
	usageLogRepo UsageLogRepository,
	usageFetcher ClaudeUsageFetcher,
	geminiQuotaService *GeminiQuotaService,
	antigravityQuotaFetcher *AntigravityQuotaFetcher,
	grokQuotaFetcher *GrokQuotaFetcher,
	grokQuotaService *GrokQuotaService,
	openAIQuotaService *OpenAIQuotaService,
	cache *UsageCache,
	identityCache IdentityCache,
	tlsFPProfileService *TLSFingerprintProfileService,
) *AccountUsageService {
	return &AccountUsageService{
		accountRepo:             accountRepo,
		usageLogRepo:            usageLogRepo,
		usageFetcher:            usageFetcher,
		geminiQuotaService:      geminiQuotaService,
		antigravityQuotaFetcher: antigravityQuotaFetcher,
		grokQuotaFetcher:        grokQuotaFetcher,
		grokQuotaService:        grokQuotaService,
		openAIQuotaService:      openAIQuotaService,
		cache:                   cache,
		identityCache:           identityCache,
		tlsFPProfileService:     tlsFPProfileService,
	}
}

func supportsAnthropicPassiveUsage(account *Account) bool {
	return account != nil && account.IsAnthropicOAuthOrSetupToken()
}

func batchUsageErrorMessage(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func (s *AccountUsageService) getUsageForAccount(ctx context.Context, account *Account, forceProbe bool) (*UsageInfo, error) {
	if err := scopedAccountAccess(ctx, s.accountRepo, account, "accounts.usage.read", true); err != nil {
		return nil, err
	}
	if account == nil {
		return nil, fmt.Errorf("account is required")
	}
	accountID := account.ID

	if account.IsSyntheticUITest() && account.IsAnthropicOAuthOrSetupToken() {
		return s.getPassiveUsageForAccount(ctx, account)
	}

	if account.Platform == PlatformOpenAI && account.Type == AccountTypeOAuth {

		return s.getOpenAIUsage(ctx, account, forceProbe)
	}

	if account.Platform == PlatformGemini {
		usage, err := s.getGeminiUsage(ctx, account)
		if err == nil {
			s.tryClearRecoverableAccountError(ctx, account)
		}
		return usage, err
	}

	if account.Platform == PlatformAntigravity {
		usage, err := s.getAntigravityUsage(ctx, account, forceProbe)
		if err == nil {
			s.tryClearRecoverableAccountError(ctx, account)
		}
		return usage, err
	}

	if account.Platform == PlatformGrok {
		usage, err := s.getGrokUsage(ctx, account, forceProbe)
		if err == nil && usage != nil && usage.Error == "" {
			s.tryClearRecoverableAccountError(ctx, account)
		}
		return usage, err
	}

	if account.CanGetUsage() {
		var apiResp *ClaudeUsageResponse

		if cached, ok := s.cache.apiCache.Load(accountID); ok {
			if cache, ok := cached.(*apiUsageCache); ok {
				age := time.Since(cache.timestamp)
				if cache.err != nil && age < apiErrorCacheTTL {

					return nil, cache.err
				}
				if cache.response != nil && age < apiCacheTTL {
					apiResp = cache.response
				}
			}
		}

		if apiResp == nil {

			jitter := time.Duration(rand.Int64N(int64(apiQueryMaxJitter)))
			select {
			case <-time.After(jitter):
			case <-ctx.Done():
				return nil, ctx.Err()
			}

			flightKey := fmt.Sprintf("usage:%d", accountID)
			result, flightErr, _ := s.cache.apiFlight.Do(flightKey, func() (any, error) {

				if cached, ok := s.cache.apiCache.Load(accountID); ok {
					if cache, ok := cached.(*apiUsageCache); ok {
						age := time.Since(cache.timestamp)
						if cache.err != nil && age < apiErrorCacheTTL {
							return nil, cache.err
						}
						if cache.response != nil && age < apiCacheTTL {
							return cache.response, nil
						}
					}
				}
				resp, fetchErr := s.fetchOAuthUsageRaw(ctx, account)
				if fetchErr != nil {

					s.cache.apiCache.Store(accountID, &apiUsageCache{
						err:       fetchErr,
						timestamp: time.Now(),
					})
					return nil, fetchErr
				}

				s.cache.apiCache.Store(accountID, &apiUsageCache{
					response:  resp,
					timestamp: time.Now(),
				})
				return resp, nil
			})
			if flightErr != nil {
				return nil, flightErr
			}
			apiResp, _ = result.(*ClaudeUsageResponse)
		}

		now := time.Now()
		usage := s.buildUsageInfo(apiResp, &now)

		s.addWindowStats(ctx, account, usage)

		s.syncActiveToPassive(ctx, account, usage)

		if usage.SevenDayFable == nil {
			usage.SevenDayFable = buildPassiveUsageWindow(account.Extra, "passive_usage_7d_oi_utilization", "passive_usage_7d_oi_reset")
		}

		s.tryClearRecoverableAccountError(ctx, account)
		return usage, nil
	}

	if account.Type == AccountTypeSetupToken {
		if supportsAnthropicPassiveUsage(account) {
			return s.getPassiveUsageForAccount(ctx, account)
		}
		usage := s.estimateSetupTokenUsage(account)
		s.addWindowStats(ctx, account, usage)
		return usage, nil
	}

	return nil, fmt.Errorf("account type %s does not support usage query", account.Type)
}

func (s *AccountUsageService) GetUsage(ctx context.Context, accountID int64, force ...bool) (*UsageInfo, error) {
	forceProbe := len(force) > 0 && force[0]

	account, err := s.accountRepo.GetByID(ctx, accountID)
	if err != nil {
		return nil, fmt.Errorf("get account failed: %w", err)
	}

	return s.getUsageForAccount(ctx, account, forceProbe)
}

func (s *AccountUsageService) GetUsageForAccount(ctx context.Context, account *Account, force ...bool) (*UsageInfo, error) {
	forceProbe := len(force) > 0 && force[0]
	return s.getUsageForAccount(ctx, account, forceProbe)
}

func (s *AccountUsageService) GetUsageBatch(ctx context.Context, accountIDs []int64, force bool) (map[int64]*UsageInfo, map[int64]string, error) {
	if adminaccess.RestrictedFromContext(ctx) != nil {
		if len(accountIDs) > 100 {
			return nil, nil, fmt.Errorf("账户数量超过限制")
		}
		for _, id := range accountIDs {
			if id <= 0 {
				return nil, nil, fmt.Errorf("账户 ID 无效")
			}
		}
	}
	uniqueIDs := make([]int64, 0, len(accountIDs))
	seen := make(map[int64]struct{}, len(accountIDs))
	for _, accountID := range accountIDs {
		if accountID <= 0 {
			continue
		}
		if _, ok := seen[accountID]; ok {
			continue
		}
		seen[accountID] = struct{}{}
		uniqueIDs = append(uniqueIDs, accountID)
	}

	usageByAccount := make(map[int64]*UsageInfo, len(uniqueIDs))
	errorsByAccount := make(map[int64]string)
	if len(uniqueIDs) == 0 {
		return usageByAccount, errorsByAccount, nil
	}

	accounts, err := s.accountRepo.GetByIDs(ctx, uniqueIDs)
	if err != nil {
		return nil, nil, fmt.Errorf("get accounts failed: %w", err)
	}

	accountsByID := make(map[int64]*Account, len(accounts))
	for _, account := range accounts {
		if account == nil {
			continue
		}
		accountsByID[account.ID] = account
	}

	if adminaccess.RestrictedFromContext(ctx) != nil {
		for _, id := range uniqueIDs {
			if err := scopedAccountAccess(ctx, s.accountRepo, accountsByID[id], "accounts.usage.read", true); err != nil {
				return nil, nil, err
			}
		}
	}
	var mu sync.Mutex
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(6)

	for _, accountID := range uniqueIDs {
		id := accountID
		account := accountsByID[id]
		if account == nil {
			errorsByAccount[id] = ErrAccountNotFound.Error()
			continue
		}

		g.Go(func() error {
			var usage *UsageInfo
			var usageErr error
			if supportsAnthropicPassiveUsage(account) {
				usage, usageErr = s.getPassiveUsageForAccount(gctx, account)
			} else {
				usage, usageErr = s.getUsageForAccount(gctx, account, force)
			}

			mu.Lock()
			defer mu.Unlock()
			if usageErr != nil {
				errorsByAccount[id] = batchUsageErrorMessage(usageErr)
				return nil
			}
			usageByAccount[id] = usage
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, nil, err
	}

	return usageByAccount, errorsByAccount, nil
}

func (s *AccountUsageService) GetPassiveUsage(ctx context.Context, accountID int64) (*UsageInfo, error) {
	account, err := s.accountRepo.GetByID(ctx, accountID)
	if err != nil {
		return nil, fmt.Errorf("get account failed: %w", err)
	}

	return s.getPassiveUsageForAccount(ctx, account)
}

func (s *AccountUsageService) getPassiveUsageForAccount(ctx context.Context, account *Account) (*UsageInfo, error) {
	if err := scopedAccountAccess(ctx, s.accountRepo, account, "accounts.usage.read", false); err != nil {
		return nil, err
	}
	if !supportsAnthropicPassiveUsage(account) {
		return nil, fmt.Errorf("passive usage only supported for Anthropic OAuth/SetupToken accounts")
	}

	info := s.estimateSetupTokenUsage(account)
	info.Source = "passive"

	if raw, ok := account.Extra["passive_usage_sampled_at"]; ok {
		if str, ok := raw.(string); ok {
			if t, err := time.Parse(time.RFC3339, str); err == nil {
				info.UpdatedAt = &t
			}
		}
	}

	info.SevenDay = buildPassiveUsageWindow(account.Extra, "passive_usage_7d_utilization", "passive_usage_7d_reset")

	info.SevenDayFable = buildPassiveUsageWindow(account.Extra, "passive_usage_7d_oi_utilization", "passive_usage_7d_oi_reset")

	s.addWindowStats(ctx, account, info)
	if account.IsSyntheticUITest() {
		applySyntheticWindowStats(info, account.Extra)
	}

	return info, nil
}

func applySyntheticWindowStats(info *UsageInfo, extra map[string]any) {
	if info == nil || info.FiveHour == nil || len(extra) == 0 {
		return
	}
	raw, ok := extra["synthetic_window_stats"].(map[string]any)
	if !ok {
		return
	}
	info.FiveHour.WindowStats = &WindowStats{
		Requests:     int64(parseExtraInt(raw["requests"])),
		Tokens:       int64(parseExtraInt(raw["tokens"])),
		Cost:         parseExtraFloat64(raw["cost"]),
		StandardCost: parseExtraFloat64(raw["standard_cost"]),
		UserCost:     parseExtraFloat64(raw["user_cost"]),
	}
}

func buildPassiveUsageWindow(extra map[string]any, utilKey, resetKey string) *UsageProgress {
	util := parseExtraFloat64(extra[utilKey])
	resetRaw := parseExtraFloat64(extra[resetKey])
	if util <= 0 && resetRaw <= 0 {
		return nil
	}
	var resetAt *time.Time
	var remaining int
	if resetRaw > 0 {
		t := time.Unix(int64(resetRaw), 0)
		resetAt = &t
		remaining = int(time.Until(t).Seconds())
		if remaining < 0 {
			remaining = 0
		}
	}
	return &UsageProgress{
		Utilization:      util * 100,
		ResetsAt:         resetAt,
		RemainingSeconds: remaining,
	}
}

func (s *AccountUsageService) syncActiveToPassive(ctx context.Context, account *Account, usage *UsageInfo) {
	if account == nil {
		return
	}
	accountID := account.ID
	extraUpdates := make(map[string]any, 4)

	if usage.FiveHour != nil {
		extraUpdates["session_window_utilization"] = usage.FiveHour.Utilization / 100
	}
	if usage.SevenDay != nil {
		extraUpdates["passive_usage_7d_utilization"] = usage.SevenDay.Utilization / 100
		if usage.SevenDay.ResetsAt != nil {
			extraUpdates["passive_usage_7d_reset"] = usage.SevenDay.ResetsAt.Unix()
		}
	}
	if usage.SevenDayFable != nil {
		extraUpdates["passive_usage_7d_oi_utilization"] = usage.SevenDayFable.Utilization / 100
		if usage.SevenDayFable.ResetsAt != nil {
			extraUpdates["passive_usage_7d_oi_reset"] = usage.SevenDayFable.ResetsAt.Unix()
		}
	}

	if len(extraUpdates) > 0 {
		extraUpdates["passive_usage_sampled_at"] = time.Now().UTC().Format(time.RFC3339)
		if err := s.accountRepo.UpdateExtra(ctx, accountID, extraUpdates); err != nil {
			slog.Warn("sync_active_to_passive_failed", "account_id", accountID, "error", err)
		}
	}

	if usage.FiveHour != nil && usage.FiveHour.ResetsAt != nil {
		if err := s.accountRepo.UpdateSessionWindowEnd(ctx, accountID, *usage.FiveHour.ResetsAt); err != nil {
			slog.Warn("sync_active_to_passive_session_window_end_failed", "account_id", accountID, "error", err)
		}
	}

	if usage.SevenDayFable != nil {
		recoverAnthropicFableWindowLimit(ctx, s.accountRepo, account, usage.SevenDayFable.Utilization/100)
	}
}

func (s *AccountUsageService) getOpenAIUsage(ctx context.Context, account *Account, force bool) (*UsageInfo, error) {
	now := time.Now()
	usage := &UsageInfo{Source: "passive"}

	if account == nil {
		return usage, nil
	}

	applyExtraToUsage(usage, account.Extra, now)

	if (force || shouldRefreshOpenAICodexSnapshot(account, usage, now)) && s.shouldProbeOpenAICodexSnapshot(account.ID, now, force) {
		if adminaccess.RestrictedFromContext(ctx) != nil {

			if s.openAIQuotaService != nil {
				if quotaUsage, err := s.openAIQuotaService.QueryUsage(WithScopedAdminAccountOperation(ctx, "accounts.usage.read"), account.ID); err == nil {
					updates := buildOpenAIAutoResetUsageUpdates(quotaUsage, now)
					if account.IsShadow() {
						updates = buildCodexSparkWindowExtraUpdates(quotaUsage, now)
					}
					if updates == nil {
						updates = make(map[string]any)
					}
					updates[openaiQuotaCreditsKey] = newOpenAICreditsSnapshot(quotaUsage.Credits, quotaUsage.FetchedAt, quotaUsage.CreditsObservedAtMs)
					if err := s.openAIQuotaService.CacheCreditsSnapshot(ctx, account.ID, quotaUsage); err != nil {
						slog.Warn("openai_credits_cache_failed", "account_id", account.ID, "error", err)
					}
					if hasOpenAIQuotaObservation(updates) {
						mergeAccountExtra(account, updates)
						s.persistOpenAICodexProbeSnapshotContext(ctx, account.ID, updates)
						applyExtraToUsage(usage, account.Extra, now)
						usage.Source = "active"
					}
				}
			}
		} else if account.IsShadow() {

			if s.openAIQuotaService != nil {
				if quotaUsage, err := s.openAIQuotaService.QueryUsage(WithScopedAdminAccountOperation(ctx, "accounts.usage.read"), account.ID); err == nil {
					if err := s.openAIQuotaService.CacheCreditsSnapshot(ctx, account.ID, quotaUsage); err != nil {
						slog.Warn("openai_credits_cache_failed", "account_id", account.ID, "error", err)
					}
					if updates := buildCodexSparkWindowExtraUpdates(quotaUsage, now); hasOpenAIQuotaObservation(updates) {
						mergeAccountExtra(account, updates)
						s.persistOpenAICodexProbeSnapshotContext(ctx, account.ID, updates)
						if account.ParentAccountID != nil && adminaccess.RestrictedFromContext(ctx) == nil {
							notifyOpenAIAutoReset(*account.ParentAccountID)
						}
						applyExtraToUsage(usage, account.Extra, now)
						usage.Source = "active"
					}
				}
			}
		} else {
			if updates, err := s.probeOpenAICodexSnapshot(ctx, account); err == nil && hasOpenAIQuotaObservation(updates) {
				mergeAccountExtra(account, updates)
				applyExtraToUsage(usage, account.Extra, now)
				usage.Source = "active"
			}
		}
		if usage.Source != "active" {

			usage.ErrorCode = errorCodeQuotaRefreshFailed
			usage.Error = "Unable to refresh OpenAI quota; displayed values may be from an earlier observation"
		}
	}

	if s.usageLogRepo == nil {
		return usage, nil
	}

	if stats, err := s.usageLogRepo.GetAccountWindowStats(ctx, account.ID, codexWindowStatsStart(usage.FiveHour, 5*time.Hour, now)); err == nil {
		if usage.FiveHour == nil {
			usage.FiveHour = &UsageProgress{UtilizationUnknown: true}
		}
		usage.FiveHour.WindowStats = windowStatsFromAccountStats(stats)
	}

	if stats, err := s.usageLogRepo.GetAccountWindowStats(ctx, account.ID, codexWindowStatsStart(usage.SevenDay, 7*24*time.Hour, now)); err == nil {
		if usage.SevenDay == nil {
			usage.SevenDay = &UsageProgress{UtilizationUnknown: true}
		}
		usage.SevenDay.WindowStats = windowStatsFromAccountStats(stats)
	}

	return usage, nil
}

func shouldRefreshOpenAICodexSnapshot(account *Account, usage *UsageInfo, now time.Time) bool {
	if account == nil {
		return false
	}
	if usage == nil {
		return true
	}
	if usage.FiveHour == nil || usage.SevenDay == nil {
		return true
	}
	if account.IsRateLimited() {
		return true
	}
	return isOpenAICodexSnapshotStale(account, now)
}

func isOpenAICodexSnapshotStale(account *Account, now time.Time) bool {
	if account == nil || !account.IsOpenAIOAuth() {
		return false
	}

	if !account.IsShadow() && !account.IsOpenAIResponsesWebSocketV2Enabled() {
		return false
	}
	if account.Extra == nil {
		return true
	}
	raw, ok := account.Extra["codex_usage_updated_at"]
	if !ok {
		return true
	}
	ts, err := parseTime(fmt.Sprint(raw))
	if err != nil {
		return true
	}
	return now.Sub(ts) >= openAIProbeCacheTTL
}

func (s *AccountUsageService) shouldProbeOpenAICodexSnapshot(accountID int64, now time.Time, force ...bool) bool {
	if s == nil || s.cache == nil || accountID <= 0 {
		return true
	}
	forceProbe := len(force) > 0 && force[0]
	if !forceProbe {
		if cached, ok := s.cache.openAIProbeCache.Load(accountID); ok {
			if ts, ok := cached.(time.Time); ok && now.Sub(ts) < openAIProbeCacheTTL {
				return false
			}
		}
	}
	s.cache.openAIProbeCache.Store(accountID, now)
	return true
}

func (s *AccountUsageService) probeOpenAICodexSnapshot(ctx context.Context, account *Account) (map[string]any, error) {
	if account == nil || !account.IsOAuth() {
		return nil, nil
	}
	accessToken := ""
	if !account.IsOpenAIAgentIdentity() {
		accessToken = account.GetOpenAIAccessToken()
	}
	if accessToken == "" && !account.IsOpenAIAgentIdentity() {
		return nil, fmt.Errorf("no access token available")
	}
	modelID := openaipkg.CodexUsageProbeModel
	payload := createOpenAITestPayload(modelID, true)
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal openai probe payload: %w", err)
	}

	reqCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, chatgptCodexURL, bytes.NewReader(payloadBytes))
	if err != nil {
		return nil, fmt.Errorf("create openai probe request: %w", err)
	}
	req.Host = "chatgpt.com"
	req.Header.Set("Content-Type", "application/json")
	if account.IsOpenAIAgentIdentity() {
		authHeaders, authErr := buildAgentIdentityAuthenticationHeaders(ctx, s.accountRepo, s.agentIdentityWS, &s.agentIdentityTaskMu, account)
		if authErr != nil {
			return nil, fmt.Errorf("build Agent Identity authentication: %w", authErr)
		}
		for key, values := range authHeaders {
			for _, value := range values {
				req.Header.Add(key, value)
			}
		}
	} else {
		req.Header.Set("Authorization", "Bearer "+accessToken)
	}
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("OpenAI-Beta", "responses=experimental")
	canonical := resolveCodexOutboundIdentity("")
	req.Header.Set("Originator", canonical.originator)
	req.Header.Set("Version", canonical.version)
	req.Header.Set("User-Agent", canonical.userAgent)
	if s.identityCache != nil {
		if fp, fpErr := s.identityCache.GetFingerprint(reqCtx, account.ID); fpErr == nil && fp != nil && strings.TrimSpace(fp.UserAgent) != "" {
			req.Header.Set("User-Agent", strings.TrimSpace(fp.UserAgent))
		}
	}

	enforceCodexIdentityHeadersWithUA(req.Header, account.GetOpenAIUserAgent())
	setOpenAIChatGPTAccountHeaders(req.Header, account)

	proxyURL := ""
	if account.ProxyID != nil && account.Proxy != nil {
		proxyURL = account.Proxy.URL()
	}
	client, err := httppool.GetClient(httppool.Options{
		ProxyURL:              proxyURL,
		Timeout:               15 * time.Second,
		ResponseHeaderTimeout: 10 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("build openai probe client: %w", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("openai codex probe request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	updates, err := extractOpenAICodexProbeUpdates(resp)
	if err != nil {
		return nil, err
	}
	if hasOpenAIQuotaObservation(updates) {
		s.persistOpenAICodexProbeSnapshotContext(ctx, account.ID, updates)
		return updates, nil
	}
	return nil, nil
}

func (s *AccountUsageService) persistOpenAICodexProbeSnapshot(accountID int64, updates map[string]any) {
	s.persistOpenAICodexProbeSnapshotContext(context.Background(), accountID, updates)
}

func (s *AccountUsageService) persistOpenAICodexProbeSnapshotContext(ctx context.Context, accountID int64, updates map[string]any) {
	if s == nil || s.accountRepo == nil || accountID <= 0 {
		return
	}
	if len(updates) == 0 {
		return
	}

	go func() {
		updateCtx, updateCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer updateCancel()
		if err := scopedAccountIDAccess(updateCtx, s.accountRepo, accountID, "accounts.usage.read", true); err != nil {
			return
		}
		if err := s.accountRepo.UpdateExtra(updateCtx, accountID, updates); err == nil && adminaccess.RestrictedFromContext(ctx) == nil {
			notifyOpenAIAutoReset(accountID)
		}
	}()
}

func extractOpenAICodexProbeUpdates(resp *http.Response) (map[string]any, error) {
	if resp == nil {
		return nil, nil
	}
	if snapshot := ParseCodexRateLimitHeaders(resp.Header); snapshot != nil {
		return buildCodexUsageExtraUpdates(snapshot, time.Now()), nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("openai codex probe returned status %d", resp.StatusCode)
	}
	return nil, nil
}

func mergeAccountExtra(account *Account, updates map[string]any) {
	if account == nil || len(updates) == 0 {
		return
	}
	if account.Extra == nil {
		account.Extra = make(map[string]any, len(updates))
	}
	for k, v := range updates {
		account.Extra[k] = v
	}
}

func applyExtraToUsage(usage *UsageInfo, extra map[string]any, now time.Time) {
	if usage == nil {
		return
	}

	usage.UpdatedAt = nil
	if raw, ok := extra["codex_usage_updated_at"]; ok {
		if observedAt, err := parseTime(fmt.Sprint(raw)); err == nil && !observedAt.IsZero() {
			usage.UpdatedAt = &observedAt
		}
	}
	if progress := buildCodexUsageProgressFromExtra(extra, "5h", now); progress != nil {
		usage.FiveHour = progress
	}
	if progress := buildCodexUsageProgressFromExtra(extra, "7d", now); progress != nil {
		usage.SevenDay = progress
	}
}

const errorCodeQuotaRefreshFailed = "quota_refresh_failed"

func hasOpenAIQuotaObservation(updates map[string]any) bool {
	_, has5h := updates["codex_5h_used_percent"]
	_, has7d := updates["codex_7d_used_percent"]
	return has5h || has7d
}

func (s *AccountUsageService) getGeminiUsage(ctx context.Context, account *Account) (*UsageInfo, error) {
	now := time.Now()
	usage := &UsageInfo{
		UpdatedAt: &now,
	}

	if s.geminiQuotaService == nil || s.usageLogRepo == nil {
		return usage, nil
	}

	quota, ok := s.geminiQuotaService.QuotaForAccount(ctx, account)
	if !ok {
		return usage, nil
	}

	dayStart := geminiDailyWindowStart(now)
	stats, err := s.usageLogRepo.GetModelStatsWithFilters(ctx, dayStart, now, 0, 0, account.ID, 0, nil, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("get gemini usage stats failed: %w", err)
	}

	dayTotals := geminiAggregateUsage(stats)
	dailyResetAt := geminiDailyResetTime(now)

	if quota.SharedRPD > 0 {
		totalReq := dayTotals.ProRequests + dayTotals.FlashRequests
		totalTokens := dayTotals.ProTokens + dayTotals.FlashTokens
		totalCost := dayTotals.ProCost + dayTotals.FlashCost
		usage.GeminiSharedDaily = buildGeminiUsageProgress(totalReq, quota.SharedRPD, dailyResetAt, totalTokens, totalCost, now)
	} else {
		usage.GeminiProDaily = buildGeminiUsageProgress(dayTotals.ProRequests, quota.ProRPD, dailyResetAt, dayTotals.ProTokens, dayTotals.ProCost, now)
		usage.GeminiFlashDaily = buildGeminiUsageProgress(dayTotals.FlashRequests, quota.FlashRPD, dailyResetAt, dayTotals.FlashTokens, dayTotals.FlashCost, now)
	}

	minuteStart := now.Truncate(time.Minute)
	minuteResetAt := minuteStart.Add(time.Minute)
	minuteStats, err := s.usageLogRepo.GetModelStatsWithFilters(ctx, minuteStart, now, 0, 0, account.ID, 0, nil, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("get gemini minute usage stats failed: %w", err)
	}
	minuteTotals := geminiAggregateUsage(minuteStats)

	if quota.SharedRPM > 0 {
		totalReq := minuteTotals.ProRequests + minuteTotals.FlashRequests
		totalTokens := minuteTotals.ProTokens + minuteTotals.FlashTokens
		totalCost := minuteTotals.ProCost + minuteTotals.FlashCost
		usage.GeminiSharedMinute = buildGeminiUsageProgress(totalReq, quota.SharedRPM, minuteResetAt, totalTokens, totalCost, now)
	} else {
		usage.GeminiProMinute = buildGeminiUsageProgress(minuteTotals.ProRequests, quota.ProRPM, minuteResetAt, minuteTotals.ProTokens, minuteTotals.ProCost, now)
		usage.GeminiFlashMinute = buildGeminiUsageProgress(minuteTotals.FlashRequests, quota.FlashRPM, minuteResetAt, minuteTotals.FlashTokens, minuteTotals.FlashCost, now)
	}

	return usage, nil
}

func (s *AccountUsageService) cachedAntigravityUsage(accountID int64, force bool) (*UsageInfo, bool) {
	if force {
		return nil, false
	}
	cached, ok := s.cache.antigravityCache.Load(accountID)
	if !ok {
		return nil, false
	}
	cache, ok := cached.(*antigravityUsageCache)
	if !ok || time.Since(cache.timestamp) >= antigravityCacheTTL(cache.usageInfo) {
		return nil, false
	}
	usage := cache.usageInfo

	recalcAntigravityRemainingSeconds(usage)
	return usage, true
}

func (s *AccountUsageService) getAntigravityUsage(ctx context.Context, account *Account, force bool) (*UsageInfo, error) {
	if s.antigravityQuotaFetcher == nil || !s.antigravityQuotaFetcher.CanFetch(account) {
		now := time.Now()
		return &UsageInfo{UpdatedAt: &now}, nil
	}

	if usage, ok := s.cachedAntigravityUsage(account.ID, force); ok {
		return usage, nil
	}

	flightKey := fmt.Sprintf("ag-usage:%d", account.ID)
	result, flightErr, _ := s.cache.antigravityFlight.Do(flightKey, func() (any, error) {

		if usage, ok := s.cachedAntigravityUsage(account.ID, force); ok {
			return usage, nil
		}

		fetchCtx, fetchCancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer fetchCancel()

		proxyURL := s.antigravityQuotaFetcher.GetProxyURL(fetchCtx, account)
		fetchResult, err := s.antigravityQuotaFetcher.FetchQuota(fetchCtx, account, proxyURL)
		if err != nil {
			degraded := buildAntigravityDegradedUsage(err)
			enrichUsageWithAccountError(degraded, account)
			s.cache.antigravityCache.Store(account.ID, &antigravityUsageCache{
				usageInfo: degraded,
				timestamp: time.Now(),
			})
			return degraded, nil
		}

		enrichUsageWithAccountError(fetchResult.UsageInfo, account)
		s.cache.antigravityCache.Store(account.ID, &antigravityUsageCache{
			usageInfo: fetchResult.UsageInfo,
			timestamp: time.Now(),
		})
		return fetchResult.UsageInfo, nil
	})

	if flightErr != nil {
		return nil, flightErr
	}
	usage, ok := result.(*UsageInfo)
	if !ok || usage == nil {
		now := time.Now()
		return &UsageInfo{UpdatedAt: &now}, nil
	}
	return usage, nil
}

func (s *AccountUsageService) getGrokUsage(ctx context.Context, account *Account, force bool) (*UsageInfo, error) {
	if s.grokQuotaFetcher == nil {
		now := time.Now()
		return &UsageInfo{UpdatedAt: &now}, nil
	}
	var billingProbeResult *GrokQuotaProbeResult
	if account != nil && account.IsGrokOAuth() && s.grokQuotaService != nil && (force || grokBillingSnapshotNeedsRefresh(account, time.Now())) && s.shouldProbeGrokBilling(account.ID, time.Now(), force) {
		result, err := s.grokQuotaService.ProbeBilling(ctx, account.ID)
		if err == nil && result != nil && result.Billing != nil {
			billingProbeResult = result
			mergeAccountExtra(account, map[string]any{grokBillingExtraKey: result.Billing})
		} else if err != nil && force {
			return nil, err
		}
	}
	usage := s.grokQuotaFetcher.BuildUsageInfo(account)
	if usage.GrokQuotaSnapshotState == "" {
		if usage.ErrorCode == "quota_unknown" {
			usage.GrokQuotaSnapshotState = "unknown_until_first_response"
		} else {
			usage.GrokQuotaSnapshotState = "observed"
		}
	}

	if account != nil {
		if s.usageLogRepo != nil {
			if stats, err := s.usageLogRepo.GetAccountTodayStats(ctx, account.ID); err == nil && stats != nil {
				usage.GrokLocalUsage = windowStatsFromAccountStats(stats)
			}
		}
		if billingProbeResult != nil {
			usage.GrokLocalUsage24h = billingProbeResult.LocalUsage24h
			usage.GrokLocalUsage7d = billingProbeResult.LocalUsage7d
			usage.GrokLocalUsageMonthly = billingProbeResult.LocalUsageMonthly
		} else if s.usageLogRepo != nil {
			usage.GrokLocalUsage24h, usage.GrokLocalUsage7d, usage.GrokLocalUsageMonthly = grokLocalUsageForQuota(
				ctx, s.usageLogRepo, account.ID, usage.GrokBilling, time.Now().UTC(),
			)
		}

		if usage.SevenDay != nil && usage.GrokLocalUsage7d != nil {
			usage.SevenDay.WindowStats = usage.GrokLocalUsage7d
		}
		if usage.ThirtyDay != nil && usage.GrokLocalUsageMonthly != nil {
			usage.ThirtyDay.WindowStats = usage.GrokLocalUsageMonthly
		}
	}

	enrichUsageWithAccountError(usage, account)
	return usage, nil
}

func grokLocalUsageForQuota(
	ctx context.Context,
	repo UsageLogRepository,
	accountID int64,
	billing *xai.BillingSummary,
	now time.Time,
) (*WindowStats, *WindowStats, *WindowStats) {
	if grokBillingHasAuthoritativeQuota(billing) {
		weekly, monthly := grokLocalUsageForBilling(ctx, repo, accountID, billing, now)
		return nil, weekly, monthly
	}
	return grokLocalUsage24h(ctx, repo, accountID, now), nil, nil
}

func grokLocalUsage24h(ctx context.Context, repo UsageLogRepository, accountID int64, now time.Time) *WindowStats {
	if repo == nil || accountID <= 0 {
		return nil
	}
	start := now.UTC().Add(-grokFreeQuotaWindow)
	stats, err := repo.GetAccountWindowStats(ctx, accountID, start)
	if err != nil {
		slog.Warn("grok_rolling_24h_usage_query_failed", "account_id", accountID, "window_start", start, "error", err)
		return nil
	}
	return windowStatsFromAccountStats(stats)
}

func grokLocalUsageForBilling(
	ctx context.Context,
	repo UsageLogRepository,
	accountID int64,
	billing *xai.BillingSummary,
	now time.Time,
) (*WindowStats, *WindowStats) {
	var weekly *WindowStats
	var monthly *WindowStats
	if repo == nil || accountID <= 0 {
		return weekly, monthly
	}
	if start, ok := currentGrokBillingWindow(billing, true, now); ok {
		if stats, err := repo.GetAccountWindowStats(ctx, accountID, start); err == nil {
			weekly = windowStatsFromAccountStats(stats)
		} else {
			slog.Warn("grok_window_usage_query_failed", "account_id", accountID, "window_start", start, "error", err)
		}
	}
	if start, ok := currentGrokBillingWindow(billing, false, now); ok {
		if stats, err := repo.GetAccountWindowStats(ctx, accountID, start); err == nil {
			monthly = windowStatsFromAccountStats(stats)
		} else {
			slog.Warn("grok_monthly_usage_query_failed", "account_id", accountID, "window_start", start, "error", err)
		}
	}
	return weekly, monthly
}

func currentGrokBillingWindow(billing *xai.BillingSummary, weekly bool, now time.Time) (time.Time, bool) {
	if billing == nil {
		return time.Time{}, false
	}
	startRaw, endRaw := billing.BillingPeriodStart, billing.BillingPeriodEnd
	if weekly {
		if billing.PeriodType != "weekly" {
			return time.Time{}, false
		}
		startRaw, endRaw = billing.PeriodStart, billing.PeriodEnd
	}
	start, startErr := parseTime(strings.TrimSpace(startRaw))
	end, endErr := parseTime(strings.TrimSpace(endRaw))
	if startErr != nil || endErr != nil || now.Before(start) || !now.Before(end) {
		return time.Time{}, false
	}
	return start, true
}

func grokBillingSnapshotNeedsRefresh(account *Account, now time.Time) bool {
	if account == nil {
		return false
	}
	billing, err := grokBillingSnapshotFromExtra(account.Extra)
	if err != nil || billing == nil || billing.Partial || len(billing.FailedWindows) > 0 {
		return true
	}
	stamp := strings.TrimSpace(billing.UpdatedAt)
	if stamp == "" {
		stamp = strings.TrimSpace(billing.FetchedAt)
	}
	updatedAt, err := parseTime(stamp)
	return err != nil || now.Sub(updatedAt) >= openAIProbeCacheTTL
}

func (s *AccountUsageService) shouldProbeGrokBilling(accountID int64, now time.Time, force bool) bool {
	if force || s == nil || s.cache == nil || accountID <= 0 {
		return true
	}
	if cached, ok := s.cache.grokProbeCache.Load(accountID); ok {
		if ts, ok := cached.(time.Time); ok && now.Sub(ts) < grokProbeRetryTTL {
			return false
		}
	}
	s.cache.grokProbeCache.Store(accountID, now)
	return true
}

func recalcAntigravityRemainingSeconds(info *UsageInfo) {
	if info == nil {
		return
	}
	if info.FiveHour != nil && info.FiveHour.ResetsAt != nil {
		remaining := int(time.Until(*info.FiveHour.ResetsAt).Seconds())
		if remaining < 0 {
			remaining = 0
		}
		info.FiveHour.RemainingSeconds = remaining
	}
}

func antigravityCacheTTL(info *UsageInfo) time.Duration {
	if info == nil {
		return antigravityErrorTTL
	}
	if info.IsForbidden {
		return apiCacheTTL
	}
	if info.ErrorCode != "" || info.Error != "" {
		return antigravityErrorTTL
	}
	return apiCacheTTL
}

func buildAntigravityDegradedUsage(err error) *UsageInfo {
	now := time.Now()
	errMsg := fmt.Sprintf("usage API error: %v", err)
	slog.Warn("antigravity usage fetch failed, returning degraded response", "error", err)

	info := &UsageInfo{
		UpdatedAt: &now,
		Error:     errMsg,
	}

	errStr := err.Error()
	switch {
	case strings.Contains(errStr, "HTTP 401") ||
		strings.Contains(errStr, "UNAUTHENTICATED") ||
		strings.Contains(errStr, "invalid_grant"):
		info.ErrorCode = errorCodeUnauthenticated
		info.NeedsReauth = true
	case strings.Contains(errStr, "HTTP 429"):
		info.ErrorCode = errorCodeRateLimited
	default:
		info.ErrorCode = errorCodeNetworkError
	}

	return info
}

func enrichUsageWithAccountError(info *UsageInfo, account *Account) {
	if info == nil || account == nil || account.Status != StatusError {
		return
	}
	msg := strings.ToLower(account.ErrorMessage)
	if !strings.Contains(msg, "403") && !strings.Contains(msg, "forbidden") &&
		!strings.Contains(msg, "violation") && !strings.Contains(msg, "validation") {
		return
	}
	fbType := classifyForbiddenType(account.ErrorMessage)
	info.IsForbidden = true
	info.ForbiddenType = fbType
	info.ForbiddenReason = account.ErrorMessage
	info.NeedsVerify = fbType == forbiddenTypeValidation
	info.IsBanned = fbType == forbiddenTypeViolation
	info.ValidationURL = extractValidationURL(account.ErrorMessage)
	info.ErrorCode = errorCodeForbidden
	info.NeedsReauth = false
}

func (s *AccountUsageService) addWindowStats(ctx context.Context, account *Account, usage *UsageInfo) {
	if s.usageLogRepo == nil || usage == nil {
		return
	}

	now := time.Now()
	for _, window := range []struct {
		progress *UsageProgress
		duration time.Duration
	}{
		{usage.FiveHour, 5 * time.Hour},
		{usage.SevenDay, 7 * 24 * time.Hour},
	} {
		if window.progress == nil {
			continue
		}

		startTime := account.GetCurrentWindowStartTime()
		if supportsAnthropicPassiveUsage(account) {

			if window.progress.ResetsAt == nil || !now.Before(*window.progress.ResetsAt) {
				continue
			}
			startTime = window.progress.ResetsAt.Add(-window.duration)
			if startTime.After(now) {
				continue
			}
		} else if window.duration != 5*time.Hour {
			continue
		}

		key := windowStatsCacheKey{accountID: account.ID, duration: window.duration}
		if cached, ok := s.cache.windowStatsCache.Load(key); ok {
			if cache, ok := cached.(*windowStatsCache); ok && cache.startTime.Equal(startTime) && now.Sub(cache.timestamp) < windowStatsCacheTTL {
				window.progress.WindowStats = cache.stats
				continue
			}
		}

		stats, err := s.usageLogRepo.GetAccountWindowStats(ctx, account.ID, startTime)
		if err != nil {
			log.Printf("Failed to get window stats for account %d: %v", account.ID, err)
			continue
		}
		if stats == nil {
			continue
		}
		windowStats := windowStatsFromAccountStats(stats)
		window.progress.WindowStats = windowStats
		s.cache.windowStatsCache.Store(key, &windowStatsCache{
			stats:     windowStats,
			timestamp: now,
			startTime: startTime,
		})
	}
}

func (s *AccountUsageService) GetTodayStats(ctx context.Context, accountID int64) (*WindowStats, error) {
	if err := scopedAccountIDAccess(ctx, s.accountRepo, accountID, "accounts.stats.read", false); err != nil {
		return nil, err
	}
	stats, err := s.usageLogRepo.GetAccountTodayStats(ctx, accountID)
	if err != nil {
		return nil, fmt.Errorf("get today stats failed: %w", err)
	}

	return &WindowStats{
		Requests:     stats.Requests,
		Tokens:       stats.Tokens,
		Cost:         stats.Cost,
		StandardCost: stats.StandardCost,
		UserCost:     stats.UserCost,
	}, nil
}

func (s *AccountUsageService) GetTodayStatsBatch(ctx context.Context, accountIDs []int64) (map[int64]*WindowStats, error) {
	if adminaccess.RestrictedFromContext(ctx) != nil {
		if len(accountIDs) > 100 {
			return nil, fmt.Errorf("账户数量超过限制")
		}
		for _, id := range accountIDs {
			if err := scopedAccountIDAccess(ctx, s.accountRepo, id, "accounts.stats.read", false); err != nil {
				return nil, err
			}
		}
	}
	uniqueIDs := make([]int64, 0, len(accountIDs))
	seen := make(map[int64]struct{}, len(accountIDs))
	for _, accountID := range accountIDs {
		if accountID <= 0 {
			continue
		}
		if _, exists := seen[accountID]; exists {
			continue
		}
		seen[accountID] = struct{}{}
		uniqueIDs = append(uniqueIDs, accountID)
	}

	result := make(map[int64]*WindowStats, len(uniqueIDs))
	if len(uniqueIDs) == 0 {
		return result, nil
	}

	startTime := timezone.Today()
	if batchReader, ok := s.usageLogRepo.(accountWindowStatsBatchReader); ok {
		statsByAccount, err := batchReader.GetAccountWindowStatsBatch(ctx, uniqueIDs, startTime)
		if err == nil {
			for _, accountID := range uniqueIDs {
				result[accountID] = windowStatsFromAccountStats(statsByAccount[accountID])
			}
			return result, nil
		}
	}

	var mu sync.Mutex
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(8)

	for _, accountID := range uniqueIDs {
		id := accountID
		g.Go(func() error {
			stats, err := s.usageLogRepo.GetAccountWindowStats(gctx, id, startTime)
			if err != nil {
				return nil
			}
			mu.Lock()
			result[id] = windowStatsFromAccountStats(stats)
			mu.Unlock()
			return nil
		})
	}

	_ = g.Wait()

	for _, accountID := range uniqueIDs {
		if _, ok := result[accountID]; !ok {
			result[accountID] = &WindowStats{}
		}
	}
	return result, nil
}

func windowStatsFromAccountStats(stats *usagestats.AccountStats) *WindowStats {
	if stats == nil {
		return &WindowStats{}
	}
	return &WindowStats{
		Requests:     stats.Requests,
		Tokens:       stats.Tokens,
		Cost:         stats.Cost,
		StandardCost: stats.StandardCost,
		UserCost:     stats.UserCost,
	}
}

func buildCodexUsageProgressFromExtra(extra map[string]any, window string, now time.Time) *UsageProgress {
	if len(extra) == 0 {
		return nil
	}

	var (
		usedPercentKey string
		resetAfterKey  string
		resetAtKey     string
	)

	switch window {
	case "5h":
		usedPercentKey = "codex_5h_used_percent"
		resetAfterKey = "codex_5h_reset_after_seconds"
		resetAtKey = "codex_5h_reset_at"
	case "7d":
		usedPercentKey = "codex_7d_used_percent"
		resetAfterKey = "codex_7d_reset_after_seconds"
		resetAtKey = "codex_7d_reset_at"
	default:
		return nil
	}

	usedRaw, ok := extra[usedPercentKey]
	if !ok {
		return nil
	}

	progress := &UsageProgress{Utilization: parseExtraFloat64(usedRaw)}
	if resetAtRaw, ok := extra[resetAtKey]; ok {
		if resetAt, err := parseTime(fmt.Sprint(resetAtRaw)); err == nil {
			progress.ResetsAt = &resetAt
			progress.RemainingSeconds = int(time.Until(resetAt).Seconds())
			if progress.RemainingSeconds < 0 {
				progress.RemainingSeconds = 0
			}
		}
	}
	if progress.ResetsAt == nil {
		if resetAfterSeconds := parseExtraInt(extra[resetAfterKey]); resetAfterSeconds > 0 {
			base := now
			if updatedAtRaw, ok := extra["codex_usage_updated_at"]; ok {
				if updatedAt, err := parseTime(fmt.Sprint(updatedAtRaw)); err == nil {
					base = updatedAt
				}
			}
			resetAt := base.Add(time.Duration(resetAfterSeconds) * time.Second)
			progress.ResetsAt = &resetAt
			progress.RemainingSeconds = int(time.Until(resetAt).Seconds())
			if progress.RemainingSeconds < 0 {
				progress.RemainingSeconds = 0
			}
		}
	}

	if progress.ResetsAt != nil && !now.Before(*progress.ResetsAt) {
		progress.Utilization = 0
	}

	return progress
}

func codexWindowStatsStart(progress *UsageProgress, fallbackWindow time.Duration, now time.Time) time.Time {
	if progress != nil && progress.ResetsAt != nil && now.Before(*progress.ResetsAt) {
		return progress.ResetsAt.Add(-fallbackWindow)
	}
	return now.Add(-fallbackWindow)
}

func (s *AccountUsageService) GetAccountUsageStats(ctx context.Context, accountID int64, startTime, endTime time.Time) (*usagestats.AccountUsageStatsResponse, error) {
	if err := scopedAccountIDAccess(ctx, s.accountRepo, accountID, "accounts.stats.read", false); err != nil {
		return nil, err
	}
	stats, err := s.usageLogRepo.GetAccountUsageStats(ctx, accountID, startTime, endTime)
	if err != nil {
		return nil, fmt.Errorf("get account usage stats failed: %w", err)
	}
	return stats, nil
}

func (s *AccountUsageService) fetchOAuthUsageRaw(ctx context.Context, account *Account) (*ClaudeUsageResponse, error) {
	accessToken := account.GetCredential("access_token")
	if accessToken == "" {
		return nil, fmt.Errorf("no access token available")
	}

	var proxyURL string
	if account.ProxyID != nil && account.Proxy != nil {
		proxyURL = account.Proxy.URL()
	}

	opts := &ClaudeUsageFetchOptions{
		AccessToken: accessToken,
		ProxyURL:    proxyURL,
		AccountID:   account.ID,
		TLSProfile:  s.tlsFPProfileService.ResolveTLSProfile(account),
	}

	if s.identityCache != nil {
		if fp, err := s.identityCache.GetFingerprint(ctx, account.ID); err == nil && fp != nil {
			opts.Fingerprint = fp
		}
	}

	return s.usageFetcher.FetchUsageWithOptions(ctx, opts)
}

func parseTime(s string) (time.Time, error) {
	formats := []string{
		time.RFC3339,
		time.RFC3339Nano,
		"2006-01-02T15:04:05Z",
		"2006-01-02T15:04:05.000Z",
	}
	for _, format := range formats {
		if t, err := time.Parse(format, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unable to parse time: %s", s)
}

func (s *AccountUsageService) tryClearRecoverableAccountError(ctx context.Context, account *Account) {

	if adminaccess.RestrictedFromContext(ctx) != nil {
		return
	}
	if account == nil || account.Status != StatusError {
		return
	}

	msg := strings.ToLower(strings.TrimSpace(account.ErrorMessage))
	if msg == "" {
		return
	}

	if !strings.Contains(msg, "token refresh failed") &&
		!strings.Contains(msg, "invalid_client") &&
		!strings.Contains(msg, "missing_project_id") &&
		!strings.Contains(msg, "unauthenticated") {
		return
	}

	if err := s.accountRepo.ClearError(ctx, account.ID); err != nil {
		log.Printf("[usage] failed to clear recoverable account error for account %d: %v", account.ID, err)
		return
	}

	account.Status = StatusActive
	account.ErrorMessage = ""
}

func (s *AccountUsageService) buildUsageInfo(resp *ClaudeUsageResponse, updatedAt *time.Time) *UsageInfo {
	info := &UsageInfo{
		UpdatedAt: updatedAt,
	}

	info.FiveHour = &UsageProgress{
		Utilization: resp.FiveHour.Utilization,
	}
	if resp.FiveHour.ResetsAt != "" {
		if fiveHourReset, err := parseTime(resp.FiveHour.ResetsAt); err == nil {
			info.FiveHour.ResetsAt = &fiveHourReset
			info.FiveHour.RemainingSeconds = int(time.Until(fiveHourReset).Seconds())
		} else {
			log.Printf("Failed to parse FiveHour.ResetsAt: %s, error: %v", resp.FiveHour.ResetsAt, err)
		}
	}

	if resp.SevenDay.ResetsAt != "" {
		if sevenDayReset, err := parseTime(resp.SevenDay.ResetsAt); err == nil {
			info.SevenDay = &UsageProgress{
				Utilization:      resp.SevenDay.Utilization,
				ResetsAt:         &sevenDayReset,
				RemainingSeconds: int(time.Until(sevenDayReset).Seconds()),
			}
		} else {
			log.Printf("Failed to parse SevenDay.ResetsAt: %s, error: %v", resp.SevenDay.ResetsAt, err)
			info.SevenDay = &UsageProgress{
				Utilization: resp.SevenDay.Utilization,
			}
		}
	}

	if resp.SevenDaySonnet.ResetsAt != "" {
		if sonnetReset, err := parseTime(resp.SevenDaySonnet.ResetsAt); err == nil {
			info.SevenDaySonnet = &UsageProgress{
				Utilization:      resp.SevenDaySonnet.Utilization,
				ResetsAt:         &sonnetReset,
				RemainingSeconds: int(time.Until(sonnetReset).Seconds()),
			}
		} else {
			log.Printf("Failed to parse SevenDaySonnet.ResetsAt: %s, error: %v", resp.SevenDaySonnet.ResetsAt, err)
			info.SevenDaySonnet = &UsageProgress{
				Utilization: resp.SevenDaySonnet.Utilization,
			}
		}
	}

	fable := resp.SevenDayOverageIncluded
	if fable.ResetsAt == "" {

		for _, limit := range resp.Limits {
			if limit.Kind != "weekly_scoped" || limit.Group != "weekly" ||
				limit.Scope.Model.DisplayName != "Fable" || limit.Percent == nil || *limit.Percent < 0 {
				continue
			}
			if surface := bytes.TrimSpace(limit.Scope.Surface); len(surface) > 0 && !bytes.Equal(surface, []byte("null")) {
				continue
			}
			if _, err := parseTime(limit.ResetsAt); err != nil {
				continue
			}
			fable = ClaudeUsageWindow{Utilization: *limit.Percent, ResetsAt: limit.ResetsAt}
			break
		}
	}
	if fable.ResetsAt != "" {
		if fableReset, err := parseTime(fable.ResetsAt); err == nil {
			info.SevenDayFable = &UsageProgress{
				Utilization:      fable.Utilization,
				ResetsAt:         &fableReset,
				RemainingSeconds: int(time.Until(fableReset).Seconds()),
			}
		} else {
			log.Printf("Failed to parse SevenDayFable.ResetsAt: %s, error: %v", fable.ResetsAt, err)
			info.SevenDayFable = &UsageProgress{
				Utilization: fable.Utilization,
			}
		}
	}

	return info
}

func (s *AccountUsageService) estimateSetupTokenUsage(account *Account) *UsageInfo {
	info := &UsageInfo{}

	if account.SessionWindowEnd != nil {
		remaining := int(time.Until(*account.SessionWindowEnd).Seconds())
		if remaining < 0 {
			remaining = 0
		}

		var utilization float64
		var found bool
		if stored, ok := account.Extra["session_window_utilization"]; ok {
			switch v := stored.(type) {
			case float64:
				utilization = v * 100
				found = true
			case json.Number:
				if f, err := v.Float64(); err == nil {
					utilization = f * 100
					found = true
				}
			}
		}

		if !found {
			switch account.SessionWindowStatus {
			case "rejected":
				utilization = 100.0
			case "allowed_warning":
				utilization = 80.0
			}
		}

		info.FiveHour = &UsageProgress{
			Utilization:      utilization,
			ResetsAt:         account.SessionWindowEnd,
			RemainingSeconds: remaining,
		}

		if info.FiveHour.ResetsAt != nil && !time.Now().Before(*info.FiveHour.ResetsAt) {
			info.FiveHour.Utilization = 0
			info.FiveHour.ResetsAt = nil
			info.FiveHour.RemainingSeconds = 0
		}
	} else {

		info.FiveHour = &UsageProgress{
			Utilization:      0,
			RemainingSeconds: 0,
		}
	}

	return info
}

func buildGeminiUsageProgress(used, limit int64, resetAt time.Time, tokens int64, cost float64, now time.Time) *UsageProgress {

	if limit <= 0 {
		return nil
	}
	utilization := (float64(used) / float64(limit)) * 100
	remainingSeconds := int(resetAt.Sub(now).Seconds())
	if remainingSeconds < 0 {
		remainingSeconds = 0
	}
	resetCopy := resetAt
	return &UsageProgress{
		Utilization:      utilization,
		ResetsAt:         &resetCopy,
		RemainingSeconds: remainingSeconds,
		UsedRequests:     used,
		LimitRequests:    limit,
		WindowStats: &WindowStats{
			Requests: used,
			Tokens:   tokens,
			Cost:     cost,
		},
	}
}

func (s *AccountUsageService) GetAccountWindowStats(ctx context.Context, accountID int64, startTime time.Time) (*usagestats.AccountStats, error) {
	return s.usageLogRepo.GetAccountWindowStats(ctx, accountID, startTime)
}
