package service

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/Wei-Shaw/sub2api/internal/pkg/openai"
	"github.com/Wei-Shaw/sub2api/internal/pkg/timezone"
	"go.uber.org/zap"
)

type OpenAIRecordUsageInput struct {
	Result             *OpenAIForwardResult
	APIKey             *APIKey
	User               *User
	Account            *Account
	Subscription       *UserSubscription
	InboundEndpoint    string
	UpstreamEndpoint   string
	UserAgent          string
	IPAddress          string
	SessionID          string
	RequestPayloadHash string
	APIKeyService      APIKeyQuotaUpdater
	QuotaPlatform      string

	PricingAt time.Time

	CyberBlocked bool

	NativeCompactionV2 bool
	ChannelUsageFields
}

type CyberPolicyUsageInput struct {
	APIKey       *APIKey
	Account      *Account
	Subscription *UserSubscription
	RequestID    string
	Model        string
	Stream       bool
	InputTokens  int
	OutputTokens int

	InboundEndpoint    string
	UpstreamEndpoint   string
	UserAgent          string
	IPAddress          string
	SessionID          string
	RequestPayloadHash string
	APIKeyService      APIKeyQuotaUpdater
	NativeCompactionV2 bool
	ChannelUsageFields
}

func (s *OpenAIGatewayService) RecordCyberPolicyUsageLog(ctx context.Context, in CyberPolicyUsageInput) {
	if s == nil || in.APIKey == nil || in.APIKey.User == nil || in.Account == nil || strings.TrimSpace(in.Model) == "" {
		return
	}
	result := &OpenAIForwardResult{
		RequestID: in.RequestID,
		Model:     in.Model,
		Stream:    in.Stream,
		Usage: OpenAIUsage{
			InputTokens:  in.InputTokens,
			OutputTokens: in.OutputTokens,
		},
	}
	if err := s.RecordUsage(ctx, &OpenAIRecordUsageInput{
		Result:             result,
		APIKey:             in.APIKey,
		User:               in.APIKey.User,
		Account:            in.Account,
		Subscription:       in.Subscription,
		InboundEndpoint:    in.InboundEndpoint,
		UpstreamEndpoint:   in.UpstreamEndpoint,
		UserAgent:          in.UserAgent,
		IPAddress:          in.IPAddress,
		SessionID:          in.SessionID,
		RequestPayloadHash: in.RequestPayloadHash,
		APIKeyService:      in.APIKeyService,
		ChannelUsageFields: in.ChannelUsageFields,
		CyberBlocked:       true,
		NativeCompactionV2: in.NativeCompactionV2,
	}); err != nil {
		logger.LegacyPrintf("service.openai_gateway", "cyber usage record failed: request_id=%s err=%v", in.RequestID, err)
	}
}

func (s *OpenAIGatewayService) ResolveUserGroupRateMultiplier(ctx context.Context, userID, groupID int64, groupDefaultMultiplier float64) float64 {
	if s == nil {
		return groupDefaultMultiplier
	}
	resolver := s.userGroupRateResolver
	if resolver == nil {
		resolver = newUserGroupRateResolver(nil, nil, resolveUserGroupRateCacheTTL(s.cfg), nil, "service.openai_gateway")
	}
	return resolver.Resolve(ctx, userID, groupID, groupDefaultMultiplier)
}

func openAIUsagePricingAt(input *OpenAIRecordUsageInput) time.Time {
	if input != nil && !input.PricingAt.IsZero() {
		return input.PricingAt
	}
	return timezone.Now()
}

func groupBillsOpenAIFastAtStandard(apiKey *APIKey, account *Account, serviceTier string) bool {
	if apiKey == nil || apiKey.Group == nil || !apiKey.Group.FreeOpenAIFast {
		return false
	}
	if account == nil || !account.IsOpenAI() {
		return false
	}
	if !groupSupportsOpenAIFast(apiKey.Group.Platform) {
		return false
	}
	switch normalizeBillingServiceTier(serviceTier) {
	case "priority", "fast":
		return true
	default:
		return false
	}
}

func (usage OpenAIUsage) billingUsageTokens() UsageTokens {
	creationTokens := usage.CacheCreationInputTokens
	if creationTokens <= 0 {
		creationTokens = max(usage.CacheCreation5mTokens, 0) + max(usage.CacheCreation1hTokens, 0)
	}
	tokens := UsageTokens{
		InputTokens:            max(usage.InputTokens-usage.CacheReadInputTokens-creationTokens, 0),
		ImageInputTokens:       max(usage.ImageInputTokens-usage.ImageCacheReadTokens, 0),
		ImageCacheReadTokens:   usage.ImageCacheReadTokens,
		OutputTokens:           usage.OutputTokens,
		CacheCreationTokens:    creationTokens,
		CacheCreation5mTokens:  usage.CacheCreation5mTokens,
		CacheCreation1hTokens:  usage.CacheCreation1hTokens,
		CacheCreation30mTokens: usage.CacheCreation30mInputTokens,
		CacheCreation12hTokens: usage.CacheCreation12hInputTokens,
		CacheReadTokens:        usage.CacheReadInputTokens,
		ImageOutputTokens:      usage.ImageOutputTokens,
	}
	return tokens
}

func (s *OpenAIGatewayService) RecordUsage(ctx context.Context, input *OpenAIRecordUsageInput) error {
	if input == nil {
		return errors.New("openai usage input is nil")
	}
	result := input.Result
	if result == nil {
		return errors.New("openai usage result is nil")
	}
	if s.rateLimitService != nil && input.Account != nil && input.Account.Platform == PlatformOpenAI {
		s.rateLimitService.ResetOpenAI403Counter(ctx, input.Account.ID)
	}

	apiKey := input.APIKey
	user := input.User
	account := input.Account
	subscription := input.Subscription
	billingAccount, err := resolveCredentialAccount(ctx, s.accountRepo, account)
	if err != nil {
		return err
	}
	if !isGrokVideoUsageResult(result, nil) {
		ApplyOpenAIImageBillingResolution(result)
	}
	logServiceTierBillingDowngrade("service.openai_gateway", account, result.RequestID, ApplyOpenAIServiceTierBillingResolution(billingAccount, result))

	tokens := result.Usage.billingUsageTokens()
	actualInputTokens := tokens.InputTokens

	multiplier := 1.0
	if s.cfg != nil {
		multiplier = s.cfg.Default.RateMultiplier
	}
	if apiKey.GroupID != nil && apiKey.Group != nil {
		multiplier = s.ResolveUserGroupRateMultiplier(ctx, user.ID, *apiKey.GroupID, apiKey.Group.RateMultiplier)
	}

	baseMultiplier := multiplier
	pricingAt := openAIUsagePricingAt(input)
	multiplier, imageMultiplier := computePeakAwareMultipliers(apiKey, baseMultiplier, pricingAt)
	videoMultiplier := resolveVideoRateMultiplier(apiKey, baseMultiplier)

	var cost *CostBreakdown
	billingModel := forwardResultBillingModel(result.Model, result.UpstreamModel)
	if result.BillingModel != "" {
		billingModel = strings.TrimSpace(result.BillingModel)
	}
	if input.BillingModelSource == BillingModelSourceChannelMapped && input.ChannelMappedModel != "" && input.ChannelMappedModel != input.OriginalModel {
		billingModel = input.ChannelMappedModel
	}
	if input.BillingModelSource == BillingModelSourceRequested && input.OriginalModel != "" {
		billingModel = input.OriginalModel
	}
	billingModels := usageBillingModelCandidates(
		billingModel,
		result.BillingModel,
		input.ChannelMappedModel,
		input.OriginalModel,
		result.UpstreamModel,
		result.Model,
	)
	billingModels = s.filterCNProviderBillingModelCandidates(ctx, account, apiKey, billingModels)
	serviceTier := ""
	if result.ServiceTier != nil {
		serviceTier = strings.TrimSpace(*result.ServiceTier)
	}
	longContextBillingGate := openAILongContextBillingGate(billingAccount)
	modelLongContextExempt := openAISubscriptionLongContextExempt(billingAccount, result)
	cost, err = s.calculateOpenAIRecordUsageCost(
		ctx,
		result,
		apiKey,
		billingModels,
		multiplier,
		imageMultiplier,
		videoMultiplier,
		baseMultiplier,
		tokens,
		serviceTier,
		longContextBillingGate,
		modelLongContextExempt,
		pricingAt,
	)
	if err != nil {
		if !isUsagePricingUnavailableError(err) {
			return err
		}
		logger.L().With(
			zap.String("component", "service.openai_gateway"),
			zap.Strings("billing_models", billingModels),
			zap.String("requested_model", input.OriginalModel),
			zap.String("mapped_model", input.ChannelMappedModel),
			zap.String("upstream_model", result.UpstreamModel),
			zap.Int64("api_key_id", apiKey.ID),
			zap.Int64("account_id", account.ID),
		).Warn("openai_usage.pricing_missing_record_zero_cost", zap.Error(err))
		cost = &CostBreakdown{BillingMode: string(BillingModeToken)}
	}

	baselineBillingModel := firstUsageBillingModel(billingModels)
	if responseModel := responseModelBillingDeclaration(
		input.BillingModelSource,
		result.UpstreamResponseModel,
		result.UpstreamResponseModelConflict,
		result.ImageCount > 0 || result.VideoCount > 0 || result.WebSearchCalls > 0 ||
			result.AudioUsage != nil || result.SearchCount > 0,
	); responseModel != "" && !strings.EqualFold(responseModel, baselineBillingModel) {
		if identified, responseChannelPriced := s.hasIdentifiedOpenAIResponsePricing(ctx, responseModel, apiKey); identified {
			responseModels := s.filterCNProviderBillingModelCandidates(ctx, account, apiKey, usageBillingModelCandidates(responseModel))
			responseCost, responseErr := s.calculateOpenAIRecordUsageCost(
				ctx, result, apiKey, responseModels, multiplier, imageMultiplier,
				videoMultiplier, baseMultiplier, tokens, serviceTier, longContextBillingGate, modelLongContextExempt, pricingAt,
			)

			baselineChannelPriced := s.resolveOpenAIChannelPricing(ctx, baselineBillingModel, apiKey) != nil
			if responseErr == nil && responseModelBillingAdoptable(cost, responseCost, baselineChannelPriced, responseChannelPriced) {
				logResponseModelBillingApplied("service.openai_gateway", account, result.RequestID,
					baselineBillingModel, responseModel, cost, responseCost)
				billingModels = responseModels
				cost = responseCost
			}
		}
	}

	if groupBillsOpenAIFastAtStandard(apiKey, billingAccount, serviceTier) {
		standardCost, standardErr := s.calculateOpenAIRecordUsageCost(
			ctx,
			result,
			apiKey,
			billingModels,
			multiplier,
			imageMultiplier,
			videoMultiplier,
			baseMultiplier,
			tokens,
			"",
			longContextBillingGate,
			modelLongContextExempt,
			pricingAt,
		)
		if standardErr != nil && !isUsagePricingUnavailableError(standardErr) {
			return standardErr
		}

		if standardErr == nil && cost != nil && standardCost != nil {
			cost.ActualCost = standardCost.ActualCost
		}
	}

	isSubscriptionBilling := subscription != nil && apiKey.Group != nil && apiKey.Group.IsSubscriptionType()
	billingType := BillingTypeBalance
	if isSubscriptionBilling {
		billingType = BillingTypeSubscription
	}

	durationMs := int(result.Duration.Milliseconds())
	accountRateMultiplier := account.BillingRateMultiplier()
	requestID := resolveUsageBillingRequestID(ctx, result.RequestID)
	if result.OpenAIWSMode {
		if upstreamRequestID := strings.TrimSpace(result.RequestID); upstreamRequestID != "" {
			requestID = upstreamRequestID
		}
	}

	if result.VideoCount > 0 {
		if stable := StableGrokVideoBillingRequestID(firstNonEmpty(
			strings.TrimPrefix(strings.TrimSpace(result.RequestID), "grok-video:"),
			strings.TrimSpace(result.ResponseID),
			strings.TrimPrefix(strings.TrimSpace(requestID), "grok-video:"),
		)); stable != "" {
			requestID = stable
		}
	}

	requestedModel := result.Model
	if input.OriginalModel != "" {
		requestedModel = input.OriginalModel
	}
	sentModel := upstreamSentModel(result.Model, result.UpstreamModel)
	if result.UpstreamResponseModelConflict {
		logger.L().Warn("upstream_response_model_conflict",
			zap.String("platform", account.Platform),
			zap.Int64("account_id", account.ID),
			zap.String("request_id", requestID),
			zap.String("sent_model", sentModel),
			zap.String("selected_response_model", strings.TrimSpace(result.UpstreamResponseModel)),
		)
	}

	imageSizeBreakdown := cloneImageSizeBreakdown(result.ImageSizeBreakdown)
	if result.Usage.ImageCacheReadTokens > 0 {
		if imageSizeBreakdown == nil {
			imageSizeBreakdown = make(map[string]int)
		}

		imageSizeBreakdown["image_cache_read_tokens"] = result.Usage.ImageCacheReadTokens
	}
	usageLog := &UsageLog{
		UserID:                   user.ID,
		APIKeyID:                 apiKey.ID,
		AccountID:                account.ID,
		RequestID:                requestID,
		UpstreamRequestID:        usageUpstreamRequestIDPtr(account, result.UpstreamHeaders, result.OpenAIWSMode),
		Model:                    result.Model,
		RequestedModel:           requestedModel,
		UpstreamModel:            optionalTrimmedStringPtr(result.UpstreamModel),
		UpstreamResponseModel:    optionalTrimmedStringPtr(result.UpstreamResponseModel),
		UpstreamModelMismatch:    upstreamModelMismatch(sentModel, result.UpstreamResponseModel),
		ServiceTier:              result.ServiceTier,
		ReasoningEffort:          result.ReasoningEffort,
		RequestedReasoningEffort: coalesceRequestedReasoningEffort(result.RequestedReasoningEffort, result.ReasoningEffort),
		InboundEndpoint:          optionalTrimmedStringPtr(input.InboundEndpoint),
		UpstreamEndpoint:         optionalTrimmedStringPtr(input.UpstreamEndpoint),
		InputTokens:              actualInputTokens,
		OutputTokens:             result.Usage.OutputTokens,
		CacheCreationTokens:      result.Usage.CacheCreationInputTokens,
		CacheCreation5mTokens:    result.Usage.CacheCreation5mTokens,
		CacheCreation1hTokens:    result.Usage.CacheCreation1hTokens,
		CacheCreation30mTokens:   result.Usage.CacheCreation30mInputTokens,
		CacheCreation12hTokens:   result.Usage.CacheCreation12hInputTokens,
		CacheReadTokens:          result.Usage.CacheReadInputTokens,
		ImageInputTokens:         result.Usage.ImageInputTokens,
		ImageOutputTokens:        result.Usage.ImageOutputTokens,
		ImageCount:               result.ImageCount,
		ImageSize:                optionalTrimmedStringPtr(result.ImageSize),
		ImageInputSize:           optionalTrimmedStringPtr(result.ImageInputSize),
		ImageOutputSize:          optionalTrimmedStringPtr(result.ImageOutputSize),
		ImageSizeSource:          optionalTrimmedStringPtr(result.ImageSizeSource),
		ImageSizeBreakdown:       imageSizeBreakdown,
		NativeCompactionV2:       input.NativeCompactionV2,
	}
	isVideoUsage := isGrokVideoUsageResult(result, billingModels)
	if isVideoUsage {
		usageLog.VideoCount = result.VideoCount
		usageLog.VideoResolution = optionalTrimmedStringPtr(NormalizeVideoBillingResolutionOrDefault(result.VideoResolution))
		videoDurationSeconds := NormalizeVideoBillingDurationSecondsOrDefault(result.VideoDurationSeconds)
		usageLog.VideoDurationSeconds = &videoDurationSeconds
	}
	if cost != nil {
		usageLog.InputCost = cost.InputCost
		usageLog.ImageInputCost = cost.ImageInputCost
		usageLog.OutputCost = cost.OutputCost
		usageLog.ImageOutputCost = cost.ImageOutputCost
		usageLog.CacheCreationCost = cost.CacheCreationCost
		usageLog.CacheReadCost = cost.CacheReadCost
		usageLog.TotalCost = cost.TotalCost
		usageLog.ActualCost = cost.ActualCost
		usageLog.LongContextBillingApplied = cost.LongContextBillingApplied
	}
	if isVideoUsage && (cost == nil || cost.BillingMode != string(BillingModeToken)) {
		usageLog.RateMultiplier = videoMultiplier
	} else if result.ImageCount > 0 && (cost == nil || cost.BillingMode != string(BillingModeToken)) {
		usageLog.RateMultiplier = imageMultiplier
	} else {
		usageLog.RateMultiplier = multiplier
	}
	usageLog.AccountRateMultiplier = &accountRateMultiplier
	usageLog.BillingType = billingType
	usageLog.Stream = result.Stream
	if input.CyberBlocked {
		usageLog.RequestType = RequestTypeCyberBlocked
	}
	usageLog.OpenAIWSMode = result.OpenAIWSMode
	usageLog.DurationMs = &durationMs
	usageLog.FirstTokenMs = result.FirstTokenMs
	usageLog.CreatedAt = time.Now()

	usageLog.ChannelID = optionalInt64Ptr(input.ChannelID)
	usageLog.ModelMappingChain = optionalTrimmedStringPtr(input.ModelMappingChain)

	if cost != nil && cost.BillingMode != "" {
		billingMode := cost.BillingMode
		usageLog.BillingMode = &billingMode
	} else if isVideoUsage {
		billingMode := string(BillingModeVideo)
		usageLog.BillingMode = &billingMode
	} else if result.ImageCount > 0 {
		billingMode := string(BillingModeImage)
		usageLog.BillingMode = &billingMode
	} else {
		billingMode := string(BillingModeToken)
		usageLog.BillingMode = &billingMode
	}

	if userAgent := input.UserAgent; userAgent != "" {
		usageLog.UserAgent = &userAgent
	}
	if ipAddress := input.IPAddress; ipAddress != "" {
		usageLog.IPAddress = &ipAddress
	}

	usageLog.SessionID = optionalTrimmedStringPtr(input.SessionID)

	if apiKey.GroupID != nil {
		usageLog.GroupID = apiKey.GroupID
	}
	if subscription != nil {
		usageLog.SubscriptionID = &subscription.ID
	}

	if apiKey.GroupID != nil {
		applyAccountStatsCost(ctx, usageLog, s.channelService, s.billingService,
			account.ID, *apiKey.GroupID, result.UpstreamModel, result.Model,
			tokens, cost.TotalCost, pricingAt,
			accountStatsLongContextPricingEnabled(longContextBillingGate) && !modelLongContextExempt, 1,
		)
	}

	simpleModeKeyRateLimitOnly := simpleModeKeyRateLimitBillingEnabled(s.cfg, apiKey)
	if s.cfg != nil && s.cfg.RunMode == config.RunModeSimple && !simpleModeKeyRateLimitOnly {
		writeUsageLogBestEffort(ctx, s.usageLogRepo, usageLog, "service.openai_gateway")
		logger.LegacyPrintf("service.openai_gateway", "[SIMPLE MODE] Usage recorded (not billed): user=%d, tokens=%d", usageLog.UserID, usageLog.TotalTokens())
		s.deferredService.ScheduleLastUsedUpdate(account.ID)
		return nil
	}

	quotaPlatform := input.QuotaPlatform
	if quotaPlatform == "" {
		quotaPlatform = PlatformFromAPIKey(apiKey)
	}

	_, billingErr := applyUsageBilling(ctx, requestID, usageLog, &postUsageBillingParams{
		Cost:                       cost,
		User:                       user,
		APIKey:                     apiKey,
		Account:                    account,
		Subscription:               subscription,
		RequestPayloadHash:         resolveUsageBillingPayloadFingerprint(ctx, input.RequestPayloadHash),
		IsSubscriptionBill:         isSubscriptionBilling && !simpleModeKeyRateLimitOnly,
		AccountRateMultiplier:      accountRateMultiplier,
		APIKeyService:              input.APIKeyService,
		Platform:                   quotaPlatform,
		SimpleModeKeyRateLimitOnly: simpleModeKeyRateLimitOnly,
	}, s.billingDeps(), s.usageBillingRepo)

	if billingErr != nil {
		usageLog.ActualCost = 0
		writeUsageLogBestEffort(ctx, s.usageLogRepo, usageLog, "service.openai_gateway")
		return billingErr
	}
	writeUsageLogBestEffort(ctx, s.usageLogRepo, usageLog, "service.openai_gateway")

	return nil
}

func (s *OpenAIGatewayService) hasIdentifiedOpenAIResponsePricing(ctx context.Context, model string, apiKey *APIKey) (identified bool, channelPriced bool) {
	model = strings.TrimSpace(model)
	if model == "" {
		return false, false
	}
	if s.resolveOpenAIChannelPricing(ctx, model, apiKey) != nil {
		return true, true
	}
	return s.billingService.HasIdentifiedTokenPricing(model), false
}

func openAILongContextBillingGate(account *Account) *bool {
	if account == nil || !account.IsOpenAI() {
		return nil
	}
	enabled := account.IsOpenAILongContextBillingEnabled()
	return &enabled
}

func openAISubscriptionLongContextExempt(account *Account, result *OpenAIForwardResult) bool {
	if !account.IsOpenAIOAuthLike() || result == nil {
		return false
	}
	model := strings.TrimSpace(result.UpstreamModel)
	if model == "" {
		model = result.Model
	}
	return openai.GPT62ModelID(model) != "" || openai.IsGPT61SolModelSpelling(model) || openai.IsGPT61TerraModelSpelling(model)
}

func (s *OpenAIGatewayService) calculateOpenAIRecordUsageCost(
	ctx context.Context,
	result *OpenAIForwardResult,
	apiKey *APIKey,
	billingModels []string,
	multiplier float64,
	imageMultiplier float64,
	videoMultiplier float64,
	webSearchMultiplier float64,
	tokens UsageTokens,
	serviceTier string,
	longContextBillingGate *bool,
	modelLongContextExempt bool,
	pricingAt time.Time,
) (*CostBreakdown, error) {
	billingModel := firstUsageBillingModel(billingModels)
	if result != nil && result.WebSearchCalls > 0 {

		return s.billingService.CalculateWebSearchCost(result.WebSearchCalls, webSearchPricePerCallFromAPIKey(apiKey), webSearchMultiplier), nil
	}
	if isGrokVideoUsageResult(result, billingModels) {
		if resolved := s.resolveOpenAIChannelPricing(ctx, billingModel, apiKey); resolved == nil || resolved.Mode != BillingModeToken {
			return s.calculateOpenAIVideoCost(ctx, billingModel, apiKey, result, videoMultiplier), nil
		}
	}
	if result != nil && result.AudioUsage != nil {
		if resolved := s.resolveOpenAIChannelPricing(ctx, billingModel, apiKey); resolved != nil &&
			(resolved.Mode == BillingModePerRequest) {
			gid := apiKey.Group.ID
			return s.billingService.CalculateCostUnified(CostInput{
				Ctx: ctx, Model: billingModel, GroupID: &gid, Group: apiKey.Group,
				UsageUnits: result.AudioUsage.DurationOrUnits, SizeTier: result.AudioUsage.Mode,
				RateMultiplier: webSearchMultiplier, Resolver: s.resolver, Resolved: resolved,
				ReasoningEffort: optionalStringValue(result.ReasoningEffort),
			})
		}
		cfg := groupAudioPriceConfigFromAPIKey(apiKey)
		return s.billingService.CalculateAudioCost(result.AudioUsage.Mode, result.AudioUsage.DurationOrUnits, cfg, webSearchMultiplier), nil
	}

	if result != nil && result.ImageCount > 0 {

		if imageCountBillingApplies(apiKey, result.ImageEndpoint) {
			return s.calculateOpenAIImageCost(ctx, billingModel, apiKey, result, imageMultiplier), nil
		}
		if resolved := s.resolveOpenAIChannelPricing(ctx, billingModel, apiKey); resolved == nil || resolved.Mode != BillingModeToken {
			return s.calculateOpenAIImageCost(ctx, billingModel, apiKey, result, imageMultiplier), nil
		}
	}

	var tokenCost *CostBreakdown
	var lastErr error
	if len(billingModels) > 0 && billingModel != "" {
		for _, candidate := range billingModels {
			candidate = strings.TrimSpace(candidate)
			if candidate == "" {
				continue
			}
			cost, err := s.calculateOpenAIRecordUsageTokenCost(
				ctx,
				apiKey,
				candidate,
				multiplier,
				pricingAt,
				tokens,
				serviceTier,
				optionalStringValue(result.ReasoningEffort),
				longContextBillingGate,
				modelLongContextExempt,
			)
			if err == nil {
				tokenCost = cost
				break
			}
			lastErr = err
		}
	}

	searchCost := (*CostBreakdown)(nil)
	if result != nil && result.SearchCount > 0 {
		price := groupSearchPricePer1kFromAPIKey(apiKey)
		if price != nil && *price == 0 {
			logger.L().Info("openai_usage.search_price_per_1k_explicit_free",
				zap.Int("search_count", result.SearchCount),
				zap.String("model", billingModel),
				zap.Int64("api_key_id", apiKey.ID),
				zap.Any("group_id", apiKey.GroupID),
			)
		}
		searchCost = s.billingService.CalculateSearchCost(result.SearchCount, price, webSearchMultiplier)
	}

	tokenBillingAttempted := len(billingModels) > 0 && billingModel != ""
	if tokenCost == nil {
		if tokenBillingAttempted {
			if lastErr == nil {
				lastErr = fmt.Errorf("%w: no non-empty billing model candidates", ErrModelPricingUnavailable)
			}
			return nil, fmt.Errorf("calculate OpenAI usage cost failed for billing models %s: %w", strings.Join(billingModels, ","), lastErr)
		}

		if searchCost != nil {
			return searchCost, nil
		}

		if lastErr == nil {
			lastErr = fmt.Errorf("%w: openai usage billing model is empty", ErrModelPricingUnavailable)
		}
		return nil, fmt.Errorf("calculate OpenAI usage cost failed for billing models %s: %w", strings.Join(billingModels, ","), lastErr)
	}
	if searchCost == nil || (searchCost.TotalCost == 0 && searchCost.ActualCost == 0) {
		return tokenCost, nil
	}

	tokenCost.TotalCost += searchCost.TotalCost
	tokenCost.ActualCost += searchCost.ActualCost
	return tokenCost, nil
}

func isGrokVideoBillingModel(model string) bool {
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(model)), "grok-imagine-video")
}

func isGrokVideoUsageResult(result *OpenAIForwardResult, billingModels []string) bool {
	if result == nil || result.VideoCount <= 0 {
		return false
	}

	candidates := append([]string{}, billingModels...)
	candidates = append(candidates, result.BillingModel, result.Model, result.UpstreamModel)
	for _, candidate := range candidates {
		if isGrokVideoBillingModel(candidate) {
			return true
		}
	}
	return true
}

func isUsagePricingUnavailableError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrModelPricingUnavailable) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "no pricing available") || strings.Contains(msg, "pricing not found")
}

func (s *OpenAIGatewayService) calculateOpenAIRecordUsageTokenCost(
	ctx context.Context,
	apiKey *APIKey,
	billingModel string,
	multiplier float64,
	pricingAt time.Time,
	tokens UsageTokens,
	serviceTier string,
	reasoningEffort string,
	longContextBillingGate *bool,
	modelLongContextExempt bool,
) (*CostBreakdown, error) {
	input := CostInput{
		Ctx: ctx, Model: billingModel, Tokens: tokens, RequestCount: 1, RateMultiplier: multiplier,
		PricingAt: pricingAt, ServiceTier: serviceTier, LongContextBillingEnabled: longContextBillingGate,
		DisableModelLongContextPricing: modelLongContextExempt,
	}
	if s.resolver != nil && apiKey.Group != nil {
		input.GroupID = &apiKey.Group.ID
		input.Group = apiKey.Group
		input.ReasoningEffort = reasoningEffort
		input.Resolver = s.resolver
	}
	return s.billingService.CalculateCostUnified(input)
}

func (s *OpenAIGatewayService) calculateOpenAIImageCost(
	ctx context.Context,
	billingModel string,
	apiKey *APIKey,
	result *OpenAIForwardResult,
	multiplier float64,
) *CostBreakdown {
	sizeTier := NormalizeImageBillingTierOrDefault(result.ImageSize)
	resolved := s.resolveOpenAIChannelPricing(ctx, billingModel, apiKey)
	if resolved != nil && resolved.Source == PricingSourceGroup &&
		(resolved.Mode == BillingModePerRequest || resolved.Mode == BillingModeImage) {
		gid := apiKey.Group.ID
		cost, err := s.billingService.CalculateCostUnified(CostInput{
			Ctx: ctx, Model: billingModel, GroupID: &gid, Group: apiKey.Group,
			RequestCount: result.ImageCount, SizeTier: sizeTier,
			RateMultiplier: multiplier, Resolver: s.resolver, Resolved: resolved,
			ReasoningEffort: optionalStringValue(result.ReasoningEffort),
		})
		if err == nil {
			return cost
		}
	}
	groupConfig := imagePriceConfigFromAPIKey(apiKey)
	if apiKeyHasConfiguredImagePrice(apiKey, sizeTier) {
		return s.billingService.CalculateImageCost(billingModel, sizeTier, result.ImageCount, groupConfig, multiplier)
	}
	if refreshed := s.apiKeyWithFreshGroupMediaPricing(ctx, apiKey); refreshed != apiKey {
		apiKey = refreshed
		groupConfig = imagePriceConfigFromAPIKey(apiKey)
		if apiKeyHasConfiguredImagePrice(apiKey, sizeTier) {
			return s.billingService.CalculateImageCost(billingModel, sizeTier, result.ImageCount, groupConfig, multiplier)
		}
	}
	if resolved != nil && resolved.Source == PricingSourceChannel &&
		(resolved.Mode == BillingModePerRequest || resolved.Mode == BillingModeImage) {
		gid := apiKey.Group.ID
		cost, err := s.billingService.CalculateCostUnified(CostInput{
			Ctx:             ctx,
			Model:           billingModel,
			GroupID:         &gid,
			Group:           apiKey.Group,
			RequestCount:    result.ImageCount,
			SizeTier:        sizeTier,
			ReasoningEffort: optionalStringValue(result.ReasoningEffort),
			RateMultiplier:  multiplier,
			Resolver:        s.resolver,
			Resolved:        resolved,
		})
		if err == nil {
			return cost
		}
		logger.LegacyPrintf("service.openai_gateway", "Calculate image channel cost failed: %v", err)
	}

	return s.billingService.CalculateImageCost(billingModel, sizeTier, result.ImageCount, groupConfig, multiplier)
}

func (s *OpenAIGatewayService) calculateOpenAIVideoCost(
	ctx context.Context,
	billingModel string,
	apiKey *APIKey,
	result *OpenAIForwardResult,
	multiplier float64,
) *CostBreakdown {
	videoCount := result.VideoCount
	if videoCount <= 0 {
		videoCount = 1
	}
	resolution := NormalizeVideoBillingResolutionOrDefault(result.VideoResolution)
	durationSeconds := NormalizeVideoBillingDurationSecondsOrDefault(result.VideoDurationSeconds)
	resolved := s.resolveOpenAIChannelPricing(ctx, billingModel, apiKey)
	if resolved != nil && resolved.Source == PricingSourceGroup && resolved.Mode == BillingModeVideo {
		gid := apiKey.Group.ID
		cost, err := s.billingService.CalculateCostUnified(CostInput{
			Ctx: ctx, Model: billingModel, GroupID: &gid, Group: apiKey.Group,
			UsageUnits: float64(videoCount * durationSeconds), SizeTier: resolution,
			RateMultiplier: multiplier, Resolver: s.resolver, Resolved: resolved,
			ReasoningEffort: optionalStringValue(result.ReasoningEffort),
		})
		if err == nil {
			return cost
		}
	}
	groupConfig := videoPriceConfigFromAPIKey(apiKey)
	if apiKeyHasConfiguredVideoPrice(apiKey, billingModel, resolution) {
		return s.billingService.CalculateVideoCost(billingModel, resolution, videoCount, durationSeconds, groupConfig, multiplier)
	}
	if refreshed := s.apiKeyWithFreshGroupMediaPricing(ctx, apiKey); refreshed != apiKey {
		apiKey = refreshed
		groupConfig = videoPriceConfigFromAPIKey(apiKey)
		if apiKeyHasConfiguredVideoPrice(apiKey, billingModel, resolution) {
			return s.billingService.CalculateVideoCost(billingModel, resolution, videoCount, durationSeconds, groupConfig, multiplier)
		}
	}
	if resolved != nil && resolved.Source == PricingSourceChannel &&
		(resolved.Mode == BillingModePerRequest || resolved.Mode == BillingModeImage || resolved.Mode == BillingModeVideo) {

		gid := apiKey.Group.ID
		units := float64(videoCount)
		if resolved.Mode == BillingModeVideo {
			units = float64(videoCount * durationSeconds)
		}
		cost, err := s.billingService.CalculateCostUnified(CostInput{
			Ctx:             ctx,
			Model:           billingModel,
			GroupID:         &gid,
			Group:           apiKey.Group,
			RequestCount:    videoCount,
			UsageUnits:      units,
			SizeTier:        resolution,
			ReasoningEffort: optionalStringValue(result.ReasoningEffort),
			RateMultiplier:  multiplier,
			Resolver:        s.resolver,
			Resolved:        resolved,
		})
		if err == nil {
			cost.BillingMode = string(BillingModeVideo)
			return cost
		}
		logger.LegacyPrintf("service.openai_gateway", "Calculate video channel cost failed: %v", err)
	}

	return s.billingService.CalculateVideoCost(billingModel, resolution, videoCount, durationSeconds, groupConfig, multiplier)
}

func (s *OpenAIGatewayService) apiKeyWithFreshGroupMediaPricing(ctx context.Context, apiKey *APIKey) *APIKey {
	if apiKey == nil || apiKey.GroupID == nil || *apiKey.GroupID <= 0 {
		return apiKey
	}
	if !groupMediaPricingLooksIncomplete(apiKey.Group) {
		return apiKey
	}
	if s == nil || s.channelService == nil || s.channelService.groupRepo == nil {
		return apiKey
	}
	group, err := s.channelService.groupRepo.GetByIDLite(ctx, *apiKey.GroupID)
	if err != nil || group == nil {
		return apiKey
	}
	clone := *apiKey
	clone.Group = group
	return &clone
}

func groupMediaPricingLooksIncomplete(group *Group) bool {
	if group == nil {
		return true
	}
	if group.ImageRateIndependent || group.VideoRateIndependent {
		return false
	}
	if group.ImageRateMultiplier != 0 || group.VideoRateMultiplier != 0 {
		return false
	}

	if len(group.VideoModelPrices) > 0 {
		return false
	}
	if len(group.ModelPricing) > 0 || group.LongContextPricingEnabled {
		return false
	}
	if group.SearchPricePer1k != nil ||
		group.AudioRealtimePricePerMin != nil ||
		group.AudioTTSPricePerMillionChars != nil ||
		group.AudioSTTPricePerHour != nil ||
		group.WebSearchPricePerCall != nil {
		return false
	}
	return group.ImagePrice1K == nil && group.ImagePrice2K == nil && group.ImagePrice4K == nil &&
		group.VideoPrice480P == nil && group.VideoPrice720P == nil && group.VideoPrice1080P == nil
}

func (s *OpenAIGatewayService) filterCNProviderBillingModelCandidates(ctx context.Context, account *Account, apiKey *APIKey, candidates []string) []string {
	if account == nil || (!account.IsCNProvider() && !account.IsOpenCodeGo()) {
		return candidates
	}
	out := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		trimmed := strings.TrimSpace(candidate)
		if trimmed == "" {
			continue
		}
		if strings.Contains(strings.ToLower(trimmed), "claude") &&
			s.resolveOpenAIChannelPricing(ctx, trimmed, apiKey) == nil {
			continue
		}
		out = append(out, candidate)
	}
	return out
}

func (s *OpenAIGatewayService) resolveOpenAIChannelPricing(ctx context.Context, billingModel string, apiKey *APIKey) *ResolvedPricing {
	if s.resolver == nil || apiKey == nil || apiKey.Group == nil {
		return nil
	}
	gid := apiKey.Group.ID
	resolved := s.resolver.Resolve(ctx, PricingInput{Model: billingModel, GroupID: &gid, Group: apiKey.Group})
	if resolved.Source == PricingSourceGroup || resolved.Source == PricingSourceChannel {
		return resolved
	}
	return nil
}

func ParseCodexRateLimitHeaders(headers http.Header) *OpenAICodexUsageSnapshot {
	snapshot := &OpenAICodexUsageSnapshot{}
	hasData := false
	creditsHeaderPresent := func(key string) bool {
		_, ok := headers[http.CanonicalHeaderKey(key)]
		if ok {
			return true
		}
		for name := range headers {
			if strings.EqualFold(name, key) {
				return true
			}
		}
		return false
	}

	parseFloat := func(key string) *float64 {
		if v := headers.Get(key); v != "" {
			if f, err := strconv.ParseFloat(v, 64); err == nil {
				return &f
			}
		}
		return nil
	}

	parseInt := func(key string) *int {
		if v := headers.Get(key); v != "" {
			if i, err := strconv.Atoi(v); err == nil {
				return &i
			}
		}
		return nil
	}

	if v := parseFloat("x-codex-primary-used-percent"); v != nil {
		snapshot.PrimaryUsedPercent = v
		hasData = true
	}
	if v := parseInt("x-codex-primary-reset-after-seconds"); v != nil {
		snapshot.PrimaryResetAfterSeconds = v
		hasData = true
	}
	if v := parseInt("x-codex-primary-window-minutes"); v != nil {
		snapshot.PrimaryWindowMinutes = v
		hasData = true
	}

	if v := parseFloat("x-codex-secondary-used-percent"); v != nil {
		snapshot.SecondaryUsedPercent = v
		hasData = true
	}
	if v := parseInt("x-codex-secondary-reset-after-seconds"); v != nil {
		snapshot.SecondaryResetAfterSeconds = v
		hasData = true
	}
	if v := parseInt("x-codex-secondary-window-minutes"); v != nil {
		snapshot.SecondaryWindowMinutes = v
		hasData = true
	}

	if v := parseFloat("x-codex-primary-over-secondary-limit-percent"); v != nil {
		snapshot.PrimaryOverSecondaryPercent = v
		hasData = true
	}

	creditHasHeader := creditsHeaderPresent("x-codex-credits-has-credits")
	creditUnlimitedHeader := creditsHeaderPresent("x-codex-credits-unlimited")
	creditBalanceHeader := creditsHeaderPresent("x-codex-credits-balance")
	if creditHasHeader || creditUnlimitedHeader || creditBalanceHeader {
		snapshot.CreditsPresent = true
		snapshot.CreditsObservedAtMs = time.Now().UnixMilli()
		snapshot.CreditsHasCreditsPresent = creditHasHeader
		snapshot.CreditsUnlimitedPresent = creditUnlimitedHeader
		snapshot.CreditsBalancePresent = creditBalanceHeader
		credits := &OpenAICredits{decoded: true, hasCreditsPresent: creditHasHeader}
		if creditHasHeader {
			raw := strings.TrimSpace(headers.Get("x-codex-credits-has-credits"))
			switch strings.ToLower(raw) {
			case "true", "1", "yes":
				credits.HasCredits = true
			case "false", "0", "no":
				credits.HasCredits = false
			default:
				snapshot.CreditsHeaderInvalid = true
			}
		}
		if creditUnlimitedHeader {
			raw := strings.TrimSpace(headers.Get("x-codex-credits-unlimited"))
			switch strings.ToLower(raw) {
			case "true", "1", "yes":
				credits.Unlimited = true
			case "false", "0", "no":
				credits.Unlimited = false
			default:
				snapshot.CreditsHeaderInvalid = true
			}
		}
		if creditBalanceHeader {
			raw := strings.TrimSpace(headers.Get("x-codex-credits-balance"))
			if raw != "" {
				credits.Balance = &raw
			} else {
				snapshot.CreditsHeaderInvalid = true
			}
		}
		credits.invalid = snapshot.CreditsHeaderInvalid
		snapshot.Credits = credits
		hasData = true
	}

	if !hasData {
		return nil
	}

	snapshot.UpdatedAt = time.Now().Format(time.RFC3339)
	return snapshot
}

func codexSnapshotBaseTime(snapshot *OpenAICodexUsageSnapshot, fallback time.Time) time.Time {
	if snapshot == nil {
		return fallback
	}
	if snapshot.UpdatedAt == "" {
		return fallback
	}
	base, err := time.Parse(time.RFC3339, snapshot.UpdatedAt)
	if err != nil {
		return fallback
	}
	return base
}

func codexResetAtRFC3339(base time.Time, resetAfterSeconds *int) *string {
	if resetAfterSeconds == nil {
		return nil
	}
	sec := *resetAfterSeconds
	if sec < 0 {
		sec = 0
	}
	resetAt := base.Add(time.Duration(sec) * time.Second).Format(time.RFC3339)
	return &resetAt
}

func buildCodexUsageExtraUpdates(snapshot *OpenAICodexUsageSnapshot, fallbackNow time.Time) map[string]any {
	if snapshot == nil {
		return nil
	}

	baseTime := codexSnapshotBaseTime(snapshot, fallbackNow)
	updates := make(map[string]any)

	if snapshot.PrimaryUsedPercent != nil {
		updates["codex_primary_used_percent"] = *snapshot.PrimaryUsedPercent
	}
	if snapshot.PrimaryResetAfterSeconds != nil {
		updates["codex_primary_reset_after_seconds"] = *snapshot.PrimaryResetAfterSeconds
	}
	if snapshot.PrimaryWindowMinutes != nil {
		updates["codex_primary_window_minutes"] = *snapshot.PrimaryWindowMinutes
	}
	if snapshot.SecondaryUsedPercent != nil {
		updates["codex_secondary_used_percent"] = *snapshot.SecondaryUsedPercent
	}
	if snapshot.SecondaryResetAfterSeconds != nil {
		updates["codex_secondary_reset_after_seconds"] = *snapshot.SecondaryResetAfterSeconds
	}
	if snapshot.SecondaryWindowMinutes != nil {
		updates["codex_secondary_window_minutes"] = *snapshot.SecondaryWindowMinutes
	}
	if snapshot.PrimaryOverSecondaryPercent != nil {
		updates["codex_primary_over_secondary_percent"] = *snapshot.PrimaryOverSecondaryPercent
	}
	if len(updates) > 0 {
		updates["codex_usage_updated_at"] = baseTime.Format(time.RFC3339)
	}
	if snapshot.CreditsPresent {
		observedAtMs := snapshot.CreditsObservedAtMs
		if observedAtMs <= 0 {
			observedAtMs = baseTime.UnixMilli()
		}
		updates[openaiQuotaCreditsKey] = newOpenAICreditsSnapshot(snapshot.Credits, baseTime.Unix(), observedAtMs)
	}

	if normalized := snapshot.Normalize(); normalized != nil {
		if normalized.Used5hPercent != nil {
			updates["codex_5h_used_percent"] = *normalized.Used5hPercent
		}
		if normalized.Reset5hSeconds != nil {
			updates["codex_5h_reset_after_seconds"] = *normalized.Reset5hSeconds
		}
		if normalized.Window5hMinutes != nil {
			updates["codex_5h_window_minutes"] = *normalized.Window5hMinutes
		}
		if normalized.Used7dPercent != nil {
			updates["codex_7d_used_percent"] = *normalized.Used7dPercent
		}
		if normalized.Reset7dSeconds != nil {
			updates["codex_7d_reset_after_seconds"] = *normalized.Reset7dSeconds
		}
		if normalized.Window7dMinutes != nil {
			updates["codex_7d_window_minutes"] = *normalized.Window7dMinutes
		}
		if reset5hAt := codexResetAtRFC3339(baseTime, normalized.Reset5hSeconds); reset5hAt != nil {
			updates["codex_5h_reset_at"] = *reset5hAt
		}
		if reset7dAt := codexResetAtRFC3339(baseTime, normalized.Reset7dSeconds); reset7dAt != nil {
			updates["codex_7d_reset_at"] = *reset7dAt
		}
	}

	return updates
}

func (s *OpenAIGatewayService) updateCodexUsageSnapshot(ctx context.Context, accountID int64, snapshot *OpenAICodexUsageSnapshot) {
	if snapshot == nil {
		return
	}
	if s == nil || s.accountRepo == nil {
		return
	}

	now := time.Now()
	updates := buildCodexUsageExtraUpdates(snapshot, now)
	if len(updates) == 0 {
		return
	}
	var credits *openAICreditsSnapshot
	if value, ok := updates[openaiQuotaCreditsKey].(openAICreditsSnapshot); ok {
		credits = &value
	}
	if !s.getCodexSnapshotThrottle().Allow(accountID, now, credits) {
		return
	}

	go func() {
		updateCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.accountRepo.UpdateExtra(updateCtx, accountID, updates); err == nil {
			notifyOpenAIAutoReset(accountID)
		}
	}()
}

func (s *OpenAIGatewayService) UpdateCodexUsageSnapshotFromHeaders(ctx context.Context, accountID int64, headers http.Header) {
	if accountID <= 0 || headers == nil {
		return
	}
	if snapshot := ParseCodexRateLimitHeaders(headers); snapshot != nil {
		s.updateCodexUsageSnapshot(ctx, accountID, snapshot)
	}
}

func cloneImageSizeBreakdown(input map[string]int) map[string]int {
	if len(input) == 0 {
		return nil
	}
	output := make(map[string]int, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}
