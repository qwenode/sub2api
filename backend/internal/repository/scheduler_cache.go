package repository

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/redis/go-redis/v9"
	"github.com/tidwall/gjson"
	"golang.org/x/sync/singleflight"
)

const (
	schedulerBucketSetKey          = "sched:buckets"
	schedulerOutboxWatermarkKey    = "sched:outbox:watermark"
	schedulerAccountPrefix         = "sched:acc:"
	schedulerAccountMetaPrefix     = "sched:meta:"
	schedulerAccountLastUsedPrefix = "sched:acc:last_used:"
	schedulerActivePrefix          = "sched:active:"
	schedulerReadyPrefix           = "sched:ready:"
	schedulerVersionPrefix         = "sched:ver:"
	schedulerEpochPrefix           = "sched:epoch:"
	schedulerRetiredPrefix         = "sched:retired:"
	schedulerSnapshotPrefix        = "sched:"
	schedulerLockPrefix            = "sched:lock:"

	schedulerAccountMetaRevisionKey            = "sched:meta:rev"
	schedulerAccountMetaPlatformRevisionPrefix = "sched:meta:rev:"

	defaultSchedulerSnapshotMGetChunkSize  = 128
	defaultSchedulerSnapshotWriteChunkSize = 256
	schedulerLastUsedUpdateChunkSize       = 256

	snapshotGraceTTLSeconds = 60

	schedulerSnapshotCacheMaxAge = 10 * time.Second

	schedulerSnapshotCacheStaleGrace = 20 * time.Second

	schedulerSnapshotCacheEvictAge = 10 * schedulerSnapshotCacheMaxAge

	schedulerLastUsedCacheMaxAge = 200 * time.Millisecond

	schedulerSharedLoadTimeout = 10 * time.Second

	schedulerAccountMetaRevisionBytes        = 16
	schedulerAccountMetaRevisionWriteTimeout = 3 * time.Second
)

const (
	schedulerGroupLifecycleLockPrefix      = "sched:group:lifecycle-lock:"
	schedulerGroupLifecycleOwnerTokenBytes = 16
)

var updateSchedulerLastUsedScript = redis.NewScript(`
local updated = 0
for index = 1, #ARGV do
    local key_index = (index - 1) * 2 + 1
    local candidate = tonumber(ARGV[index])
    if candidate == nil then
        return redis.error_reply('invalid last_used value')
    end
    if redis.call('EXISTS', KEYS[key_index]) == 1 then
        local current = tonumber(redis.call('GET', KEYS[key_index + 1]))
        if current == nil or candidate > current then
            redis.call('SET', KEYS[key_index + 1], ARGV[index])
            updated = updated + 1
        end
    end
end
return updated
`)

var (
	captureBucketWriteTokenScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[2]) == 1 then
    return -1
end

local currentEpoch = redis.call('GET', KEYS[1])
if currentEpoch == false then
    redis.call('SET', KEYS[1], '1')
    return 1
end

local parsedEpoch = tonumber(currentEpoch)
if parsedEpoch == nil or parsedEpoch < 1 then
    return -2
end
return parsedEpoch
`)

	allocateSnapshotVersionScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[2]) == 1 then
    return -1
end

local currentEpoch = tonumber(redis.call('GET', KEYS[1]))
local expectedEpoch = tonumber(ARGV[1])
if currentEpoch == nil or expectedEpoch == nil or currentEpoch ~= expectedEpoch then
    return -2
end

return redis.call('INCR', KEYS[3])
`)

	retireBucketScript = redis.NewScript(`
local retired = redis.call('GET', KEYS[2])
local currentEpoch = tonumber(redis.call('GET', KEYS[1])) or 0

if retired == false then
    currentEpoch = currentEpoch + 1
    if currentEpoch < 1 then
        currentEpoch = 1
    end
    redis.call('SET', KEYS[1], tostring(currentEpoch))
    redis.call('SET', KEYS[2], tostring(currentEpoch))
elseif currentEpoch < 1 then
    currentEpoch = tonumber(retired) or 1
    redis.call('SET', KEYS[1], tostring(currentEpoch))
end

redis.call('SREM', KEYS[3], ARGV[1])
local currentActive = redis.call('GET', KEYS[5])
if currentActive ~= false then
    redis.call('EXPIRE', ARGV[2] .. currentActive, tonumber(ARGV[3]))
end
redis.call('DEL', KEYS[4], KEYS[5])
return currentEpoch
`)

	reopenBucketScript = redis.NewScript(`
local currentEpochRaw = redis.call('GET', KEYS[1])
local currentEpoch = tonumber(currentEpochRaw)
local retiredEpochRaw = redis.call('GET', KEYS[2])

if retiredEpochRaw == false then
    if currentEpochRaw == false then
        redis.call('SET', KEYS[1], '1')
        return 1
    end
    if currentEpoch == nil or currentEpoch < 1 then
        return -2
    end
    return currentEpoch
end

local retiredEpoch = tonumber(retiredEpochRaw)
if retiredEpoch == nil or retiredEpoch < 1 then
    return -2
end
if currentEpoch == nil or currentEpoch < retiredEpoch then
    currentEpoch = retiredEpoch
end

redis.call('SET', KEYS[1], tostring(currentEpoch))
redis.call('DEL', KEYS[2])
redis.call('SREM', KEYS[3], ARGV[1])
local currentActive = redis.call('GET', KEYS[5])
if currentActive ~= false then
    redis.call('EXPIRE', ARGV[2] .. currentActive, tonumber(ARGV[3]))
end
redis.call('DEL', KEYS[4], KEYS[5])
return currentEpoch
`)

	releaseGroupLifecycleLeaseScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
`)

	activateSnapshotScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[6]) == 1 then
    redis.call('DEL', KEYS[4])
    return -1
end

local currentEpoch = tonumber(redis.call('GET', KEYS[5]))
local expectedEpoch = tonumber(ARGV[5])
if currentEpoch == nil or expectedEpoch == nil or currentEpoch ~= expectedEpoch then
    redis.call('DEL', KEYS[4])
    return -2
end

local currentActive = redis.call('GET', KEYS[1])
local newVersion = tonumber(ARGV[1])

if currentActive ~= false then
	local curVersion = tonumber(currentActive)
	if curVersion and newVersion < curVersion then
		redis.call('DEL', KEYS[4])
		return 0
	end
end

redis.call('SET', KEYS[1], ARGV[1])
redis.call('SET', KEYS[2], '1')
redis.call('SADD', KEYS[3], ARGV[2])

if currentActive ~= false and currentActive ~= ARGV[1] then
	redis.call('EXPIRE', ARGV[3] .. currentActive, tonumber(ARGV[4]))
end

return 1
`)
)

type schedulerCache struct {
	rdb            *redis.Client
	mgetChunkSize  int
	writeChunkSize int

	now func() time.Time

	snapshotMu      sync.RWMutex
	snapshotEntries map[string]*schedulerSnapshotEntry

	snapshotLoads singleflight.Group

	lastUsedGeneration atomic.Uint64

	globalRevisionPending atomic.Bool
	globalRevisionWriting atomic.Bool
}

type schedulerSnapshotEntry struct {
	active       string
	revisions    []string
	loadedAt     time.Time
	ids          []string
	accounts     []*service.Account
	payloads     []string
	lastUsedKeys []string

	lastUsed      atomic.Pointer[schedulerLastUsedValues]
	lastUsedLoads singleflight.Group

	refreshing atomic.Bool
}

type schedulerLastUsedValues struct {
	fetchedAt  time.Time
	generation uint64
	values     []*time.Time
}

func (v *schedulerLastUsedValues) freshAt(now time.Time, generation uint64) bool {
	if v == nil || v.generation != generation {
		return false
	}
	age := now.Sub(v.fetchedAt)
	return age >= 0 && age < schedulerLastUsedCacheMaxAge
}

func NewSchedulerCache(rdb *redis.Client) service.SchedulerCache {
	return newSchedulerCacheWithChunkSizes(rdb, defaultSchedulerSnapshotMGetChunkSize, defaultSchedulerSnapshotWriteChunkSize)
}

func newSchedulerCacheWithChunkSizes(rdb *redis.Client, mgetChunkSize, writeChunkSize int) service.SchedulerCache {
	if mgetChunkSize <= 0 {
		mgetChunkSize = defaultSchedulerSnapshotMGetChunkSize
	}
	if writeChunkSize <= 0 {
		writeChunkSize = defaultSchedulerSnapshotWriteChunkSize
	}
	return &schedulerCache{
		rdb:             rdb,
		mgetChunkSize:   mgetChunkSize,
		writeChunkSize:  writeChunkSize,
		now:             time.Now,
		snapshotEntries: make(map[string]*schedulerSnapshotEntry),
	}
}

func (c *schedulerCache) GetSnapshot(ctx context.Context, bucket service.SchedulerBucket) ([]*service.Account, bool, error) {
	values, hit, err := c.GetSnapshotValues(ctx, bucket)
	if err != nil || !hit {
		return nil, false, err
	}
	accounts := make([]*service.Account, len(values))
	for i := range values {
		accounts[i] = &values[i]
	}
	return accounts, true, nil
}

func (c *schedulerCache) GetSnapshotValues(ctx context.Context, bucket service.SchedulerBucket) ([]service.Account, bool, error) {
	entry, err := c.snapshotEntry(ctx, bucket)
	if err != nil {
		return nil, false, err
	}
	if entry == nil {
		return nil, false, nil
	}
	accounts, err := c.snapshotAccountsFromEntry(ctx, entry)
	if err != nil {
		return nil, false, err
	}
	return accounts, true, nil
}

func (c *schedulerCache) snapshotEntry(ctx context.Context, bucket service.SchedulerBucket) (*schedulerSnapshotEntry, error) {
	bucketKey := bucket.String()
	tagKeys := schedulerSnapshotTagKeys(bucket)
	tags, err := c.rdb.MGet(ctx, tagKeys...).Result()
	if err != nil {
		return nil, err
	}
	if len(tags) != len(tagKeys) {
		return nil, fmt.Errorf("unexpected scheduler snapshot tag count: got %d, want %d", len(tags), len(tagKeys))
	}
	readyVal, err := schedulerCacheOptionalString(tags[0])
	if err != nil {
		return nil, err
	}
	activeVal, err := schedulerCacheOptionalString(tags[1])
	if err != nil {
		return nil, err
	}

	revisions := make([]string, 0, len(tags)-2)
	for _, tag := range tags[2:] {
		revision, err := schedulerCacheOptionalString(tag)
		if err != nil {
			return nil, err
		}
		revisions = append(revisions, revision)
	}
	if readyVal != "1" || activeVal == "" {
		c.dropSnapshotEntry(bucketKey)
		return nil, nil
	}

	now := c.now()
	if entry := c.lookupSnapshotEntry(bucketKey); entry != nil && entry.sameVersion(activeVal, revisions) {
		age := now.Sub(entry.loadedAt)
		if age >= 0 && age < schedulerSnapshotCacheMaxAge {
			return entry, nil
		}
		if age >= 0 && age < schedulerSnapshotCacheMaxAge+schedulerSnapshotCacheStaleGrace {
			if entry.refreshing.CompareAndSwap(false, true) {
				results := c.loadSnapshotEntryShared(ctx, bucket, bucketKey, activeVal, revisions, now)
				go func() {
					if result := <-results; result.Err != nil {
						slog.Warn("scheduler snapshot background reload failed", "bucket", bucketKey, "error", result.Err)
					}
					entry.refreshing.Store(false)
				}()
			}
			return entry, nil
		}
	}

	select {
	case result := <-c.loadSnapshotEntryShared(ctx, bucket, bucketKey, activeVal, revisions, now):
		if result.Err != nil {
			return nil, result.Err
		}
		entry, _ := result.Val.(*schedulerSnapshotEntry)
		return entry, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *schedulerCache) loadSnapshotEntryShared(ctx context.Context, bucket service.SchedulerBucket, bucketKey, active string, revisions []string, now time.Time) <-chan singleflight.Result {
	key := bucketKey + "\x00" + active + "\x00" + strings.Join(revisions, "\x00")
	loadCtx := context.WithoutCancel(ctx)
	return c.snapshotLoads.DoChan(key, recoverSchedulerSharedLoad("scheduler snapshot load", func() (any, error) {
		ctx, cancel := context.WithTimeout(loadCtx, schedulerSharedLoadTimeout)
		defer cancel()
		entry, err := c.loadSnapshotEntry(ctx, bucket, bucketKey, active, revisions, now)
		if err != nil || entry == nil {
			return nil, err
		}
		return entry, nil
	}))
}

func (c *schedulerCache) loadSnapshotEntry(ctx context.Context, bucket service.SchedulerBucket, bucketKey, active string, revisions []string, now time.Time) (*schedulerSnapshotEntry, error) {
	previous := c.lookupSnapshotEntry(bucketKey)
	if previous != nil && previous.matches(active, revisions, c.now()) {
		return previous, nil
	}

	ids, err := c.rdb.ZRange(ctx, schedulerSnapshotKey(bucket, active), 0, -1).Result()
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {

		c.dropSnapshotEntry(bucketKey)
		return nil, nil
	}

	keys := make([]string, 0, len(ids))
	lastUsedKeys := make([]string, 0, len(ids))
	for _, id := range ids {
		keys = append(keys, schedulerAccountMetaKey(id))
		lastUsedKeys = append(lastUsedKeys, schedulerLastUsedKey(id))
	}
	values, err := c.mgetChunked(ctx, keys)
	if err != nil {
		return nil, err
	}
	if len(values) != len(ids) {
		return nil, fmt.Errorf("unexpected scheduler account metadata count: got %d, want %d", len(values), len(ids))
	}

	reusable := previous.masterIndex()
	masters := make([]*service.Account, len(values))
	payloads := make([]string, len(values))
	for i, val := range values {
		if val == nil {
			c.dropSnapshotEntry(bucketKey)
			return nil, nil
		}
		payload, err := schedulerCachePayload(val)
		if err != nil {
			return nil, err
		}
		if j, ok := reusable[ids[i]]; ok && previous.payloads[j] == payload && previous.accounts[j].SharedSnapshotCurrent() {
			masters[i] = previous.accounts[j]
			payloads[i] = previous.payloads[j]
			continue
		}
		account, err := decodeCachedAccountPayload(payload)
		if err != nil {
			return nil, err
		}

		account.PrepareSharedSnapshot()
		masters[i] = account
		payloads[i] = payload
	}

	entry := &schedulerSnapshotEntry{
		active:       active,
		revisions:    revisions,
		loadedAt:     now,
		ids:          ids,
		accounts:     masters,
		payloads:     payloads,
		lastUsedKeys: lastUsedKeys,
	}
	c.storeSnapshotEntry(bucketKey, entry)
	return entry, nil
}

func (e *schedulerSnapshotEntry) sameVersion(active string, revisions []string) bool {
	return e.active == active && slices.Equal(e.revisions, revisions)
}

func (e *schedulerSnapshotEntry) matches(active string, revisions []string, now time.Time) bool {
	if !e.sameVersion(active, revisions) {
		return false
	}
	age := now.Sub(e.loadedAt)
	return age >= 0 && age < schedulerSnapshotCacheMaxAge
}

func (e *schedulerSnapshotEntry) masterIndex() map[string]int {
	if e == nil || len(e.ids) == 0 || len(e.ids) != len(e.accounts) || len(e.ids) != len(e.payloads) {
		return nil
	}
	index := make(map[string]int, len(e.ids))
	for i, id := range e.ids {
		index[id] = i
	}
	return index
}

func (c *schedulerCache) snapshotAccountsFromEntry(ctx context.Context, entry *schedulerSnapshotEntry) ([]service.Account, error) {
	lastUsed, err := c.entryLastUsed(ctx, entry)
	if err != nil {
		return nil, err
	}
	accounts := make([]service.Account, len(entry.accounts))
	for i, master := range entry.accounts {
		accounts[i] = *master
		if lastUsed[i] != nil {
			accounts[i].LastUsedAt = lastUsed[i]
		}
	}
	return accounts, nil
}

func (c *schedulerCache) entryLastUsed(ctx context.Context, entry *schedulerSnapshotEntry) ([]*time.Time, error) {
	generation := c.lastUsedGeneration.Load()
	if current := entry.lastUsed.Load(); current.freshAt(c.now(), generation) {
		return current.values, nil
	}
	loadCtx := context.WithoutCancel(ctx)
	results := entry.lastUsedLoads.DoChan(strconv.FormatUint(generation, 10), recoverSchedulerSharedLoad("scheduler last_used load", func() (any, error) {
		fetchedAt := c.now()
		if current := entry.lastUsed.Load(); current.freshAt(fetchedAt, generation) {
			return current, nil
		}
		ctx, cancel := context.WithTimeout(loadCtx, schedulerSharedLoadTimeout)
		defer cancel()
		raw, err := c.mgetChunked(ctx, entry.lastUsedKeys)
		if err != nil {
			return nil, err
		}
		if len(raw) != len(entry.accounts) {
			return nil, fmt.Errorf("unexpected scheduler last_used value count: got %d, want %d", len(raw), len(entry.accounts))
		}
		values := make([]*time.Time, len(raw))
		for i, value := range raw {
			lastUsedAt, err := parseSchedulerLastUsed(value)
			if err != nil {
				return nil, err
			}
			if lastUsedAt != nil && (entry.accounts[i].LastUsedAt == nil || lastUsedAt.After(*entry.accounts[i].LastUsedAt)) {
				values[i] = lastUsedAt
			}
		}
		loaded := &schedulerLastUsedValues{fetchedAt: fetchedAt, generation: generation, values: values}
		entry.lastUsed.Store(loaded)
		return loaded, nil
	}))
	select {
	case result := <-results:
		if result.Err != nil {
			return nil, result.Err
		}
		loaded, _ := result.Val.(*schedulerLastUsedValues)
		if loaded == nil {
			return nil, errors.New("scheduler last_used load returned no values")
		}
		return loaded.values, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func recoverSchedulerSharedLoad(name string, load func() (any, error)) func() (any, error) {
	return func() (result any, err error) {
		defer func() {
			if recovered := recover(); recovered != nil {
				slog.Error("scheduler shared load panicked", "load", name, "panic", recovered, "stack", string(debug.Stack()))
				result, err = nil, fmt.Errorf("%s panicked: %v", name, recovered)
			}
		}()
		return load()
	}
}

func (c *schedulerCache) lookupSnapshotEntry(bucketKey string) *schedulerSnapshotEntry {
	c.snapshotMu.RLock()
	defer c.snapshotMu.RUnlock()
	return c.snapshotEntries[bucketKey]
}

func (c *schedulerCache) storeSnapshotEntry(bucketKey string, entry *schedulerSnapshotEntry) {
	c.snapshotMu.Lock()
	defer c.snapshotMu.Unlock()
	if current := c.snapshotEntries[bucketKey]; current == nil || !current.loadedAt.After(entry.loadedAt) {
		c.snapshotEntries[bucketKey] = entry
	}
	for key, other := range c.snapshotEntries {
		if key != bucketKey && entry.loadedAt.Sub(other.loadedAt) > schedulerSnapshotCacheEvictAge {
			delete(c.snapshotEntries, key)
		}
	}
}

func (c *schedulerCache) dropSnapshotEntry(bucketKey string) {
	c.snapshotMu.RLock()
	_, ok := c.snapshotEntries[bucketKey]
	c.snapshotMu.RUnlock()
	if !ok {
		return
	}
	c.snapshotMu.Lock()
	delete(c.snapshotEntries, bucketKey)
	c.snapshotMu.Unlock()
}

func schedulerSnapshotTagKeys(bucket service.SchedulerBucket) []string {
	keys := make([]string, 0, 5)
	keys = append(keys,
		schedulerBucketKey(schedulerReadyPrefix, bucket),
		schedulerBucketKey(schedulerActivePrefix, bucket),
		schedulerAccountMetaRevisionKey,
		schedulerAccountMetaRevisionKeyForPlatform(bucket.Platform),
	)
	if bucket.Mode == service.SchedulerModeMixed && bucket.Platform != service.PlatformAntigravity {
		keys = append(keys, schedulerAccountMetaRevisionKeyForPlatform(service.PlatformAntigravity))
	}
	return keys
}

func schedulerAccountMetaRevisionKeyForPlatform(platform string) string {
	if platform == "" {
		return schedulerAccountMetaRevisionKey
	}
	return schedulerAccountMetaPlatformRevisionPrefix + platform
}

func schedulerCacheOptionalString(value any) (string, error) {
	switch typed := value.(type) {
	case nil:
		return "", nil
	case string:
		return typed, nil
	case []byte:
		return string(typed), nil
	default:
		return "", fmt.Errorf("unexpected scheduler cache value type: %T", value)
	}
}

func (c *schedulerCache) CaptureBucketWriteToken(ctx context.Context, bucket service.SchedulerBucket) (service.SchedulerBucketWriteToken, error) {
	result, err := captureBucketWriteTokenScript.Run(ctx, c.rdb, []string{
		schedulerBucketKey(schedulerEpochPrefix, bucket),
		schedulerBucketKey(schedulerRetiredPrefix, bucket),
	}).Int64()
	if err != nil {
		return service.SchedulerBucketWriteToken{}, err
	}
	if err := schedulerBucketWriteResultError(result, bucket); err != nil {
		return service.SchedulerBucketWriteToken{}, err
	}
	return service.SchedulerBucketWriteToken{Bucket: bucket, Epoch: result}, nil
}

func (c *schedulerCache) RetireBucket(ctx context.Context, bucket service.SchedulerBucket) error {
	snapshotKeyPrefix := fmt.Sprintf("%s%d:%s:%s:v", schedulerSnapshotPrefix, bucket.GroupID, bucket.Platform, bucket.Mode)
	result, err := retireBucketScript.Run(ctx, c.rdb, []string{
		schedulerBucketKey(schedulerEpochPrefix, bucket),
		schedulerBucketKey(schedulerRetiredPrefix, bucket),
		schedulerBucketSetKey,
		schedulerBucketKey(schedulerReadyPrefix, bucket),
		schedulerBucketKey(schedulerActivePrefix, bucket),
	}, bucket.String(), snapshotKeyPrefix, snapshotGraceTTLSeconds).Int64()
	if err != nil {
		return err
	}
	if result < 1 {
		return fmt.Errorf("retire scheduler bucket %s returned invalid epoch %d", bucket.String(), result)
	}
	return nil
}

func (c *schedulerCache) ReopenBucket(ctx context.Context, bucket service.SchedulerBucket) (service.SchedulerBucketWriteToken, error) {
	snapshotKeyPrefix := fmt.Sprintf("%s%d:%s:%s:v", schedulerSnapshotPrefix, bucket.GroupID, bucket.Platform, bucket.Mode)
	result, err := reopenBucketScript.Run(ctx, c.rdb, []string{
		schedulerBucketKey(schedulerEpochPrefix, bucket),
		schedulerBucketKey(schedulerRetiredPrefix, bucket),
		schedulerBucketSetKey,
		schedulerBucketKey(schedulerReadyPrefix, bucket),
		schedulerBucketKey(schedulerActivePrefix, bucket),
	}, bucket.String(), snapshotKeyPrefix, snapshotGraceTTLSeconds).Int64()
	if err != nil {
		return service.SchedulerBucketWriteToken{}, err
	}
	if err := schedulerBucketWriteResultError(result, bucket); err != nil {
		return service.SchedulerBucketWriteToken{}, err
	}
	return service.SchedulerBucketWriteToken{Bucket: bucket, Epoch: result}, nil
}

func (c *schedulerCache) TryAcquireGroupLifecycleLease(ctx context.Context, groupID int64, ttl time.Duration) (service.SchedulerGroupLifecycleLease, bool, error) {
	if groupID <= 0 {
		return service.SchedulerGroupLifecycleLease{}, false, fmt.Errorf("%w: group id must be positive", service.ErrSchedulerGroupLifecycleLeaseInvalid)
	}
	if ttl <= 0 {
		return service.SchedulerGroupLifecycleLease{}, false, fmt.Errorf("%w: ttl must be positive", service.ErrSchedulerGroupLifecycleLeaseInvalid)
	}
	ownerToken, err := newSchedulerGroupLifecycleOwnerToken()
	if err != nil {
		return service.SchedulerGroupLifecycleLease{}, false, err
	}
	acquired, err := c.rdb.SetNX(ctx, schedulerGroupLifecycleLockKey(groupID), ownerToken, ttl).Result()
	if err != nil {
		return service.SchedulerGroupLifecycleLease{}, false, err
	}
	if !acquired {
		return service.SchedulerGroupLifecycleLease{}, false, nil
	}
	return service.SchedulerGroupLifecycleLease{GroupID: groupID, OwnerToken: ownerToken}, true, nil
}

func (c *schedulerCache) ReleaseGroupLifecycleLease(ctx context.Context, lease service.SchedulerGroupLifecycleLease) error {
	if !lease.ValidFor(lease.GroupID) {
		return service.ErrSchedulerGroupLifecycleLeaseInvalid
	}
	result, err := releaseGroupLifecycleLeaseScript.Run(
		ctx,
		c.rdb,
		[]string{schedulerGroupLifecycleLockKey(lease.GroupID)},
		lease.OwnerToken,
	).Int64()
	if err != nil {
		return err
	}
	if result == 0 {
		return fmt.Errorf("%w: group=%d", service.ErrSchedulerGroupLifecycleLeaseLost, lease.GroupID)
	}
	if result != 1 {
		return fmt.Errorf("release scheduler group lifecycle lease returned %d", result)
	}
	return nil
}

func newSchedulerGroupLifecycleOwnerToken() (string, error) {
	raw := make([]byte, schedulerGroupLifecycleOwnerTokenBytes)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("generate scheduler group lifecycle owner token: %w", err)
	}
	return hex.EncodeToString(raw), nil
}

func (c *schedulerCache) SetSnapshot(ctx context.Context, bucket service.SchedulerBucket, token service.SchedulerBucketWriteToken, accounts []service.Account) error {
	if !token.ValidFor(bucket) {
		return fmt.Errorf("%w: bucket=%s", service.ErrSchedulerBucketWriteFenced, bucket.String())
	}

	version, err := c.allocateSnapshotVersion(ctx, bucket, token)
	if err != nil {
		return err
	}

	if _, err := c.writeSnapshotVersionAndReturnAccountIDs(ctx, bucket, version, accounts); err != nil {
		return err
	}
	return c.activateSnapshotVersion(ctx, bucket, token, version)
}

func (c *schedulerCache) SetSnapshotAndReturnAccountIDs(ctx context.Context, bucket service.SchedulerBucket, token service.SchedulerBucketWriteToken, accounts []service.Account) ([]int64, error) {
	if !token.ValidFor(bucket) {
		return nil, fmt.Errorf("%w: bucket=%s", service.ErrSchedulerBucketWriteFenced, bucket.String())
	}

	version, err := c.allocateSnapshotVersion(ctx, bucket, token)
	if err != nil {
		return nil, err
	}
	accountIDs, err := c.writeSnapshotVersionAndReturnAccountIDs(ctx, bucket, version, accounts)
	if err != nil {
		return nil, err
	}
	if err := c.activateSnapshotVersion(ctx, bucket, token, version); err != nil {
		return nil, err
	}
	return accountIDs, nil
}

func (c *schedulerCache) SetSnapshotByAccountIDs(ctx context.Context, bucket service.SchedulerBucket, token service.SchedulerBucketWriteToken, accountIDs []int64) error {
	if !token.ValidFor(bucket) {
		return fmt.Errorf("%w: bucket=%s", service.ErrSchedulerBucketWriteFenced, bucket.String())
	}
	version, err := c.allocateSnapshotVersion(ctx, bucket, token)
	if err != nil {
		return err
	}
	if err := c.writeSnapshotAccountIDs(ctx, bucket, version, accountIDs); err != nil {
		return err
	}
	return c.activateSnapshotVersion(ctx, bucket, token, version)
}

func (c *schedulerCache) allocateSnapshotVersion(ctx context.Context, bucket service.SchedulerBucket, token service.SchedulerBucketWriteToken) (string, error) {
	result, err := allocateSnapshotVersionScript.Run(ctx, c.rdb, []string{
		schedulerBucketKey(schedulerEpochPrefix, bucket),
		schedulerBucketKey(schedulerRetiredPrefix, bucket),
		schedulerBucketKey(schedulerVersionPrefix, bucket),
	}, token.Epoch).Int64()
	if err != nil {
		return "", err
	}
	if err := schedulerBucketWriteResultError(result, bucket); err != nil {
		return "", err
	}
	return strconv.FormatInt(result, 10), nil
}

func (c *schedulerCache) writeSnapshotVersionAndReturnAccountIDs(ctx context.Context, bucket service.SchedulerBucket, version string, accounts []service.Account) ([]int64, error) {
	accountIDs, err := c.writeAccountIDs(ctx, accounts)
	if err != nil {
		return nil, err
	}
	if err := c.writeSnapshotAccountIDs(ctx, bucket, version, accountIDs); err != nil {
		return nil, err
	}
	return accountIDs, nil
}

func (c *schedulerCache) writeSnapshotAccountIDs(ctx context.Context, bucket service.SchedulerBucket, version string, accountIDs []int64) error {
	members := schedulerSnapshotMembers(accountIDs)
	return c.writeSnapshotMembers(ctx, bucket, version, members)
}

func schedulerSnapshotMembers(accountIDs []int64) []redis.Z {
	if len(accountIDs) == 0 {
		return nil
	}

	members := make([]redis.Z, 0, len(accountIDs))
	for idx, accountID := range accountIDs {
		members = append(members, redis.Z{
			Score:  float64(idx),
			Member: strconv.FormatInt(accountID, 10),
		})
	}
	return members
}

func (c *schedulerCache) writeSnapshotMembers(ctx context.Context, bucket service.SchedulerBucket, version string, members []redis.Z) error {
	if len(members) == 0 {
		return nil
	}
	snapshotKey := schedulerSnapshotKey(bucket, version)
	pipe := c.rdb.Pipeline()
	for start := 0; start < len(members); start += c.writeChunkSize {
		end := start + c.writeChunkSize
		if end > len(members) {
			end = len(members)
		}
		pipe.ZAdd(ctx, snapshotKey, members[start:end]...)
	}
	_, err := pipe.Exec(ctx)
	return err
}

func (c *schedulerCache) activateSnapshotVersion(ctx context.Context, bucket service.SchedulerBucket, token service.SchedulerBucketWriteToken, version string) error {
	snapshotKey := schedulerSnapshotKey(bucket, version)

	activeKey := schedulerBucketKey(schedulerActivePrefix, bucket)
	readyKey := schedulerBucketKey(schedulerReadyPrefix, bucket)
	snapshotKeyPrefix := fmt.Sprintf("%s%d:%s:%s:v", schedulerSnapshotPrefix, bucket.GroupID, bucket.Platform, bucket.Mode)

	keys := []string{
		activeKey,
		readyKey,
		schedulerBucketSetKey,
		snapshotKey,
		schedulerBucketKey(schedulerEpochPrefix, bucket),
		schedulerBucketKey(schedulerRetiredPrefix, bucket),
	}
	args := []any{version, bucket.String(), snapshotKeyPrefix, snapshotGraceTTLSeconds, token.Epoch}

	result, err := activateSnapshotScript.Run(ctx, c.rdb, keys, args...).Int64()
	if err != nil {
		return err
	}
	return schedulerBucketWriteResultError(result, bucket)
}

func schedulerBucketWriteResultError(result int64, bucket service.SchedulerBucket) error {
	switch result {
	case -1:
		return fmt.Errorf("%w: bucket=%s", service.ErrSchedulerBucketRetired, bucket.String())
	case -2:
		return fmt.Errorf("%w: bucket=%s", service.ErrSchedulerBucketWriteFenced, bucket.String())
	default:
		return nil
	}
}

func (c *schedulerCache) GetAccount(ctx context.Context, accountID int64) (*service.Account, error) {
	id := strconv.FormatInt(accountID, 10)
	values, err := c.rdb.MGet(ctx, schedulerAccountKey(id), schedulerLastUsedKey(id)).Result()
	if err != nil {
		return nil, err
	}
	if len(values) != 2 || values[0] == nil {
		return nil, nil
	}
	account, err := decodeCachedAccount(values[0])
	if err != nil {
		return nil, err
	}
	if err := applySchedulerLastUsed(account, values[1]); err != nil {
		return nil, err
	}
	return account, nil
}

func (c *schedulerCache) SetAccount(ctx context.Context, account *service.Account) error {
	if account == nil || account.ID <= 0 {
		return nil
	}
	accountIDs, err := c.writeAccountIDs(ctx, []service.Account{*account})
	if err != nil {
		return err
	}
	if len(accountIDs) == 0 {
		return c.DeleteAccount(ctx, account.ID)
	}
	return nil
}

func (c *schedulerCache) DeleteAccount(ctx context.Context, accountID int64) error {
	if accountID <= 0 {
		return nil
	}
	id := strconv.FormatInt(accountID, 10)

	pipe := c.rdb.TxPipeline()
	pipe.Del(ctx, schedulerAccountKey(id), schedulerAccountMetaKey(id), schedulerLastUsedKey(id))
	pipe.Set(ctx, schedulerAccountMetaRevisionKey, newSchedulerAccountMetaRevision(), 0)
	_, err := pipe.Exec(ctx)
	return err
}

func (c *schedulerCache) UpdateLastUsed(ctx context.Context, updates map[int64]time.Time) error {
	if len(updates) == 0 {
		return nil
	}

	pipe := c.rdb.Pipeline()
	queued := 0
	metaDeleted := false
	keys := make([]string, 0, schedulerLastUsedUpdateChunkSize*2)
	args := make([]any, 0, schedulerLastUsedUpdateChunkSize)
	queueBatch := func() {
		if len(args) == 0 {
			return
		}
		updateSchedulerLastUsedScript.Eval(ctx, pipe, keys, args...)
		queued++
		keys = make([]string, 0, schedulerLastUsedUpdateChunkSize*2)
		args = make([]any, 0, schedulerLastUsedUpdateChunkSize)
	}
	for id, usedAt := range updates {
		if id <= 0 {
			continue
		}
		millis, err := schedulerLastUsedMillis(usedAt)
		if err != nil {
			slog.Warn("scheduler cache removes account with unencodable payload",
				"account_id", id,
				"error", err,
			)
			idText := strconv.FormatInt(id, 10)
			pipe.Del(ctx, schedulerAccountKey(idText), schedulerAccountMetaKey(idText), schedulerLastUsedKey(idText))
			queued++
			metaDeleted = true
			continue
		}
		idText := strconv.FormatInt(id, 10)
		keys = append(keys, schedulerAccountKey(idText), schedulerLastUsedKey(idText))
		args = append(args, millis)
		if len(args) >= schedulerLastUsedUpdateChunkSize {
			queueBatch()
		}
	}
	queueBatch()
	if queued == 0 {
		return nil
	}
	if metaDeleted {

		pipe.Set(ctx, schedulerAccountMetaRevisionKey, newSchedulerAccountMetaRevision(), 0)
	}
	_, err := pipe.Exec(ctx)

	c.lastUsedGeneration.Add(1)
	return err
}

func (c *schedulerCache) TryLockBucket(ctx context.Context, bucket service.SchedulerBucket, ttl time.Duration) (bool, error) {
	key := schedulerBucketKey(schedulerLockPrefix, bucket)
	return c.rdb.SetNX(ctx, key, time.Now().UnixNano(), ttl).Result()
}

func (c *schedulerCache) UnlockBucket(ctx context.Context, bucket service.SchedulerBucket) error {
	key := schedulerBucketKey(schedulerLockPrefix, bucket)
	return c.rdb.Del(ctx, key).Err()
}

func (c *schedulerCache) ListBuckets(ctx context.Context) ([]service.SchedulerBucket, error) {
	raw, err := c.rdb.SMembers(ctx, schedulerBucketSetKey).Result()
	if err != nil {
		return nil, err
	}
	out := make([]service.SchedulerBucket, 0, len(raw))
	for _, entry := range raw {
		bucket, ok := service.ParseSchedulerBucket(entry)
		if !ok {
			continue
		}
		out = append(out, bucket)
	}
	return out, nil
}

func (c *schedulerCache) GetOutboxWatermark(ctx context.Context) (int64, error) {
	val, err := c.rdb.Get(ctx, schedulerOutboxWatermarkKey).Result()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	id, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return 0, err
	}
	return id, nil
}

func (c *schedulerCache) SetOutboxWatermark(ctx context.Context, id int64) error {
	return c.rdb.Set(ctx, schedulerOutboxWatermarkKey, strconv.FormatInt(id, 10), 0).Err()
}

func schedulerBucketKey(prefix string, bucket service.SchedulerBucket) string {
	return fmt.Sprintf("%s%d:%s:%s", prefix, bucket.GroupID, bucket.Platform, bucket.Mode)
}

func schedulerGroupLifecycleLockKey(groupID int64) string {
	return schedulerGroupLifecycleLockPrefix + strconv.FormatInt(groupID, 10)
}

func schedulerSnapshotKey(bucket service.SchedulerBucket, version string) string {
	return fmt.Sprintf("%s%d:%s:%s:v%s", schedulerSnapshotPrefix, bucket.GroupID, bucket.Platform, bucket.Mode, version)
}

func schedulerAccountKey(id string) string {
	return schedulerAccountPrefix + id
}

func schedulerAccountMetaKey(id string) string {
	return schedulerAccountMetaPrefix + id
}

func schedulerLastUsedKey(id string) string {
	return schedulerAccountLastUsedPrefix + id
}

func ptrTime(t time.Time) *time.Time {
	return &t
}

func schedulerLastUsedMillis(value time.Time) (int64, error) {
	if _, err := value.MarshalJSON(); err != nil {
		return 0, err
	}
	return value.UTC().UnixMilli(), nil
}

func applySchedulerLastUsed(account *service.Account, value any) error {
	if account == nil {
		return nil
	}
	lastUsedAt, err := parseSchedulerLastUsed(value)
	if err != nil {
		return err
	}
	if lastUsedAt != nil && (account.LastUsedAt == nil || lastUsedAt.After(*account.LastUsedAt)) {
		account.LastUsedAt = lastUsedAt
	}
	return nil
}

func parseSchedulerLastUsed(value any) (*time.Time, error) {
	if value == nil {
		return nil, nil
	}
	var raw string
	switch typed := value.(type) {
	case string:
		raw = typed
	case []byte:
		raw = string(typed)
	default:
		return nil, fmt.Errorf("unexpected last_used cache type: %T", value)
	}
	millis, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid last_used cache value %q: %w", raw, err)
	}
	return ptrTime(time.UnixMilli(millis).UTC()), nil
}

func decodeCachedAccount(val any) (*service.Account, error) {
	payload, err := schedulerCachePayload(val)
	if err != nil {
		return nil, err
	}
	return decodeCachedAccountPayload(payload)
}

func schedulerCachePayload(val any) (string, error) {
	switch raw := val.(type) {
	case string:
		return raw, nil
	case []byte:
		return string(raw), nil
	default:
		return "", fmt.Errorf("unexpected account cache type: %T", val)
	}
}

func decodeCachedAccountPayload(payload string) (*service.Account, error) {
	var account service.Account
	if err := json.Unmarshal([]byte(payload), &account); err != nil {
		return nil, err
	}
	return &account, nil
}

func (c *schedulerCache) writeAccountIDs(ctx context.Context, accounts []service.Account) ([]int64, error) {
	if len(accounts) == 0 {
		return nil, nil
	}

	type metaWrite struct {
		platform string
		previous *redis.StatusCmd
	}
	pipe := c.rdb.Pipeline()
	accountIDs := make([]int64, 0, len(accounts))
	platforms := make([]string, 0, 1)
	writes := make([]metaWrite, 0, min(len(accounts), c.writeChunkSize))
	pending := 0
	execFailed := false
	platformChanged := false
	defer func() {
		if execFailed {
			c.scheduleGlobalAccountMetaRevision(ctx)
		}
	}()
	exec := func() error {
		cmds, err := pipe.Exec(ctx)
		if err != nil && !errors.Is(err, redis.Nil) {
			execFailed = true
			return err
		}

		for _, cmd := range cmds {
			if cmdErr := cmd.Err(); cmdErr != nil && !errors.Is(cmdErr, redis.Nil) {
				execFailed = true
				return cmdErr
			}
		}
		for _, write := range writes {
			if schedulerMetaPlatformChanged(write.previous, write.platform) {
				platformChanged = true
			}
		}
		writes = writes[:0]
		pipe = c.rdb.Pipeline()
		pending = 0
		return nil
	}

	for _, account := range accounts {
		fullPayload, metaPayload, err := marshalSchedulerCacheAccount(account)
		if err != nil {
			slog.Warn("scheduler cache skips account with unencodable payload",
				"account_id", account.ID,
				"error", err,
			)
			continue
		}
		if pending >= c.writeChunkSize {
			if err := exec(); err != nil {
				return nil, err
			}
		}

		id := strconv.FormatInt(account.ID, 10)
		pipe.Set(ctx, schedulerAccountKey(id), fullPayload, 0)
		writes = append(writes, metaWrite{
			platform: account.Platform,
			previous: pipe.SetArgs(ctx, schedulerAccountMetaKey(id), metaPayload, redis.SetArgs{Get: true}),
		})

		accountIDs = append(accountIDs, account.ID)
		if !slices.Contains(platforms, account.Platform) {
			platforms = append(platforms, account.Platform)
		}
		pending++
	}

	if pending == 0 {
		return accountIDs, nil
	}
	for _, platform := range platforms {
		pipe.Set(ctx, schedulerAccountMetaRevisionKeyForPlatform(platform), newSchedulerAccountMetaRevision(), 0)
	}
	if err := exec(); err != nil {
		return nil, err
	}
	if platformChanged {
		c.advanceGlobalAccountMetaRevision(ctx)
	}
	return accountIDs, nil
}

func schedulerMetaPlatformChanged(previous *redis.StatusCmd, platform string) bool {
	raw, err := previous.Result()
	if err != nil {
		return false
	}
	if !gjson.Valid(raw) {
		return true
	}
	old := gjson.Get(raw, "Platform")
	return old.Type != gjson.String || old.Str != platform
}

func (c *schedulerCache) scheduleGlobalAccountMetaRevision(ctx context.Context) {
	c.globalRevisionPending.Store(true)
	if !c.globalRevisionWriting.CompareAndSwap(false, true) {
		return
	}
	go func() {
		for {
			c.globalRevisionPending.Store(false)
			c.advanceGlobalAccountMetaRevision(ctx)
			c.globalRevisionWriting.Store(false)
			if !c.globalRevisionPending.Load() || !c.globalRevisionWriting.CompareAndSwap(false, true) {
				return
			}
		}
	}()
}

func (c *schedulerCache) advanceGlobalAccountMetaRevision(ctx context.Context) {
	writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), schedulerAccountMetaRevisionWriteTimeout)
	defer cancel()
	if err := c.rdb.Set(writeCtx, schedulerAccountMetaRevisionKey, newSchedulerAccountMetaRevision(), 0).Err(); err != nil {
		slog.Warn("scheduler cache failed to advance account metadata revision", "error", err)
	}
}

func newSchedulerAccountMetaRevision() string {
	raw := make([]byte, schedulerAccountMetaRevisionBytes)

	_, _ = rand.Read(raw)
	return hex.EncodeToString(raw)
}

func marshalSchedulerCacheAccount(account service.Account) ([]byte, []byte, error) {

	account.Groups = nil
	account.AccountGroups = filterSchedulerAccountGroups(account.AccountGroups)
	fullPayload, err := json.Marshal(account)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal account: %w", err)
	}
	metaPayload, err := json.Marshal(buildSchedulerMetadataAccount(account))
	if err != nil {
		return nil, nil, fmt.Errorf("marshal account metadata: %w", err)
	}
	return fullPayload, metaPayload, nil
}

func (c *schedulerCache) mgetChunked(ctx context.Context, keys []string) ([]any, error) {
	if len(keys) == 0 {
		return []any{}, nil
	}

	chunkSize := c.mgetChunkSize
	if chunkSize <= 0 {
		chunkSize = defaultSchedulerSnapshotMGetChunkSize
	}
	if len(keys) <= chunkSize {
		return c.rdb.MGet(ctx, keys...).Result()
	}

	pipe := c.rdb.Pipeline()
	cmds := make([]*redis.SliceCmd, 0, (len(keys)+chunkSize-1)/chunkSize)
	for start := 0; start < len(keys); start += chunkSize {
		end := start + chunkSize
		if end > len(keys) {
			end = len(keys)
		}
		cmds = append(cmds, pipe.MGet(ctx, keys[start:end]...))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, err
	}
	out := make([]any, 0, len(keys))
	for _, cmd := range cmds {
		out = append(out, cmd.Val()...)
	}
	return out, nil
}

func buildSchedulerMetadataAccount(account service.Account) service.Account {
	return service.Account{
		ID:                      account.ID,
		Name:                    account.Name,
		Platform:                account.Platform,
		Type:                    account.Type,
		Concurrency:             account.Concurrency,
		LoadFactor:              account.LoadFactor,
		Priority:                account.Priority,
		RateMultiplier:          account.RateMultiplier,
		Status:                  account.Status,
		LastUsedAt:              account.LastUsedAt,
		ExpiresAt:               account.ExpiresAt,
		AutoPauseOnExpired:      account.AutoPauseOnExpired,
		Schedulable:             account.Schedulable,
		RateLimitedAt:           account.RateLimitedAt,
		RateLimitResetAt:        account.RateLimitResetAt,
		OverloadUntil:           account.OverloadUntil,
		TempUnschedulableUntil:  account.TempUnschedulableUntil,
		TempUnschedulableReason: account.TempUnschedulableReason,
		SessionWindowStart:      account.SessionWindowStart,
		SessionWindowEnd:        account.SessionWindowEnd,
		SessionWindowStatus:     account.SessionWindowStatus,
		ParentAccountID:         account.ParentAccountID,
		QuotaDimension:          account.QuotaDimension,
		AccountGroups:           filterSchedulerAccountGroups(account.AccountGroups),
		GroupIDs:                filterSchedulerGroupIDs(account.GroupIDs, account.AccountGroups),
		Credentials:             filterSchedulerCredentials(account.Credentials),
		Extra:                   filterSchedulerExtra(account.Extra),
	}
}

func filterSchedulerAccountGroups(accountGroups []service.AccountGroup) []service.AccountGroup {
	if len(accountGroups) == 0 {
		return nil
	}

	filtered := make([]service.AccountGroup, 0, len(accountGroups))
	for _, ag := range accountGroups {
		if ag.GroupID <= 0 {
			continue
		}
		filtered = append(filtered, service.AccountGroup{
			AccountID: ag.AccountID,
			GroupID:   ag.GroupID,
			Priority:  ag.Priority,
			CreatedAt: ag.CreatedAt,
		})
	}
	if len(filtered) == 0 {
		return nil
	}
	return filtered
}

func filterSchedulerGroupIDs(groupIDs []int64, accountGroups []service.AccountGroup) []int64 {
	if len(groupIDs) == 0 && len(accountGroups) == 0 {
		return nil
	}

	seen := make(map[int64]struct{}, len(groupIDs)+len(accountGroups))
	filtered := make([]int64, 0, len(groupIDs)+len(accountGroups))
	for _, id := range groupIDs {
		if id <= 0 {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		filtered = append(filtered, id)
	}
	for _, ag := range accountGroups {
		if ag.GroupID <= 0 {
			continue
		}
		if _, ok := seen[ag.GroupID]; ok {
			continue
		}
		seen[ag.GroupID] = struct{}{}
		filtered = append(filtered, ag.GroupID)
	}
	if len(filtered) == 0 {
		return nil
	}
	return filtered
}

func filterSchedulerCredentials(credentials map[string]any) map[string]any {
	if len(credentials) == 0 {
		return nil
	}

	keys := []string{"model_mapping", "compact_model_mapping", "api_key", "project_id", "oauth_type", "plan_type", "account_scheduling_threshold",

		"openai_capabilities",

		"base_url", "auth_mode", service.GeminiExplicitCacheUpstreamCredentialKey, service.GeminiExplicitCacheUpstreamMaxTTLCredentialKey}
	filtered := make(map[string]any)
	for _, key := range keys {
		if value, ok := credentials[key]; ok && value != nil {
			filtered[key] = value
		}
	}
	if len(filtered) == 0 {
		return nil
	}
	return filtered
}

func filterSchedulerExtra(extra map[string]any) map[string]any {
	if len(extra) == 0 {
		return nil
	}
	keys := []string{

		"session_window_utilization",
		"passive_usage_7d_utilization",
		"passive_usage_7d_reset",
		"passive_usage_7d_oi_utilization",
		"passive_usage_7d_oi_reset",
		"quota_limit",
		"quota_used",
		"quota_daily_limit",
		"quota_daily_used",
		"quota_daily_start",
		"quota_daily_reset_mode",
		"quota_daily_reset_hour",
		"quota_weekly_limit",
		"quota_weekly_used",
		"quota_weekly_start",
		"quota_weekly_reset_mode",
		"quota_weekly_reset_day",
		"quota_weekly_reset_hour",
		"quota_reset_timezone",
		"mixed_scheduling",
		"window_cost_limit",
		"window_cost_sticky_reserve",

		"base_rpm",
		"rpm_strategy",
		"rpm_sticky_buffer",
		"max_sessions",
		"session_idle_timeout_minutes",
		"openai_oauth_responses_websockets_v2_enabled",
		"openai_oauth_responses_websockets_v2_mode",
		"openai_apikey_responses_websockets_v2_enabled",
		"openai_apikey_responses_websockets_v2_mode",
		"responses_websockets_v2_enabled",
		"openai_ws_enabled",
		"openai_ws_force_http",
		"openai_responses_mode",
		"openai_responses_supported",

		"openai_passthrough",
		"openai_oauth_passthrough",

		"openai_passthrough_model_filter",

		service.AccountExtraOpenAIImageSizeTiers,
		"codex_fingerprint_mode",
		"codex_fingerprint_seed",
		"codex_5h_used_percent",
		"codex_7d_used_percent",
		"codex_5h_reset_at",
		"codex_7d_reset_at",
		"codex_5h_reset_after_seconds",
		"codex_7d_reset_after_seconds",
		"codex_usage_updated_at",
		"codex_credits_snapshot",
		"codex_credits_refresh_state",
		"auto_pause_5h_threshold",
		"auto_pause_7d_threshold",
		"auto_pause_5h_disabled",
		"auto_pause_7d_disabled",

		service.OpenAIAutoResetCreditEnabledExtraKey,
		service.OpenAIAutoResetCredit5hThresholdExtraKey,
		service.OpenAIAutoResetCredit7dThresholdExtraKey,
		service.OpenAIAutoResetCreditStateExtraKey,
		"model_rate_limits",
		service.UpstreamBillingProbeExtraKey,
		service.GrokMediaEligibleExtraKey,
		"grok_billing_snapshot",
	}
	filtered := make(map[string]any)
	for _, key := range keys {
		if value, ok := extra[key]; ok && value != nil {
			if key == service.UpstreamBillingProbeExtraKey {
				filteredProbe := filterSchedulerUpstreamBillingProbe(value)
				if filteredProbe == nil {
					continue
				}
				value = filteredProbe
			}
			filtered[key] = value
		}
	}
	if len(filtered) == 0 {
		return nil
	}
	return filtered
}

func filterSchedulerUpstreamBillingProbe(value any) map[string]any {
	source, ok := value.(map[string]any)
	if !ok {
		return nil
	}

	status, ok := source["status"].(string)
	if !ok || status == "" {
		return nil
	}
	filtered := map[string]any{"status": status}
	for _, key := range []string{"received_at", "fresh_until", "next_probe_at"} {
		if field, exists := source[key]; exists && field != nil {
			filtered[key] = field
		}
	}
	data, ok := source["data"].(map[string]any)
	if !ok {
		return filtered
	}
	filteredData := make(map[string]any)
	for _, key := range []string{
		"billing_scope",
		"resolved_rate_multiplier",
		"peak_rate_enabled",
		"peak_start",
		"peak_end",
		"peak_rate_multiplier",
		"timezone",
	} {
		if field, exists := data[key]; exists && field != nil {
			filteredData[key] = field
		}
	}
	if len(filteredData) > 0 {
		filtered["data"] = filteredData
	}
	return filtered
}
