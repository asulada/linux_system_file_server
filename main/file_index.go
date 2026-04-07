package main

import (
	"hash/fnv"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
)

const (
	ShardCount = 256 // 分片锁数量
	MinGram    = 1   // 最小 N-gram 长度
	MaxGram    = 10  // 最大 N-gram 长度
	CacheTTL   = 5 * time.Minute
)

var splitRegexp = regexp.MustCompile(`[^\p{L}\p{Nd}]+`)

type IndexManager struct {
	shards [ShardCount]*NgramShard
}

type NgramShard struct {
	mu   sync.RWMutex
	data map[uint64]*roaring64.Bitmap
}

func NewIndexManager() *IndexManager {
	mgr := &IndexManager{}
	for i := 0; i < ShardCount; i++ {
		mgr.shards[i] = &NgramShard{data: make(map[uint64]*roaring64.Bitmap, 8192)}
	}
	return mgr
}

func (mgr *IndexManager) hash(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return h.Sum64()
}

// AddToIndex 将路径分解为 N-gram 并存入位图
func (mgr *IndexManager) AddToIndex(name string, id uint64) {
	grams := mgr.generateNgrams(name)

	for _, g := range grams {
		h := mgr.hash(g)
		shard := mgr.shards[h%ShardCount]

		shard.mu.Lock()
		rb, ok := shard.data[h]
		if !ok {
			rb = roaring64.NewBitmap()
			shard.data[h] = rb
		}
		rb.Add(id)
		shard.mu.Unlock()
	}
}

// RemoveFromIndex 从位图中移除特定 ID 的索引
func (mgr *IndexManager) RemoveFromIndex(name string, id uint64) {
	grams := mgr.generateNgrams(name)
	for _, g := range grams {
		h := mgr.hash(g)
		shard := mgr.shards[h%ShardCount]

		shard.mu.Lock()
		if rb, ok := shard.data[h]; ok {
			rb.Remove(id)
		}
		shard.mu.Unlock()
	}
}

func (mgr *IndexManager) generateNgrams(path string) []string {
	chunks := splitRegexp.Split(strings.ToLower(path), -1)
	var res []string
	for _, chunk := range chunks {
		runes := []rune(chunk)
		n := len(runes)
		for i := 0; i < n; i++ {
			for length := MinGram; length <= MaxGram && i+length <= n; length++ {
				res = append(res, string(runes[i:i+length]))
			}
		}
	}
	return res
}

var (
	// ... existing code ...

	// N-gram 索引更新队列（保证顺序执行）
	ngramUpdateChan = make(chan func(), 10000)
	ngramOnce       sync.Once
)

// startNgramWorker 启动 N-gram 索引更新工作协程（只启动一次）
func startNgramWorker() {
	ngramOnce.Do(func() {
		go func() {
			for task := range ngramUpdateChan {
				task() // 顺序执行每个任务
			}
		}()
	})
}

// submitNgramTask 提交 N-gram 索引更新任务
func submitNgramTask(task func()) {
	startNgramWorker() // 确保 worker 已启动
	ngramUpdateChan <- task
	//select {
	//case ngramUpdateChan <- task:
	//	// 成功提交
	//default:
	//	// 队列满，降级为同步执行
	//	logger.Warn("N-gram 队列已满，降级为同步执行")
	//	task()
	//}
}

type SearchResultCache struct {
	mu        sync.RWMutex
	cache     map[string]*CachedResult
	idToKeys  map[uint64]map[string]struct{}
	lastClean time.Time
}

type CachedResult struct {
	IDs       []uint64
	Timestamp time.Time
	Total     int
}

func NewSearchResultCache() *SearchResultCache {
	return &SearchResultCache{
		cache:     make(map[string]*CachedResult),
		idToKeys:  make(map[uint64]map[string]struct{}),
		lastClean: time.Now(),
	}
}
func (c *SearchResultCache) Get(key string) *CachedResult {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if cached, exists := c.cache[key]; exists {
		if time.Since(cached.Timestamp) < CacheTTL {
			return cached
		}
	}
	return nil
}

func (c *SearchResultCache) Set(key string, ids []uint64, total int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cache[key] = &CachedResult{
		IDs:       ids,
		Timestamp: time.Now(),
		Total:     total,
	}

	for _, id := range ids {
		if c.idToKeys[id] == nil {
			c.idToKeys[id] = make(map[string]struct{})
		}
		c.idToKeys[id][key] = struct{}{}
	}

	if time.Since(c.lastClean) > 10*time.Minute {
		c.cleanExpired()
	}
}

func (c *SearchResultCache) cleanExpired() {
	now := time.Now()
	for key, cached := range c.cache {
		if now.Sub(cached.Timestamp) >= CacheTTL {
			delete(c.cache, key)

			for _, id := range cached.IDs {
				if keys, exists := c.idToKeys[id]; exists {
					delete(keys, key)
					if len(keys) == 0 {
						delete(c.idToKeys, id)
					}
				}
			}
		}
	}
	c.lastClean = now
}

func (c *SearchResultCache) InvalidateByID(id uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if keys, exists := c.idToKeys[id]; exists {
		for key := range keys {
			if cached, ok := c.cache[key]; ok {
				delete(c.cache, key)

				for _, cachedID := range cached.IDs {
					if cachedKeys, exists := c.idToKeys[cachedID]; exists {
						delete(cachedKeys, key)
						if len(cachedKeys) == 0 {
							delete(c.idToKeys, cachedID)
						}
					}
				}
			}
		}
		delete(c.idToKeys, id)
	}
}

func (c *SearchResultCache) ClearAll() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cache = make(map[string]*CachedResult)
	c.idToKeys = make(map[uint64]map[string]struct{})
	c.lastClean = time.Now()
}

var searchCache = NewSearchResultCache()

type SearchPageResult struct {
	IDs    []uint64
	Total  int
	Offset int
	Limit  int
}

func (mgr *IndexManager) Search(keywords []string, sortBy string, offset int, limit int) *SearchPageResult {
	cacheKey := strings.Join(keywords, "|") + "|" + sortBy

	if cached := searchCache.Get(cacheKey); cached != nil {
		totalCount := len(cached.IDs)

		if offset >= totalCount {
			return &SearchPageResult{
				IDs:    []uint64{},
				Total:  totalCount,
				Offset: offset,
				Limit:  limit,
			}
		}

		endIdx := offset + limit
		if endIdx > totalCount {
			endIdx = totalCount
		}

		pageIDs := make([]uint64, endIdx-offset)
		copy(pageIDs, cached.IDs[offset:endIdx])

		return &SearchPageResult{
			IDs:    pageIDs,
			Total:  totalCount,
			Offset: endIdx,
			Limit:  limit,
		}
	}

	var resultBitmap *roaring64.Bitmap

	for _, k := range keywords {
		if len(k) < MinGram {
			continue
		}
		h := mgr.hash(k)
		shard := mgr.shards[h%ShardCount]

		shard.mu.RLock()
		rb, ok := shard.data[h]
		if !ok {
			shard.mu.RUnlock()
			return &SearchPageResult{
				IDs:    []uint64{},
				Total:  0,
				Offset: offset,
				Limit:  limit,
			}
		}
		if resultBitmap == nil {
			resultBitmap = rb.Clone()
		} else {
			resultBitmap.And(rb)
		}
		shard.mu.RUnlock()
	}

	if resultBitmap == nil {
		return &SearchPageResult{
			IDs:    []uint64{},
			Total:  0,
			Offset: offset,
			Limit:  limit,
		}
	}

	allIDs := make([]uint64, 0, resultBitmap.GetCardinality())
	it := resultBitmap.Iterator()
	for it.HasNext() {
		id := it.Next()
		allIDs = append(allIDs, id)
	}

	sortedIDs := sortResults(allIDs, sortBy)

	totalCount := len(sortedIDs)
	searchCache.Set(cacheKey, sortedIDs, totalCount)

	if offset >= totalCount {
		return &SearchPageResult{
			IDs:    []uint64{},
			Total:  totalCount,
			Offset: offset,
			Limit:  limit,
		}
	}

	endIdx := offset + limit
	if endIdx > totalCount {
		endIdx = totalCount
	}

	pageIDs := make([]uint64, endIdx-offset)
	copy(pageIDs, sortedIDs[offset:endIdx])

	return &SearchPageResult{
		IDs:    pageIDs,
		Total:  totalCount,
		Offset: endIdx,
		Limit:  limit,
	}
}
func sortResults(ids []uint64, sortBy string) []uint64 {
	mu.RLock()
	defer mu.RUnlock()

	switch sortBy {
	case "size":
		sort.Slice(ids, func(i, j int) bool {
			nodeI, existsI := Nodes[ids[i]]
			nodeJ, existsJ := Nodes[ids[j]]
			if !existsI || !existsJ {
				return false
			}
			return nodeI.Size > nodeJ.Size
		})
	case "time":
		sort.Slice(ids, func(i, j int) bool {
			nodeI, existsI := Nodes[ids[i]]
			nodeJ, existsJ := Nodes[ids[j]]
			if !existsI || !existsJ {
				return false
			}
			return nodeI.ModTime > nodeJ.ModTime
		})
	default:
		sort.Slice(ids, func(i, j int) bool {
			nodeI, existsI := Nodes[ids[i]]
			nodeJ, existsJ := Nodes[ids[j]]
			if !existsI || !existsJ {
				return false
			}
			nameI := Store.Get(nodeI.NameOff, nodeI.NameLen)
			nameJ := Store.Get(nodeJ.NameOff, nodeJ.NameLen)
			return nameI < nameJ
		})
	}

	return ids
}

func (c *SearchResultCache) InvalidateByNewFile(fileName string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	lowerFileName := strings.ToLower(fileName)
	keysToDelete := make([]string, 0)

	for key := range c.cache {
		keywords := strings.Split(key, "|")
		numParts := len(keywords)

		if numParts < 2 {
			continue
		}

		searchKeywords := keywords[:numParts-1]

		for _, kw := range searchKeywords {
			if strings.Contains(lowerFileName, kw) {
				keysToDelete = append(keysToDelete, key)
				break
			}
		}
	}

	for _, key := range keysToDelete {
		if cached, ok := c.cache[key]; ok {
			delete(c.cache, key)

			for _, cachedID := range cached.IDs {
				if cachedKeys, exists := c.idToKeys[cachedID]; exists {
					delete(cachedKeys, key)
					if len(cachedKeys) == 0 {
						delete(c.idToKeys, cachedID)
					}
				}
			}
		}
	}

	return len(keysToDelete) > 0
}
