package main

import (
	"fmt"
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

const (
	SortByTime = 0
	SortByName = 1
	SortBySize = 2
)

const (
	SortOrderDesc = 0
	SortOrderAsc  = 1
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
	DirIDs    []uint64
	FileIDs   []uint64
	Timestamp time.Time
}

func NewSearchResultCache() *SearchResultCache {
	return &SearchResultCache{
		cache:     make(map[string]*CachedResult),
		idToKeys:  make(map[uint64]map[string]struct{}),
		lastClean: time.Now(),
	}
}
func (c *SearchResultCache) Get(key string) *CachedResult {
	if cached, exists := c.cache[key]; exists {
		if time.Since(cached.Timestamp) < CacheTTL {
			return cached
		}
	}
	return nil
}

func (c *SearchResultCache) Set(key string, dirIDs []uint64, fileIDs []uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cache[key] = &CachedResult{
		DirIDs:    dirIDs,
		FileIDs:   fileIDs,
		Timestamp: time.Now(),
		//Total:     total,
	}

	for _, id := range dirIDs {
		if c.idToKeys[id] == nil {
			c.idToKeys[id] = make(map[string]struct{})
		}
		c.idToKeys[id][key] = struct{}{}
	}

	for _, id := range fileIDs {
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

			for _, id := range cached.DirIDs {
				if keys, exists := c.idToKeys[id]; exists {
					delete(keys, key)
					if len(keys) == 0 {
						delete(c.idToKeys, id)
					}
				}
			}
			for _, id := range cached.FileIDs {
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

	keys, exists := c.idToKeys[id]
	if !exists {
		return
	}

	// 收集所有需要删除的 key
	keysToDelete := make([]string, 0, len(keys))
	for key := range keys {
		keysToDelete = append(keysToDelete, key)
	}

	// 执行删除
	for _, key := range keysToDelete {
		if cached, ok := c.cache[key]; ok {
			delete(c.cache, key)

			// 清理该 key 关联的所有 ID 的反向索引
			// 注意：这里必须遍历 cached 中的所有 ID，而不仅仅是传入的 id
			for _, cachedID := range cached.DirIDs {
				if cachedKeys, exists := c.idToKeys[cachedID]; exists {
					delete(cachedKeys, key)
					if len(cachedKeys) == 0 {
						delete(c.idToKeys, cachedID)
					}
				}
			}
			for _, cachedID := range cached.FileIDs {
				if cachedKeys, exists := c.idToKeys[cachedID]; exists {
					delete(cachedKeys, key)
					if len(cachedKeys) == 0 {
						delete(c.idToKeys, cachedID)
					}
				}
			}
		}
	}

	// 最后删除触发此次失效的 ID 本身的映射（其实上面的循环已经覆盖了这个 ID，但为了保险可以保留或移除）
	// 实际上，上面的循环已经处理了 id 对应的 key 删除，如果 id 还在 idToKeys 中，说明它还有其他 key，
	// 但我们是要失效包含 id 的所有 key，所以 idToKeys[id] 应该最终为空并被删除。
	delete(c.idToKeys, id)
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

const (
	FileTypeAll  = 0 // 全部
	FileTypeDir  = 1 // 仅文件夹
	FileTypeFile = 2 // 仅文件
)

func (mgr *IndexManager) Search(keywords []string, sortBy int, sortOrder int, offset int, limit int, fileType int) *SearchPageResult {
	cacheKey := fmt.Sprintf("%s|%d|%d", strings.Join(keywords, "|"), sortBy, sortOrder)

	searchCache.mu.RLock()
	if cached := searchCache.Get(cacheKey); cached != nil {
		// 直接利用你提出的高性能按需截取算法，拒绝 slices.Concat 的大对象分配
		defer searchCache.mu.RUnlock()

		return paginate(cached.DirIDs, cached.FileIDs, fileType, offset, limit)
	}
	searchCache.mu.RUnlock()

	// 2. 🧩 缓存未命中，走原有的倒排索引、Bitmap 计算
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
			return &SearchPageResult{IDs: []uint64{}, Total: 0, Offset: offset, Limit: limit}
		}
		if resultBitmap == nil {
			resultBitmap = rb.Clone()
		} else {
			resultBitmap.And(rb)
		}
		shard.mu.RUnlock()
	}

	if resultBitmap == nil {
		return &SearchPageResult{IDs: []uint64{}, Total: 0, Offset: offset, Limit: limit}
	}

	// 从 Bitmap 提取 ID 列表
	allIDs := make([]uint64, 0, resultBitmap.GetCardinality())
	it := resultBitmap.Iterator()
	for it.HasNext() {
		id := it.Next()
		allIDs = append(allIDs, id)
	}

	// 结果排序
	sortedIDs := sortResults(allIDs, sortBy, sortOrder)

	// 3. 📂 分离文件夹和文件
	mu.RLock()
	dirIDs := make([]uint64, 0)
	fileIDs := make([]uint64, 0)
	for _, id := range sortedIDs {
		if node, exists := Nodes[id]; exists {
			if node.IsDir() {
				dirIDs = append(dirIDs, id)
			} else {
				fileIDs = append(fileIDs, id)
			}
		}
	}
	mu.RUnlock()

	// 4. 💾 异步/同步写入缓存 (Set 内部直接存储这两份新创建的独立切片)
	searchCache.Set(cacheKey, dirIDs, fileIDs)

	// 5. 📊 未命中缓存时，同样采用你的高性能按需截取算法进行分页返回
	return paginate(dirIDs, fileIDs, fileType, offset, limit)
}

// ✨ 核心优化：你提出的“先目录、后文件”高性能数学边界截取算法
// 无论底层数据有几万条，此函数对内存的分配永远是常数阶 O(Limit)，固定耗时在微秒级
func paginate(dirIDs, fileIDs []uint64, fileType int, offset, limit int) *SearchPageResult {
	dirCount := len(dirIDs)
	fileCount := len(fileIDs)
	totalCount := 0

	// 1. 精准计算当前过滤类型下的总数，不创建、不合并任何新切片
	switch fileType {
	case FileTypeAll:
		totalCount = dirCount + fileCount
	case FileTypeDir:
		totalCount = dirCount
	case FileTypeFile:
		totalCount = fileCount
	}

	if offset >= totalCount || limit <= 0 {
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

	// 2. 预分配且仅分配当前分页精准所需的物理内存
	pageIDs := make([]uint64, endIdx-offset)
	n := 0 // 已填充计数器

	// 👉 步骤 A：填充目录数据 (FileTypeAll 或 FileTypeDir)
	if fileType == FileTypeAll || fileType == FileTypeDir {
		if offset < dirCount {
			dirStart := offset
			dirEnd := endIdx
			if dirEnd > dirCount {
				dirEnd = dirCount
			}
			n += copy(pageIDs[n:], dirIDs[dirStart:dirEnd])
		}
	}

	// 👉 步骤 B：填充文件数据 (FileTypeAll 或 FileTypeFile)
	if fileType == FileTypeAll || fileType == FileTypeFile {
		if n < len(pageIDs) {
			var fileStart int
			if fileType == FileTypeAll {
				if offset >= dirCount {
					fileStart = offset - dirCount
				} else {
					fileStart = 0
				}
			} else {
				fileStart = offset
			}

			fileEnd := fileStart + (len(pageIDs) - n)
			if fileEnd > fileCount {
				fileEnd = fileCount
			}

			copy(pageIDs[n:], fileIDs[fileStart:fileEnd])
		}
	}

	return &SearchPageResult{
		IDs:    pageIDs,
		Total:  totalCount,
		Offset: endIdx,
		Limit:  limit,
	}
}

func sortResults(ids []uint64, sortBy int, sortOrder int) []uint64 {
	mu.RLock()
	defer mu.RUnlock()

	desc := sortOrder == SortOrderDesc

	switch sortBy {
	case SortBySize:
		sort.Slice(ids, func(i, j int) bool {
			nodeI, existsI := Nodes[ids[i]]
			nodeJ, existsJ := Nodes[ids[j]]
			if !existsI || !existsJ {
				return false
			}
			if desc {
				return nodeI.Size > nodeJ.Size
			}
			return nodeI.Size < nodeJ.Size
		})
	case SortByTime:
		sort.Slice(ids, func(i, j int) bool {
			nodeI, existsI := Nodes[ids[i]]
			nodeJ, existsJ := Nodes[ids[j]]
			if !existsI || !existsJ {
				return false
			}
			if desc {
				return nodeI.ModTime > nodeJ.ModTime
			}
			return nodeI.ModTime < nodeJ.ModTime
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
			if desc {
				return nameI > nameJ
			}
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

		if numParts < 3 {
			continue
		}

		searchKeywords := keywords[:numParts-2]

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

			for _, cachedID := range cached.DirIDs {
				if cachedKeys, exists := c.idToKeys[cachedID]; exists {
					delete(cachedKeys, key)
					if len(cachedKeys) == 0 {
						delete(c.idToKeys, cachedID)
					}
				}
			}
			for _, cachedID := range cached.FileIDs {
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
