package main

import (
	"sort"
	"sync"
	"time"
)

// 重置所有排序
func (f *FileSystemIndex) RebuildAllIndexes() {
	defer TimeTrack(time.Now(), "RebuildAllIndexes")

	mu.Lock()
	defer mu.Unlock()

	count := uint64(len(Nodes))

	// 初始化索引向量
	TimeIdx = make([]uint64, count)
	SizeIdx = make([]uint64, count)
	NameIdx = make([]uint64, count)

	//遍历节点,获取序号和key
	i := uint64(0)
	for id, _ := range Nodes {
		TimeIdx[i], SizeIdx[i], NameIdx[i] = id, id, id
		i++
	}

	var wg sync.WaitGroup
	wg.Add(3)

	// 并行排序 (Go 1.25 pdqsort 优化)
	go func() {
		defer wg.Done()
		sort.Slice(TimeIdx, func(i, j int) bool {
			return Nodes[TimeIdx[i]].ModTime > Nodes[TimeIdx[j]].ModTime // 倒序
		})
	}()

	go func() {
		defer wg.Done()
		sort.Slice(SizeIdx, func(i, j int) bool {
			return Nodes[SizeIdx[i]].Size > Nodes[SizeIdx[j]].Size
		})
	}()

	go func() {
		defer wg.Done()
		sort.Slice(NameIdx, func(i, j int) bool {
			inode := Nodes[NameIdx[i]]
			iName := Store.Get(inode.NameOff, inode.NameLen)

			jnode := Nodes[NameIdx[j]]
			jName := Store.Get(jnode.NameOff, jnode.NameLen)
			return iName < jName
		})
	}()

	wg.Wait()
	NameSort = false
	SizeSort = false
}

// 重置大小排序
func (f *FileSystemIndex) RebuildSizeIndexes() {
	//logger.Info("重建大小排序")
	defer TimeTrack(time.Now(), "RebuildSizeIndexes")

	mu.Lock()
	defer mu.Unlock()

	count := uint64(len(Nodes))

	// 初始化索引向量
	TimeIdx = make([]uint64, count)
	SizeIdx = make([]uint64, count)

	//遍历节点,获取序号和key
	i := uint64(0)
	for id, _ := range Nodes {
		TimeIdx[i], SizeIdx[i] = id, id
		i++
	}

	var wg sync.WaitGroup
	wg.Add(2)

	// 并行排序 (Go 1.25 pdqsort 优化)
	go func() {
		defer wg.Done()
		sort.Slice(TimeIdx, func(i, j int) bool {
			return Nodes[TimeIdx[i]].ModTime > Nodes[TimeIdx[j]].ModTime // 倒序
		})
	}()

	go func() {
		defer wg.Done()
		sort.Slice(SizeIdx, func(i, j int) bool {
			return Nodes[SizeIdx[i]].Size > Nodes[SizeIdx[j]].Size
		})
	}()

	wg.Wait()
	SizeSort = false
}

// 重置名称排序
func (f *FileSystemIndex) RebuildNameIndexes() {
	//logger.Info("重建名称排序")

	defer TimeTrack(time.Now(), "RebuildNameIndexes")

	locked := mu.TryLock()
	if !locked {
		logger.Warn("无法获取 mu 锁")
		return
	}
	defer mu.Unlock()
	logger.Info("成功获取 mu 锁")
	logger.Info("node 长度", len(Nodes))
	count := uint64(len(Nodes))
	logger.Info("node 长度", count)
	// 初始化索引向量
	TimeIdx = make([]uint64, count)
	NameIdx = make([]uint64, count)

	//遍历节点,获取序号和key
	i := uint64(0)
	for id, _ := range Nodes {
		TimeIdx[i], NameIdx[i] = id, id
		i++
	}
	logger.Info(" TimeIdx长度", len(TimeIdx), " NameIdx长度", len(NameIdx))
	var wg sync.WaitGroup
	wg.Add(2)

	// 并行排序 (Go 1.25 pdqsort 优化)
	go func() {
		defer wg.Done()
		sort.Slice(TimeIdx, func(i, j int) bool {
			return Nodes[TimeIdx[i]].ModTime > Nodes[TimeIdx[j]].ModTime // 倒序
		})
	}()

	go func() {
		defer wg.Done()
		sort.Slice(NameIdx, func(i, j int) bool {
			inode := Nodes[NameIdx[i]]
			iName := Store.Get(inode.NameOff, inode.NameLen)

			jnode := Nodes[NameIdx[j]]
			jName := Store.Get(jnode.NameOff, jnode.NameLen)
			return iName < jName
		})
	}()

	wg.Wait()
	NameSort = false
}

// 集成在 持久化守护任务 或 Dirty 标记检查 中。建议当“失效比例”超过 10% 时触发：
func (f *FileSystemIndex) SortCheckAndCompact() {
	mu.RLock()
	nodeCount := len(Nodes)
	indexLen := len(TimeIdx)
	mu.RUnlock()

	// 如果索引长度比实际节点多出 10% 以上，执行压实
	if indexLen > 10000 && float64(indexLen-nodeCount)/float64(indexLen) > 0.1 {
		f.SortCompact()
	}
}

// SortCompact 压实索引，清理无效 ID
func (f *FileSystemIndex) SortCompact() {
	mu.Lock()
	defer mu.Unlock()

	start := time.Now()
	initialLen := len(TimeIdx)

	// 定义原地过滤函数
	filterInPlace := func(ids []uint64) []uint64 {
		n := 0
		for _, id := range ids {
			// 只保留主表中存在的 ID
			if _, exists := Nodes[id]; exists {
				ids[n] = id
				n++
			}
		}
		return ids[:n]
	}

	// 并行处理各个维度的索引压实
	var wg sync.WaitGroup
	wg.Add(3)

	go func() { defer wg.Done(); TimeIdx = filterInPlace(TimeIdx) }()
	go func() { defer wg.Done(); SizeIdx = filterInPlace(SizeIdx) }()
	go func() { defer wg.Done(); NameIdx = filterInPlace(NameIdx) }()

	wg.Wait()

	logger.Infof("[Compaction] 完成! 清理条目: %d, 剩余: %d, 耗时: %v\n",
		initialLen-len(TimeIdx), len(TimeIdx), time.Since(start))
}
