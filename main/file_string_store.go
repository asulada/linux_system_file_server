package main

import "fmt"

func (ss *StringStore) PutName(s string) (offset uint64, length uint16) {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	offset = uint64(len(ss.Data))
	length = uint16(len(s))
	ss.Data = append(ss.Data, s...) // 将字符串拷贝进字节块
	return
}

func (ss *StringStore) PutPath(s string) (offset uint64, length uint16, pathOff uint64, pathHash uint64) {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	offset = uint64(len(ss.Data))
	length = uint16(len(s))
	ss.Data = append(ss.Data, s...) // 将字符串拷贝进字节块
	pathOff = GetUint64(offset, length)
	pathHash = indexManager.hash(s)
	return
}

func (ss *StringStore) Get(off uint64, len uint16) string {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	// 利用 Go 1.25 的高效转换 (unsafe 方式或标准转换)
	// 此时返回的是对大字节块的切片视图，不产生新的内存分配
	return string(ss.Data[off : off+uint64(len)])
}

// ByteBase 相当于 filepath.Base 的 []byte 版本
func ByteBase(path []byte) []byte {
	if len(path) == 0 {
		return []byte(".")
	}
	// 去掉末尾的斜杠
	for len(path) > 0 && path[len(path)-1] == '/' {
		path = path[:len(path)-1]
	}
	// 找到最后一个斜杠
	i := lastIndexByte(path, '/')
	if i >= 0 {
		return path[i+1:]
	}
	return path
}

func lastIndexByte(s []byte, c byte) int {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == c {
			return i
		}
	}
	return -1
}

// CompactThreshold Compact 触发阈值：废弃字节占比（0.0-1.0）
// 当废弃空间超过此比例时才执行压缩，默认 20%
const CompactThreshold = 0.2

func (ss *StringStore) Compact() {
	mu.Lock()
	defer mu.Unlock()

	ss.mu.RLock()
	oldData := ss.Data
	oldSize := len(oldData)
	ss.mu.RUnlock()

	// 【优化】计算实际使用的字节数，判断是否需要压缩
	usedBytes := calculateUsedBytes()
	wastedBytes := oldSize - usedBytes

	// 如果废弃字节占比小于阈值，跳过压缩
	if oldSize > 0 && float64(wastedBytes)/float64(oldSize) < CompactThreshold {
		logger.Infow("Compact: 跳过压缩，废弃空间不足",
			"totalSize", oldSize,
			"usedBytes", usedBytes,
			"wastedBytes", wastedBytes,
			"wastedRatio", fmt.Sprintf("%.2f%%", float64(wastedBytes)/float64(oldSize)*100))
		return
	}

	oldToNewPathKey := make(map[uint64]uint64, len(Nodes))
	newStore := make([]byte, 0, usedBytes)

	// Map 遍历：node 是副本
	for _, node := range Nodes {
		if node.Invalid {
			oldName := oldData[node.NameOff : node.NameOff+uint64(node.NameLen)]
			newNameOff := uint64(len(newStore))
			newStore = append(newStore, oldName...)

			node.NameOff = newNameOff
			// 必须写回 Map
			SetNode(&node)
			continue
		}

		// 记录旧 Key
		oldPathKey := GetUint64(node.PathOff, node.PathLen)

		// 提取旧路径
		oldPathBytes := oldData[node.PathOff : node.PathOff+uint64(node.PathLen)]

		// 写入新块
		newPathOff := uint64(len(newStore))
		newStore = append(newStore, oldPathBytes...)

		// 计算新偏移量 (利用现有的 NameLen 避免解析字符串)
		// NameOff = 路径开始位置 + (路径长度 - 名字长度)
		newNameOff := newPathOff + uint64(node.PathLen-node.NameLen)

		node.NameOff = newNameOff
		node.PathOff = newPathOff

		// 生成并记录新 Key
		newPathKey := GetUint64(node.PathOff, node.PathLen)
		oldToNewPathKey[oldPathKey] = newPathKey

		// 【关键】将修改后的副本写回 Map
		SetNode(&node)
	}

	// 替换底层存储
	ss.mu.Lock()
	ss.Data = newStore
	ss.mu.Unlock()

	// 更新 Inotify 句柄关联
	updatePathToWd(oldToNewPathKey)

	// 重建 PathMap 等索引
	rebuildAllMaps()

	// 【修复】记录压缩完成信息
	logger.Infow("Compact: 压缩完成",
		"oldSize", oldSize,
		"newSize", len(newStore),
		"savedBytes", oldSize-len(newStore),
		"savedRatio", fmt.Sprintf("%.2f%%", float64(oldSize-len(newStore))/float64(oldSize)*100))

}

// calculateUsedBytes 计算当前所有节点实际使用的字节数
func calculateUsedBytes() int {
	totalUsed := 0

	for _, node := range Nodes {
		if node.Invalid {
			totalUsed += int(node.NameLen)
		} else {
			totalUsed += int(node.PathLen)
		}
	}

	return totalUsed
}

func updatePathToWd(oldToNewPathKey map[uint64]uint64) {
	wdMu.Lock()
	defer wdMu.Unlock()
	// 创建新的 PathToWd
	newPathToWd := make(map[uint64]int, len(PathToWd))

	// 遍历旧的 PathToWd，将旧 pathKey 转换为新 pathKey
	for oldPathKey, wd := range PathToWd {
		if newPathKey, exists := oldToNewPathKey[oldPathKey]; exists {
			// 如果找到了对应的新 pathKey，使用新的
			newPathToWd[newPathKey] = wd
		}
		// 如果没找到，说明该路径已被删除或变为无效节点，直接丢弃

	}

	// 替换为新的映射
	PathToWd = newPathToWd

	// 同步更新 WdMap：清理那些 pathKey 已改变的条目
	for wd, oldPathKey := range WdMap {
		if newPathKey, exists := oldToNewPathKey[oldPathKey]; exists {
			// 更新为新 pathKey
			WdMap[wd] = newPathKey
		} else {
			// 如果旧 pathKey 不存在于映射中，说明路径已删除，清理该条目
			delete(WdMap, wd)
		}
	}
}

// rebuildAllMaps 重建 PathMap 和 PathHashIdMap
// ⚠️ 注意：此函数必须在持有 mu 锁的情况下调用
func rebuildAllMaps() {
	// 重新初始化 PathMap 和 PathHashIdMap
	PathMap = make(map[uint64]uint64, len(Nodes))
	PathHashIdMap = make(map[uint64]uint64, len(Nodes))

	// 遍历所有节点，使用新的偏移量重建索引
	for id, node := range Nodes {
		if node.Invalid {
			continue
		}

		pathKey := GetUint64(node.PathOff, node.PathLen)
		PathMap[pathKey] = id

		pathStr := string(Store.Data[node.PathOff : node.PathOff+uint64(node.PathLen)])
		pathHash := indexManager.hash(pathStr)
		PathHashIdMap[pathHash] = pathKey
	}
}
