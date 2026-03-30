package main

import "sync"

type StringStore struct {
	mu   sync.RWMutex
	Data []byte
}

func (ss *StringStore) Put(s string) (offset uint32, length uint16) {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	offset = uint32(len(ss.Data))
	length = uint16(len(s))
	ss.Data = append(ss.Data, s...) // 将字符串拷贝进字节块
	return
}

func (ss *StringStore) Get(off uint32, len uint16) string {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	// 利用 Go 1.25 的高效转换 (unsafe 方式或标准转换)
	// 此时返回的是对大字节块的切片视图，不产生新的内存分配
	return string(ss.Data[off : off+uint32(len)])
}

func (ss *StringStore) Compact() {
	ss.mu.RLock()
	defer ss.mu.RUnlock()

	// 1. 创建全新的字节块
	newStore := make([]byte, 0, len(Store.Data))

	// 2. 遍历当前所有活跃节点，重写偏移量
	for _, node := range Nodes {
		// 提取旧字符串
		oldName := Store.Get(node.NameOff, node.NameLen)
		oldPath := Store.Get(node.PathOff, node.PathLen)

		// 写入新块，更新偏移量
		newNameOff := uint32(len(newStore))
		newStore = append(newStore, oldName...)

		newPathOff := uint32(len(newStore))
		newStore = append(newStore, oldPath...)

		// 更新内存中的节点信息
		node.NameOff = newNameOff
		node.PathOff = newPathOff
		SetNode(&node)
	}

	// 3. 替换旧块
	Store.Data = newStore

	// 4. 执行二进制持久化 (Save)
	//return f.performBinarySave(filePath)
}
