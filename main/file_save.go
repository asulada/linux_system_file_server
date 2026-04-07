package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

func CheckAndCheckpoint() {
	// 1. 检查 WAL 文件大小
	info, err := os.Stat(selfConfig.WalPath)
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		logger.Error("读取 WAL 状态失败", err)
		return
	}

	// CheckPoint 前先强制刷新 WAL
	FlushWAL()
	// 2. 如果文件没超过阈值，直接返回
	if info.Size() < selfConfig.WALThreshold {
		return
	}

	logger.Info("WAL 文件过大，开始执行 Checkpoint...")

	// 3. 执行全量快照 (SaveSnapshot 会生成 index.db)
	// 注意：SaveSnapshot 内部会加 RLock，保证数据一致性
	err = SaveSnapshot(selfConfig.DumpPath)
	if err != nil {
		logger.Error("快照保存失败，推迟清理 WAL", err)
		return
	}

	// 4. 快照保存成功后，安全清空 WAL
	// 使用原子操作：先重命名旧日志（备份），再创建新的
	walMu.Lock()
	defer walMu.Unlock()

	err = os.Remove(selfConfig.WalPath)
	if err != nil {
		logger.Error("清空 WAL 失败", err)
		return
	}

	logger.Info("Checkpoint 完成，WAL 已重置，系统已达到最简状态")
}

const (
	OpUpsert  uint8 = 1 // 新增
	OpUpdate  uint8 = 2 // 更新元数据
	OpDelete  uint8 = 3 // 删除
	OpRename  uint8 = 4 // 重命名/移动
	OpInvalid uint8 = 5 // 重命名/移动
)

// WriteWAL 将单次操作增量写入日志文件（已废弃，保留兼容）
func WriteWAL(op uint8, n *FileNode, path string) error {
	SubmitWAL(&WALEntry{
		Op:   op,
		Node: n,
		Path: path,
	})
	return nil
}

// WriteWALRename 专门用于写入重命名操作的 WAL 日志（已废弃，保留兼容）
func WriteWALRename(id uint64, modTime int64, oldPath string, newPath string) error {
	SubmitWAL(&WALEntry{
		Op:      OpRename,
		Node:    &FileNode{ID: id},
		OldPath: oldPath,
		NewPath: newPath,
		ModTime: modTime,
	})
	return nil
}

// WriteWALInvalid 专门用于写入标记无效操作的 WAL 日志
func WriteWALInvalid(node *FileNode, path string) error {
	SubmitWAL(&WALEntry{
		Op:   OpInvalid,
		Node: node,
		Path: path,
	})
	return nil
}

// ReplayWAL 从头读取日志文件并恢复内存状态
func ReplayWAL() {
	file, err := os.Open(selfConfig.WalPath) // 打开增量日志文件
	if err != nil {
		return
	} // 文件不存在说明没有增量，直接返回
	defer file.Close()

	br := bufio.NewReader(file) // 使用带缓冲的读取器，极大减少系统调用次数
	for {
		op, err := br.ReadByte() // 读取 1 字节操作码
		if err == io.EOF {
			break
		} // 读到文件末尾，重放结束

		switch op {
		case OpUpsert, OpUpdate, OpDelete:
			// 这三种操作需要完整的 Node 数据
			var n FileNode
			binary.Read(br, binary.LittleEndian, &n.ID)
			binary.Read(br, binary.LittleEndian, &n.ParentID)
			binary.Read(br, binary.LittleEndian, &n.Size)
			binary.Read(br, binary.LittleEndian, &n.ModTime)

			var pLen uint16
			binary.Read(br, binary.LittleEndian, &pLen)
			pBuf := make([]byte, pLen)
			io.ReadFull(br, pBuf)
			path := string(pBuf)

			switch op {
			case OpUpsert:
				ApplyToMemory(n, path)
			case OpUpdate:
				UpdateNodeMetadata(n, path)
			case OpDelete:
				fileSystem.Remove(path, n.IsDir())
			}

		case OpRename:
			// 重命名操作：读取 ID、ModTime 和两个路径
			var id uint64
			binary.Read(br, binary.LittleEndian, &id)

			// 读取 ModTime（uint64 转 int64）
			var modTimeUint uint64
			binary.Read(br, binary.LittleEndian, &modTimeUint)
			modTime := int64(modTimeUint)

			// 读取旧路径
			var oldPLen uint16
			binary.Read(br, binary.LittleEndian, &oldPLen)
			oldPBuf := make([]byte, oldPLen)
			io.ReadFull(br, oldPBuf)
			oldPath := string(oldPBuf)

			// 读取新路径
			var newPLen uint16
			binary.Read(br, binary.LittleEndian, &newPLen)
			newPBuf := make([]byte, newPLen)
			io.ReadFull(br, newPBuf)
			newPath := string(newPBuf)

			// 调用 RenameNode 恢复重命名操作
			oldName := filepath.Base(oldPath)
			newName := filepath.Base(newPath)
			oldPPath := filepath.Dir(oldPath)
			newPPath := filepath.Dir(newPath)
			RenameNodeRebuild(oldPath, oldName, newPath, newName, fileSystem.fd, oldPPath, newPPath, modTime)
		case OpInvalid:
			var n FileNode
			binary.Read(br, binary.LittleEndian, &n.ID)

			var modTimeUint uint64
			binary.Read(br, binary.LittleEndian, &modTimeUint)
			n.ModTime = int64(modTimeUint)

			var pLen uint16
			binary.Read(br, binary.LittleEndian, &pLen)
			pBuf := make([]byte, pLen)
			io.ReadFull(br, pBuf)
			path := string(pBuf)

			MarkNodeInvalid(n, path)

		default:
			logger.Warn("未知的 WAL 操作码", op)
		}
	}
}

func MarkNodeInvalid(n FileNode, path string) {
	mu.Lock()
	defer mu.Unlock()

	n.Invalid = true
	SetNode(&n)

	searchCache.InvalidateByID(n.ID)
	logger.Info("WAL 重放：节点已标记为无效", path)
}

// UpdateNodeMetadata 仅更新节点的元数据（用于 WAL 重放时的更新操作）
func UpdateNodeMetadata(n FileNode, path string) {
	mu.Lock()
	defer mu.Unlock()

	// 通过路径查找节点 ID
	pathOff, _ := GetPathOff(path)
	id, ok := PathMap[pathOff]
	if !ok {
		// 节点不存在，降级为 Upsert
		logger.Warn("WAL 重放：节点不存在，降级为 Upsert", path)
		ApplyToMemoryUnLock(n, path)
		return
	}

	// 获取现有节点
	node := Nodes[id]
	oldSize := node.Size

	// 只更新元数据
	node.Size = n.Size
	node.ModTime = n.ModTime

	// 更新父节点的 Size
	if oldSize != n.Size {
		fileSystem.UpsertParentSize(id, n.Size-oldSize)
	}

	// 保存节点
	SetNode(&node)

	// 标记排序失效
	//SizeSort = true
	//NameSort = true
}

// SaveSnapshot 将当前内存中的所有数据全量备份到磁盘

func SaveSnapshot(filePath string) error {
	// 先写到 .tmp 临时文件，防止写入一半时崩溃导致旧快照损坏
	f, err := os.Create(filePath + ".tmp")
	if err != nil {
		return err
	}
	defer f.Close()

	bw := bufio.NewWriter(f)     // 使用 4KB 缓冲区进行聚合写入
	bw.Write([]byte(IndexMagic)) // 写入 8 字节魔数，校验文件合法性

	// 写入当前内存中的节点总数（用于加载时预分配 Map 空间）
	binary.Write(bw, binary.LittleEndian, uint32(len(Nodes)))

	for _, node := range Nodes {
		path := Store.Get(node.PathOff, node.PathLen) // 从 PathStore 获取该 ID 对应的路径字符串
		pBuf := []byte(path)

		binary.Write(bw, binary.LittleEndian, node.ID)       // 写入 ID
		binary.Write(bw, binary.LittleEndian, node.ParentID) // 写入父 ID
		binary.Write(bw, binary.LittleEndian, node.Size)     // 写入大小
		binary.Write(bw, binary.LittleEndian, node.ModTime)  // 写入时间
		var flags byte
		if node.Invalid {
			flags |= 0x01
		}
		bw.WriteByte(flags)
		bw.Write(make([]byte, 7))
		binary.Write(bw, binary.LittleEndian, uint16(len(pBuf))) // 写入路径长度
		bw.Write(pBuf)                                           // 直接写入原始路径字节
	}
	bw.Flush() // 确保缓冲区剩余数据全部进入磁盘
	f.Close()
	return os.Rename(filePath+".tmp", filePath) // 原子重命名，替换旧快照
}

// LoadSnapshot 从磁盘加载全量数据，恢复速度极快
func LoadSnapshot(filePath string) error {
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		logger.Info("快照文件不存在，跳过加载", filePath)
		return nil
	}
	f, err := os.Open(filePath) // 打开快照文件
	if err != nil {
		return err
	}
	defer f.Close()

	br := bufio.NewReader(f) // 带缓冲读取
	magic := make([]byte, 8)
	io.ReadFull(br, magic) // 读取前 8 字节魔数
	if string(magic) != IndexMagic {
		return fmt.Errorf("invalid snapshot")
	}

	var count uint32
	binary.Read(br, binary.LittleEndian, &count) // 读取节点总数

	for i := uint32(0); i < count; i++ {
		var n FileNode
		binary.Read(br, binary.LittleEndian, &n.ID)
		binary.Read(br, binary.LittleEndian, &n.ParentID)
		binary.Read(br, binary.LittleEndian, &n.Size)
		binary.Read(br, binary.LittleEndian, &n.ModTime)

		flags, _ := br.ReadByte()
		n.Invalid = (flags & 0x01) != 0
		br.Read(make([]byte, 7))

		var pLen uint16
		binary.Read(br, binary.LittleEndian, &pLen)
		pBuf := make([]byte, pLen)
		io.ReadFull(br, pBuf) // 读取路径内容

		ApplyToMemory(n, string(pBuf)) // 恢复到内存数据结构中
	}
	return nil
}

// O(1) 路径重命名 (RenameNode)
// 由于我们存储了全路径，重命名时只需修改对应的 Node。如果是目录，则需要级联修改其下所有子项的路径前缀。
func RenameNodeRebuild(oldPath string, oldName string, newPath string, newName string, fd int, oldPPath string, newPPath string, time int64) {
	mu.Lock()
	defer mu.Unlock()
	oldPathOff, olfHash := GetPathOff(oldPath)
	id, ok := PathMap[oldPathOff]
	if !ok {
		return
	}

	indexManager.RemoveFromIndex(oldName, id)
	node := Nodes[id]
	// 获取父节点信息
	oldParentID := node.ParentID

	oldPathLen := len(oldPath)

	// 1. 将字符串存入字节块
	pOff, pLen, ppOff, ppHash := Store.PutPath(newPath)
	nOff, nLen := Store.PutName(newName)
	// 1. 处理节点本身数据
	delete(PathMap, oldPathOff)
	delete(PathHashIdMap, olfHash)

	node.NameOff = nOff
	node.NameLen = nLen
	node.PathOff = pOff
	node.PathLen = pLen

	// 更新父节点的 Size
	fileSystem.UpsertParentSize(id, -node.Size) // 减去旧父节点的 size

	// 2. 更新父子关系
	// 从旧父节点的子节点集合中移除
	if oldParentChildren, exists := TreeMap[oldParentID]; exists {
		delete(oldParentChildren, id)
	}

	parentOff, _ := GetPathOff(newPPath)
	// 计算新父节点 ID
	newParentID, exists := PathMap[parentOff]
	if !exists {
		// 如果新父节点不存在，说明是跨根目录移动或父节点已被删除
		logger.Error("新父节点不存在", newPPath)
		return
	}
	// 更新为新父节点的引用
	node.ParentID = newParentID

	// 确保新父节点的子节点集合存在并添加
	if TreeMap[newParentID] == nil {
		TreeMap[newParentID] = make(map[uint64]struct{})
	}
	TreeMap[newParentID][id] = struct{}{}
	ChildTreeMap[id] = newParentID

	node.ModTime = time
	SetNode(&node)
	PathMap[ppOff] = id
	PathHashIdMap[ppHash] = ppOff

	// 2. 处理子项
	if node.IsDir() {
		var renameChild func(uint64)
		renameChild = func(pid uint64) {
			for childID := range TreeMap[pid] {
				childNode := Nodes[childID]
				childOldPath := Store.Get(childNode.PathOff, childNode.PathLen)
				//childOldPathOffset := GetUint64(childNode.PathOff, childNode.PathLen)
				childOldPathOff, childOldPathHash := GetPathOff(childOldPath)

				// 安全且高效的路径拼接
				updatedPath := newPath + childOldPath[oldPathLen:]

				// 更新 PathMap 索引
				delete(PathMap, childOldPathOff)
				delete(PathHashIdMap, childOldPathHash)

				cPathOff, cPathlen, cpPathOff, cpPathHash := Store.PutPath(updatedPath)
				childNode.PathOff = cPathOff
				childNode.PathLen = cPathlen
				SetNode(&childNode)

				PathMap[cpPathOff] = childID
				PathHashIdMap[cpPathHash] = cpPathOff

				// 3. 【核心优化】如果是目录，直接通过 O(1) 反向索引更新 wd 信息
				if childNode.IsDir() {
					renameChild(childID)
				}
			}
		}
		renameChild(id)
	}
	fileSystem.UpsertParentSize(id, node.Size) // 加到新父节点上
	//NameSort = true
	indexManager.AddToIndex(newName, id)
}

// WAL 异步写入器
var (
	walWriter     *WALAsyncWriter
	walWriterOnce sync.Once
)

// WALEntry WAL 日志条目
type WALEntry struct {
	Op      uint8
	Node    *FileNode
	Path    string
	OldPath string // 仅用于重命名
	NewPath string // 仅用于重命名
	ModTime int64  // 仅用于重命名
}

// WALAsyncWriter 异步 WAL 写入器
type WALAsyncWriter struct {
	ch     chan *WALEntry
	done   chan struct{}
	file   *os.File
	writer *bufio.Writer
	mu     sync.Mutex
}

// initWALWriter 初始化 WAL 异步写入器（只调用一次）
func initWALWriter() {
	walWriterOnce.Do(func() {
		walWriter = &WALAsyncWriter{
			ch:   make(chan *WALEntry, 10000), // 缓冲区 10000 条
			done: make(chan struct{}),
		}
		go walWriter.run()
	})
}

// run WAL 写入协程

func (w *WALAsyncWriter) run() {
	defer func() {
		// 确保退出前刷新所有数据
		w.mu.Lock()
		if w.writer != nil {
			w.writer.Flush()
		}
		if w.file != nil {
			w.file.Sync() // 强制刷到磁盘
			w.file.Close()
		}
		w.mu.Unlock()
		close(w.done)
	}()

	// 定时刷盘 ticker（每 100ms 强制刷一次）
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case entry, ok := <-w.ch:
			if !ok {
				// channel 关闭，退出
				return
			}
			w.writeEntry(entry)

		case <-ticker.C:
			// 定时强制刷盘，防止数据丢失
			w.mu.Lock()
			if w.writer != nil && w.writer.Buffered() > 0 {
				w.writer.Flush()
				w.file.Sync() // 确保数据真正落盘
			}
			w.mu.Unlock()
		}
	}
}

// SubmitWAL 提交 WAL 记录（异步）
func SubmitWAL(entry *WALEntry) {
	initWALWriter()
	walWriter.ch <- entry
}

// writeEntry 写入单条 WAL 记录
func (w *WALAsyncWriter) writeEntry(entry *WALEntry) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// 懒加载文件句柄
	if w.file == nil {
		f, err := os.OpenFile(selfConfig.WalPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			logger.Error("打开 WAL 文件失败", err)
			return
		}
		w.file = f
		w.writer = bufio.NewWriterSize(f, 32*1024) // 32KB 缓冲区
	}

	var buf []byte
	switch entry.Op {
	case OpRename:
		// 重命名格式：1字节操作码 + 8字节ID + 8字节ModTime + 2字节旧路径长度 + 旧路径 + 2字节新路径长度 + 新路径
		oldPathBuf := []byte(entry.OldPath)
		newPathBuf := []byte(entry.NewPath)
		buf = make([]byte, 1+8+8+2+len(oldPathBuf)+2+len(newPathBuf))
		buf[0] = OpRename
		binary.LittleEndian.PutUint64(buf[1:9], entry.Node.ID)
		binary.LittleEndian.PutUint64(buf[9:17], uint64(entry.ModTime))

		offset := 17
		binary.LittleEndian.PutUint16(buf[offset:offset+2], uint16(len(oldPathBuf)))
		offset += 2
		copy(buf[offset:], oldPathBuf)
		offset += len(oldPathBuf)

		binary.LittleEndian.PutUint16(buf[offset:offset+2], uint16(len(newPathBuf)))
		offset += 2
		copy(buf[offset:], newPathBuf)
	case OpInvalid:
		pBuf := []byte(entry.Path)
		buf = make([]byte, 1+16+2+len(pBuf))
		buf[0] = OpInvalid
		binary.LittleEndian.PutUint64(buf[1:9], entry.Node.ID)
		binary.LittleEndian.PutUint64(buf[9:17], uint64(entry.Node.ModTime))
		binary.LittleEndian.PutUint16(buf[17:19], uint16(len(entry.Path)))
		copy(buf[19:], pBuf)

	default:
		// 其他操作：1字节操作码 + 32字节Node数据 + 2字节路径长度 + N字节路径内容
		pBuf := []byte(entry.Path)
		buf = make([]byte, 1+32+2+len(pBuf))
		buf[0] = entry.Op
		binary.LittleEndian.PutUint64(buf[1:9], entry.Node.ID)
		binary.LittleEndian.PutUint64(buf[9:17], entry.Node.ParentID)
		binary.LittleEndian.PutUint64(buf[17:25], uint64(entry.Node.Size))
		binary.LittleEndian.PutUint64(buf[25:33], uint64(entry.Node.ModTime))
		binary.LittleEndian.PutUint16(buf[33:35], uint16(len(pBuf)))
		copy(buf[35:], pBuf)
	}

	w.writer.Write(buf)
}

// FlushWAL 强制刷新 WAL 缓冲区（用于 Checkpoint 前）
func FlushWAL() {
	if walWriter != nil {
		walWriter.mu.Lock()
		defer walWriter.mu.Unlock()
		if walWriter.writer != nil {
			walWriter.writer.Flush()
			walWriter.file.Sync() // 强制落盘
		}
	}
}

// CloseWAL 关闭 WAL 写入器（程序退出时调用）
func CloseWAL() {
	if walWriter != nil {
		close(walWriter.ch)
		<-walWriter.done // 等待所有数据写入完成并刷盘
	}
}
