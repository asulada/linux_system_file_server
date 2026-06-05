package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

func CheckAndCheckpoint() {
	// 1. 检查 WAL 文件大小
	info, err := os.Stat(selfConfig.WalPath)
	if err != nil {
		if os.IsNotExist(err) {
			logger.Warn("WAL 文件不存在")
			return
		}
		logger.Errorw("读取 WAL 状态失败", zap.Error(err))
		return
	}

	// CheckPoint 前先强制刷新 WAL
	FlushWAL()
	// 2. 如果文件没超过阈值，直接返回
	if info.Size() < selfConfig.WALThreshold {
		logger.Infof("WAL 文件大小 %d 小于阈值 %d，无需执行 Checkpoint", info.Size(), selfConfig.WALThreshold)
		return
	}

	logger.Info("WAL 文件过大，开始执行 Checkpoint...")

	// 3. 执行全量快照 (SaveSnapshot 会生成 index.db)
	// 注意：SaveSnapshot 内部会加 RLock，保证数据一致性
	err = SaveSnapshot(selfConfig.DumpPath)
	if err != nil {
		logger.Errorw("快照保存失败，推迟清理 WAL", zap.Error(err))
		return
	}

	// 4. 快照保存成功后，安全清空 WAL
	// 使用原子操作：先重命名旧日志（备份），再创建新的
	walMu.Lock()
	defer walMu.Unlock()

	err = os.Remove(selfConfig.WalPath)
	if err != nil {
		logger.Errorw("清空 WAL 失败", zap.Error(err))
		return
	}

	logger.Info("Checkpoint 完成，WAL 已重置，系统已达到最简状态")
}

const (
	OpUpsert        uint8 = 1 // 新增
	OpUpdate        uint8 = 2 // 更新元数据
	OpDelete        uint8 = 3 // 删除
	OpRename        uint8 = 4 // 重命名/移动
	OpInvalid       uint8 = 5
	OpInvalidDelete uint8 = 6
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
	file, err := os.Open(selfConfig.WalPath)
	if err != nil {
		if os.IsNotExist(err) {
			logger.Warn("WAL 文件不存在，跳过重放") // ← 添加这行
		}
		logger.Errorw("打开 WAL 文件失败", zap.Error(err))
		return
	}
	defer file.Close()

	br := bufio.NewReaderSize(file, 64*1024)
	// 预分配缓冲区，减少循环内分配
	tmp := make([]byte, 64)

	for {
		op, err := br.ReadByte()
		if err == io.EOF {
			break
		}
		if err != nil {
			logger.Errorw("读取 WAL 操作码失败", zap.Error(err))
			break
		}

		switch op {
		case OpUpsert, OpUpdate, OpDelete:
			// 基础结构：ID(8) + ParentID(8) + Size(8) + ModTime(8) + pLen(2) = 34 字节
			if _, err := io.ReadFull(br, tmp[:34]); err != nil {
				goto OffsetError
			}
			var n FileNode
			n.ID = binary.LittleEndian.Uint64(tmp[0:8])
			n.ParentID = binary.LittleEndian.Uint64(tmp[8:16])
			n.Size = int64(binary.LittleEndian.Uint64(tmp[16:24]))
			n.ModTime = int64(binary.LittleEndian.Uint64(tmp[24:32]))
			pLen := binary.LittleEndian.Uint16(tmp[32:34])

			pBuf := make([]byte, pLen)
			if _, err := io.ReadFull(br, pBuf); err != nil {
				goto OffsetError
			}
			path := string(pBuf)

			switch op {
			case OpUpsert:
				ApplyToMemory(&n, path)
			case OpUpdate:
				UpdateNodeMetadata(&n, path)
			case OpDelete:
				fileSystem.Remove(path, n.IsDir())
			}

		case OpRename:
			// ID(8) + ModTime(8) + oldPLen(2) = 18 字节
			if _, err := io.ReadFull(br, tmp[:18]); err != nil {
				goto OffsetError
			}
			//id := binary.LittleEndian.Uint64(tmp[0:8])
			modTime := int64(binary.LittleEndian.Uint64(tmp[8:16]))
			oldPLen := binary.LittleEndian.Uint16(tmp[16:18])

			oldPBuf := make([]byte, oldPLen)
			if _, err := io.ReadFull(br, oldPBuf); err != nil {
				goto OffsetError
			}
			oldPath := string(oldPBuf)

			// 读新路径长度 (2字节)
			if _, err := io.ReadFull(br, tmp[:2]); err != nil {
				goto OffsetError
			}
			newPLen := binary.LittleEndian.Uint16(tmp[:2])
			newPBuf := make([]byte, newPLen)
			if _, err := io.ReadFull(br, newPBuf); err != nil {
				goto OffsetError
			}
			newPath := string(newPBuf)

			RenameNodeRebuild(oldPath, filepath.Base(oldPath), newPath, filepath.Base(newPath), fileSystem.fd, filepath.Dir(oldPath), filepath.Dir(newPath), modTime)

		case OpInvalid:
			// ID(8) + ModTime(8) + pLen(2) = 18 字节
			if _, err := io.ReadFull(br, tmp[:18]); err != nil {
				goto OffsetError
			}
			var n FileNode
			n.ID = binary.LittleEndian.Uint64(tmp[0:8])
			n.ModTime = int64(binary.LittleEndian.Uint64(tmp[8:16]))
			pLen := binary.LittleEndian.Uint16(tmp[16:18])

			pBuf := make([]byte, pLen)
			if _, err := io.ReadFull(br, pBuf); err != nil {
				goto OffsetError
			}
			MarkNodeInvalid(n, string(pBuf))

		case OpInvalidDelete:
			// ID(8) + ModTime(8) + nameLen(2) = 18 字节
			if _, err := io.ReadFull(br, tmp[:18]); err != nil {
				goto OffsetError
			}
			id := binary.LittleEndian.Uint64(tmp[0:8])
			nameLen := binary.LittleEndian.Uint16(tmp[16:18])

			nameBuf := make([]byte, nameLen)
			if _, err := io.ReadFull(br, nameBuf); err != nil {
				goto OffsetError
			}
			DeleteInvalidNode(id, string(nameBuf))

		default:
			zap.L().Warn("未知的 WAL 操作码", zap.Uint8("op", op))
			return // 遇到未知操作码建议直接停止，防止后续数据错位
		}
	}
	return

OffsetError:
	logger.Error("WAL 文件损坏或不完整，重放提前中止")
}

func DeleteInvalidNode(id uint64, name string) {
	mu.Lock()
	defer mu.Unlock()

	delete(Nodes, id)

	indexManager.RemoveFromIndex(name, id)

	zap.L().Info("WAL 重放：已删除无效名称节点", zap.Uint64("id", id), zap.String("name", name))
}
func MarkNodeInvalid(n FileNode, name string) {
	mu.Lock()
	defer mu.Unlock()
	nOff, nLen := Store.PutName(name)

	node := &FileNode{
		ID:       n.ID,
		ParentID: 0,
		Size:     0,
		ModTime:  n.ModTime,
		NameOff:  nOff,
		NameLen:  nLen,
		PathOff:  0,
		PathLen:  0,
		Invalid:  true,
	}

	SetNode(node)
	indexManager.AddToIndex(name, node.ID)
}

// UpdateNodeMetadata 仅更新节点的元数据（用于 WAL 重放时的更新操作）
func UpdateNodeMetadata(n *FileNode, path string) {
	mu.Lock()
	defer mu.Unlock()

	// 直接通过 ID 查找节点（更高效）
	node, ok := Nodes[n.ID]
	if !ok {
		// 节点不存在，降级为 Upsert
		zap.L().Warn("WAL 重放：节点不存在，降级为 Upsert", zap.Uint64("id", n.ID), zap.String("path", path))
		ApplyToMemoryUnLock(n, path)
		return
	}

	// 获取现有节点
	oldSize := node.Size

	// 只更新元数据
	node.Size = n.Size
	node.ModTime = n.ModTime

	// 更新父节点的 Size
	if oldSize != n.Size {
		fileSystem.UpsertParentSize(n.ID, n.Size-oldSize)
	}

	// 保存节点
	SetNode(&node)

}

// SaveSnapshot 将当前内存中的所有数据全量备份到磁盘

func SaveSnapshot(filePath string) error {
	// 1. 先统计有效节点数量，确保与加载时的 count 严格一致
	var validNodes []*FileNode
	for _, node := range Nodes {
		if node.ID != 0 {
			validNodes = append(validNodes, &node)
		}
	}

	f, err := os.Create(filePath + ".tmp")
	if err != nil {
		return err
	}
	defer f.Close()

	// 建议加大缓冲区，提高长路径写入速度
	bw := bufio.NewWriterSize(f, 64*1024)

	// 写入魔数
	if _, err := bw.WriteString(IndexMagic); err != nil {
		return err
	}

	// 写入【准确】的有效节点总数
	if err := binary.Write(bw, binary.LittleEndian, uint32(len(validNodes))); err != nil {
		return err
	}

	// 预定义填充字节，避免在循环内重复 make
	padding := make([]byte, 7)

	for _, node := range validNodes {
		// 批量写入基础字段
		binary.Write(bw, binary.LittleEndian, node.ID)
		binary.Write(bw, binary.LittleEndian, node.ParentID)
		binary.Write(bw, binary.LittleEndian, node.Size)
		binary.Write(bw, binary.LittleEndian, node.ModTime)

		var flags byte
		if node.Invalid {
			flags |= 0x01
		}
		bw.WriteByte(flags)
		bw.Write(padding) // 使用复用的填充切片

		// 获取名称字符串
		var name string
		if node.Invalid {
			name = Store.Get(node.NameOff, node.NameLen)
		} else {
			name = Store.Get(node.PathOff, node.PathLen)
		}

		if name == "" && !node.Invalid {
			zap.L().Error("节点路径为空", zap.Uint64("id", node.ID))
		}

		// 写入名称
		nBuf := []byte(name)
		binary.Write(bw, binary.LittleEndian, uint16(len(nBuf)))
		bw.Write(nBuf)
	}

	// 极其重要：必须检查 Flush 错误
	if err := bw.Flush(); err != nil {
		return err
	}

	// 显式关闭文件以确保句柄释放（Rename 前必须关闭）
	f.Close()

	// 原子替换
	return os.Rename(filePath+".tmp", filePath)
}

// LoadSnapshot 从磁盘加载全量数据，恢复速度极快
func LoadSnapshot(filePath string) error {
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		logger.Infow("快照文件不存在，跳过加载", "path", filePath)
		return nil
	}
	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	// 1. 增大缓冲区至 256KB，减少系统调用，长路径表现更佳
	br := bufio.NewReaderSize(f, 256*1024)

	// 2. 预分配一个通用复用缓存 (足以容纳 Magic, Count 和节点基础头部)
	// 节点基础头部：ID(8)+PID(8)+Size(8)+Time(8)+Flag(1)+Padding(7)+Len(2) = 42 字节
	tmpBuf := make([]byte, 64)

	// 读取并校验魔数 (8字节)
	if _, err := io.ReadFull(br, tmpBuf[:8]); err != nil {
		return fmt.Errorf("read magic failed: %v", err)
	}
	if string(tmpBuf[:8]) != IndexMagic {
		return fmt.Errorf("invalid snapshot magic")
	}

	// 读取总数 (4字节 uint32)
	if _, err := io.ReadFull(br, tmpBuf[:4]); err != nil {
		return fmt.Errorf("read count failed: %v", err)
	}
	count := binary.LittleEndian.Uint32(tmpBuf[:4])

	for i := uint32(0); i < count; i++ {
		var n FileNode

		// 3. 一次性读取节点所有固定长度字段 (42 字节)
		// 这样可以彻底规避缓冲区边界错位问题
		if _, err := io.ReadFull(br, tmpBuf[:42]); err != nil {
			return fmt.Errorf("node %d: read header failed: %v", i, err)
		}

		// 手动解析（无反射，极快）
		n.ID = binary.LittleEndian.Uint64(tmpBuf[0:8])
		n.ParentID = binary.LittleEndian.Uint64(tmpBuf[8:16])
		n.Size = int64(binary.LittleEndian.Uint64(tmpBuf[16:24]))
		n.ModTime = int64(binary.LittleEndian.Uint64(tmpBuf[24:32]))

		// Flag 位在第 32 字节
		n.Invalid = (tmpBuf[32] & 0x01) != 0

		// 33-39 字节是 Padding (Discard 7)，直接无视

		// 40-41 字节是 NameLen (uint16)
		strLen := binary.LittleEndian.Uint16(tmpBuf[40:42])

		// 4. 处理字符串 (长路径核心)
		strBuf := make([]byte, strLen)
		if _, err := io.ReadFull(br, strBuf); err != nil {
			return fmt.Errorf("node %d: read name failed: %v", i, err)
		}
		str := string(strBuf)

		// 5. 业务逻辑
		if n.Invalid {
			nOff, nLen := Store.PutName(str)
			n.NameOff = nOff
			n.NameLen = nLen
			SetNode(&n)
			indexManager.AddToIndex(str, n.ID)

			// 线程安全地更新最大 ID
			newID := n.GetRealID()
			for {
				current := atomic.LoadUint64(&lastID)
				if newID <= current || atomic.CompareAndSwapUint64(&lastID, current, newID) {
					break
				}
			}
		} else {
			ApplyToMemory(&n, str)
		}
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
	// 从完整路径中计算文件名的偏移量，避免重复存储
	dirLen := len(newPath) - len(newName)
	nOff := pOff + uint64(dirLen)
	nLen := len(newName)
	if nLen > 65535 {
		logger.Errorf("文件名过长: %s, 长度: %d", newName, nLen)
		return
	}
	// 1. 处理节点本身数据
	delete(PathMap, oldPathOff)
	delete(PathHashIdMap, olfHash)

	node.NameOff = nOff
	node.NameLen = uint16(nLen)
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
		logger.Errorw("新父节点不存在", "path", newPPath)
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
			logger.Errorw("打开 WAL 文件失败", zap.Error(err))
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
	case OpInvalidDelete:
		nameBuf := []byte(entry.Path)
		buf = make([]byte, 1+8+8+2+len(nameBuf))
		buf[0] = OpInvalidDelete
		binary.LittleEndian.PutUint64(buf[1:9], entry.Node.ID)
		binary.LittleEndian.PutUint64(buf[9:17], uint64(entry.Node.ModTime))
		binary.LittleEndian.PutUint16(buf[17:19], uint16(len(nameBuf)))
		copy(buf[19:], nameBuf)
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

// WriteWALInvalidDelete 专门用于写入删除无效名称的 WAL 日志
func WriteWALInvalidDelete(node *FileNode, name string) error {
	SubmitWAL(&WALEntry{
		Op:   OpInvalidDelete,
		Node: node,
		Path: name,
	})
	return nil
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
