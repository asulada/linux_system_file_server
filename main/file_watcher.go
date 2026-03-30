package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

// 1 启动入口：根据 storageExists 判断加载模式
func (f *FileSystemIndex) Start(roots []string, storagePath string) {
	fd, _ := unix.InotifyInit()

	// 1. 尝试从二进制文件加载
	err := f.Load(storagePath)

	pathChan := make(chan string, 1000)
	//加载到数据
	if err == nil && len(Nodes) > 0 {
		logger.Info("热启动：正在从内存恢复监听...")
		go func() {
			mu.RLock()
			for path, id := range PathMap {
				node := Nodes[id]
				if node.IsDir() {
					pathChan <- path
				}
			}
			mu.RUnlock()
			close(pathChan)
		}()
	} else {
		logger.Info("冷启动：正在遍历磁盘初始化...")
		go f.ParallelBFSScan(roots, pathChan)
	}

	f.setupWatches(fd, pathChan)
	go f.runEventLoop(fd)
	go f.StartPersistenceTask(context.Background(), storagePath)
}

// 1.1 ParallelBFSScan 广度优先扫描，确保父节点先于子节点入库
func (f *FileSystemIndex) ParallelBFSScan(roots []string, pathChan chan<- string) {
	logger.Info("广度优先扫描开始...", roots)

	workQueue := make(chan string, 1000)
	limit := make(chan struct{}, 4)
	var globalWg sync.WaitGroup
	var enqueueWg sync.WaitGroup

	// 启动消费者池：固定数量的 goroutine 从队列取任务
	for i := 0; i < 4; i++ {
		go func() {
			for path := range workQueue {
				func() {
					limit <- struct{}{}
					defer func() { <-limit }()

					defer globalWg.Done()
					pathChan <- path
					logger.Info("广度优先扫描：正在处理", path)

					entries, _ := os.ReadDir(path)
					for _, entry := range entries {
						fullPath := filepath.Join(path, entry.Name())
						if entry.IsDir() {
							logger.Info("广度优先扫描：发现子目录", fullPath)
							info, _ := os.Stat(fullPath)
							f.UpsertDir(fullPath, entry.Name(), path, info.ModTime())

							enqueueWg.Add(1)
							workQueue <- fullPath
							globalWg.Add(1)
						} else {
							f.Upsert(fullPath)
						}
					}
				}()
			}
		}()
	}

	// 生产者：先加入所有根目录
	for _, root := range roots {
		info, err := os.Stat(root)
		if err != nil {
			logger.Error("根目录不存在", root, err)
			continue
		}
		f.UpsertDir(root, info.Name(), filepath.Dir(root), info.ModTime())

		enqueueWg.Add(1)
		workQueue <- root
		globalWg.Add(1)
	}

	// 等待所有入队操作完成，然后关闭队列
	go func() {
		enqueueWg.Wait()
		close(workQueue)
	}()

	globalWg.Wait()
	close(pathChan)
	logger.Info("广度优先扫描完成")
}

// 2 并行建立监听的核心逻辑
func (f *FileSystemIndex) setupWatches(fd int, paths <-chan string) {
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ { // 32个并发协程建立监听
		wg.Add(1)
		go func() {
			defer wg.Done()
			for path := range paths {
				wd, err := unix.InotifyAddWatch(fd, path, watchMask)
				if err == nil {
					mu.Lock()
					WdMap[wd] = path
					PathToWd[path] = wd
					mu.Unlock()
				} else {
					logger.Error("监听目录失败", path, err)
				}
			}
		}()
	}
	wg.Wait()
}

// 3. Searcher 核心逻辑：Upsert 与级联删除
func (f *FileSystemIndex) Upsert(path string) {
	info, err := os.Stat(path)
	if err != nil {
		return
	}

	mu.Lock()
	defer mu.Unlock()

	// 如果已存在，仅更新元数据（如 Size, ModTime）
	if id, exists := PathMap[path]; exists {
		node := Nodes[id]
		oldSize := node.Size
		node.Size = info.Size()

		node.ModTime = info.ModTime().Unix()

		f.UpsertParentSize(id, node.Size-oldSize)
		SetNode(&node)
		return
	}

	// 新增节点
	id := SetRealID(atomic.AddUint64(&lastID, 1), info.IsDir())
	parentPath := filepath.Dir(path)

	// 通过 PathMap 查找父节点 ID
	parentID := PathMap[parentPath]

	// 1. 将字符串存入字节块
	pOff, pLen := Store.Put(path)
	nOff, nLen := Store.Put(info.Name())

	node := &FileNode{
		ID: id, ParentID: parentID,
		Size: info.Size(), ModTime: info.ModTime().Unix(),
		NameOff: nOff,
		NameLen: nLen,
		PathOff: pOff,
		PathLen: pLen,
	}

	SetNode(node)
	PathMap[path] = id

	// 1. 获取或创建 Parent 的子节点集合
	if TreeMap[parentID] == nil {
		TreeMap[parentID] = make(map[uint64]struct{})
	}

	// 2. 直接插入，Map 内部会自动处理，不需要赋值写回
	TreeMap[parentID][id] = struct{}{}

	ChildTreeMap[id] = parentID

	f.UpsertParentSize(id, info.Size())
	SizeSort = true
	NameSort = true
}
func (f *FileSystemIndex) UpsertParentSize(childId uint64, size int64) {
	// 更新父节点的 Size
	for true {
		if parentID, ok := ChildTreeMap[childId]; ok {
			parentNode := Nodes[parentID]
			parentNode.Size += size
			SetNode(&parentNode)
			childId = parentID
		} else {
			return
		}
	}
}

func (f *FileSystemIndex) UpsertDir(fullPath string, name string, parentPath string, time time.Time) {
	mu.Lock()
	defer mu.Unlock()

	// 如果已存在，仅更新元数据（如 Size, ModTime）
	if id, exists := PathMap[fullPath]; exists {
		node := Nodes[id]
		node.ModTime = time.Unix()
		return
	}

	// 新增节点
	id := SetRealID(atomic.AddUint64(&lastID, 1), true)

	// 通过 PathMap 查找父节点 ID
	parentID := PathMap[parentPath]

	// 1. 将字符串存入字节块
	pOff, pLen := Store.Put(fullPath)
	nOff, nLen := Store.Put(name)

	node := &FileNode{
		ID: id, ParentID: parentID, Size: 0, ModTime: time.Unix(),
		NameOff: nOff,
		NameLen: nLen,
		PathOff: pOff,
		PathLen: pLen,
	}

	SetNode(node)
	PathMap[fullPath] = id

	// 1. 获取或创建 Parent 的子节点集合
	if TreeMap[parentID] == nil {
		TreeMap[parentID] = make(map[uint64]struct{})
	}

	// 2. 直接插入，Map 内部会自动处理，不需要赋值写回
	TreeMap[parentID][id] = struct{}{}
	ChildTreeMap[id] = parentID

	SizeSort = true
	NameSort = true

}

func (f *FileSystemIndex) Remove(path string, isDir bool) {
	mu.Lock()
	defer mu.Unlock()

	id, ok := PathMap[path]
	if !ok {
		return
	}

	var walkDelete func(uint64)
	walkDelete = func(currID uint64) {
		// 先获取节点信息，后面会用到
		node, nodeExists := Nodes[currID]
		if !nodeExists {
			// 节点不存在，直接清理集合
			delete(TreeMap, currID)
			delete(ChildTreeMap, currID)
			return
		}

		// 1. 递归清理所有子节点
		for childID := range TreeMap[currID] {
			walkDelete(childID)
		}

		// 2. 获取路径并清理索引
		curPath := Store.Get(node.PathOff, node.PathLen)
		delete(PathMap, curPath)

		// 3. 清理 WdMap 和 PathToWd
		if wd, exists := PathToWd[curPath]; exists {
			delete(WdMap, wd)
			delete(PathToWd, curPath)
		}

		// 4. 更新父节点的 Size（在删除 ChildTreeMap 之前调用）
		//f.UpsertParentSize(currID, -node.Size)

		// 5. 删除父子关系
		delete(ChildTreeMap, currID)
		delete(TreeMap, currID)

		// 6. 最后删除节点本身
		delete(Nodes, currID)
	}

	if isDir {
		// 获取要删除的目录节点
		node := Nodes[id]
		// 【核心修复】只在最外层更新一次父节点的 Size
		f.UpsertParentSize(id, -node.Size)
		// 递归删除所有子节点（不更新父节点 Size）
		walkDelete(id)
	} else {
		// 单个文件删除：O(1) 移除父子关系
		if node, exists := Nodes[id]; exists {
			// 从父节点的子节点集合中移除
			if children, ok := TreeMap[node.ParentID]; ok {
				delete(children, id) // O(1) 操作

				// 更新父节点的 Size
				f.UpsertParentSize(id, -node.Size)
			}

			// 清理 PathMap 和 Nodes
			delete(PathMap, path)
			delete(Nodes, id)

			// 清理反向索引
			delete(ChildTreeMap, id)

			// 确保清理空的子节点集合（理论上文件不应该有子节点）
			delete(TreeMap, id)
		}
	}
}
func (f *FileSystemIndex) ignoreSuffix(name string) bool {
	//判断字符串结尾是否以 .swp结尾
	for _, suffix := range selfConfig.excludeSuffix {
		if strings.HasSuffix(name, suffix) {
			return true
		}
	}
	return false
}

// 4. Watcher 核心：Inotify 事件循环
func (f *FileSystemIndex) runEventLoop(fd int) {
	buf := make([]byte, 4096)
	for {
		n, _ := unix.Read(fd, buf)
		var offset uint32
		for offset < uint32(n) {
			event := (*unix.InotifyEvent)(unsafe.Pointer(&buf[offset]))
			// 逻辑分支处理
			mask := event.Mask

			if mask == 0 {
				logger.Warn("警告：接收到无效的 inotify 事件")
				continue
			}
			mu.RLock()
			dirPath := WdMap[int(event.Wd)]
			mu.RUnlock()

			isDir := (mask & unix.IN_ISDIR) != 0

			name := unix.ByteSliceToString(buf[offset+unix.SizeofInotifyEvent : offset+unix.SizeofInotifyEvent+event.Len])

			if f.ignoreSuffix(name) {
				continue
			}
			fullPath := filepath.Join(dirPath, name)

			if mask&(unix.IN_MOVED_FROM|unix.IN_MOVED_TO) != 0 {
				logger.Info("处理移动事件：", event.Mask, fullPath)
				f.handleMoveEvent(event, fullPath, fd, name, isDir, dirPath)
			} else if mask&(unix.IN_DELETE_SELF) != 0 {
				logger.Info("处理删除事件 1 ：", event.Mask, fullPath)
				f.Remove(dirPath, isDir)
			} else if mask&unix.IN_DELETE != 0 {
				logger.Info("处理删除事件 2 ：", event.Mask, fullPath)
				f.Remove(fullPath, isDir)
			} else {
				if isDir {
					f.UpsertDir(fullPath, name, dirPath, time.Now())
					f.addWatch(fd, fullPath)
				} else {
					f.Upsert(fullPath)
				}
			}
			offset += unix.SizeofInotifyEvent + event.Len
		}
	}
}

func (f *FileSystemIndex) addWatch(fd int, fullPath string) {
	wd, _ := unix.InotifyAddWatch(fd, fullPath, watchMask)
	mu.Lock()
	defer mu.Unlock()
	WdMap[wd] = fullPath
	PathToWd[fullPath] = wd
}

// 监听 移动事件
// 引入一个临时的 pendingMoves 缓存，用来暂存等待匹配的移动事件。
func (f *FileSystemIndex) handleMoveEvent(event *unix.InotifyEvent, fullPath string, fd int, fileName string, isDir bool, pPath string) {
	f.muMove.Lock()
	defer f.muMove.Unlock()

	// 1. 处理移出 (FROM)
	if (event.Mask & unix.IN_MOVED_FROM) != 0 {
		logger.Info("目录移除", fullPath)
		f.pendingMoves[event.Cookie] = &MoveEvent{
			OldPath:  fullPath,
			OldName:  fileName,
			OldPPath: pPath,
			Expiry:   time.Now().Add(100 * time.Millisecond), // 100ms 内等待匹配
		}
		// 启动一个清理协程，如果 100ms 没匹配上，说明是移出到监控区域外，执行删除
		go func(cookie uint32, path string) {
			time.Sleep(110 * time.Millisecond)
			f.muMove.Lock()
			if _, exists := f.pendingMoves[cookie]; exists {
				delete(f.pendingMoves, cookie)
				f.muMove.Unlock()
				f.Remove(path, isDir) // 确定是删除
			} else {
				f.muMove.Unlock()
			}
		}(event.Cookie, fullPath)
		return
	}

	// 2. 处理移入 (TO)
	if (event.Mask & unix.IN_MOVED_TO) != 0 {
		if move, exists := f.pendingMoves[event.Cookie]; exists {
			logger.Info("目录移动", fullPath)
			// 【核心优化】匹配成功：执行重命名而不是删除再新建
			delete(f.pendingMoves, event.Cookie)
			f.RenameNode(move.OldPath, move.OldName, fullPath, fileName, fd, move.OldPPath, pPath)
		} else {
			logger.Info("目录移入", fullPath)
			if isDir {
				//f.UpsertDir(fullPath, fileName, filepath.Dir(fullPath), time.Now())
				//f.addWatch(fd, fullPath)
				pathChan := make(chan string, 1000)
				go f.ParallelBFSScan([]string{fullPath}, pathChan)
				f.setupWatches(fd, pathChan)

			} else {
				// 如果没匹配到 Cookie，说明是从外部移入，执行新增
				f.Upsert(fullPath)
			}

		}

	}
}

// addWatchLocked 内部版本：假设调用者已经持有 mu 锁
func (f *FileSystemIndex) addWatchLocked(fd int, fullPath string) {
	wd, _ := unix.InotifyAddWatch(fd, fullPath, watchMask)
	WdMap[wd] = fullPath
	PathToWd[fullPath] = wd
}

// O(1) 路径重命名 (RenameNode)
// 由于我们存储了全路径，重命名时只需修改对应的 Node。如果是目录，则需要级联修改其下所有子项的路径前缀。
func (f *FileSystemIndex) RenameNode(oldPath string, oldName string, newPath string, newName string, fd int, oldPPath string, newPPath string) {
	mu.Lock()
	defer mu.Unlock()

	id, ok := PathMap[oldPath]
	if !ok {
		return
	}

	node := Nodes[id]
	// 获取父节点信息
	oldParentID := node.ParentID

	oldPathLen := len(oldPath)

	// 1. 将字符串存入字节块
	pOff, pLen := Store.Put(newPath)
	nOff, nLen := Store.Put(newName)
	// 1. 处理节点本身数据
	delete(PathMap, oldPath)
	node.NameOff = nOff
	node.NameLen = nLen
	node.PathOff = pOff
	node.PathLen = pLen

	// 更新父节点的 Size
	f.UpsertParentSize(id, -node.Size) // 减去旧父节点的 size

	// 2. 更新父子关系
	// 从旧父节点的子节点集合中移除
	if oldParentChildren, exists := TreeMap[oldParentID]; exists {
		delete(oldParentChildren, id)
	}

	// 计算新父节点 ID
	newParentID, exists := PathMap[newPPath]
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

	SetNode(&node)
	PathMap[newPath] = id

	// 2. 处理子项
	if node.IsDir() {
		// 3 同步更新自身的 wd 索引
		if wd, exists := PathToWd[oldPath]; exists {
			delete(PathToWd, oldPath)
			delete(WdMap, wd)

			f.addWatchLocked(fd, newPath)
		}

		var renameChild func(uint64)
		renameChild = func(pid uint64) {
			for childID := range TreeMap[pid] {
				childNode := Nodes[childID]
				childOldPath := Store.Get(childNode.PathOff, childNode.PathLen)

				// 安全且高效的路径拼接
				updatedPath := newPath + childOldPath[oldPathLen:]

				// 更新 PathMap 索引
				delete(PathMap, childOldPath)

				cPathOff, cPathlen := Store.Put(updatedPath)
				childNode.PathOff = cPathOff
				childNode.PathLen = cPathlen
				SetNode(&childNode)
				PathMap[updatedPath] = childID

				// 3. 【核心优化】如果是目录，直接通过 O(1) 反向索引更新 wd 信息
				if childNode.IsDir() {
					if wd, exists := PathToWd[childOldPath]; exists {
						delete(PathToWd, childOldPath)
						delete(WdMap, wd)

						f.addWatchLocked(fd, updatedPath)
					}
					renameChild(childID)
				}
			}
		}
		renameChild(id)
	}
	f.UpsertParentSize(id, node.Size) // 加到新父节点上
	NameSort = true
}
