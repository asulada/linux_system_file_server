package main

import (
	"context"
	"os"
	"path/filepath"
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
	queue := roots
	limit := make(chan struct{}, 4) // 限制并发数
	var globalWg sync.WaitGroup     // 全局 WaitGroup

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:] // 取出当前路径

		globalWg.Add(1)
		go func(path string) {
			pathChan <- path
			defer globalWg.Done()
			limit <- struct{}{}
			defer func() { <-limit }()

			logger.Info("广度优先扫描：正在处理", path)
			// 插入当前节点
			info, err := os.Stat(path)
			if err != nil {
				logger.Error("广度优先扫描失败", path)
				return
			}
			f.UpsertDir(path, info.Name(), filepath.Dir(path), info.ModTime())

			// 读取子目录并加入队列
			entries, _ := os.ReadDir(path)
			for _, entry := range entries {
				fullPath := filepath.Join(path, entry.Name())
				if entry.IsDir() {
					queue = append(queue, fullPath) // 动态扩展队列
				} else {
					f.Upsert(fullPath) // 文件直接插入
				}
			}
		}(current)
	}

	globalWg.Wait() // 等待所有任务完成
	close(pathChan)
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
		Nodes[id] = node
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

	node := FileNode{
		ID: id, ParentID: parentID,
		Size: info.Size(), ModTime: info.ModTime().Unix(),
		NameOff: nOff,
		NameLen: nLen,
		PathOff: pOff,
		PathLen: pLen,
	}

	Nodes[id] = node
	PathMap[path] = id

	// 1. 获取或创建 Parent 的子节点集合
	if TreeMap[parentID] == nil {
		TreeMap[parentID] = make(map[uint64]struct{})
	}

	// 2. 直接插入，Map 内部会自动处理，不需要赋值写回
	TreeMap[parentID][id] = struct{}{}

	ChildTreeMap[id] = parentID

	f.UpsertParentSize(id, info.Size())
	sizeSort = true
	nameSort = true
}
func (f *FileSystemIndex) UpsertParentSize(childId uint64, size int64) {
	// 更新父节点的 Size
	if parentID, ok := ChildTreeMap[childId]; ok {
		parentNode := Nodes[parentID]
		parentNode.Size += size
		Nodes[parentID] = parentNode
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

	Nodes[id] = *node
	PathMap[fullPath] = id

	// 1. 获取或创建 Parent 的子节点集合
	if TreeMap[parentID] == nil {
		TreeMap[parentID] = make(map[uint64]struct{})
	}

	// 2. 直接插入，Map 内部会自动处理，不需要赋值写回
	TreeMap[parentID][id] = struct{}{}
	ChildTreeMap[id] = parentID

	sizeSort = true
	nameSort = true

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
		// 递归清理：直接遍历 Map 的 Key
		for childID := range TreeMap[currID] {
			walkDelete(childID)
		}

		delete(ChildTreeMap, currID)
		if node, exists := Nodes[currID]; exists {
			curPath := Store.Get(node.PathOff, node.PathLen)
			delete(PathMap, curPath)
			delete(Nodes, currID)
			f.UpsertParentSize(currID, -node.Size)

			// 【新增】清理 WdMap 和 PathToWd
			if wd, exists := PathToWd[curPath]; exists {
				delete(WdMap, wd)
				delete(PathToWd, curPath)
			}
		}
		// 清理整个集合
		delete(TreeMap, currID)
	}

	if isDir {
		walkDelete(id)
	} else {
		// 单个文件删除：O(1) 移除父子关系
		if node, exists := Nodes[id]; exists {
			if children, ok := TreeMap[node.ParentID]; ok {
				delete(children, id) // O(1) 操作
				f.UpsertParentSize(id, -node.Size)
			}
		}
		delete(ChildTreeMap, id)
		delete(PathMap, path)
		delete(Nodes, id)
	}
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
			fullPath := filepath.Join(dirPath, name)

			if mask&(unix.IN_MOVED_FROM|unix.IN_MOVED_TO) != 0 {
				f.handleMoveEvent(event, fullPath, fd, name, isDir)
			} else if mask&(unix.IN_DELETE_SELF|unix.IN_MOVE_SELF) != 0 {
				f.Remove(dirPath, isDir)
			} else if mask&unix.IN_DELETE != 0 {
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
	WdMap[wd] = fullPath
	PathToWd[fullPath] = wd
	mu.Unlock()
}

// 监听 移动事件
// 引入一个临时的 pendingMoves 缓存，用来暂存等待匹配的移动事件。
func (f *FileSystemIndex) handleMoveEvent(event *unix.InotifyEvent, fullPath string, fd int, fileName string, isDir bool) {
	f.muMove.Lock()
	defer f.muMove.Unlock()

	// 1. 处理移出 (FROM)
	if (event.Mask & unix.IN_MOVED_FROM) != 0 {
		f.pendingMoves[event.Cookie] = &MoveEvent{
			OldPath: fullPath,
			OldName: fileName,
			Expiry:  time.Now().Add(100 * time.Millisecond), // 100ms 内等待匹配
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
			// 【核心优化】匹配成功：执行重命名而不是删除再新建
			delete(f.pendingMoves, event.Cookie)
			f.RenameNode(move.OldPath, move.OldName, fullPath, fileName)
		} else {
			// 如果没匹配到 Cookie，说明是从外部移入，执行新增
			f.Upsert(fullPath)

			if isDir {
				f.addWatch(fd, fullPath)
			}

		}

	}
}

// O(1) 路径重命名 (RenameNode)
// 由于我们存储了全路径，重命名时只需修改对应的 Node。如果是目录，则需要级联修改其下所有子项的路径前缀。
func (f *FileSystemIndex) RenameNode(oldPath string, oldName string, newPath string, newName string) {
	mu.Lock()
	defer mu.Unlock()

	id, ok := PathMap[oldPath]
	if !ok {
		return
	}

	node := Nodes[id]
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

	PathMap[newPath] = id

	// 2. 处理子项
	if node.IsDir() {
		// 3 同步更新自身的 wd 索引
		if wd, exists := PathToWd[oldPath]; exists {
			delete(PathToWd, oldPath)
			PathToWd[newPath] = wd
			WdMap[wd] = newPath
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
				PathMap[updatedPath] = childID

				// 3. 【核心优化】如果是目录，直接通过 O(1) 反向索引更新 wd 信息
				if childNode.IsDir() {
					if wd, exists := PathToWd[childOldPath]; exists {
						delete(PathToWd, childOldPath)
						PathToWd[updatedPath] = wd
						WdMap[wd] = updatedPath
					}
					renameChild(childID)
				}
			}
		}
		renameChild(id)
	}
	nameSort = true
}
