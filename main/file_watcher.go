package main

import (
	"golang.org/x/sys/unix"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

// 1 启动入口：根据 storageExists 判断加载模式
func (f *FileSystemIndex) Start(roots []string, storagePath string) {
	fd, _ := unix.InotifyInit()

	// 1. 尝试从二进制文件加载
	err := f.Load(storagePath)

	pathChan := make(chan string, 1000)
	//加载到数据
	if err == nil && len(f.Nodes) > 0 {
		logger.Info("热启动：正在从内存恢复监听...")
		go func() {
			f.mu.RLock()
			for path, id := range f.PathMap {
				if f.Nodes[id].IsDir {
					pathChan <- path
				}
			}
			f.mu.RUnlock()
			close(pathChan)
		}()
	} else {
		logger.Info("冷启动：正在遍历磁盘初始化...")
		go f.ParallelBFSScan(roots, pathChan)
	}

	f.setupWatches(fd, pathChan)
	go f.runEventLoop(fd)
}

// 1.1 ParallelBFSScan 广度优先扫描，确保父节点先于子节点入库
func (f *FileSystemIndex) ParallelBFSScan(roots []string, pathChan chan<- string) {
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

			// 插入当前节点
			f.Upsert(path)

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
					f.mu.Lock()
					f.WdMap[wd] = path
					f.PathToWd[path] = wd
					f.mu.Unlock()
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

	f.mu.Lock()
	defer f.mu.Unlock()

	// 如果已存在，仅更新元数据（如 Size, ModTime）
	if id, exists := f.PathMap[path]; exists {
		node := f.Nodes[id]
		node.Size = info.Size()
		node.ModTime = info.ModTime().Unix()
		return
	}

	// 新增节点
	id := atomic.AddUint64(&f.lastID, 1)
	parentPath := filepath.Dir(path)

	// 通过 PathMap 查找父节点 ID
	parentID := f.PathMap[parentPath]

	node := &FileNode{
		ID: id, ParentID: parentID, Name: info.Name(), Path: path,
		IsDir: info.IsDir(), Size: info.Size(), ModTime: info.ModTime().Unix(),
		Ext: filepath.Ext(path),
	}

	f.Nodes[id] = node
	f.PathMap[path] = id

	// 1. 获取或创建 Parent 的子节点集合
	if f.TreeMap[parentID] == nil {
		f.TreeMap[parentID] = make(map[uint64]struct{})
	}

	// 2. 直接插入，Map 内部会自动处理，不需要赋值写回
	f.TreeMap[parentID][id] = struct{}{}

	f.sizeSort = true
}
func (f *FileSystemIndex) UpsertDir(fullPath string, name string, parentPath string, time time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// 如果已存在，仅更新元数据（如 Size, ModTime）
	if id, exists := f.PathMap[fullPath]; exists {
		node := f.Nodes[id]
		node.ModTime = time.Unix()
		return
	}

	// 新增节点
	id := atomic.AddUint64(&f.lastID, 1)

	// 通过 PathMap 查找父节点 ID
	parentID := f.PathMap[parentPath]

	node := &FileNode{
		ID: id, ParentID: parentID, Name: name, Path: fullPath,
		IsDir: true, Size: 0, ModTime: time.Unix(),
		Ext: filepath.Ext(name),
	}

	f.Nodes[id] = node
	f.PathMap[fullPath] = id

	// 1. 获取或创建 Parent 的子节点集合
	if f.TreeMap[parentID] == nil {
		f.TreeMap[parentID] = make(map[uint64]struct{})
	}

	// 2. 直接插入，Map 内部会自动处理，不需要赋值写回
	f.TreeMap[parentID][id] = struct{}{}

	f.sizeSort = true
}

func (f *FileSystemIndex) Remove(path string, isDir bool) {
	f.mu.Lock()
	defer f.mu.Unlock()

	id, ok := f.PathMap[path]
	if !ok {
		return
	}

	var walkDelete func(uint64)
	walkDelete = func(currID uint64) {
		// 递归清理：直接遍历 Map 的 Key
		for childID := range f.TreeMap[currID] {
			walkDelete(childID)
		}

		if node, exists := f.Nodes[currID]; exists {
			delete(f.PathMap, node.Path)
			delete(f.Nodes, currID)

			// 【新增】清理 WdMap 和 PathToWd
			if wd, exists := f.PathToWd[node.Path]; exists {
				delete(f.WdMap, wd)
				delete(f.PathToWd, node.Path)
			}
		}
		// 清理整个集合
		delete(f.TreeMap, currID)
	}

	if isDir {
		walkDelete(id)
	} else {
		// 单个文件删除：O(1) 移除父子关系
		if node, exists := f.Nodes[id]; exists {
			if children, ok := f.TreeMap[node.ParentID]; ok {
				delete(children, id) // O(1) 操作
			}
		}
		delete(f.PathMap, path)
		delete(f.Nodes, id)
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
			f.mu.RLock()
			dirPath := f.WdMap[int(event.Wd)]
			f.mu.RUnlock()

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
				} else {
					f.Upsert(fullPath)
				}
				// 若新建目录，自动加入并行监听队列或直接同步加 Watch
				if mask&(unix.IN_ISDIR|unix.IN_CREATE) != 0 {
					f.addWatch(fd, fullPath)
				}
			}
			offset += unix.SizeofInotifyEvent + event.Len
		}
	}
}

func (f *FileSystemIndex) addWatch(fd int, fullPath string) {
	wd, _ := unix.InotifyAddWatch(fd, fullPath, watchMask)
	f.mu.Lock()
	f.WdMap[wd] = fullPath
	f.PathToWd[fullPath] = wd
	f.mu.Unlock()
}

// 监听 移动事件
// 引入一个临时的 pendingMoves 缓存，用来暂存等待匹配的移动事件。
func (f *FileSystemIndex) handleMoveEvent(event *unix.InotifyEvent, fullPath string, fd int, newName string, isDir bool) {
	f.muMove.Lock()
	defer f.muMove.Unlock()

	// 1. 处理移出 (FROM)
	if (event.Mask & unix.IN_MOVED_FROM) != 0 {
		f.pendingMoves[event.Cookie] = &MoveEvent{
			OldPath: fullPath,
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
			f.RenameNode(move.OldPath, fullPath, newName)
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
func (f *FileSystemIndex) RenameNode(oldPath, newPath string, newName string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	id, ok := f.PathMap[oldPath]
	if !ok {
		return
	}

	node := f.Nodes[id]
	oldPathLen := len(oldPath)

	// 1. 处理节点本身数据
	delete(f.PathMap, oldPath)
	node.Path = newPath
	node.Name = newName
	node.Ext = filepath.Ext(newName)
	f.PathMap[newPath] = id

	// 2. 处理子项
	if node.IsDir {
		// 3 同步更新自身的 wd 索引
		if wd, exists := f.PathToWd[oldPath]; exists {
			delete(f.PathToWd, oldPath)
			f.PathToWd[newPath] = wd
			f.WdMap[wd] = newPath
		}

		var renameChild func(uint64)
		renameChild = func(pid uint64) {
			for childID := range f.TreeMap[pid] {
				childNode := f.Nodes[childID]
				childOldPath := childNode.Path

				// 安全且高效的路径拼接
				updatedPath := newPath + childOldPath[oldPathLen:]

				// 更新 PathMap 索引
				delete(f.PathMap, childOldPath)
				childNode.Path = updatedPath
				f.PathMap[updatedPath] = childID

				// 3. 【核心优化】如果是目录，直接通过 O(1) 反向索引更新 wd 信息
				if childNode.IsDir {
					if wd, exists := f.PathToWd[childOldPath]; exists {
						delete(f.PathToWd, childOldPath)
						f.PathToWd[updatedPath] = wd
						f.WdMap[wd] = updatedPath
					}
					renameChild(childID)
				}
			}
		}
		renameChild(id)
	}
	f.nameSort = true
}
