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

type OffsetStringChan struct {
	offset uint64
	path   string
}

func (f *FileSystemIndex) Close() {
	if f.fd != 0 {
		unix.Close(f.fd)
		f.fd = 0
	}
}

// 1 启动入口：根据 storageExists 判断加载模式
func (f *FileSystemIndex) Start(roots []string, storagePath string) {
	fd, _ := unix.InotifyInit()

	f.fd = fd
	// 1. 尝试从二进制文件加载
	err := LoadSnapshot(storagePath)
	ReplayWAL()

	//加载到数据
	if err == nil && len(Nodes) > 0 {
		logger.Info("热启动：正在从内存恢复监听...")
		pathChan := make(chan *OffsetStringChan, 10000)
		go func() {
			for pathOffset, id := range PathMap {
				node := Nodes[id]
				if node.IsDir() {
					pathChan <- &OffsetStringChan{
						offset: pathOffset,
						path:   GetPathUint64(pathOffset),
					}
				}
			}
			close(pathChan)
			logger.Info("热启动：完成")
		}()
		func() {
			mu.RLock()
			defer mu.RUnlock()
			for path := range pathChan {
				f.addWatch(fd, path.path, path.offset)
			}
		}()
	} else {
		logger.Info("冷启动：正在遍历磁盘初始化...")
		scanChan := make(chan *ScanResult, 10000)

		go f.ParallelBFSScan(roots, scanChan)
		f.setupWatches(fd, scanChan)
	}

	go f.runEventLoop(fd)
	go f.StartPersistenceTask(context.Background(), storagePath)
}

// ParallelBFSScan 执行广度优先扫描
func (f *FileSystemIndex) ParallelBFSScan(roots []string, scanChan chan<- *ScanResult) {
	logger.Info("广度优先扫描开始...", roots)

	// 1. 内部任务队列：存放待扫描的目录路径
	// 缓冲区设为 50000 左右，兼顾内存与并发吞吐
	taskQueue := make(chan string, 100000)
	var wg sync.WaitGroup

	// 2. 启动 Worker 池（消费 taskQueue，生产结果到 scanChan）
	const numWorkers = 4 // 根据磁盘性能调整，SSD 建议 8-16
	for i := 0; i < numWorkers; i++ {
		go func() {
			for path := range taskQueue {
				f.doProcess(path, taskQueue, scanChan, &wg)
				wg.Done()
			}
		}()
	}

	// 3. 注入初始根目录
	for _, root := range roots {
		info, err := os.Stat(root)
		if err != nil {
			logger.Error("根目录不可达", root, err)
			continue
		}

		// 根目录直接作为结果发送
		scanChan <- &ScanResult{
			Path:       root,
			Name:       info.Name(),
			ParentPath: filepath.Dir(root),
			IsDir:      true,
			ModTime:    info.ModTime(),
		}

		// 增加计数并存入待处理队列
		wg.Add(1)
		taskQueue <- root
	}

	// 4. 阻塞直到所有任务（包含后续发现的子目录）完成
	wg.Wait()
	close(taskQueue)
	close(scanChan) // 扫描彻底结束，关闭结果通道通知 setupWatches 退出
	logger.Info("所有目录扫描及任务分发完成")
}

// doProcess 处理单个目录的扫描逻辑
func (f *FileSystemIndex) doProcess(path string, taskQueue chan string, scanChan chan<- *ScanResult, wg *sync.WaitGroup) {
	logger.Info("广度优先扫描：发现目录", path)
	// 使用 ReadDir 减少一次 os.Stat 调用
	entries, err := os.ReadDir(path)
	if err != nil {
		logger.Error("读取目录失败", path, err)
		return
	}

	for _, entry := range entries {
		fullPath := filepath.Join(path, entry.Name())
		if entry.IsDir() {
			info, _ := entry.Info()
			// 发送目录数据到结果通道
			scanChan <- &ScanResult{
				Path:       fullPath,
				Name:       entry.Name(),
				ParentPath: path,
				IsDir:      true,
				ModTime:    info.ModTime(),
			}

			// 发现新子目录，增加全局计数
			wg.Add(1)

			// 【死锁防御】：如果任务队列满了，不能让当前 Worker 阻塞
			select {
			case taskQueue <- fullPath:
				// 成功放入队列
			default:
				// 队列满时，开启临时协程投递，确保当前 Worker 能继续消费 taskQueue
				go func(p string) { taskQueue <- p }(fullPath)
			}
		} else {
			if f.ignoreSuffix(entry.Name()) {
				continue
			}
			// 文件直接发送结果
			scanChan <- &ScanResult{
				Path:  fullPath,
				IsDir: false,
			}
		}
	}
}

// setupWatches 并行处理索引入库与 Inotify 监听
func (f *FileSystemIndex) setupWatches(fd int, scanChan <-chan *ScanResult) {
	var watchWg sync.WaitGroup
	const workers = 4

	for i := 0; i < workers; i++ {
		watchWg.Add(1)
		go func() {
			defer watchWg.Done()
			for result := range scanChan {
				if result.IsDir {
					// 1. 更新数据库/内存索引
					f.UpsertDir(result.Path, result.Name, result.ParentPath, result.ModTime)

				} else {
					//logger.Info("索引文件", result.Path)
					f.Upsert(result.Path)
				}
			}
		}()
	}

	watchWg.Wait()
	logger.Info("所有索引及监听建立完成")
}

type ScanResult struct {
	Path       string
	Name       string
	ParentPath string
	IsDir      bool
	ModTime    time.Time
}

//func (f *FileSystemIndex) setupWatches(fd int, paths <-chan string) {
//	for i := 0; i < 4; i++ { // 32个并发协程建立监听
//		go func() {
//			for path := range paths {
//				wd, err := unix.InotifyAddWatch(fd, path, watchMask)
//				if err == nil {
//					PutWd(path, wd)
//				} else {
//					logger.Error("监听目录失败", path, err)
//				}
//			}
//		}()
//	}
//	logger.Info("所有目录已添加监听")
//}

// 3. Searcher 核心逻辑：Upsert 与级联删除
func (f *FileSystemIndex) Upsert(path string) {
	info, err := os.Stat(path)
	if err != nil {
		logger.Error("索引文件失败", path, err)
		return
	}

	mu.Lock()
	defer mu.Unlock()

	offset, _ := GetPathOff(path)
	// 如果已存在，仅更新元数据（如 Size, ModTime）
	if offset != 0 {
		if id, exists := PathMap[offset]; exists {
			node := Nodes[id]
			oldSize := node.Size
			node.Size = info.Size()

			node.ModTime = info.ModTime().Unix()

			f.UpsertParentSize(id, node.Size-oldSize)
			SetNode(&node)
			WriteWAL(OpUpdate, &node, path)
			searchCache.InvalidateByID(id)
			return
		}
	}

	// 新增节点
	id := SetRealID(atomic.AddUint64(&lastID, 1), info.IsDir())
	parentPath := filepath.Dir(path)
	pOffset, _ := GetPathOff(parentPath)

	// 通过 PathMap 查找父节点 ID
	parentID := PathMap[pOffset]

	// 1. 将字符串存入字节块
	pOff, pLen, offset, ppHash := Store.PutPath(path)
	nOff, nLen := Store.PutName(info.Name())

	node := &FileNode{
		ID: id, ParentID: parentID,
		Size: info.Size(), ModTime: info.ModTime().Unix(),
		NameOff: nOff,
		NameLen: nLen,
		PathOff: pOff,
		PathLen: pLen,
	}
	SetNode(node)

	PathMap[offset] = id
	PathHashIdMap[ppHash] = offset

	// 1. 获取或创建 Parent 的子节点集合
	if TreeMap[parentID] == nil {
		TreeMap[parentID] = make(map[uint64]struct{})
	}

	// 2. 直接插入，Map 内部会自动处理，不需要赋值写回
	TreeMap[parentID][id] = struct{}{}

	ChildTreeMap[id] = parentID

	f.UpsertParentSize(id, info.Size())
	//SizeSort = true
	//NameSort = true
	indexManager.AddToIndex(info.Name(), id)
	WriteWAL(OpUpsert, node, path)
	invalidated := searchCache.InvalidateByNewFile(info.Name())
	if invalidated {
		logger.Infof("新增文件 '%s' 触发了 %d 个缓存失效", info.Name(), 1)
	}
}
func (f *FileSystemIndex) UpsertParentSize(childId uint64, size int64) {
	// 更新父节点的 Size
	for true {
		if parentID, ok := ChildTreeMap[childId]; ok {
			if childId == 0 {
				return
			}
			parentNode := Nodes[parentID]
			parentNode.Size += size
			SetNode(&parentNode)
			childId = parentID
		} else {
			return
		}
	}
}

func (f *FileSystemIndex) UpsertDir(path string, name string, parentPath string, time time.Time) {
	mu.Lock()
	defer mu.Unlock()

	offset, _ := GetPathOff(path)
	// 如果已存在，仅更新元数据（如 Size, ModTime）
	if offset != 0 {
		if id, exists := PathMap[offset]; exists {
			node := Nodes[id]
			node.ModTime = time.Unix()
			SetNode(&node)
			WriteWAL(OpUpdate, &node, path)
			searchCache.InvalidateByID(id)
		}
		// 2. 添加系统监听
		f.addWatch(f.fd, path, offset)
		return
	}

	// 新增节点
	id := SetRealID(atomic.AddUint64(&lastID, 1), true)

	// 通过 PathMap 查找父节点 ID
	parentOffset, _ := GetPathOff(parentPath)
	parentID := PathMap[parentOffset]

	// 1. 将字符串存入字节块
	pOff, pLen, offset, pHash := Store.PutPath(path)
	nOff, nLen := Store.PutName(name)

	node := &FileNode{
		ID: id, ParentID: parentID, Size: 0, ModTime: time.Unix(),
		NameOff: nOff,
		NameLen: nLen,
		PathOff: pOff,
		PathLen: pLen,
	}
	SetNode(node)

	offset = GetUint64(pOff, pLen)
	PathMap[offset] = id
	PathHashIdMap[pHash] = offset

	// 1. 获取或创建 Parent 的子节点集合
	if TreeMap[parentID] == nil {
		TreeMap[parentID] = make(map[uint64]struct{})
	}

	// 2. 直接插入，Map 内部会自动处理，不需要赋值写回
	TreeMap[parentID][id] = struct{}{}
	ChildTreeMap[id] = parentID

	f.addWatch(f.fd, path, offset)

	indexManager.AddToIndex(name, id)
	WriteWAL(OpUpsert, node, path)
	invalidated := searchCache.InvalidateByNewFile(name)
	if invalidated {
		logger.Infof("新增文件 '%s' 触发了 %d 个缓存失效", name, 1)
	}
}

func (f *FileSystemIndex) Remove(path string, isDir bool) {
	mu.Lock()
	defer mu.Unlock()

	pathOffset, hash := GetPathOff(path)
	id, ok := PathMap[pathOffset]
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
		pathOff, hash := GetPathOff(curPath)

		// 3. 清理搜索索引 (N-gram)
		indexManager.RemoveFromIndex(Store.Get(node.NameOff, node.NameLen), currID)
		WriteWAL(OpDelete, &node, curPath)

		delete(PathMap, pathOff)
		delete(PathHashIdMap, hash)

		// 3. 清理 WdMap 和 PathToWd
		DeleteWd(pathOffset)

		// 4. 更新父节点的 Size（在删除 ChildTreeMap 之前调用）
		//f.UpsertParentSize(currID, -node.Size)

		// 5. 删除父子关系
		delete(ChildTreeMap, currID)
		delete(TreeMap, currID)

		// 6. 最后删除节点本身
		delete(Nodes, currID)
		searchCache.InvalidateByID(currID)
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

			// 清理搜索索引
			indexManager.RemoveFromIndex(Store.Get(node.NameOff, node.NameLen), id)
			WriteWAL(OpDelete, &node, path)

			// 清理 PathMap 和 Nodes
			delete(PathMap, pathOffset)
			delete(PathHashIdMap, hash)

			delete(Nodes, id)

			// 清理反向索引
			delete(ChildTreeMap, id)

			// 确保清理空的子节点集合（理论上文件不应该有子节点）
			delete(TreeMap, id)
			searchCache.InvalidateByID(id)
		}
	}
}
func (f *FileSystemIndex) ignoreSuffix(name string) bool {
	//判断字符串结尾是否以 .swp结尾
	for _, suffix := range selfConfig.ExcludeSuffix {
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
			name := unix.ByteSliceToString(buf[offset+unix.SizeofInotifyEvent : offset+unix.SizeofInotifyEvent+event.Len])

			offset += unix.SizeofInotifyEvent + event.Len

			if name == "" || name == "." || name == ".." {
				continue
			}
			if f.ignoreSuffix(name) {
				continue
			}

			parentOffset := GetWdPath(int(event.Wd))
			isDir := (mask & unix.IN_ISDIR) != 0
			dirPath := GetPathUint64(parentOffset)
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
					logger.Info("创建或更新目录 ", fullPath)
					f.UpsertDir(fullPath, name, dirPath, time.Now())
				} else {
					logger.Info("创建或更新文件 ", fullPath)
					f.Upsert(fullPath)
				}
			}
		}
	}
}

func (f *FileSystemIndex) addWatch(fd int, path string, offset uint64) {
	if _, ok := PathToWd[offset]; ok {
		return
	}
	wd, err := unix.InotifyAddWatch(fd, path, watchMask)
	if err == nil {
		//logger.Info("监听目录成功 ", fullPath, " wd ", wd)
		PutWd(offset, wd)
	} else {
		logger.Error("监听目录失败", path, err)
	}
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
			info, err := os.Stat(fullPath)
			if err != nil {
				logger.Error("文件不存在", fullPath)
				return
			}
			f.RenameNode(move.OldPath, move.OldName, fullPath, fileName, fd, move.OldPPath, pPath, info.ModTime().Unix())
		} else {
			logger.Info("目录移入", fullPath)
			if isDir {
				//f.UpsertDir(fullPath, fileName, filepath.Dir(fullPath), time.Now())
				//f.addWatch(fd, fullPath)
				scanChan := make(chan *ScanResult, 10000)
				go f.ParallelBFSScan([]string{fullPath}, scanChan)
				f.setupWatches(fd, scanChan)

			} else {
				// 如果没匹配到 Cookie，说明是从外部移入，执行新增
				f.Upsert(fullPath)
			}

		}

	}
}

// addWatchLocked 内部版本：假设调用者已经持有 mu 锁
func (f *FileSystemIndex) addWatchLocked(fd int, fullPath string, pathOff uint64) {
	wd, _ := unix.InotifyAddWatch(fd, fullPath, watchMask)
	PutWd(pathOff, wd)
}

// O(1) 路径重命名 (RenameNode)
// 由于我们存储了全路径，重命名时只需修改对应的 Node。如果是目录，则需要级联修改其下所有子项的路径前缀。
func (f *FileSystemIndex) RenameNode(oldPath string, oldName string, newPath string, newName string, fd int, oldPPath string, newPPath string, time int64) {
	mu.Lock()
	defer mu.Unlock()
	oldPathOff, olfHash := GetPathOff(oldPath)
	id, ok := PathMap[oldPathOff]
	if !ok {
		return
	}

	indexManager.RemoveFromIndex(oldName, id)
	WriteWALRename(id, time, oldPath, newPath)

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
	f.UpsertParentSize(id, -node.Size) // 减去旧父节点的 size

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
		// 3 同步更新自身的 wd 索引
		DeleteWd(oldPathOff)
		f.addWatchLocked(fd, newPath, ppOff)

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
					DeleteWd(childOldPathOff)
					f.addWatchLocked(fd, updatedPath, cpPathOff)

					renameChild(childID)
					searchCache.InvalidateByID(childID)
				}
			}
		}
		renameChild(id)
	}
	f.UpsertParentSize(id, node.Size) // 加到新父节点上
	//NameSort = true
	indexManager.AddToIndex(newName, id)
	searchCache.InvalidateByID(id)
}

func ApplyToMemory(n FileNode, path string) {
	mu.Lock()
	defer mu.Unlock()

	// 1. 提取文件名 (Name)
	name := filepath.Base(path)

	// 2. 存入你的 StringStore (Store)，获取偏移量
	// 这样 Nodes 里的 NameOff 和 NameLen 就恢复了
	nOff, nLen := Store.PutName(name)
	pOff, pLen, ppOff, pHash := Store.PutPath(path)

	// 3. 更新 Node 字段
	n.NameOff = nOff
	n.NameLen = nLen
	n.PathOff = pOff
	n.PathLen = pLen

	// 4. 填充 Nodes Map
	Nodes[n.ID] = n

	// 5. 恢复路径反查 ID 的 Map
	PathMap[ppOff] = n.ID
	PathHashIdMap[pHash] = ppOff

	// 6. 恢复树形结构 (TreeMap)
	if TreeMap[n.ParentID] == nil {
		TreeMap[n.ParentID] = make(map[uint64]struct{})
	}
	TreeMap[n.ParentID][n.ID] = struct{}{}
	ChildTreeMap[n.ID] = n.ParentID

	// 7. 恢复搜索索引 (N-gram)
	indexManager.AddToIndex(name, n.ID)

	// 8. 维护全局最大 ID，确保新产生的 ID 不冲突
	realID := n.GetRealID()
	if realID > atomic.LoadUint64(&lastID) {
		atomic.StoreUint64(&lastID, realID)
	}
}

func ApplyToMemoryUnLock(n FileNode, path string) {
	// 1. 提取文件名 (Name)
	name := filepath.Base(path)

	// 2. 存入你的 StringStore (Store)，获取偏移量
	// 这样 Nodes 里的 NameOff 和 NameLen 就恢复了
	nOff, nLen := Store.PutName(name)
	pOff, pLen, ppOff, pHash := Store.PutPath(path)

	// 3. 更新 Node 字段
	n.NameOff = nOff
	n.NameLen = nLen
	n.PathOff = pOff
	n.PathLen = pLen

	// 4. 填充 Nodes Map
	Nodes[n.ID] = n

	// 5. 恢复路径反查 ID 的 Map
	PathMap[ppOff] = n.ID
	PathHashIdMap[pHash] = ppOff

	// 6. 恢复树形结构 (TreeMap)
	if TreeMap[n.ParentID] == nil {
		TreeMap[n.ParentID] = make(map[uint64]struct{})
	}
	TreeMap[n.ParentID][n.ID] = struct{}{}
	ChildTreeMap[n.ID] = n.ParentID

	// 7. 恢复搜索索引 (N-gram)
	indexManager.AddToIndex(name, n.ID)

	// 8. 维护全局最大 ID，确保新产生的 ID 不冲突
	realID := n.GetRealID()
	if realID > atomic.LoadUint64(&lastID) {
		atomic.StoreUint64(&lastID, realID)
	}
}
