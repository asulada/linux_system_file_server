package main

import (
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

// 2. 并行扫描与监听逻辑
const (
	IndexMagic   = "IDXGO125" // 魔数，标识文件类型
	IndexVersion = 1          // 版本号，结构变更时升级
	watchMask    = unix.IN_MODIFY | unix.IN_CREATE | unix.IN_DELETE |
		unix.IN_MOVE | unix.IN_ATTRIB | unix.IN_DELETE_SELF
)

// FileNode 存储在内存中的详细信息
type FileNode struct {
	ID       uint64
	ParentID uint64

	Size    int64
	ModTime int64
	//IsDir   bool
	//是否无效文件
	Invalid bool

	// 指向字节块的偏移量和长度
	NameOff uint64
	NameLen uint16
	PathOff uint64
	PathLen uint16
}

type StringStore struct {
	mu   sync.RWMutex
	Data []byte
}

// 辅助方法：处理 IsDir 标志位
const IsDirMask uint64 = 1 << 63

func (n *FileNode) IsDir() bool       { return n.ID&IsDirMask != 0 }
func (n *FileNode) GetRealID() uint64 { return n.ID &^ IsDirMask }

func SetRealID(id uint64, isDir bool) uint64 {
	if isDir {
		id |= IsDirMask // 只在是目录时设置标志位
	}
	// 是文件时什么都不做，因为 realID 最高位1本来就是 0
	return id
}
func SetNode(node *FileNode) {
	Nodes[node.ID] = *node
}

type MoveEvent struct {
	OldPath  string
	OldName  string
	OldPPath string
	Expiry   time.Time
}

var (
	mu     sync.RWMutex
	lastID uint64
	Nodes  = make(map[uint64]FileNode, 200000)
	//PathMap = make(map[string]uint64, 200000)
	// 路径偏移量 -> id
	PathMap = make(map[uint64]uint64, 200000)
	// 新增：路径哈希 -> 路径偏移量
	// 因为 Key 是 uint64，Value 是 uint32，依然是纯数字，GC 会直接跳过扫描！
	PathHashIdMap = make(map[uint64]uint64, 200000) // 修改为嵌套 Map：ParentID -> {ChildID: struct{}}
	TreeMap       = make(map[uint64]map[uint64]struct{}, 50000)
	ChildTreeMap  = make(map[uint64]uint64, 50000)

	wdMu sync.RWMutex
	//WdMap    = make(map[int]string, 50000)
	//PathToWd = make(map[string]int, 50000) // Path -> wd (用于重命名时快速更新路径)
	WdMap    = make(map[int]uint64, 50000)
	PathToWd = make(map[uint64]int, 50000) // Path -> wd (用于重命名时快速更新路径)

	// 3. 排序向量：只存 Nodes 的下标，极致省内存
	TimeIdx []uint64 // 按时间排序
	SizeIdx []uint64 // 按大小排序
	NameIdx []uint64 // 按名称排序

	SizeSort bool
	NameSort bool

	Store = &StringStore{} // ✅ 初始化为空实例

	indexManager = NewIndexManager()

	walMu sync.Mutex
)

func PutWd(offset uint64, wd int) {
	wdMu.Lock()
	defer wdMu.Unlock()
	WdMap[wd] = offset
	PathToWd[offset] = wd
}
func DeleteWd(offset uint64) {
	wdMu.Lock()
	defer wdMu.Unlock()
	if wd, exists := PathToWd[offset]; exists {
		delete(WdMap, wd)
		delete(PathToWd, offset)
	}
}
func GetWdPath(wd int) uint64 {
	wdMu.RLock()
	defer wdMu.RUnlock()
	return WdMap[wd]
}

type FileSystemIndex struct {
	fd           int
	muMove       sync.Mutex
	pendingMoves map[uint32]*MoveEvent // Cookie -> Event
}

// 根据 路径 的 偏移量 和 长度 获取 uint64 数字
func GetUint64(offset uint64, length uint16) uint64 {
	return (offset)<<16 | uint64(length)
}

func GetPathUint64(key uint64) string {
	offset := (key >> 16)
	length := uint16(key & 0xFFFF)

	// 这里会产生一个临时 string，但因为是按需产生，不会常驻堆区，不增加 GC 常驻压力
	return Store.Get(offset, length)
}

// 根据 路径字符串 获取 字符串hash值 后 ，判断是否存在 路径偏移量和长度的uint64数字
func GetPathOff(path string) (uint64, uint64) {
	offsetHash := indexManager.hash(path)
	if offset, exists := PathHashIdMap[offsetHash]; exists {
		return offset, offsetHash
	}
	//logger.Error("路径不存在 ", path)
	return 0, 0
}

func NewFileSystemIndex() *FileSystemIndex {
	return &FileSystemIndex{
		pendingMoves: make(map[uint32]*MoveEvent, 1000),
	}
}

// 性能监控装饰器
func TimeTrack(start time.Time, funcName string) {
	elapsed := time.Since(start)
	logger.Infow("方法执行时间",
		"function", funcName,
		"duration", elapsed)
}
