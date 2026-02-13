package main

import (
	"golang.org/x/sys/unix"
	"sync"
	"time"
)

// 2. 并行扫描与监听逻辑
const (
	IndexMagic   = "IDX-GO125" // 魔数，标识文件类型
	IndexVersion = 1           // 版本号，结构变更时升级
	watchMask    = unix.IN_MODIFY | unix.IN_CREATE | unix.IN_DELETE |
		unix.IN_MOVE | unix.IN_ATTRIB | unix.IN_DELETE_SELF | unix.IN_MOVE_SELF
)

// FileNode 存储在内存中的详细信息
type FileNode struct {
	ID       uint64
	ParentID uint64
	Name     string
	Path     string // 全路径，换取搜索和清理的 O(1) 性能
	Ext      string
	Size     int64
	ModTime  int64
	IsDir    bool
}

type MoveEvent struct {
	OldPath string
	Expiry  time.Time
}

type FileSystemIndex struct {
	mu      sync.RWMutex
	lastID  uint64
	Nodes   map[uint64]*FileNode
	PathMap map[string]uint64
	// 修改为嵌套 Map：ParentID -> {ChildID: struct{}}
	TreeMap  map[uint64]map[uint64]struct{}
	WdMap    map[int]string
	PathToWd map[string]int // Path -> wd (用于重命名时快速更新路径)

	muMove       sync.Mutex
	pendingMoves map[uint32]*MoveEvent // Cookie -> Event

	// 3. 排序向量：只存 Nodes 的下标，极致省内存
	TimeIdx []uint64 // 按时间排序
	SizeIdx []uint64 // 按大小排序
	NameIdx []uint64 // 按名称排序

	sizeSort bool
	nameSort bool
}

func NewFileSystemIndex() *FileSystemIndex {
	return &FileSystemIndex{
		Nodes:   make(map[uint64]*FileNode, 200000),
		PathMap: make(map[string]uint64, 200000),
		TreeMap: make(map[uint64]map[uint64]struct{}, 50000),
		WdMap:   make(map[int]string, 50000),
	}
}
