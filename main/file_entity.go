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

	Size    int64
	ModTime int64
	//IsDir   bool

	// 指向字节块的偏移量和长度
	NameOff uint32
	NameLen uint16
	PathOff uint32
	PathLen uint16
}

// 辅助方法：处理 IsDir 标志位
const IsDirMask uint64 = 1 << 63

func (n *FileNode) IsDir() bool       { return n.ID&IsDirMask != 0 }
func (n *FileNode) GetRealID() uint64 { return n.ID &^ IsDirMask }

func SetRealID(id uint64, isDir bool) uint64 {
	if isDir {
		id |= IsDirMask // 只在是目录时设置标志位
	}
	// 是文件时什么都不做，因为 realID 最高位本来就是 0
	return id
}

type MoveEvent struct {
	OldPath string
	OldName string
	Expiry  time.Time
}

var (
	mu      sync.RWMutex
	lastID  uint64
	Nodes   = make(map[uint64]FileNode, 200000)
	PathMap = make(map[string]uint64, 200000)
	// 修改为嵌套 Map：ParentID -> {ChildID: struct{}}
	TreeMap = make(map[uint64]map[uint64]struct{}, 50000)

	ChildTreeMap = make(map[uint64]uint64, 50000)
	WdMap        = make(map[int]string, 50000)
	PathToWd     = make(map[string]int, 50000) // Path -> wd (用于重命名时快速更新路径)

	// 3. 排序向量：只存 Nodes 的下标，极致省内存
	TimeIdx = make([]uint64, 200000) // 按时间排序
	SizeIdx = make([]uint64, 200000) // 按大小排序
	NameIdx = make([]uint64, 200000) // 按名称排序

	sizeSort bool
	nameSort bool

	Store *StringStore
)

type FileSystemIndex struct {
	muMove       sync.Mutex
	pendingMoves map[uint32]*MoveEvent // Cookie -> Event
}

func NewFileSystemIndex() *FileSystemIndex {
	return &FileSystemIndex{
		pendingMoves: make(map[uint32]*MoveEvent, 1000),
	}
}
