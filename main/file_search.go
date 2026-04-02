package main

import (
	"strings"
)

// SearchRequest 搜索请求参数
type SearchReq struct {
	Keywords []string `json:"key"`
	SortBy   string   `json:"sortBy"` // "time", "size", "name"
	Offset   int      `json:"offset"`
	Limit    int      `json:"limit"`
}

type SearchResult struct {
	ID      uint64 `json:"id"`
	Name    string `json:"name"`
	Size    int64  `json:"size"`
	Path    string `json:"path"`
	ModTime int64  `json:"modTime"`
	IsDir   bool   `json:"isDir"`
}

type SearchRes struct {
	Results []*SearchResult `json:"results"`
	Offset  int             `json:"offset"`
}

func NewSeachResult(name string, node *FileNode) *SearchResult {
	return &SearchResult{
		ID:      node.GetRealID(),
		Size:    node.Size,
		Name:    name,
		Path:    Store.Get(node.PathOff, node.PathLen),
		ModTime: node.ModTime,
		IsDir:   node.IsDir(),
	}
}

// SearchPaged 分页多关键词搜索

func (f *FileSystemIndex) Search(req SearchReq) *SearchRes {
	mu.RLock()
	defer mu.RUnlock()

	// 1. 选择索引向量
	var targetIdx *[]uint64
	switch req.SortBy {
	case "size":
		if SizeSort {
			f.RebuildSizeIndexes()
		}
		targetIdx = &SizeIdx
	case "time":
		targetIdx = &TimeIdx
	default:
		if NameSort {
			f.RebuildNameIndexes()
		}
		targetIdx = &NameIdx
	}

	results := make([]*SearchResult, 0, req.Limit)
	var searchRes SearchRes
	// 2. 遍历索引向量 (Everything 的核心搜索逻辑)
	for index, nodeIdx := range *targetIdx {
		if nodeIdx == 0 {
			continue
		}
		if index > req.Offset {
			node, exists := Nodes[nodeIdx]
			if !exists {
				continue // 发现 ID 已不在主表，说明被删了，跳过
			}
			// 关键词匹配
			name := Store.Get(node.NameOff, node.NameLen)
			if MatchKeywordsZeroAlloc(name, req.Keywords) {
				results = append(results, NewSeachResult(name, &node))
				if len(results) >= req.Limit {
					searchRes.Offset = index
					break // 达标即止，极速响应
				}
			}
		}
	}
	searchRes.Results = results
	return &searchRes
}

func matchKeywords(fileName *string, keywords []string) bool {
	for _, keyword := range keywords {
		if !strings.Contains(strings.ToLower(*fileName), keyword) {
			return false
		}
	}
	return true
}

// MatchKeywordsZeroAlloc 零内存分配的关键词匹配
// 要求：传入的 keywords 必须预先处理为全小写
func MatchKeywordsZeroAlloc(fileName string, keywords []string) bool {
	for _, k := range keywords {
		if !containsIgnoreCase(fileName, k) {
			return false
		}
	}
	return true
}

// containsIgnoreCase 模拟 strings.Contains 但忽略大小写，且不产生新字符串
func containsIgnoreCase(s, substr string) bool {
	if len(substr) > len(s) {
		return false
	}

	// 利用标准库的高效实现，结合 EqualFold 思想
	// 注意：如果 substr 是纯英文小写，可以显著提升速度
	for i := 0; i <= len(s)-len(substr); i++ {
		// 尝试匹配子串
		if strings.HasPrefix(s[i:], substr) { // 理想情况：直接匹配
			return true
		}
		// 如果直接匹配失败，进行逐字符的大小写不敏感对比
		// 这里的优化点：如果性能极其敏感，可以使用更底层的字节对比
		if len(s[i:]) >= len(substr) && strings.EqualFold(s[i:i+len(substr)], substr) {
			return true
		}
	}
	return false
}
