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
	Total   int             `json:"total"`
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
	pageResult := indexManager.Search(req.Keywords, req.SortBy, req.Offset, req.Limit)

	results := make([]*SearchResult, 0, len(pageResult.IDs))

	if len(pageResult.IDs) == 0 {
		return &SearchRes{
			Results: results,
			Offset:  pageResult.Total,
			Total:   pageResult.Total,
		}
	}

	mu.RLock()
	for _, nodeIdx := range pageResult.IDs {
		if nodeIdx == 0 {
			continue
		}

		node, exists := Nodes[nodeIdx]
		if !exists {
			continue
		}

		name := Store.Get(node.NameOff, node.NameLen)
		results = append(results, NewSeachResult(name, &node))
	}
	mu.RUnlock()

	return &SearchRes{
		Results: results,
		Offset:  pageResult.Offset,
		Total:   pageResult.Total,
	}
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
