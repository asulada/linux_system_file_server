package main

import (
	"strings"
)

// SearchRequest 搜索请求参数
type SearchReq struct {
	Keywords []string
	SortBy   string // "time", "size", "name"
	Offset   int
	Limit    int
}

// SearchPaged 分页多关键词搜索

func (f *FileSystemIndex) Search(req SearchReq) []*FileNode {
	//f.mu.RLock()
	//defer f.mu.RUnlock()

	// 1. 选择索引向量
	var targetIdx *[]uint64
	switch req.SortBy {
	case "size":
		if sizeSort {
			f.RebuildSizeIndexes()
		}
		targetIdx = &SizeIdx
	case "time":
		targetIdx = &TimeIdx
	default:
		if nameSort {
			f.RebuildNameIndexes()
		}
		targetIdx = &NameIdx
	}

	var results []*FileNode
	matchCount := 0

	// 2. 遍历索引向量 (Everything 的核心搜索逻辑)
	for _, nodeIdx := range *targetIdx {
		node, exists := Nodes[nodeIdx]
		if !exists {
			continue // 发现 ID 已不在主表，说明被删了，跳过
		}
		// 关键词匹配
		if matchKeywords(Store.Get(node.NameOff, node.NameLen), &req.Keywords) {
			matchCount++
			if matchCount > req.Offset {
				results = append(results, node)
			}
			if len(results) >= req.Limit {
				break // 达标即止，极速响应
			}
		}
	}
	return results
}

func matchKeywords(fileName *string, keywords *[]string) bool {
	for _, keyword := range *keywords {
		if !strings.Contains(*fileName, keyword) {
			return false
		}
	}
	return true
}

//func (f *FileSystemIndex) Search(keyword string) (results []FileNode) {
//	f.mu.RLock()
//	defer f.mu.RUnlock()
//	for _, node := range f.Nodes {
//		if strings.Contains(node.Name, keyword) {
//			results = append(results, *node)
//		}
//	}
//	return
//}
