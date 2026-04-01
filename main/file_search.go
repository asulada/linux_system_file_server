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

func NewSeachResult(name *string, node *FileNode) *SearchResult {
	return &SearchResult{
		ID:      node.GetRealID(),
		Size:    node.Size,
		Name:    *name,
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

	//logger.Info("索引长度 ", len(*targetIdx))
	results := make([]*SearchResult, 0)
	//matchCount := 0
	var searchRes SearchRes
	// 2. 遍历索引向量 (Everything 的核心搜索逻辑)
	for index, nodeIdx := range *targetIdx {
		//logger.Info("索引: ", index, " 值 ", nodeIdx)
		if nodeIdx == 0 {
			continue
		}
		if index > req.Offset {
			node, exists := Nodes[nodeIdx]
			if !exists {
				continue // 发现 ID 已不在主表，说明被删了，跳过
			}
			//logger.Infof("遍历: %s", Store.Get(node.NameOff, node.NameLen))
			// 关键词匹配
			name := Store.Get(node.NameOff, node.NameLen)
			if matchKeywords(&name, &req.Keywords) {
				//matchCount++
				//if matchCount > req.Offset {
				results = append(results, NewSeachResult(&name, &node))
				//}
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
