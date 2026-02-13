package main

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"os"
	"strings"
)

// 持久化 (Gob)
func (f *FileSystemIndex) Save(filePath string) error {
	f.mu.RLock()
	defer f.mu.RUnlock()

	tempPath := filePath + ".tmp"
	file, err := os.Create(tempPath)
	if err != nil {
		return err
	}
	defer file.Close()

	bw := bufio.NewWriter(file)
	// 1. 写入文件头：魔数 + 版本号
	fmt.Fprintln(bw, IndexMagic)
	fmt.Fprintln(bw, IndexVersion)

	// 2. 写入数据
	enc := gob.NewEncoder(bw)
	if err := enc.Encode(f.Nodes); err != nil {
		return err
	}
	// 写入 TimeIdx
	if err := enc.Encode(f.TimeIdx); err != nil {
		return err
	}
	// 写入 SizeIdx
	if err := enc.Encode(f.SizeIdx); err != nil {
		return err
	}
	// 写入 NameIdx
	if err := enc.Encode(f.NameIdx); err != nil {
		return err
	}
	bw.Flush()
	file.Close()

	// 3. 原子重命名：确保文件完整性
	return os.Rename(tempPath, filePath)
}

func (f *FileSystemIndex) Load(filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	br := bufio.NewReader(file)
	// 1. 校验魔数
	magic, _ := br.ReadString('\n')
	if strings.TrimSpace(magic) != IndexMagic {
		return fmt.Errorf("invalid file format")
	}

	// 2. 校验版本号
	var ver int
	fmt.Fscanf(br, "%d\n", &ver)
	if ver != IndexVersion {
		return fmt.Errorf("version mismatch: need %d, got %d", IndexVersion, ver)
	}

	// 3. 解码数据
	f.mu.Lock()
	defer f.mu.Unlock()

	dec := gob.NewDecoder(br)
	// 解码 Nodes
	if err := dec.Decode(&f.Nodes); err != nil {
		return err
	}
	// 解码 TimeIdx
	if err := dec.Decode(&f.TimeIdx); err != nil {
		return err
	}
	// 解码 SizeIdx
	if err := dec.Decode(&f.SizeIdx); err != nil {
		return err
	}
	// 解码 NameIdx
	if err := dec.Decode(&f.NameIdx); err != nil {
		return err
	}

	// 重建 PathMap 和 TreeMap
	for id, node := range f.Nodes {
		f.PathMap[node.Path] = id

		// 关键：初始化并填充 TreeMap
		if f.TreeMap[node.ParentID] == nil {
			f.TreeMap[node.ParentID] = make(map[uint64]struct{})
		}
		f.TreeMap[node.ParentID][id] = struct{}{}

		if id > f.lastID {
			f.lastID = id
		}
	}
	return nil
}
