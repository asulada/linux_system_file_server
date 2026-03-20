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
	mu.RLock()
	defer mu.RUnlock()

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
	if err := enc.Encode(Nodes); err != nil {
		return err
	}
	// 写入 TimeIdx
	if err := enc.Encode(TimeIdx); err != nil {
		return err
	}
	// 写入 SizeIdx
	if err := enc.Encode(SizeIdx); err != nil {
		return err
	}
	// 写入 NameIdx
	if err := enc.Encode(NameIdx); err != nil {
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
	mu.Lock()
	defer mu.Unlock()

	dec := gob.NewDecoder(br)
	// 解码 Nodes
	if err := dec.Decode(&Nodes); err != nil {
		return err
	}
	// 解码 TimeIdx
	if err := dec.Decode(&TimeIdx); err != nil {
		return err
	}
	// 解码 SizeIdx
	if err := dec.Decode(&SizeIdx); err != nil {
		return err
	}
	// 解码 NameIdx
	if err := dec.Decode(&NameIdx); err != nil {
		return err
	}

	// 重建 PathMap 和 TreeMap
	for id, node := range Nodes {
		PathMap[Store.Get(node.PathOff, node.PathLen)] = id

		// 关键：初始化并填充 TreeMap
		if TreeMap[node.ParentID] == nil {
			TreeMap[node.ParentID] = make(map[uint64]struct{})
		}
		TreeMap[node.ParentID][id] = struct{}{}

		ChildTreeMap[id] = node.ParentID
		if id > lastID {
			lastID = id
		}
	}
	return nil
}
