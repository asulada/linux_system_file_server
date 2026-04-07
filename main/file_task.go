package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// StartPersistenceTask 开启持久化守护任务
func (f *FileSystemIndex) StartPersistenceTask(ctx context.Context, storagePath string) {
	// 1. 创建每 10 分钟触发一次的定时器
	saveTicker := time.NewTicker(24 * time.Hour)
	defer saveTicker.Stop()

	// 2. 监听系统中断信号 (Ctrl+C 或 kill)
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	logger.Info("持久化守护协程已启动...")

	for {
		select {
		case <-saveTicker.C:
			//清理无用字节块
			logger.Infof("执行定时清理无用字节块...")
			Store.Compact()

			// 清理失效排序
			logger.Infof("执行定时清理失效排序...")
			f.SortCheckAndCompact()

			// 定时保存
			logger.Infof("执行定时自动保存...")
			CheckAndCheckpoint()

		case sig := <-sigChan:
			// 捕获到退出信号
			logger.Infof("n收到信号 [%v]，正在执行安全退出前的数据保存...", sig)
			start := time.Now()

			CloseWAL()
			//if err := SaveSnapshot(storagePath); err != nil {
			//	logger.Error("退出前保存失败: ", err)
			//}
			logger.Infof("数据已安全持久化，耗时: %v", time.Since(start))
			os.Exit(0)

		case <-ctx.Done():
			// 如果父 context 被取消（用于程序内部逻辑退出）
			SaveSnapshot(storagePath)
			return
		}
	}
}
