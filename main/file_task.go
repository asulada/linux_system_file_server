package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// getNextRunTime 计算下一个执行时间（每天指定时刻）
func getNextRunTime(hour, minute, second int) time.Time {
	now := time.Now()

	// 构造今天的执行时间
	next := time.Date(now.Year(), now.Month(), now.Day(), hour, minute, second, 0, now.Location())

	// 如果今天的执行时间已经过了，就设置为明天
	if now.After(next) || now.Equal(next) {
		next = next.Add(24 * time.Hour)
	}

	return next
}

// StartPersistenceTask 开启持久化守护任务
func (f *FileSystemIndex) StartPersistenceTask(ctx context.Context, storagePath string) {
	// 1. 计算到下一个凌晨3点的时间间隔
	nextRun := getNextRunTime(3, 0, 0)
	saveTimer := time.NewTimer(time.Until(nextRun))
	defer saveTimer.Stop()

	// 2. 监听系统中断信号 (Ctrl+C 或 kill)
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	logger.Info("持久化守护协程已启动，下次执行时间: ", nextRun.Format("2006-01-02 15:04:05"))

	for {
		select {
		case <-saveTimer.C:
			// 清理无用字节块
			logger.Infof("执行定时清理无用字节块...")
			Store.Compact()

			// 定时保存
			logger.Infof("执行定时自动保存...")
			CheckAndCheckpoint()

			// 重置定时器到下一个凌晨3点
			nextRun = getNextRunTime(3, 0, 0)
			saveTimer.Reset(time.Until(nextRun))
			logger.Info("下次执行时间: ", nextRun.Format("2006-01-02 15:04:05"))

		case sig := <-sigChan:
			Cancel.Store(true)
			// 捕获到退出信号
			logger.Infof("收到信号 [%v]，正在执行安全退出前的数据保存...", sig)
			start := time.Now()

			if err := SaveSnapshot(storagePath); err != nil {
				logger.Error("退出前保存失败: ", err)
			} else {
				// 快照保存成功后，清理 WAL 文件
				walMu.Lock()
				if err := os.Remove(selfConfig.WalPath); err == nil {
					logger.Info("WAL 文件已清理")
				}
				walMu.Unlock()
			}

			logger.Infof("数据已安全持久化，耗时: %v", time.Since(start))
			os.Exit(0)

		case <-ctx.Done():
			// 如果父 context 被取消（用于程序内部逻辑退出）
			if err := SaveSnapshot(storagePath); err == nil {
				walMu.Lock()
				os.Remove(selfConfig.WalPath)
				walMu.Unlock()
			}
			return
		}
	}
}
