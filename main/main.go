package main

import (
	"fmt"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	logConfig "system_file_server/logger"
)

var (
	logger     *zap.SugaredLogger
	selfConfig Config
)

type Config struct {
	Log logConfig.LogConfig
}

func initLog() *zap.SugaredLogger {
	err := logConfig.InitLogger(selfConfig.Log)
	if err != nil {
		fmt.Println(err)
	}
	// L()：获取全局logger
	logger := zap.L()
	return logger.Sugar()
}

func initConfig() {
	//读取yaml文件
	v := viper.New()
	//设置读取的配置文件名
	//v.SetConfigName("go")
	v.SetConfigFile("go.yaml")
	//windows环境下为%GOPATH，linux环境下为$GOPATH
	//v.AddConfigPath("/Users/yangyue/workspace/go/src/webDemo/")
	//设置配置文件类型
	//v.SetConfigType("yaml")

	v.SetDefault("log.level", "info")
	v.SetDefault("log.filename", "./logs/default.log")
	v.SetDefault("log.maxsize", 100)
	v.SetDefault("log.maxbackups", 3)
	v.SetDefault("log.maxage", 30)
	v.SetDefault("log.isstacktrace", true)
	v.SetDefault("log.isstdout", true)
	if err := v.ReadInConfig(); err != nil {
		logger.Error("err", err)
	}
	//也可以直接反序列化为Struct
	if err := v.Unmarshal(&selfConfig); err != nil {
		logger.Error("err", err)
	}
}

func main() {
	initConfig()
	initLog()

}
