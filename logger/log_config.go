package logger

import (
	"fmt"
	"github.com/natefinch/lumberjack"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type LogConfig struct {
	Level        string `json:"level"`          // Level 最低日志等级，DEBUG<INFO<WARN<ERROR<FATAL 例如：info-->收集info等级以上的日志
	FileName     string `json:"file_name"`      // FileName 日志文件位置
	MaxSize      int    `json:"max_size"`       // MaxSize 进行切割之前，日志文件的最大大小(MB为单位)，默认为100MB
	MaxAge       int    `json:"max_age"`        // MaxAge 是根据文件名中编码的时间戳保留旧日志文件的最大天数。
	MaxBackups   int    `json:"max_backups"`    // MaxBackups 是要保留的旧日志文件的最大数量。默认是保留所有旧的日志文件（尽管 MaxAge 可能仍会导致它们被删除。）
	IsStdout     bool   `json:"is_stdout"`      // IsStdout 是否输出到控制台
	IsStackTrace bool   `json:"is_stack_trace"` // IsStackTrace 是否输出堆栈信息
}

// InitLogger 初始化Logger
func InitLogger(lCfg LogConfig) (err error) {
	writeSyncer := getLogWriter(lCfg.FileName, lCfg.MaxSize, lCfg.MaxBackups, lCfg.MaxAge, lCfg.IsStdout)
	encoder := getEncoder()
	var l = new(zapcore.Level)
	err = l.UnmarshalText([]byte(lCfg.Level))
	if err != nil {
		return
	}
	core := zapcore.NewCore(encoder, writeSyncer, l)
	var logger *zap.Logger
	if lCfg.IsStackTrace {
		logger = zap.New(core, zap.AddCaller(), zap.AddStacktrace(zap.ErrorLevel))
	} else {
		logger = zap.New(core, zap.AddCaller())
	}
	zap.ReplaceGlobals(logger)
	return
}

// 负责设置 encoding 的日志格式
func getEncoder() zapcore.Encoder {
	encodeConfig := zap.NewProductionEncoderConfig()
	encodeConfig.EncodeTime = TimeEncoder
	encodeConfig.TimeKey = "time"
	encodeConfig.EncodeLevel = zapcore.CapitalLevelEncoder
	encodeConfig.EncodeCaller = zapcore.ShortCallerEncoder
	return zapcore.NewJSONEncoder(encodeConfig)
}
func TimeEncoder(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString(t.Format("2006-01-02 15:04:05.000"))
}

// 负责日志写入的位置
func getLogWriter(filename string, maxsize, maxBackup, maxAge int, isStdout bool) zapcore.WriteSyncer {
	// 动态生成带日期的文件名
	currentDate := time.Now().Format("2006-01-02")

	// 分离文件名和后缀
	ext := filepath.Ext(filename)                       // 获取文件后缀（如 .log）
	nameWithoutExt := strings.TrimSuffix(filename, ext) // 去掉后缀的文件名

	// 拼接新的文件名：name + date + ext
	dailyFilename := fmt.Sprintf("%s.%s%s", nameWithoutExt, currentDate, ext)

	lumberJackLogger := &lumberjack.Logger{
		Filename:   dailyFilename, // 文件名包含日期
		MaxSize:    maxsize,       // 进行切割之前,日志文件的最大大小(MB为单位)
		MaxAge:     maxAge,        // 保留旧文件的最大天数
		MaxBackups: maxBackup,     // 保留旧文件的最大个数
		Compress:   false,         // 是否压缩/归档旧文件
	}
	if isStdout {
		return zapcore.NewMultiWriteSyncer(zapcore.AddSync(lumberJackLogger), zapcore.AddSync(os.Stdout))
	} else {
		return zapcore.AddSync(lumberJackLogger)
	}
}
