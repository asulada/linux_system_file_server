package logger

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/natefinch/lumberjack"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
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

// dailyRotateWriter 实现了按天滚动的日志写入器
type dailyRotateWriter struct {
	baseFilename string // 基础文件名（不含日期）
	ext          string // 文件扩展名
	currentDate  string // 当前日期
	currentFile  *lumberjack.Logger
	maxSize      int
	maxBackups   int
	maxAge       int
	isStdout     bool
	mu           sync.Mutex
}

// Write 实现 io.Writer 接口
func (w *dailyRotateWriter) Write(p []byte) (n int, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// 检查日期是否变化
	now := time.Now()
	today := now.Format("2006-01-02")

	if today != w.currentDate {
		// 日期变化，关闭旧文件并创建新文件
		if w.currentFile != nil {
			w.currentFile.Close()
		}
		w.currentDate = today
		w.currentFile = w.createLogger()
	}

	return w.currentFile.Write(p)
}

// Sync 实现 zapcore.WriteSyncer 接口
// lumberjack.Logger 本身不支持 Sync，这里不做任何操作
func (w *dailyRotateWriter) Sync() error {
	// lumberjack 内部会在每次 Write 后自动 flush
	// 如果需要强制落盘，可以在应用退出时调用 Close()
	return nil
}

// createLogger 创建新的 lumberjack.Logger 实例
func (w *dailyRotateWriter) createLogger() *lumberjack.Logger {
	dailyFilename := fmt.Sprintf("%s.%s%s", w.baseFilename, w.currentDate, w.ext)

	return &lumberjack.Logger{
		Filename:   dailyFilename,
		MaxSize:    w.maxSize,
		MaxAge:     w.maxAge,
		MaxBackups: w.maxBackups,
		Compress:   false,
	}
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
	ext := filepath.Ext(filename)
	nameWithoutExt := strings.TrimSuffix(filename, ext)

	rotateWriter := &dailyRotateWriter{
		baseFilename: nameWithoutExt,
		ext:          ext,
		currentDate:  time.Now().Format("2006-01-02"),
		maxSize:      maxsize,
		maxBackups:   maxBackup,
		maxAge:       maxAge,
		isStdout:     isStdout,
	}

	rotateWriter.currentFile = rotateWriter.createLogger()

	if isStdout {
		// dailyRotateWriter 已实现 WriteSyncer 接口，直接使用
		return zapcore.NewMultiWriteSyncer(
			rotateWriter,
			zapcore.AddSync(os.Stdout),
		)
	}
	// 直接返回，因为 dailyRotateWriter 已经实现了 WriteSyncer
	return rotateWriter
}
