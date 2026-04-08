package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"sync/atomic"
	jwt "system_file_server/auth"
	logConfig "system_file_server/logger"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

var (
	logger     *zap.SugaredLogger
	selfConfig Config
	fileSystem *FileSystemIndex
	re         = regexp.MustCompile(`^[^\p{L}\p{N}]+|[^\p{L}\p{N}]+$`)
)

type Config struct {
	Log           logConfig.LogConfig
	Roots         []string
	DumpPath      string
	Account       string
	Password      string
	JwtSecret     string `mapstructure:"jwtSecret"`
	ExcludeSuffix []string
	WalPath       string
	WALThreshold  int64
}

func initLog() *zap.SugaredLogger {
	err := logConfig.InitLogger(selfConfig.Log)
	if err != nil {
		fmt.Println(err)
	}
	// L()：获取全局logger
	logger := zap.L()
	zap.ReplaceGlobals(logger)
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
	v.SetDefault("dumpPath", "./dump")
	if err := v.ReadInConfig(); err != nil {
		logger.Errorw("err", zap.Error(err))
	}
	//也可以直接反序列化为Struct
	if err := v.Unmarshal(&selfConfig); err != nil {
		logger.Errorw("err", zap.Error(err))
	}
	selfConfig.WALThreshold *= 1024 * 1024
	jwt.SetJwtSecret(selfConfig.JwtSecret)
}

func moveChar(name *string) *string {
	// 使用正则表达式去除两边的符号，只保留字母、数字和其他语言文字
	nameStr := re.ReplaceAllString(*name, "")
	return &nameStr
}

// 自定义 BasicAuth 中间件
func BasicAuthMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// 获取 Authorization 头部  satoken=VqYmmj3GWjlubAcXsBkoP4KZHtdpFTHIv6NPmcZbRHkzOcbjxnViPxpJMeRb
		authHeader := c.GetHeader("token")
		if authHeader == "" {
			c.AbortWithStatus(http.StatusUnauthorized)
			return
		}

		_, err := jwt.ParseToken(authHeader)
		if err != nil {
			c.AbortWithStatus(http.StatusUnauthorized)
			return
		}
		// 身份验证成功，继续执行后续处理
		c.Next()
	}
}

func login(context *gin.Context) {
	reqBody := getJson(context)
	name := reqBody["name"]
	pwd := reqBody["pwd"].(string)
	if name, ok := name.(string); ok {
		if selfConfig.Account == name && selfConfig.Password == pwd {
			if token, err := jwt.GenerateToken(1); err == nil {
				OkResponse(context, http.StatusOK, "登录成功", token)
			} else {
				OkResponse(context, http.StatusInternalServerError, "登录失败", nil)
			}
		} else {
			OkResponse(context, http.StatusUnauthorized, "用户名或者密码错误", nil)
		}
	} else {
		OkResponse(context, http.StatusBadRequest, "无效的用户名", nil)
	}
}

// 将[]interface{}转换为[]string的方法
func convertToStringSlice(data interface{}) ([]string, error) {
	// 断言类型为[]interface{}
	interfaceSlice, ok := data.([]interface{})
	if !ok {
		return nil, errors.New("数据不是[]interface{}类型")
	}

	// 转换为[]string
	var stringSlice []string
	for _, v := range interfaceSlice {
		str, ok := v.(string)
		if !ok {
			return nil, errors.New("元素不是字符串类型")
		}
		stringSlice = append(stringSlice, str)
	}

	return stringSlice, nil
}

func search(c *gin.Context) {
	var req SearchReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"code": 400,
			"msg":  "参数错误：" + err.Error(),
		})
		return
	}
	result := fileSystem.Search(req)
	SendResponse(c, http.StatusOK, "", result)
}

func create(context *gin.Context) {
	reqBody := getJson(context)
	names := reqBody["names"]

	nameSlice, err := convertToStringSlice(names)
	if err != nil {
		OkResponse(context, http.StatusBadRequest, "参数错误："+err.Error(), nil)
		return
	}

	if len(nameSlice) == 0 {
		SendResponse(context, http.StatusBadRequest, "名称列表不能为空", nil)
		return
	}

	mu.Lock()
	defer mu.Unlock()
	successCount := 0
	for _, nameStr := range nameSlice {
		cleanedName := moveChar(&nameStr)
		if cleanedName == nil || *cleanedName == "" {
			logger.Warn("跳过空名称")
			continue
		}
		saveName(*cleanedName)
		successCount++
	}

	SendResponse(context, http.StatusOK, fmt.Sprintf("成功保存 %d 个无效名称", successCount), nil)
}
func saveName(name string) {
	id := atomic.AddUint64(&lastID, 1)
	nOff, nLen := Store.PutName(name)
	node := FileNode{
		ID: id, ParentID: 0,
		Size: 0, ModTime: time.Now().Unix(),
		NameOff: nOff,
		NameLen: nLen,
		PathOff: 0,
		PathLen: 0,
		Invalid: true,
	}
	SetNode(&node)
	indexManager.AddToIndex(name, id)
	searchCache.InvalidateByNewFile(name)
	WriteWALInvalid(&node, name)
}

func exportInvalid(context *gin.Context) {

	var invalidNodes []string
	for _, node := range Nodes {
		if node.Invalid {
			name := Store.Get(node.NameOff, node.NameLen)
			invalidNodes = append(invalidNodes, name)
		}
	}

	jsonData, err := json.MarshalIndent(gin.H{
		"count": len(invalidNodes),
		"data":  invalidNodes,
	}, "", "  ")
	if err != nil {
		logger.Errorw("JSON 序列化失败", zap.Error(err))
		SendResponse(context, http.StatusInternalServerError, "导出失败", nil)
		return
	}
	filename := fmt.Sprintf("invalid_nodes_%s.json", time.Now().Format("20060102_150405"))

	context.Header("Content-Description", "File Transfer")
	context.Header("Content-Transfer-Encoding", "binary")
	context.Header("Content-Disposition", fmt.Sprintf("attachment; filename=%s", filename))
	context.Header("Content-Type", "application/json")
	context.Data(http.StatusOK, "application/json", jsonData)
}

func deleteFile(context *gin.Context) {
	reqBody := getJson(context)
	path := reqBody["path"]
	pathStr := path.(string)
	info, err := os.Stat(pathStr)
	if err != nil {
		OkResponse(context, http.StatusInternalServerError, "获取文件信息出错", nil)
		return
	}
	if info.IsDir() {
		err = os.RemoveAll(pathStr)
	} else {
		err = os.Remove(pathStr)
	}
	if err != nil {
		OkResponse(context, http.StatusInternalServerError, "删除错误", err)
		logger.Errorw("", zap.Error(err))
		return
	}
	SendResponse(context, http.StatusOK, "删除成功", err)
}
func deleteInvalid(context *gin.Context) {
	reqBody := getJson(context)
	id := reqBody["id"]
	mu.Lock()
	defer mu.Unlock()
	// 【修复】将 float64 转换为 uint64
	var nodeID uint64
	switch v := id.(type) {
	case float64:
		nodeID = uint64(v)
	case uint64:
		nodeID = v
	default:
		logger.Errorw("无效的 ID 类型", "id", id)
		SendResponse(context, http.StatusBadRequest, "无效的 ID 类型", nil)
		return
	}
	node, exists := Nodes[nodeID]
	if !exists {
		SendResponse(context, http.StatusNotFound, "节点不存在", nil)
		return
	}

	if !node.Invalid {
		SendResponse(context, http.StatusBadRequest, "该节点不是无效节点", nil)
		return
	}
	name := Store.Get(node.NameOff, node.NameLen)

	indexManager.RemoveFromIndex(name, nodeID)

	delete(Nodes, nodeID)

	searchCache.InvalidateByID(nodeID)
	WriteWALInvalidDelete(&node, name)

	SendResponse(context, http.StatusOK, "删除成功", nil)
}
func TimingMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()

		c.Next()

		duration := time.Since(start)
		logger.Infow("接口耗时",
			"path", c.Request.URL.Path,
			"method", c.Request.Method,
			"status", c.Writer.Status(),
			"duration", duration.String(),
			"duration_ms", float64(duration.Nanoseconds())/1e6,
		)
	}
}

func main() {
	initConfig()
	logger = initLog()
	defer logger.Sync() // 别忘了退出前刷新缓存

	fileSystem = NewFileSystemIndex()
	fileSystem.Start(selfConfig.Roots, selfConfig.DumpPath)

	//创建一个服务
	ginServer := gin.Default()

	// Apply the error handling middleware
	ginServer.Use(ErrorHandlingMiddleware())
	ginServer.Use(TimingMiddleware())

	// 自定义404错误处理
	ginServer.NoRoute(func(c *gin.Context) {
		c.JSON(http.StatusNotFound, gin.H{
			"message": "Page not found",
		})
	})
	files := ginServer.Group("/filess")
	{
		//访问地址，处理我们的请求 Request Response
		files.POST("/login", login)
		files.POST("/search", BasicAuthMiddleware(), search)
		files.POST("/create", BasicAuthMiddleware(), create)
		files.POST("/delete", BasicAuthMiddleware(), deleteFile)
		files.POST("/deleteInvalid", BasicAuthMiddleware(), deleteInvalid)
		files.POST("/exportInvalid", BasicAuthMiddleware(), exportInvalid)
	}
	logger.Info("9102端口启动成功")
	//服务器端口
	ginServer.Run(":9102") /*默认是8080*/
}
