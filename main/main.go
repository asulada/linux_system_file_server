package main

import (
	"errors"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"net/http"
	jwt "system_file_server/auth"
	logConfig "system_file_server/logger"
)

var (
	logger     *zap.SugaredLogger
	selfConfig Config
	fileSystem *FileSystemIndex
)

type Config struct {
	Log       logConfig.LogConfig
	Roots     []string
	DumpPath  string
	Account   string
	Password  string
	JwtSecret string `mapstructure:"jwtSecret"`
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
	v.SetDefault("dumpPath", "./dump")
	if err := v.ReadInConfig(); err != nil {
		logger.Error("err", err)
	}
	//也可以直接反序列化为Struct
	if err := v.Unmarshal(&selfConfig); err != nil {
		logger.Error("err", err)
	}
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

func main() {
	initConfig()
	initLog()

	fileSystem = NewFileSystemIndex()
	fileSystem.Start(selfConfig.Roots, selfConfig.DumpPath)

	//创建一个服务
	ginServer := gin.Default()

	// Apply the error handling middleware
	ginServer.Use(ErrorHandlingMiddleware())
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
	}
	logger.Info("9102端口启动成功")
	//服务器端口
	ginServer.Run(":9102") /*默认是8080*/

}
