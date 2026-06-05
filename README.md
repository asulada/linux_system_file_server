# System File Server

一个基于 Go 语言开发的Linux高性能文件系统索引和搜索服务器，支持实时文件监控、全文搜索和 Web 界面管理（web访问 http://localhost:9102/）。

## 功能特性

- 🚀 **高性能索引**: 使用内存映射和优化的数据结构实现快速文件索引
- 🔍 **智能搜索**: 支持关键词分词搜索、高亮显示和模糊匹配
- 👁️ **实时监控**: 基于 inotify 的文件系统实时监控，自动更新索引
- 💾 **持久化存储**: 支持索引数据持久化和 WAL（Write-Ahead Logging）日志
- 🔐 **JWT 认证**: 基于 JWT 的用户认证机制
- 🌐 **Web 界面**: 提供美观的 Vue + Element UI 前端界面
- 📊 **文件管理**: 支持文件删除、无效文件导出等功能
- ⚡ **并发处理**: 支持并行扫描和多线程索引构建

## 技术栈

### 后端
- **Go 1.25.7** - 编程语言
- **Gin** - Web 框架
- **Viper** - 配置管理
- **Zap** - 高性能日志库
- **JWT** - 身份认证
- **Roaring Bitmap** - 高效的位图索引
- **inotify** - Linux 文件系统监控

### 前端
- **Vue 2.7.10** - 前端框架
- **Element UI 2.15.10** - UI 组件库
- **Axios** - HTTP 客户端

## 项目结构

```
system_file_server/
├── auth/                    # 认证模块
│   └── jwt_util.go         # JWT 工具类
├── logger/                  # 日志模块
│   └── log_config.go       # 日志配置
├── main/                    # 主程序
│   ├── main.go             # 入口文件和路由配置
│   ├── file_entity.go      # 文件实体定义
│   ├── file_index.go       # 索引管理
│   ├── file_search.go      # 搜索逻辑
│   ├── file_sort.go        # 排序算法
│   ├── file_save.go        # 数据持久化
│   ├── file_string_store.go # 字符串存储
│   ├── file_task.go        # 任务处理
│   ├── file_watcher.go     # 文件监控
│   └── api_util.go         # API 工具函数
├── static/                  # 静态资源
│   └── index-go.html       # 前端页面
├── target/                  # 编译输出目录
├── go.yaml                  # 配置文件
├── go.mod                   # Go 模块依赖
└── go.sum                   # 依赖校验文件
```

## 快速开始

### 环境要求

- Go 1.25.7 或更高版本
- Linux 系统（使用 inotify 进行文件监控）
- 足够的内存用于索引存储

### 安装步骤

1. **克隆项目**
```bash
git clone <repository-url>
cd system_file_server
```

2. **安装依赖**
```bash
go mod download
```

3. **配置参数**

编辑 `go.yaml` 文件：

```yaml
# 日志配置
log:
  level: info                    # 日志级别: debug, info, warn, error
  filename: ./logs/default.log   # 日志文件路径
  maxsize: 100                   # 单个日志文件最大大小 (MB)
  maxbackups: 3                  # 保留的旧日志文件数量
  maxage: 30                     # 日志文件保留天数
  isstacktrace: true             # 是否记录堆栈信息
  isstdout: true                 # 是否输出到控制台

# 需要索引的根目录列表
roots:
  - /root/go/2
  - /root/go/5

# 排除的文件后缀
excludeSuffix:
  - .log
  - .torrent
  - .llc
  - .swp
  - .swx
  - "4913"
  - "~"

# 索引数据库路径
dumpPath: ./index.db

# 管理员账号
account: admin
password: admin

# JWT 密钥（生产环境请修改）
jwtSecret: your-super-secret-key-here-2024

# WAL 日志配置
walPath: ./index.wal
wALThreshold: 100  # WAL 文件大小阈值 (MB)
```

4. **运行服务**
```bash
go run main/main.go
```

5. **访问应用**

打开浏览器访问: `http://localhost:9102/`

默认登录账号:
- 用户名: `admin`
- 密码: `admin`

### 编译部署

**Linux 编译:**
```bash
GOOS=linux GOARCH=amd64 go build -o target/system_file_server_linux main/main.go
```

## API 接口

### 认证接口

#### 登录
- **URL**: `/filess/login`
- **方法**: `POST`
- **请求体**:
```json
{
  "name": "admin",
  "pwd": "admin"
}
```
- **响应**: 返回 JWT Token

### 文件搜索接口

#### 搜索文件
- **URL**: `/filess/search`
- **方法**: `POST`
- **认证**: 需要 JWT Token
- **请求体**:
```json
{
  "offset": 0,
  "limit": 10,
  "key": ["keyword1", "keyword2"],
  "sortBy": "time"
}
```
- **响应**: 返回搜索结果列表

### 文件管理接口

#### 创建虚拟文件
- **URL**: `/filess/create`
- **方法**: `POST`
- **认证**: 需要 JWT Token
- **请求体**:
```json
{
  "names": ["filename1", "filename2"]
}
```

#### 删除文件
- **URL**: `/filess/delete`
- **方法**: `POST`
- **认证**: 需要 JWT Token
- **请求体**:
```json
{
  "path": "/path/to/file"
}
```

#### 删除无效节点
- **URL**: `/filess/deleteInvalid`
- **方法**: `POST`
- **认证**: 需要 JWT Token
- **请求体**:
```json
{
  "id": 12345
}
```

#### 导出无效文件
- **URL**: `/filess/exportInvalid`
- **方法**: `POST`
- **认证**: 需要 JWT Token
- **响应**: 下载 JSON 文件

## 核心架构

### 索引系统

系统采用多级索引结构：

1. **FileNode**: 文件节点，存储文件元数据
2. **StringStore**: 字符串存储，统一管理文件名和路径
3. **IndexManager**: 倒排索引管理器，支持快速关键词搜索

### 持久化机制

- **Dump 文件**: 定期将内存索引序列化到磁盘
- **WAL 日志**: Write-Ahead Logging 保证数据一致性
- **增量更新**: 只记录变更操作，提高恢复速度

### 文件监控

使用 Linux inotify 机制实时监控文件系统变化：
- 文件创建
- 文件修改
- 文件删除
- 文件移动/重命名

## 性能优化

### GC（垃圾回收）优化策略

本项目针对大规模文件索引场景，实施了多项 Go GC 优化措施，显著降低了内存分配压力和 GC 停顿时间：

#### 1. **纯数值 Map 设计** 🎯
- **PathHashIdMap**: 使用 `map[uint64]uint64` 替代 `map[string]uint64`
  - Key 和 Value 都是纯数字类型
  - Go GC 会直接跳过扫描纯数值对象，大幅减少 GC 标记时间
  - 避免了字符串对象的堆分配和指针追踪开销

```go
// 优化前: map[string]uint64 - GC 需要扫描字符串指针
// 优化后: map[uint64]uint64 - GC 直接跳过，零扫描成本
PathHashIdMap = make(map[uint64]uint64, 200000)
```

#### 2. **按需生成临时字符串** ⚡
- 路径查询时采用延迟加载策略
- 只在真正需要时才从字节数组转换为字符串
- 临时字符串不会常驻堆区，不增加 GC 常驻压力

```go
func GetPathUint64(key uint64) string {
    offset := (key >> 16)
    length := uint16(key & 0xFFFF)
    // 按需产生临时 string，不会常驻堆区，不增加 GC 常驻压力
    return Store.Get(offset, length)
}
```

#### 4. **Roaring Bitmap 压缩索引** 🔥
- 使用 `github.com/RoaringBitmap/roaring` 库
- 位图压缩技术将文件 ID 集合压缩到极小空间
- 相比传统 map/set，内存占用降低 10-100 倍
- 减少 GC 需要管理的对象数量

#### 5. **StringStore 统一存储** 💾
- 所有文件名和路径集中存储在连续的字节数组中
- 通过偏移量 + 长度引用，避免重复字符串分配
- 支持 Compact 压缩机制，定期清理碎片
- 减少堆内存碎片化，提升 GC 效率

#### 6. **预分配容量** 🚀
- Map 初始化时指定合理容量：`make(map[uint64]FileNode, 200000)`
- 避免频繁扩容导致的内存重新分配
- 减少 GC 需要处理的旧对象

#### 7. **并行排序与索引构建** ⚙️
- 使用 `sync.WaitGroup` 并行执行多个排序任务
- 充分利用多核 CPU，缩短 STW（Stop-The-World）时间窗口
- 减少单次操作持续时间，间接降低 GC 压力

### 其他性能优化

- ✅ 内存映射文件减少 I/O
- ✅ 并发扫描加速索引构建
- ✅ 搜索缓存减少重复计算
- ✅ 延迟加载优化内存使用
- ✅ WAL 日志批量写入减少系统调用

## 注意事项

⚠️ **重要提示**:

1. **首次启动**: 首次启动时会扫描所有配置的根目录，可能需要较长时间
2. **内存占用**: 索引会占用一定内存，建议为大量文件预留足够内存
3. **平台限制**: 文件监控功能仅支持 Linux 系统
4. **生产环境**: 请务必修改默认的 `jwtSecret` 和管理员密码
5. **权限要求**: 确保程序有权限读取配置的根目录

## 常见问题

### Q: 如何添加新的监控目录？
A: 在 `go.yaml` 的 `roots` 配置中添加新路径，重启服务即可。

### Q: 索引文件太大怎么办？
A: 可以通过调整 `excludeSuffix` 排除不必要的文件类型，或者定期清理无效节点。

### Q: 如何查看日志？
A: 日志文件位于 `./logs/default.log`，也可以通过控制台实时查看。

### Q: 忘记管理员密码怎么办？
A: 直接修改 `go.yaml` 中的 `account` 和 `password` 字段，然后重启服务。

## 许可证

本项目仅供学习和内部使用。

## 联系方式

如有问题或建议，请联系项目维护者。
