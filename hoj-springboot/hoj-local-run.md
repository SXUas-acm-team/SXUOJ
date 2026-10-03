# SXUOJ 本地 Java 开发

## 项目架构

| 目录 | 作用 | 启动入口 / 端口 |
| --- | --- | --- |
| `api` | 公共实体、DTO、工具和依赖；不独立启动 | Maven 模块 `hoj-api` |
| `DataBackup` | OJ 主后端：用户、题目、比赛、提交、管理接口 | `top.hcode.hoj.DataBackupApplication`，6688 |
| `JudgeServer` | 独立判题服务；执行代码还需要 Linux 判题沙箱 | `top.hcode.hoj.JudgeServerApplication`，8088 |
| `../hoj-vue` | Vue 2 前端，`/api` 代理到本机后端 | 8066 |
| `../hoj-scrollBoard` | 比赛滚榜页面 | 独立页面 |

主后端连接 MySQL 与 Redis，正常部署通过 Nacos 获取配置及发现判题节点。项目使用 Spring Boot 2.2.6、Spring Cloud Hoxton.SR1 和 Java 8 源码/字节码；本地采用 JDK 8。

## 当前机器的 IDEA 配置

- 项目目录：`Q:\Projects\SXUOJ\hoj-springboot`。
- 项目 SDK：`SXUOJ-JDK8`，路径 `Q:\Projects\SXUOJ\.tools\jdk8u504-b01`。
- Java language level / 编译目标：8；编码 UTF-8，Lombok 注解处理已启用。
- Maven 使用 IDEA 自带版本，设置文件为 `../.mvn/settings.xml`；Maven 导入使用 IDEA 内置运行时，运行使用项目 JDK 8。
- 运行配置：`HOJ Backend Local`，入口 `DataBackupApplication`，工作目录为 `hoj-springboot`，激活 `dev,local`。

配置文件已写入 `.idea` 和 `.run`。如果当前 IDEA 仍显示旧 SDK 或运行配置，关闭并重新打开此项目，再刷新 Maven 项目。选择 `HOJ Backend Local` 后即可运行或调试。

## 本地环境与 Nacos

`dev` 使用小云确认的开发地址 `100.77.29.68`，MySQL 端口 `16603`、Redis 端口 `17291`；`local` 在 bootstrap 阶段关闭 Nacos 配置、服务发现及自动注册，业务代码同时跳过 Nacos 客户端连接和发布。生产配置默认行为保留。

本地配置还关闭远程判题账号重置、语言表升级和后台定时维护。`local` 应放在激活配置列表的最后：`dev,local`。

本地连接池使用 1 个初始连接、最多 10 个连接，MySQL 连接超时为 5 秒、读超时为 10 秒，Redis 超时为 5 秒；未配置 SMTP 账号，因此关闭邮件健康检查。MySQL/Redis 健康检查仍保留。

覆盖数据库或 Redis 连接时，在 `hoj-springboot` 放置 `.env`（已忽略，不提交）：

```dotenv
MYSQL_HOST=100.77.29.68
MYSQL_PORT=16603
MYSQL_PUBLIC_HOST=100.77.29.68
MYSQL_PUBLIC_PORT=16603
MYSQL_DB=hoj
MYSQL_USERNAME=your_dev_username
MYSQL_PASSWORD=your_dev_password
REDIS_HOST=100.77.29.68
REDIS_PORT=17291
REDIS_PASSWORD=your_dev_password
NACOS_ENABLED=false
```

`.env` 在 Spring Boot 2.2 的环境准备阶段加载；操作系统环境变量和 Java 系统属性优先于文件值。工作目录需为 `hoj-springboot`，也可用 `-Dhoj.dotenv.directory=绝对目录` 指定文件目录。

## Windows 命令行启动

从仓库根目录在 PowerShell 执行：

```powershell
# 编译全部 Java 模块
.\hoj-springboot\local.ps1 Build

# 启动主后端，Ctrl+C 停止
.\hoj-springboot\local.ps1 Run
```

脚本自动选取项目 `.tools` 中的 JDK 8 和 IDEA 自带 Maven；其他机器可传 `-JavaHome`、`-MavenHome`。`Run` 使用已有 JAR，修改源码后先执行 `Build`；没有 JAR 时会自动构建。

后端地址：`http://localhost:6688`；业务接口示例：`http://localhost:6688/api/get-website-config`。安全修复后 `/actuator/health` 受 JWT 认证保护，匿名访问返回 401；不能用匿名健康请求判断数据库是否恢复，可通过公开只读数据库接口检查。

## 判题服务的范围

主后端可单独用于接口开发。禁用 Nacos 后不发现判题节点，因此本地提交不能完成评测；完整判题还需要 JudgeServer、直连/发现方案、判题数据目录和 `localhost:5050` 的 Linux 沙箱。

JudgeServer 已提供 `local` 的 Nacos 禁用配置，独立启动时仍需显式提供原先由 Nacos 下发的 `hoj.db.*`、`hoj.judge.*` 配置；当前一键运行配置启动主后端。

## 验证与日志

针对性单元测试不连接数据库或 Nacos：

```powershell
$env:JAVA_HOME = 'Q:\Projects\SXUOJ\.tools\jdk8u504-b01'
& 'C:\Program Files\JetBrains\IntelliJ IDEA 2026.2.3\plugins\maven-plugin\lib\maven3\bin\mvn.cmd' `
  -f .\hoj-springboot\pom.xml -s .\.mvn\settings.xml -B -ntp package `
  '-DskipTests=false' '-Dtest=DotenvEnvironmentPostProcessorTest,NacosSwitchConfigTest' `
  '-Dsurefire.failIfNoSpecifiedTests=false'
```

通常后端日志在工作目录的 `hoj_backend`。本次自动验证的控制台、构建输出与进程记录位于仓库 `.tools` 目录。

### 本次验证（2026-10-03）

- Java 8 下全部 Maven 模块构建通过，4 项 dotenv 测试与 2 项 Nacos 禁用测试通过。
- `local.ps1 Run` 已实际启动主后端，6688 端口和网站配置接口返回正常，未发现 Nacos 连接。
- 开发 MySQL `100.97.103.105:3306` 可建立 TCP 连接，但不返回 MySQL 握手；独立协议检测与 JDBC 线程栈均确认等待在认证之前。数据库接口返回 500，健康接口返回 503，需确认开发地址/端口与服务状态后继续联调。
- 临时验证进程已停止，6688 留给 IDEA 启动使用。IDEA 窗口工具无法可靠读取设置控件，配置文件已准备，重新打开项目后加载。

### 链接恢复后的复测（2026-10-03）

- 已重新使用 JDK8 和 `dev,local` 启动最终构建，Nacos 关闭，启动数据库更新和定时维护禁用。
- 网站配置 HTTP/业务状态均为 200；语言、公开题目分页、公告和 Redis 缓存读取接口均为 500。MySQL/Redis TCP 连接后返回 EOF，尚未收到协议握手/响应，不能记为认证或业务验证通过。
- 本机到开发地址的当前路由为 Meta 虚拟网卡、下一跳 `198.18.0.1`，Clash Verge 运行配置为 global；Tailscale 运行中，但没有列出 `100.97.103.105` 对应节点。未改全局网络、代理或服务端配置，等待确认目标节点对当前电脑的可达性及实际连接方式。
- 复测只查询公开配置与业务数据，不输出正文或个人信息；未执行数据库写入。原始摘要位于 `.tools/security/restored-connectivity-summary.json`，接口结果位于 `.tools/security/restored-api-results.json`。

### 新开发地址验证（2026-10-03）

- 根据小云更正，开发地址改为 `100.77.29.68`；MySQL `16603`，Redis `17291`。当前 dev 默认值、别名和文档示例均已同步，Java 三模块重新打包通过。
- Tailscale 已找到节点；MySQL 返回有效握手，Redis 返回需认证响应，网络与协议连通已验证。
- 后端使用 `dev,local` 启动通过，网站配置接口返回 200。真实数据库查询仍返回 500：配置中的 MySQL 账号被拒绝，Redis 返回 `ERR invalid password`。当前缺少这套服务的有效凭据或 MySQL 远程授权。
- 已准备未跟踪的 `hoj-springboot/.env`，填写 `MYSQL_USERNAME`、`MYSQL_PASSWORD`、`REDIS_PASSWORD` 后无需重新编译，重新启动即可加载。默认库名为 `hoj`；若实际不同，修改 `MYSQL_DB`。
- 本次只执行公开只读查询；临时测试进程已停止，未改全局网络或服务端权限。证据位于 `.tools/security/corrected-connectivity-summary.json` 和 `corrected-api-results.json`。

### 正确凭据后的联调结果（2026-10-03）

- 使用小云提供的凭据重新启动后，MySQL `100.77.29.68:16603`、Redis `100.77.29.68:17291` 均已通过认证。凭据保存在 Git 忽略的 `.env`，启动控制台未出现其值。
- 9 个真实业务 GET 接口全部 HTTP/业务状态 200：网站配置、Redis 其它比赛缓存、语言、训练分类、标签分类、题目分页、比赛分页、训练分页及公告。
- 7 项实际安全检查全部通过：分页请求 101 被限制至 100、公开题目 SPJ/判题额外文件为空、匿名私有代码拒绝、无效 JWT 拒绝、未登录 multipart 上传拒绝、缺失图片/附件 404 且保留安全响应头。
- 本轮只执行只读业务查询，未更新数据库、语言表或运行后台维护；Nacos、远程判题保持关闭。SMTP、远程 OJ、管理员登录/写入及 Linux 沙箱未进行集成测试。
- `.env` 无需重新编译；IDEA 的 `HOJ Backend Local` 会从 `hoj-springboot` 工作目录加载它。临时验证进程在联调完成后停止，6688 留给 IDEA。
- 状态证据保存在 `.tools/security/authenticated-integration-summary.json`、`authenticated-api-results.json`、`authenticated-security-checks.json`，不含响应正文或用户信息。

### 空密码错误修复（2026-10-03）

- 后续日志中的 MySQL `using password: NO` 和 Redis `NOAUTH Authentication required` 对应本地 `.env` 的两个密码字段为空；已恢复用户授权的凭据，未写入受版本控制的文件。
- 重新启动后，语言、题目分页、公告和近期其它比赛 4 个接口的 HTTP/业务状态均为 200，证据见 `.tools/security/empty-password-repair-results.json`。
- IDEA SDK 表已补回 `SXUOJ-JDK8`，项目 SDK 改为该 JDK，Maven Runner 与共享运行配置使用同一套 JDK 8。需要重新启动 IDEA 加载全局 SDK 配置。
- 如果 IDEA 已打开 `.env`，先从磁盘重新加载再保存，避免旧缓冲区覆盖已恢复的值。密码配置只在进程启动时读取，修改后需重启后端。
