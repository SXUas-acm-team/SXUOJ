# 本次部署适配结论

适合现有 Linux amd64 Docker Compose 部署：前后端作为网站和业务服务更新，原 MySQL、Redis、Nacos、文件卷与 JudgeServer 编译器/沙箱继续使用。后台密码校验、封禁榜单过滤和保密访问均发生在后端，不依赖重新安装评测机。

这次修复了三处实际部署问题：

- 旧 Compose 把 `1015` 挂在前端静态目录，导致镜像里的新版页面被遮住。新版活动目录改为 `/opt/hoj/frontend`，替换镜像即换页面；原目录保留。Docker 的这项行为见[挂载说明](https://docs.docker.com/engine/storage/bind-mounts/)。
- 新镜像兼容原后端地址变量，恢复独立滚榜的全部静态资源，并让管理员在封存期通过同源登录凭据请求滚榜数据。
- 前端支持已有 HTTPS 网关，也支持显式挂载证书的容器内 HTTPS；错误 upstream、无效端口和缺少证书时拒绝启动，不悄悄降级。

原备份的三项共享凭据未通过现有安全策略，不能直接断言该备份只换前后端就可上线。准备工具仅在显式 `--write` 时写入受限备份、更新 `.env` 并生成 SQL，不自动改库。SQL 修改 Nacos 的 `users` 账号，随后用新环境重建所有 Nacos 客户端；原 JudgeServer 镜像可以保留。Nacos 1.4.2 的账号表结构见[官方 SQL](https://raw.githubusercontent.com/alibaba/nacos/1.4.2/distribution/conf/nacos-mysql.sql)。

多机评测需保留 Nacos 和数据库对评测机的内网可达性，按实际服务地址与端口设白名单。Compose 内部通过服务地址通信，发布到宿主机的端口与内部端口用途不同，见[Docker 网络说明](https://docs.docker.com/compose/how-tos/networking/)。本次不改变评测沙箱的特权配置，也不把本地测试的禁用配置中心参数用于线上。

验证范围：使用生产备份的独立副本做业务接口与镜像更新验收；另用一次性容器测试旧挂载、旧变量、API 代理、滚榜资源和 HTTPS，使用合成账号验证密钥准备/SQL/回退。没有连接真实生产服务，没有使用真实远程 OJ 账号，也没有据此声称真实评测机给出 AC。上线仍需提交一份已知正确程序验证实际编译、沙箱和结果回写。
