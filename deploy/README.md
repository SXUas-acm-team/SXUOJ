# SXUOJ 后端 Docker 构建与升级

线上更新按“检查数据库表 → 替换前后端镜像”操作；原 Compose、`.env`、密码、挂载和评测机继续沿用。不要用本地测试 Compose 覆盖线上部署。

数据库迁移文件均在本目录：

- [contest-security-schema.sql](contest-security-schema.sql)：保密赛权限与题目资源保护表。
- [contest-identity-v1.1.0.sql](contest-identity-v1.1.0.sql)：实名核验配置、身份绑定、豁免邀请码与豁免记录四张表。

在 OJ 正在使用的数据库中执行相应 SQL；已经建好且结构一致的表无需重建。缺表时后端会拒绝启动。所有新表都就绪后，只修改原 Compose 中前后端服务的 `image` 为要部署的明确版本，并在原目录执行：

```bash
docker compose pull hoj-backend hoj-frontend
docker compose up -d --no-deps --force-recreate hoj-backend hoj-frontend
docker compose ps hoj-backend hoj-frontend
```

原来使用的 `-f`、`--env-file`、`-p` 参数继续带上。只修改仓库源码、未发布新镜像时，不会更新线上容器。

实名核验接口地址和 API 密钥在比赛设置中填写；密钥不会进入公开源码。后端的 `/hoj/log/backend/.runtime-secrets/identity.key` 应随日志卷保留并与数据库一同备份，多实例部署需共享同一份加密密钥。后端需要能解析核验域名并访问 HTTPS。

这套流程面向**已经运行的 HOJ/SXUOJ Docker Compose 部署**：GitHub Actions 自动测试、打包和发布后端镜像，服务器用一条命令替换 `hoj-backend`。原 Compose 文件、数据库、Redis、Nacos、判题容器、网络、端口和数据卷继续沿用。

## 1. 自动构建

提交到 `master` / `main`，推送 `v*` 标签，或在 GitHub Actions 手动运行 **Backend jars and Docker image**：

1. 验证部署脚本。
2. 在 Java 8 / Maven 构建容器中执行指定的离线回归测试，打包 api、DataBackup、JudgeServer。
3. 上传 Actions 产物 `backend-jars-<commit>`，包含 `backend.jar`、`judgeserver.jar`、`SHA256SUMS`。
4. 用随机命名的临时 Compose 工程演练升级、失败恢复、回滚及数据卷保留，通过后清理测试资源。
5. 发布 `linux/amd64` 和 `linux/arm64` 主后端镜像：

```text
YOUR_DOCKERHUB_USERNAME/sxuoj-backend:v1.1.0
YOUR_DOCKERHUB_USERNAME/sxuoj-backend:v<版本号>
```

镜像只在推送版本 Git 标签（例如 `v1.1.0`）时发布，并使用相同版本号；分支提交仅验证构建，不再发布日期、commit 或 `latest` 标签。生产使用明确版本号或镜像 digest。PR 只运行验证，不登录仓库、不发布镜像。自动发布需要仓库变量 `DOCKERHUB_USERNAME` 和仓库 Secret `DOCKERHUB_TOKEN`；未配置用户名时只运行构建验证。不要把服务器密码或生产 `.env` 放进 Actions。私有镜像的生产拉取使用单独只读凭据。

工作流文件：[`backend-image.yml`](../.github/workflows/backend-image.yml)。所有构建只读取源码白名单；实际 `.env`、密钥文件、日志、历史数据和本机工具不会进入 Docker 构建上下文。

## 2. 服务器准备（一次）

要求 Linux、Docker Engine、Docker Compose v2.20+ 和 Python 3.8+，运行账号必须能操作 Docker，并能写原 Compose 所在目录。把本目录复制到服务器，或拉取本仓库；**不要用新建的 Compose 工程去覆盖旧工程**。

先确认原配置的路径、服务名和项目名：

```bash
docker compose ls
docker compose -f /opt/hoj/docker-compose.yml config --services
```

下文假设原配置为 `/opt/hoj/docker-compose.yml`、原变量文件为 `/opt/hoj/.env`、服务名为 `hoj-backend`。替换成实际路径即可。原部署如果使用了多个 `-f`，按原顺序全部传入；如果用过 `-p my-oj`，每次加上 `-p my-oj`。脚本会检查已有容器的 Compose 标签，路径、顺序、项目或服务不符时提前退出。

不要输出完整 `docker compose config` 到共享日志，它包含插值后的密码。脚本内部读取配置，不打印密钥值。

## 3. 导入环境变量

两种文件用途不同：

| 参数 | 用途 |
| --- | --- |
| `--env-file /opt/hoj/.env` | 沿用原 Compose 的 `${变量}` 插值，与之前部署保持一致；可重复传入 |
| `--runtime-env /opt/hoj/backend.env` | 把文件中的变量直接导入后端容器；同名值覆盖原 Compose 的 `environment` |

只换镜像、不改参数时，沿用原 `.env` 就够了。需要增加参数时，把 [`.env.example`](.env.example) 复制成服务器上的 `backend.env`，只取消需要覆盖的行的注释，例如：

```dotenv
JAVA_OPTS=-Xms192m -Xmx768m
```

```bash
chmod 600 /opt/hoj/.env /opt/hoj/backend.env
```

文件由 Compose 解析，**不会作为 shell 脚本执行**。含字面 `$`、`#` 的密码使用单引号，例如 `MYSQL_PASSWORD='literal$example#value'`。成功升级后脚本记住 `--runtime-env` 的路径，下次可以省略；加 `--clear-runtime-env` 可停止导入。修改文件后必须再次执行升级才会应用，不会自动重启服务。

保留现有数据库、Redis、Nacos 和判题共享令牌。当前代码要求 `JWT_TOKEN_SECRET` 为至少 64 个随机字节的 Base64、`JUDGE_TOKEN` 至少 32 字符、`NACOS_PASSWORD` 至少 12 字符。旧环境若仍用 `default` 或弱密钥，先协调修改真实配置与关联服务；脚本会提前拒绝，不会随机生成一套新凭据替换线上值。`MYSQL_PASSWORD` 与旧 `MYSQL_ROOT_PASSWORD` 都兼容。

本流程保持 `SPRING_PROFILES_ACTIVE=prod` 与已有 Nacos，**不支持只设置 `NACOS_ENABLED=false` 就切换成无配置中心生产部署**。现有后端启动逻辑会将部分环境配置写回 Nacos，并可能更新远程判题账号，所以不能用空白模板覆盖原部署配置。

## 4. 一键替换后端镜像

更新前必须执行并复检本目录相应的数据库迁移，再更新后端和前端。部署脚本不执行 SQL 迁移；缺表时新后端会拒绝启动。建议使用已验证的版本标签或 digest，避免直接跟随 `latest`。

先做只读检查（不拉镜像、不创建快照、不重建容器）：

```bash
python3 deploy/backend.py update \
  -f /opt/hoj/docker-compose.yml --env-file /opt/hoj/.env \
  --image YOUR_DOCKERHUB_USERNAME/sxuoj-backend:v1.1.0 --dry-run
```

确认路径后，一条命令升级；需要导入新变量时附加 `--runtime-env /opt/hoj/backend.env`：

```bash
python3 deploy/backend.py update \
  -f /opt/hoj/docker-compose.yml --env-file /opt/hoj/.env \
  --image YOUR_DOCKERHUB_USERNAME/sxuoj-backend:v1.1.0
```

脚本会拉取镜像、核验镜像标识、记录当前容器的**实际镜像 ID**和环境，然后执行仅针对后端的 `up -d --no-deps --no-build --pull never`。新容器必须通过匿名 `/api/get-website-config` 探针；启动失败、探针失败或超时会自动恢复旧镜像与旧环境。可用 `--timeout 360` 调整等待时间。单个后端容器重建会有短暂中断，不是零停机滚动升级。

更新不执行 `down`、`prune`、SQL 迁移，也不重建依赖服务。原有自定义 `entrypoint` / `command` 会保留；如果它硬编码了旧镜像 jar 或脚本路径，先调整为新镜像的默认入口 `/usr/local/bin/hoj-backend`，后端 jar 位于 `/opt/hoj/backend.jar`。`JAVA_OPTS` 只按空白分词，禁止 shell 展开和命令替换；含空格的 JVM 参数使用 `JAVA_TOOL_OPTIONS`。

探针确认 HTTP 服务可用，**不替代登录、提交判题、数据库、邮件等业务验收**。上线后仍应完成这些检查。

## 5. 回滚与日常管理

启用过保密比赛或实名参赛校验后（包括验收赛），回退目标必须具备同样的权限校验能力。旧后端会忽略新表并可能暴露封存内容或绕过赛时核验；更新失败时的自动恢复目标也受此限制。若恢复点仍是旧后端，先停止相关比赛的对外访问，再协调回退，不能直接执行下面的回滚命令。保留数据库新表和加密密钥文件。

```bash
python3 deploy/backend.py status \
  -f /opt/hoj/docker-compose.yml --env-file /opt/hoj/.env

python3 deploy/backend.py rollback \
  -f /opt/hoj/docker-compose.yml --env-file /opt/hoj/.env
```

快照保存在原 Compose 目录的 `.sxuoj-deploy/<项目名>/<服务名>/`：

- `current.compose.json`：当前成功发布的解析后配置。
- `previous.compose.json`：上次运行的配置和实际镜像 ID，手动回滚使用它。
- `recovery.compose.json`：本次操作前保存的恢复点，意外中断或自动回滚失败时保留。
- `release.json`：当前镜像与运行变量文件路径。
- `origin.json`：原 Compose 文件列表与顺序，防止后续升级误用其他配置。

快照目录权限为 `0700`，文件为 `0600`，**包含实际凭据，不要提交或公开**。保留旧镜像；执行 Docker 镜像清理后可能失去离线回滚能力。回滚只恢复容器配置和镜像，不回滚业务数据或 Nacos 中的写入。

更新后，原 Compose 的 `image` 字段仍然指向原镜像，因此后续发布继续用本脚本；不要对后端直接执行原文件的 `up`，否则可能切回旧镜像。日常查看 `logs` / `ps` 使用原项目参数即可。需要直接重建当前版本时，显式使用当前快照，并保持项目名：

```bash
docker compose -p <原项目名> \
  -f /opt/hoj/.sxuoj-deploy/<原项目名>/hoj-backend/current.compose.json \
  up -d --no-deps --no-build --pull never hoj-backend
```

若发布进程被强制终止，先确认没有另一个升级进程，再检查 `recovery.compose.json` 恢复点；恢复时用同一项目名，只启动后端。确认完成后才移除该状态目录中的空 `lock` 目录。

## 6. 本地构建与离线导入

在仓库根目录执行，不需要本机安装 Java 或 Maven：

```bash
docker buildx build -f docker/backend.Dockerfile --target backend \
  -t sxuoj-backend:local --load .

docker buildx build -f docker/backend.Dockerfile --target artifacts \
  --output type=local,dest=dist/backend .
```

构建使用仓库 `.mvn/settings.xml` 的 Maven 镜像源，首次构建需要网络，后续复用缓存。若需离线部署，在构建机导出镜像，再在服务器导入：

```bash
docker save -o sxuoj-backend.tar sxuoj-backend:local
# 将 tar 传到服务器后：
docker load -i sxuoj-backend.tar
python3 deploy/backend.py update \
  -f /opt/hoj/docker-compose.yml --env-file /opt/hoj/.env \
  --image sxuoj-backend:local --no-pull
```

镜像默认以 root 运行，以兼容旧数据卷所有者；已有目录权限允许时可在原 Compose 指定 `user`。沿用 `/hoj/file`、`/hoj/testcase`、`/hoj/log/backend`；可执行文件在 `/opt/hoj` 与 `/usr/local/bin`，避免被旧 `/hoj` 挂载覆盖。

## 7. 判题服务范围

流水线同时打包 `judgeserver.jar`，但**不自动替换判题容器**。它依赖同一网络命名空间里的 `localhost:5050` 沙箱、编译器、权限以及 `/judge` 数据路径。先核实原判题镜像的 jar 位置与启动方式，再在原镜像运行环境中替换产物。不要把主后端镜像用作判题镜像。

## 参考

- [Docker Compose 环境变量插值](https://docs.docker.com/compose/how-tos/environment-variables/variable-interpolation/)
- [Docker Compose 多文件合并规则](https://docs.docker.com/compose/how-tos/multiple-compose-files/merge/)
- [GitHub Actions 发布 Docker 镜像](https://docs.github.com/en/actions/tutorials/publish-packages/publish-docker-images)
