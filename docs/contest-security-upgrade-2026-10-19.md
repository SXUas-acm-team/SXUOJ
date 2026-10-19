# 比赛保密权限生产升级手册（2026-10-19，初稿）

本文用于现有 SXUOJ/HOJ 生产部署的增量更新。**镜像发布、生产迁移和生产验收结果尚待发布负责人补充；本文中的命令是操作步骤，不表示这些操作已经完成。**

暂定镜像标签：

```text
ghcr.io/sxuas-acm-team/sxuoj-backend:contest-security-20261019
ghcr.io/sxuas-acm-team/sxuoj-frontend:contest-security-20261019
```

上线前将实际提交号、构建结果和两个镜像的不可变 digest 填入文末记录。生产优先固定 digest，避免标签之后变化。

## 1. 本次行为与变更范围

新增“密码保密赛”，底层继续使用 `contest.auth=1` 私有赛，通过独立策略表启用；没有新增 `auth=3`。

| 阶段 | 普通用户查看比赛内容 | 提交 |
| --- | --- | --- |
| 未开始 | 不可查看题目 | 不可提交 |
| 比赛中，未登录或未通过密码 | 不可查看题目 | 不可提交 |
| 比赛中，已登录且密码正确 | 可查看；不检查参赛名单 | 可提交 |
| 比赛结束，尚未解封 | 所有普通用户均不可查看，包括先前通过密码和参加过比赛的人 | 不可提交 |
| 管理员手动解封，或到达指定解封时间 | **所有人可查看，包括匿名用户** | **所有登录用户可直接补题，不再要求比赛密码或名单资格** |

比赛管理员保留必要的管理权限。解封仅在比赛结束后生效。重新封存后恢复访问限制，但不能收回用户已经下载、截图或保存的内容。

本次同时包含：

- 对题面、题单、榜单、提交及相关比赛内容实施后端统一权限检查，覆盖直接请求接口和前端全屏题目地址。
- 对题目在题库、训练、讨论等其他入口的关联访问，以及本地图片、附件的直接 URL 实施保护，避免只隐藏比赛菜单。
- 前端到期清理题面和题单，忽略过期异步响应；重新聚焦及定期刷新访问状态；切换账号时撤销原账号的前端权限。
- 取消前端打印代码/内容的 50 字符下限；非空的短程序可走正常提交、测试提交和打印流程，仍保留各入口其他有效限制。
- 新建题目默认开启已有 `isRemoveEndBlank` 选项，即“自动去除代码每行末尾空白符”。不通过前端正则重写用户代码。

**不会批量修改旧比赛、旧题目或旧账号。** 原公开、私有、保护赛在未启用保密策略时保留各自规则；尤其旧“保护赛”仍是可查看、提交需密码。已有题目的 `is_remove_end_blank` 不批量改为开启。新表初始为空，创建表本身不启用任何比赛的保密功能。管理员旧页面漏传 `confidential` 时，后端保留已有策略，避免误解封。

## 2. 现有部署结构与保留项

仅从提供的 `main/docker-compose.yml` 提取了非敏感的服务和挂载结构，未读取或输出生产 `.env`。实际服务器的绝对路径、项目名和覆盖文件仍须现场核对。

| 服务/容器 | 需保留的结构 |
| --- | --- |
| `hoj-mysql` | `${HOJ_DATA_DIRECTORY}/data/mysql/data:/var/lib/mysql`，原数据库和用户继续使用 |
| `hoj-redis` | `${HOJ_DATA_DIRECTORY}/data/redis/data:/data`，原 Redis 继续使用 |
| `hoj-nacos` | 原 Nacos 服务、网络及配置继续使用 |
| `hoj-backend` | `${HOJ_DATA_DIRECTORY}/file:/hoj/file`、`testcase:/hoj/testcase`、`log/backend:/hoj/log/backend`；原后端端口默认 `6688` |
| `hoj-frontend` | `./1015:/usr/share/nginx/html/`；原对外端口 `8081:80`、`8082:443` |
| `hoj-judgeserver` | 原 testcase、judge/log、judge/run、judge/spj、judge/interactive、log/judgeserver 挂载继续使用 |
| `hoj-mysql-checker`、`hoj-autohealth` | 原辅助服务继续使用，不在本次单独重建范围 |

原服务连接 `hoj-network`。不要新建一个同名但隔离的 Compose 工程来替代现有生产服务，也不要执行 `docker compose down -v` 或清理数据卷。

数据库备份的 binlog 版本信息证明来源为 **MySQL 8.0.43**；生产迁移仍以 `SELECT VERSION()` 的现场结果为准，不能凭旧镜像仓库名推断版本。不要直接使用 MySQL 5.7 或任意 `latest` 去启动该物理数据目录。

生产原有 Nginx、TLS 证书、域名、代理配置和挂载必须保留。新前端镜像的默认 Nginx 只提供 HTTP `80`，不自带生产证书；直接替换原 `hoj-frontend` 镜像不能保证原 `443` 配置继续工作，而且原 `./1015` 挂载会遮住镜像内静态文件。**本手册优先采用从新镜像提取 dist、更新原静态目录的方式。**

## 3. 发布前准备与备份

建议先在维护窗口操作，整个验收完成前不要启用生产比赛的保密策略。

以下均为服务器路径示例；替换为实际路径，沿用原部署全部 `-f`、`--env-file` 及 `-p` 参数。不要输出完整 `docker compose config`、`docker inspect` 环境变量或 `.env` 内容到共享日志。

```bash
SOURCE_DIR=/opt/sxuoj
COMPOSE_DIR=/opt/hoj
COMPOSE_FILE="$COMPOSE_DIR/docker-compose.yml"
COMPOSE_ENV_FILE="$COMPOSE_DIR/.env"
RELEASE_DIR="$COMPOSE_DIR/releases/contest-security-20261019"

umask 077
mkdir -p "$RELEASE_DIR"
docker compose ls
docker compose -f "$COMPOSE_FILE" --env-file "$COMPOSE_ENV_FILE" config --services
docker inspect --format '{{index .Config.Labels "com.docker.compose.project"}}' hoj-backend
docker inspect --format '{{.Image}}' hoj-backend
docker inspect --format '{{.Image}}' hoj-frontend
```

记录原镜像实际 ID、现有健康状态、原 Compose 项目名和文件顺序。原部署曾使用 `-p` 时，后面的 `deploy/backend.py` 命令也加 `-p <原项目名>`。

备份至少包括：

1. 数据库一致性备份，验证可恢复。
2. 原 Compose、受限保存的变量文件、Nginx/TLS 配置和证书。
3. 原前端 `1015` 静态目录、题目图片及附件、测试数据和判题相关共享文件。
4. 原后端镜像及部署快照；本次验收前不要做镜像清理。

逻辑数据库备份示例，凭据只从 MySQL 容器已有环境读取，不放到宿主机命令行参数：

```bash
docker exec hoj-mysql sh -c \
  'export MYSQL_PWD="${MYSQL_ROOT_PASSWORD:?}"; exec mysqldump -uroot --single-transaction --quick --routines --triggers --events --hex-blob --set-gtid-purged=OFF --no-tablespaces --databases hoj' \
  > "$RELEASE_DIR/hoj-before-upgrade.sql"
test -s "$RELEASE_DIR/hoj-before-upgrade.sql"
sha256sum "$RELEASE_DIR/hoj-before-upgrade.sql" > "$RELEASE_DIR/hoj-before-upgrade.sql.sha256"
```

该 SQL 包含生产业务数据和认证资料，按敏感备份保存，不提交仓库或上传公开产物。不要将正在运行的 MySQL 数据目录直接打包后当作一致性数据库备份；物理备份应使用原有一致性快照/停库流程。

## 4. 数据库增量迁移：先建空表

此次只新增两张表，不对 `contest`、`problem`、用户、提交等旧业务表执行 `ALTER`、`UPDATE` 或数据回填：

| 新表 | 字段与索引 | 用途 |
| --- | --- | --- |
| `contest_access_policy` | `cid` 主键；`release_at DATETIME NULL`；`gmt_create`、`gmt_modified` | 行存在即启用保密；`release_at=NULL` 表示等待手动解封；结束且到达指定时间后公开 |
| `contest_protected_resource` | `cid`；`resource_hash CHAR(64)`，ASCII/`ascii_bin`；`resource_path TEXT`；联合主键 `(cid, resource_hash)`；hash 单列索引 | 关联保护题目/资源，保留历史引用，避免编辑、移除关联后旧 URL 意外恢复公开 |

`cid` 必须与实际 `contest.id` 的整数类型及 unsigned 属性一致。迁移脚本从 `information_schema` 读取后生成 DDL，不要照抄其他环境的 `BIGINT` 定义。

迁移前只读检查：

```bash
python3 "$SOURCE_DIR/deploy/contest_policy_migrate.py" \
  --mysql-container hoj-mysql --database hoj

python3 "$SOURCE_DIR/deploy/contest_policy_migrate.py" \
  --mysql-container hoj-mysql --database hoj --print-ddl
```

默认只读。缺表时返回码 `2` 表示尚未就绪；错误为 `1`。`--print-ddl` 输出待创建 DDL，之后还带 JSON 检查结果，**不要将完整输出直接作为 SQL 导入**。脚本不自动修复结构不兼容的同名表，遇到不兼容应停止并人工核对。

确认备份完成后创建缺失表：

```bash
python3 "$SOURCE_DIR/deploy/contest_policy_migrate.py" \
  --mysql-container hoj-mysql --database hoj --apply

python3 "$SOURCE_DIR/deploy/contest_policy_migrate.py" \
  --mysql-container hoj-mysql --database hoj
```

迁移后应返回 `ready:true`、`missing_tables:[]`；重复执行不应重建表或改动旧业务行。保密功能启用前，两张新表的行数都应为 `0`。

以下只读 SQL 可在现有受控 MySQL 会话执行。迁移前保存前半部分结果，迁移后比较；线上仍有业务写入时，数量自然增长需与业务日志对照。

```sql
SELECT VERSION(), DATABASE(), @@global.time_zone, @@session.time_zone;

SELECT TABLE_NAME, COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_KEY,
       CHARACTER_SET_NAME, COLLATION_NAME
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = DATABASE()
  AND ((TABLE_NAME = 'contest' AND COLUMN_NAME = 'id')
       OR TABLE_NAME IN ('contest_access_policy', 'contest_protected_resource'))
ORDER BY TABLE_NAME, ORDINAL_POSITION;

SELECT COUNT(*) AS contests FROM contest;
SELECT auth, COUNT(*) AS contests FROM contest GROUP BY auth ORDER BY auth;
SELECT COUNT(*) AS problems FROM problem;
SELECT is_remove_end_blank, COUNT(*) AS problems
FROM problem GROUP BY is_remove_end_blank;

-- 以下在两表创建后执行。
SELECT COUNT(*) AS policy_rows FROM contest_access_policy;
SELECT COUNT(*) AS resource_rows FROM contest_protected_resource;

SELECT TABLE_NAME, INDEX_NAME, NON_UNIQUE,
       GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) AS index_columns
FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME IN ('contest_access_policy', 'contest_protected_resource')
GROUP BY TABLE_NAME, INDEX_NAME, NON_UNIQUE;

SELECT COUNT(*) AS orphan_policies
FROM contest_access_policy p LEFT JOIN contest c ON c.id = p.cid
WHERE c.id IS NULL;

SELECT COUNT(*) AS release_before_end
FROM contest_access_policy p JOIN contest c ON c.id = p.cid
WHERE p.release_at IS NOT NULL AND p.release_at < c.end_time;
```

后端启动也会检查新表是否可读取。缺表或权限查询故障时应失败并阻止访问，不能当作普通旧比赛放行。应用账号需要两张新表正常读写权限。

## 5. 更新后端：保留原服务、环境和卷

顺序必须是：**空表迁移 → 新后端 → 新前端 → 全量验收 → 启用保密比赛**。

先确认镜像已经发布，并将下面的标签替换为本次确认的 digest。未发布时不要将拉取失败描述为升级成功。

```bash
BACKEND_IMAGE=ghcr.io/sxuas-acm-team/sxuoj-backend:contest-security-20261019

python3 "$SOURCE_DIR/deploy/backend.py" update \
  -f "$COMPOSE_FILE" --env-file "$COMPOSE_ENV_FILE" \
  --service hoj-backend --image "$BACKEND_IMAGE" --dry-run

python3 "$SOURCE_DIR/deploy/backend.py" update \
  -f "$COMPOSE_FILE" --env-file "$COMPOSE_ENV_FILE" \
  --service hoj-backend --image "$BACKEND_IMAGE"

python3 "$SOURCE_DIR/deploy/backend.py" status \
  -f "$COMPOSE_FILE" --env-file "$COMPOSE_ENV_FILE" --service hoj-backend
```

脚本只重建后端，不执行数据库迁移，不重建 MySQL、Redis、Nacos 和 JudgeServer。它会保留原环境、挂载、网络和原服务配置，并保存受限权限的恢复快照。快照包含解析后的凭据，不要公开。

健康探针只说明 HTTP 服务可用，不代表比赛权限、图片附件和判题流程已通过验收。原环境不符合部署脚本的凭据校验时，应在服务器受控处理；不要复制生产 `.env` 到构建目录，也不要为绕过校验随机替换现有共享密钥。

原 Compose 文件的镜像字段可能仍指向旧镜像。后续后端更新继续使用该部署脚本；不要直接用原文件执行后端 `up`，以免意外切回旧版。

## 6. 更新前端：优先保留生产 Nginx/TLS

从新前端镜像提取静态产物，不启动临时容器：

```bash
FRONTEND_IMAGE=ghcr.io/sxuas-acm-team/sxuoj-frontend:contest-security-20261019
docker pull "$FRONTEND_IMAGE"
mkdir -p "$RELEASE_DIR/dist"
extract_container=$(docker create "$FRONTEND_IMAGE")
docker cp "$extract_container:/usr/share/nginx/html/." "$RELEASE_DIR/dist/"
docker rm "$extract_container"
test -s "$RELEASE_DIR/dist/index.html"

# 仅显示挂载路径，不显示容器环境。
docker inspect --format '{{range .Mounts}}{{if eq .Destination "/usr/share/nginx/html"}}{{.Source}}{{end}}{{end}}' hoj-frontend
```

根据该挂载信息确认真实静态目录。备份 Compose 中的 `./1015` 只是相对路径，不能假定现场一定是 `/opt/hoj/1015`。

下面以核对后确实为 `$COMPOSE_DIR/1015` 为例。保留原目录本身，先复制新静态资源，最后替换入口，避免 Docker 仍绑定重命名前的目录；先不删除旧 hash 资源，方便已有页面完成切换和前端回退。

```bash
WEB_ROOT="$COMPOSE_DIR/1015"
test -d "$WEB_ROOT"
test -f "$WEB_ROOT/index.html"
tar -C "$WEB_ROOT" -czf "$RELEASE_DIR/frontend-before-upgrade.tar.gz" .
rsync -a --exclude=index.html "$RELEASE_DIR/dist/" "$WEB_ROOT/"
cp "$RELEASE_DIR/dist/index.html" "$WEB_ROOT/.index.security-upgrade.html"
mv "$WEB_ROOT/.index.security-upgrade.html" "$WEB_ROOT/index.html"
docker exec hoj-frontend nginx -t
docker exec hoj-frontend nginx -s reload
```

保留原 TLS、域名、证书路径和代理规则。核验以下配置要求，按原配置结构调整，避免重复定义已有 location：

- `/api/` 和受保护的图片、文件请求必须到达新后端；不能由 Nginx/CDN 直接从共享文件目录绕过后端读取。
- 私有/保密 API、图片及附件不能使用共享缓存；原代理如果启用了缓存，应对这些请求禁用缓存并清理历史缓存。
- `index.html` 不缓存或强制重新验证；带 hash 的前端静态资源可以继续长缓存。
- 原反向代理若固定了旧后端 IP 或端口，核验新容器仍可达。不要修改生产对外域名或 TLS 端口来迁就测试环境。

新镜像也可作为原入口 Nginx 后面的独立 HTTP 服务，连接**实际已有** Docker 网络，并设置 `BACKEND_UPSTREAM=hoj-backend:6688`；若生产后端端口不同，使用实际内部端口。这是可选部署方式，需单独准备经过审查的服务覆盖配置，不能直接用本镜像的默认配置覆盖原生产 TLS 容器。

## 7. 启用前验收矩阵

先用专用验收比赛和测试账号执行。不要直接把正式待用赛题公开来测试“解封”。分别准备匿名窗口、未验证密码账号、已验证密码账号和比赛管理员。检查浏览器页面，也直接请求相应 API；界面按钮隐藏不能作为后端鉴权通过的证据。

| 验收项 | 应有结果 |
| --- | --- |
| 原公开/私有/保护比赛 | 未启用保密策略时保持旧行为，无批量转换 |
| 保密赛中、未登录 | 题目、题单及受限比赛内容拒绝；可显示入场状态及登录提示 |
| 保密赛中、已登录未输密码/密码错误 | 不能读取题面，不能提交 |
| 保密赛中、正确密码但不在旧名单 | 可查看和参赛；确认名单确实不再是此模式门槛 |
| 直接题目 URL、全屏 URL、直接题面/榜单 API | 与正常页面一致，不能绕过验证 |
| 到达结束时间，已验证密码账号保持页面打开 | 立即隐藏已有题面及题单；再次请求拒绝，不因历史注册继续放行 |
| 已结束未解封，匿名及任意普通账号 | 题面、题单、榜单等受限内容均拒绝；管理员管理访问仍有效 |
| 到达指定解封时间 | 无需重新保存比赛，按服务器时间公开 |
| 手动公开解封 | 匿名可看题面、题单、榜单等公开内容；此前未验证密码的登录账号可补题 |
| 重新封存 | 普通用户再次被拒绝；历史页面重新请求不能复用旧授权 |
| 修改比赛密码/安排下一阶段 | 原密码资格失效，下一阶段用新密码重新验证 |
| 修改名单或旧页面保存其他配置 | 不会替代密码门槛，也不会因缺少新字段取消策略 |
| 题库、训练、讨论、源码/历史提交等关联入口 | 不暴露尚未解封的关联比赛题目或内容 |
| 图片/附件裸 URL、带旧 token URL、HEAD/Range/条件请求 | 封存期无法从旧链接绕过；结束/撤销后旧资源授权不可继续放行 |
| 编辑题面或移除图片/题目关联后访问原 URL | 历史资源关联保护仍生效，不能通过移除引用解锁 |
| 登录后切换账号、退出、多个标签页及迟到响应 | 不把前一个账号的密码/管理员权限带给新账号，不恢复旧题面 |
| 普通提交、测试提交、打印短代码 | 少于 50 字符的非空内容不被前端长度门槛拦截；后端和判题正常处理 |
| 新建与编辑既有题目 | 新建默认去除行末空白；编辑旧题保留原选项，未批量更改旧题 |
| 文件/测试数据/邮件及一次正常判题 | 原挂载、Nacos、Redis、JudgeServer 联动未受升级影响 |

附件保护需要资源关联记录和后端拦截共同生效。验收所用题目应覆盖本地 `/api/public/img/` 与 `/api/public/file/` 图片/文件，以及公告或讨论中的关联资源。若同一资源还关联另一场未解封比赛，它仍可能受那场比赛限制；不要用复用 URL 作为绕过封存的方法。站外链接和用户已保存副本无法由本系统撤回。

首次保存验收保密赛后，确认策略和资源记录已产生，且没有错误的孤立策略；不要手工删除资源关联记录来“修复”访问问题：

```sql
SELECT COUNT(*) AS policy_rows FROM contest_access_policy;
SELECT cid, release_at FROM contest_access_policy ORDER BY cid;
SELECT cid, COUNT(*) AS protected_resources
FROM contest_protected_resource GROUP BY cid ORDER BY cid;
```

此时已经启用了新功能，后端回退适用下一节的限制。验收完成并记录结果后，才对正式比赛启用。

## 8. 跨校区分时比赛操作

1. 第一阶段启用密码保密赛，设置该校区时间和独立密码，选择手动解封或最后一阶段之后的公开时间。
2. 第一阶段结束后保持封存，先验证普通用户和旧链接均不可继续访问。
3. 安排下一校区阶段时，更新比赛起止时间、**更换密码**，并明确把解封方式设为手动/重新安排最后公开时间。不要沿用已经过去的 `releaseAt`。
4. 保存后使用第一阶段账号验证：旧密码资格不能直接进入下一阶段；正确的新密码才允许参加。
5. 最后一阶段全部结束，再手动公开解封或等待计划时间。公开意味着匿名可读、所有登录用户可补题，不再保留密码限制。

仅“重新封存”不能阻止第一阶段已经掌握密码的人在下一阶段继续使用同一密码，因此跨阶段换密码是必要操作。系统不能防止合法参赛者在授权期间复制题目；分时举行仍需相应考试纪律。

## 9. 回滚与维护阻断

### 尚未启用过任何保密策略

在确认两张新表为空、没有比赛开始依赖新策略、保留了升级前镜像与配置时，可在维护窗口回退前后端。新增空表可保留，不必为了回退删除；不要回滚整库覆盖升级后正常产生的提交。

```bash
python3 "$SOURCE_DIR/deploy/backend.py" rollback \
  -f "$COMPOSE_FILE" --env-file "$COMPOSE_ENV_FILE" --service hoj-backend
```

前端可以恢复先前备份到原静态目录，再执行 Nginx 配置检查及 reload。恢复前确认文件目标和原挂载路径一致。

### 已启用过保密策略，包括专用验收赛

**不能直接回到不理解保密策略的旧后端。** 旧后端会忽略新表，按普通私有赛/旧接口规则访问，可能在赛后重新开放内容。只保留新前端不能补上后端缺失的鉴权。

出现故障时先在原生产入口开启维护阻断，停止对外题目、比赛、源码及资源请求，再选择保留新权限能力的修复镜像。必须回退旧版时，先完成明确的保密数据处置和风险确认，并持续维护阻断；不要通过删除策略或新表来绕过启动检查。

`deploy/backend.py` 在更新失败时会自动恢复上个镜像。启用功能后，任何更新之前都应确认自动恢复目标同样具备保密权限能力；若目标仍为旧后端，先维护阻断，或先建立具备相同安全能力的恢复点。健康探针成功不能替代这一核验。

前端单独回退不会解除新后端的实际鉴权，但旧页面可能无法显示封存、公开和匿名补题流程；优先恢复匹配的新前端版本。禁止直接恢复生产数据库快照来回滚应用，除非另有停写、增量数据保护与恢复方案。

## 10. 本地隔离恢复来源与发布记录

本次收到的生产备份来源为仓库工作区之外发布范围的 `main/`：

- `main/docker-compose.yml`：用于核对原服务、挂载和网络结构。
- `main/hoj/data/mysql/data/`：提供的 MySQL 物理数据来源，包含 binlog；版本信息为 8.0.43。
- `main/hoj/file/`、`main/hoj/testcase/` 及相关 judge 目录：附件与判题数据来源。
- `main/1015/`：原前端静态文件来源。

这些目录以及 `.env`、证书、物理数据库和用户数据均不属于 Docker 源码构建上下文或公开发布产物。隔离恢复只能使用这些备份的独立副本/独立卷，不能把新测试容器挂到生产实际数据卷，也不能对原备份执行数据库升级或就地恢复。隔离环境不得连接生产 Nacos、Redis、邮件或远程判题账号。

恢复环境和实际验证结果由发布负责人补充；在补齐前，不宣称生产备份恢复或线上升级已经成功。

| 记录项 | 结果 |
| --- | --- |
| 实际 Git 提交号 | 待补 |
| 后端镜像 digest / 构建结果 | 待补 |
| 前端镜像 digest / 构建结果 | 待补 |
| 镜像实际发布位置及时间 | 待补 |
| 隔离恢复所用 MySQL 镜像、容器、独立副本/卷 | 待补 |
| 隔离恢复的数据校验和业务验收结果 | 待补 |
| 生产备份位置、校验值和可恢复验证 | 待补，敏感位置仅限受控记录 |
| 生产迁移只读检查 / apply / 复检 | 待执行或待补 |
| 后端 / 前端上线时间 | 待执行或待补 |
| 验收矩阵执行人、结果与问题单 | 待执行或待补 |
| 首次启用保密策略时间及安全回退目标 | 待执行或待补 |
