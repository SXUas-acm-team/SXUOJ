# v1.1.0 更新手册

这次操作顺序：**MySQL 新增两张表 → 换前后端镜像 → 在比赛设置里配置核验**。不用上传新的 Compose，不用改原 `.env`、服务密码、端口、数据挂载或评测机镜像。

## 1. 先在 MySQL 执行

选中正在使用的 HOJ 数据库（通常叫 `hoj`），复制下面整段执行。只创建新表，不修改原业务表，也不动原有保密赛的两张表。SQL 源文件是 [contest-identity-v1.1.0.sql](../deploy/contest-identity-v1.1.0.sql)。

```sql
CREATE TABLE IF NOT EXISTS contest_identity_config (
  cid BIGINT UNSIGNED NOT NULL PRIMARY KEY,
  enabled TINYINT(1) NOT NULL DEFAULT 0,
  replace_realname TINYINT(1) NOT NULL DEFAULT 0,
  star_prefixes VARCHAR(1536) NOT NULL DEFAULT '',
  api_url VARCHAR(1024) NOT NULL,
  api_key TEXT NULL COMMENT 'AES-GCM encrypted; never return to browser',
  gmt_create DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  gmt_modified DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  CONSTRAINT fk_identity_config_contest FOREIGN KEY (cid) REFERENCES contest(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS contest_identity_binding (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  cid BIGINT UNSIGNED NOT NULL,
  uid VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  student_number VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  verified_name VARCHAR(128) NOT NULL,
  verified_at DATETIME NOT NULL,
  UNIQUE KEY uk_contest_identity_user (cid, uid),
  UNIQUE KEY uk_contest_identity_student (cid, student_number),
  CONSTRAINT fk_identity_binding_contest FOREIGN KEY (cid) REFERENCES contest(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
```

执行后查看结果：

```sql
SHOW CREATE TABLE contest_identity_config;
SHOW CREATE TABLE contest_identity_binding;
```

第二张表必须有 `(cid, uid)` 和 `(cid, student_number)` 两个唯一索引，分别保证同场一个账号只绑定一个身份、同一学号只被一个账号绑定。新后端会检查表和这两个约束；漏建表时会明确提示迁移，避免上线后比赛接口才报错。

两张新表通过比赛 ID 的外键自动清理：比赛被删除，或随团队解散而删除时，对应核验配置和身份绑定一起删除。上面的 `BIGINT UNSIGNED` 与此前备份的比赛 ID 类型一致。

## 2. 替换镜像

只改原 Compose 中两处 `image`：

```yaml
# hoj-backend
image: yunyunjuan/sxuoj-backend:v1.1.0
# hoj-frontend
image: yunyunjuan/sxuoj-frontend:v1.1.0
```

在原 Compose 目录执行；平时使用的 `-f`、`--env-file`、`-p` 等参数继续带上：

```bash
docker compose pull hoj-backend hoj-frontend
docker compose up -d --no-deps --force-recreate hoj-backend hoj-frontend
docker compose ps hoj-backend hoj-frontend
```

页面按 `Ctrl+F5` 刷新。此次版本使用明确的 `v1.1.0` 标签，后续版本递增，不再用日期或 `latest` 作为发布版本。

## 3. 打开比赛入场核验

到后台编辑比赛，找到“竞赛入场核验”。新建比赛先保存一次，拿到比赛 ID 后再配置：

1. 打开“开启竞赛入场核验”。
2. 请求地址默认已填：`https://sf-zt.gwy.fun/cloudxxsaacloud/v1/identities/verify-name`。这里填的是完整接口地址，不是网站根地址。
3. 填入你已提供的 API 密钥。密钥没有硬编码进公开源码或镜像，首次需要在此填写；已有配置时留空会保留原密钥。修改地址必须重新填写密钥。
4. 按需填写“学号前缀自动打星”，例如 `2024,2025`。`0020` 等前导零会保留；留空关闭自动打星。原有手工打星名单继续生效。
5. 按需打开“榜单真实姓名使用核验姓名”。它替换本场榜单的真实姓名字段；榜单显示名称选择“真实姓名”后即可显示。未核验用户的该字段为空，不修改个人资料。
6. 点 **保存核验设置**。这组设置独立保存、立即生效，和下方比赛主表单的保存按钮分开。

前后端不会把 API 密钥发给参赛用户。后端使用 HTTPS 请求服务，限制请求频率、连接时间和响应大小；服务失败、不匹配或返回异常时都不会放行。

服务密钥以加密形式保存在新配置表，加密密钥自动保存在原后端日志卷内的 `/hoj/log/backend/.runtime-secrets/identity.key`。原日志挂载保持即可；以后迁移服务器时应与数据库一起备份、恢复该文件。多个后端实例需要使用同一份加密密钥，不能各自生成后共享同一个配置数据库。

## 4. 用户入场与榜单规则

- 用户登录后填写学号、姓名；私有赛、保密赛还需要先通过原有比赛密码校验。
- 核验通过才写入本场绑定，之后再次进入无需重复调用服务。不能自助更换身份，同场同一学号也不能重复绑定其他账号。
- 此接口核对学号和姓名是否对应，不能证明操作者本人身份；本版按提供的姓名核验接口实现，没有收集学校账号密码。
- 学号前缀打星会影响排名、奖项和“移除打星队伍”，同时作用于 ACM/OI 榜单、外榜、榜单导出及滚榜。核验姓名也在这些输出中保持一致。
- 已存在的绑定不会因关闭核验而删除；打星前缀及姓名替换由各自设置控制。
- 保密赛结束后仍按原规则封存，核验不能绕过封存。管理员手动或定时解封后，所有人可以查看，登录用户可赛后提交，无需再进行入场核验。

## 5. 普通管理员权限

`admin` 现在可以像超管一样管理题目、训练、比赛、讨论，包括其他人创建的这些内容，以及相关导入导出、标签和附件操作。删除比赛仍要求输入当前管理员自己的账号密码。

同时修复后台登录接口未标记为公开接口的问题，可以直接从后台登录页验证账号密码并进入后台。

后台顶部用户名换行时，工具栏会自动增高，避免把编辑表单挤成右侧窄条。

系统配置、账号和角色、用户批量导入、评测服务全局配置仍沿用原超管权限；没有把普通管理员改成 `root` 角色。`problem_admin` 的原有范围不做同样扩张。

## 6. 验收与回退

本地备份环境使用合成账号、合成学号和姓名验收，没有修改线上服务或查询真实学生资料。外部核验服务使用随机虚构资料检查了地址与密钥：有密钥返回人员不存在的 404，无密钥返回 401，健康检查 200。真实姓名匹配通过的协议、落库写入和异常响应由自动测试覆盖；本次没有真实学生匹配样本。

更新后可检查：普通用户未核验无法看题；成功绑定后可进入；更换身份被拒绝；前缀用户显示打星；认证姓名按开关展示；普通管理员能管理四类内容而不能管理账号。

如果已经启用入场核验，不能直接回退到忽略该核验的旧后端继续开放比赛。需要回退时先暂停相关比赛访问，再处理镜像回退。新表和日志卷中的加密密钥保留，不需要删表。
