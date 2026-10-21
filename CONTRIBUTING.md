# 提交与发布约定

- 新增功能：`feature：中文简略描述`，例如 `feature：新增比赛学号姓名入场核验`。
- 修复问题：`fix：中文简略描述`，例如 `fix：修复未核验用户访问比赛题目`。
- 后续提交按此格式书写；已发布的历史提交保留原记录。
- 镜像使用版本号，如 `yunyunjuan/sxuoj-backend:v1.1.0`、`yunyunjuan/sxuoj-frontend:v1.1.0`。对应 Git 标签也使用 `v1.1.0`。
- GitHub Actions 对分支运行验证，只对版本标签发布镜像，不再随主分支更新 `latest`。
- 不提交生产备份、`.env`、核验服务 API 密钥或实名测试数据。
