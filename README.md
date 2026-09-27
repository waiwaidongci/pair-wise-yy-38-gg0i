# 水库防汛调度与操作确认

根据库位、入库流量、下游警戒和施工限制生成复核授权的泄洪指令。

## 模块结构

- `app.py`：参数解析、依赖组装和HTTP服务启动。
- `src/domain.py`：数据结构、错误、状态和基础校验。
- `src/rules.py`：状态机、角色矩阵、优先级、期限和关闭不变量。
- `src/repository.py`：SQLite建表、事务、版本控制和审计链。
- `src/service.py`：权限检查、用例编排、并发控制和审计。
- `src/http_api.py`：JSON路由和统一错误响应。
- `src/audit.py`：UTC时间和SHA-256审计事件。
- `static/index.html`：最小演示页。
- `tests/`：完整流程、规则和失败测试。

## 初始化与启动

```bash
python3 app.py --db ./data.db --port 8315
```

默认端口为`8315`，首次启动自动建库。使用`X-Actor`和`X-Role`请求头传递身份。

## 主要接口

- `GET /health`
- `GET /api/items`
- `POST /api/items`
- `GET /api/items/{id}`
- `POST /api/items/{id}/records`
- `POST /api/items/{id}/transition`，必须提交`expected_version`
- `GET /api/audit`

允许角色：duty_officer, chief_engineer, dispatcher, viewer。库位超过汛限或入库流量上升时提升紧迫度；授权前必须有复核记录，执行后仍要闭环现场反馈。

## 多库联调排队

上游几座水库同时腾库时，各库计划共享下游控制断面的安全流量上限，系统按提交顺序分配额度，避免重复占满。

- `POST /api/sections`：调度员登记控制断面与共享安全流量上限。
- `GET /api/sections`、`GET /api/sections/{id}`：查看断面占用/剩余额度与排队；待排队计划显示前序占用额度和缺口。
- `POST /api/sections/{id}/plans`：调度员登记水库计划下泄量，按提交顺序分配额度，超出上限或前面有待排计划时进入待排队。
- `POST /api/plans/{id}/execute`：执行后回填实际下泄量。偏小时释放剩余额度并按序提升待排计划；偏大时冻结断面内全部未执行计划并转总工复核；已执行的计划不能改写。
- `POST /api/sections/{id}/review`：总工复核后解冻，按剩余额度重新按序分配。

演示页`/`支持登记断面与计划、查看排队、回填执行和总工复核。

## 测试

```bash
python3 -m unittest discover -s tests -v
```
