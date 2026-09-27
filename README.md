# 水库防汛调度与操作确认

根据库位、入库流量、下游警戒和施工限制生成复核授权的泄洪指令。

## 模块结构

- `app.py`：参数解析、依赖组装和HTTP服务启动。
- `src/domain.py`：数据结构、错误、状态和基础校验。
- `src/rules.py`：状态机、角色矩阵、优先级、期限和关闭不变量。
- `src/repository.py`：SQLite建表、事务、版本控制和审计链。
- `src/service.py`：权限检查、用例编排、并发控制和审计。
- `src/http_api.py`：JSON路由和统一错误响应。
- `src/queueing.py`：联调排队的额度占用、FIFO分配与冻结规则。
- `src/audit.py`：UTC时间和SHA-256审计事件。
- `static/index.html`：联调排队演示页（登记断面/计划、查看排队、回填与复核）。
- `tests/`：完整流程、规则、联调排队和失败测试。

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

### 联调排队

上游多库同时腾库时，共享安全流量按提交顺序（FIFO）分配，避免重复占满：

- `POST /api/queue/sections`：调度员登记控制断面 `{name, shared_cap}`。
- `GET /api/queue/sections` / `GET /api/queue/sections/{id}`：查看断面汇总（共享上限、占用、剩余、冻结标记）和排队明细。
- `POST /api/queue/sections/{id}/plans`：调度员登记各库计划 `{reservoir, planned}`；装得下剩余额度即为`allocated`，否则留在`waiting`待排队，返回值显示每个计划的占用额度。
- `POST /api/queue/plans/{id}/execute`：调度员回填`{actual}`实际下泄量。实际偏小释放剩余额度，后续待排队计划按顺序递补（队头装不下时阻塞，不越级）；偏大则冻结该断面后续全部计划（`frozen`）并交总工复核。已执行计划再次回填返回409，不可改写。
- `POST /api/queue/plans/{id}/review`：总工对冻结计划`{decision: approve|reject, note?}`逐条复核；全部冻结解除后再按FIFO重新分配。

排队角色：登记/执行限`dispatcher`，复核限`chief_engineer`，`duty_officer`与`viewer`可查看。所有操作写入审计链。

允许角色：duty_officer, chief_engineer, dispatcher, viewer。库位超过汛限或入库流量上升时提升紧迫度；授权前必须有复核记录，执行后仍要闭环现场反馈。

## 测试

```bash
python3 -m unittest discover -s tests -v
```
