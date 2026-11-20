# 流域归属与观测服务

站点观测按**采样时的流域归属**供数。站点转属、历史更正、历史补录在一笔事务内完成，
提交时统一校验完整性；API 与任意直连 SQL 会话（READ COMMITTED）受同一套数据库约束。

## 架构

```
┌────────────┐   HTTP    ┌─────────────────┐   SQL    ┌──────────────────┐
│ 客户端/curl │ ────────▶ │ FastAPI (uvicorn) │ ──────▶ │ PostgreSQL 16     │
│ 直连 psql   │ ──────────┼──────────────────┼────────▶ │ 约束在库内强制执行 │
└────────────┘           └─────────────────┘          └──────────────────┘
```

- `db/init.sql` —— 全部约束逻辑（表、延迟约束触发器、站点行锁、供数视图），数据库启动时装载，持久化于系统目录
- `api/` —— FastAPI 应用（变更集 + 转属/拆分/合并/更正便捷端点 + 供数查询）
- `docker-compose.yml` —— `db`(postgres:16-alpine) + `api`，健康检查串联启动顺序
- `examples/` —— 转属、历史更正、直连 SQL、并发冲突 四个示例
- `tests/` —— pytest 集成测试（15 个用例）

## 数据模型与约束

- `stations(station_id, name)` —— 站点，同时充当**每站一把的锁锚点**
- `attribution_periods(period_id, station_id, watershed, valid_from, valid_to)`
  —— 归属时段，`[valid_from, valid_to)` **左闭右开**，`valid_to` 可空（至今有效），`CHECK (valid_to > valid_from)`
- `observations(observation_id, station_id, observed_at, value)` —— 自动读数
- `manual_readings(manual_reading_id, station_id, observed_at, value, report_no, withdrawn, withdrawn_at)`
  —— 断网期间值班员的**水尺抄报**，`UNIQUE (station_id, observed_at)` 保证同站同刻只登记一次
- `reading_slots(station_id, observed_at, kind)` —— **读数槽位锚表**，主键 `(station_id, observed_at)`，
  全库硬性保证同站同刻至多一个**有效来源**（`manual` 或 `automatic`），不依赖锁等待唤醒时序
- `observation_attribution`（视图）—— 自动读数供数：按 `observed_at ∈ [valid_from, valid_to)` 关联流域
- `reading_feed`（视图）—— 统一供数：自动读数 + 生效抄报，带 `source` 来源标记与抄报单号
- `manual_reading_status`（视图）—— 抄报台账（含已退出抄报的原值/单号/退出状态，失去归属时流域为空）

### 人工抄报规则（API 与直连 SQL 同一套库级约束）

1. **接收条件**（持锁即时检查 + 提交时延迟约束双重校验）：
   采样时刻必须有流域归属（落入某归属时段），且同站同刻无自动读数，否则整笔拒绝（409 / SQL `23514`）；
2. **同站同刻只登记一次**：`manual_readings` 唯一约束 + `reading_slots` 主键双重保证；
3. **暂停供数**：抄报登记后若时段调整导致采样时刻失去归属，抄报记录保留但从 `reading_feed` 消失，
   重新有归属后自动恢复供数（台账 `watershed` 为空即"暂停供数"状态）；
4. **自动读数顶替（永久退出）**：同站同刻自动读数一旦**提交成功**，抄报置 `withdrawn=true` 永久退出供数，
   槽位改派 `automatic`；此后即使删除自动读数，槽位与 `withdrawn` 都不回退，**旧抄报不恢复**；
5. **回滚 / 提交前撤销不退出**：补传整笔回滚（延迟触发器随事务消失）、或自动读数 INSERT 后在提交前 DELETE
   （提交时行已不存在，顶替函数提前返回），抄报都不会退出；
6. **历史可查**：已退出抄报的原值、抄报单号、退出状态/时间仍可经 `/manual-readings` 与台账视图查询。

两条完整性规则由 **`DEFERRABLE INITIALLY DEFERRED` 约束触发器在提交时**统一校验：

1. 同一站点任意两个时段不得重叠（`tstzrange … && …`，GiST 索引加速）；
2. 每条观测在采样时刻必须落入本站某一个时段（不重叠 ⇒ 至多一段；覆盖检查 ⇒ 至少一段；合起来**恰好一段**）。

事务**中途允许暂时违规**（先拆/删时段再补、先补观测再改时段均可），**提交时必须全部满足**，
否则整笔回滚。任何写路径（API、变更集、直连 SQL）都走同一套约束。

### 并发设计（撤段 vs 补录）

- 行级 BEFORE 触发器在每次写时段/观测前对该站点行执行 `SELECT … FOR UPDATE`
  （多站按字典序加锁，降低死锁概率）——**同站并发写被串行化**；
- 在 READ COMMITTED 下，锁等待解除后提交时校验能看到对方**已提交**的最新状态：
  - 撤段先提交 ⇒ 补录提交时发现观测无归属 ⇒ **补录整笔拒绝**；
  - 补录先提交 ⇒ 撤段提交时发现会留下孤儿观测 ⇒ **撤段整笔拒绝**；
  - 两种时序都不会出现"各自通过却留下无归属观测"；
- 不同站点持不同锁行，**无关站互不阻塞、可各自提交**；
- 死锁（多站交叉加锁）由 PG 检测，API 映射为 409，可整笔重试；
- 禁止 `TRUNCATE`（会绕过行触发器）；约束保存在系统目录，**重启后仍有效**。

## 快速开始

```bash
docker compose up -d --build --wait
curl http://localhost:8000/health        # {"status":"ok"}
xdg-open http://localhost:8000/docs      # Swagger UI（或浏览器打开）
```

数据库同时暴露在 `localhost:5432`（用户/密码/库名均为 `watershed`），便于直连 SQL。

## API 一览

| 方法/路径 | 说明 |
|---|---|
| `POST /stations` · `GET /stations` | 建站 / 列表 |
| `GET /stations/{sid}/periods` | 查某站归属时段 |
| `POST /changesets` | **核心写入口**：一笔事务按序执行多个 `insert/update/delete_period`、`insert/update/delete_observation` 操作，提交时统一校验，失败整笔回滚（409） |
| `POST /observations` | 单条补录（等价于单操作变更集） |
| `POST /stations/{sid}/transfer` | 转属：`at` 时刻截断当前时段并开启新流域时段 |
| `POST /periods/{pid}/split` | 在 `at` 处拆分为两段（右段可换流域） |
| `POST /stations/{sid}/merge` | 合并 `at` 处首尾相接的两段 |
| `POST /stations/{sid}/correct` | 历史更正：把子区间改属另一流域（自动切分原时段） |
| `GET /stations/{sid}/observations` · `GET /attributed-observations` | 自动读数供数：按采样时刻关联流域（只见已提交状态） |
| `POST /manual-readings` | **登记水尺抄报**：站号、UTC 采样时刻、测值、抄报单号；须有归属且同刻无自动读数，同站同刻仅一条（失败 409） |
| `GET /manual-readings?station_id=&from=&to=&include_withdrawn=` | 抄报台账：按站号与 UTC 时段查询同一已提交状态，返回采样时流域、测值、单号、退出状态（失去归属时流域为空） |
| `GET /readings?station_id=&from=&to=&source=` | **统一供数**：自动读数 + 生效抄报，含 `source=automatic/manual` 与抄报单号；抄报失去归属自动暂停、被顶替后永久剔除 |

`update_period` 中 `valid_to` 缺省表示不修改；显式置空（恢复"至今有效"）用 `"clear_valid_to": true`。

## 示例

（示例脚本依赖 `curl` 与 `jq`；SQL 示例也可用本地 `psql "$DATABASE_URL" -f examples/03_direct_sql.sql` 运行）

```bash
bash examples/01_transfer.sh                # 转属：历史观测归属不变, 新观测属新流域
bash examples/02_historical_correction.sh   # 历史更正：子区间改属 + 同事务补录
docker compose exec -T db psql postgresql://watershed:watershed@localhost:5432/watershed \
  -f - < examples/03_direct_sql.sql          # 直连 SQL：中途违规→提交前修复→成功; 提交仍违规→整笔回滚
bash examples/04_concurrent_conflict.sh      # 并发：撤段 vs 补录, 必有一方整笔被拒
API_URL=http://localhost:8000 bash examples/05_manual_reading_backfill.sh
                                             # 抄报登记→补传前取数(manual)→自动读数补传顶替→
                                             # 补传后取数(automatic)→删自动读数旧抄报不恢复
```

`04` 的场景 1 依赖正常的锁等待/唤醒语义，请在 Docker/常规 Linux 环境运行
（个别阻断进程间信号的沙箱中，锁等待方无法被唤醒）。

## 测试

```bash
python3 -m venv .venv && .venv/bin/pip install -r tests/requirements.txt
.venv/bin/python -m pytest tests -v          # 需要 compose 栈已启动
# 可选: API_URL / DATABASE_URL 指向其他实例; PG_LOCK_WAIT_OK=1 启用真并发锁等待用例
bash scripts/selftest.sh                     # 一键: 起栈 + 全部示例 + pytest
bash scripts/restart_check.sh                # 重启 db/api 后约束仍生效(409/SQL ERROR)
```

`tests/test_manual_readings.py` 覆盖抄报全部关键流程（14 用例）：登记接收/重复拒绝/无归属拒绝/
自动读数冲突拒绝、暂停供数与恢复、自动读数顶替永久退出、删除不恢复、补传回滚与提交前撤销不退出、
直连 SQL 同等约束、API 与 SQL 并发只允许一个有效来源。

## 验证状态（如实说明）

已在 **PostgreSQL 16.16 + FastAPI（本机 conda 环境直连，与 compose 相同的 init.sql 与 uvicorn 启动命令）** 上验证：

- ✅ 既有 15 个集成用例：14 通过、1 个按环境跳过（无回归）；
- ✅ 新增抄报 **14 个集成用例全部通过并连续多轮稳定**：登记成功与供数来源、同站同刻重复拒绝、
  无归属拒绝、同刻自动读数冲突拒绝、未知站 404、失去归属暂停供数与重新覆盖恢复、
  自动读数补传永久退出（原值/单号/退出时间可查）、删除自动读数不恢复、台账时段过滤、
  **补传整笔回滚不退出**、**INSERT 后提交前 DELETE 不退出**、直连 SQL 同等规则、
  API 与直连 SQL 快速并发（10 轮）只允许一个有效来源、两个抄报并发一个被拒；
- ✅ 13 步 SQL 规则脚本（`/tmp/sql_manual_check.sql`，覆盖含直连 SQL 的全部边界）在全新 initdb 集群上
  按预期通过/拒绝，且脚本结束后 `reading_slots` 零残留；
- ✅ 示例脚本 `examples/05_manual_reading_backfill.sh` 端到端实际运行，补传前后 `/readings`
  取数由 `manual` 切换为 `automatic`、删除后旧抄报不恢复，输出正确；
- ✅ 并发正确性不依赖锁等待唤醒：`reading_slots` 主键唯一约束在数据库层硬性保证同站同刻只有一个有效来源，
  快速并发压测（40 轮独立站点）零"双有效来源"；
- ✅ `/docs` 与 `/openapi.json` 可访问，新端点 `POST /manual-readings`、`GET /manual-readings`、`GET /readings` 已注册。

未能在本环境验证（本机无 Docker，且沙箱阻断进程间信号导致**锁等待无法被唤醒**）：

- ⚠️ `docker compose up` 的端到端启动未实际执行；compose/Dockerfile 未改动，栈内组件
  （同一 `init.sql` 的 PG16、同一 uvicorn 命令）已分别本机验证；
- ⚠️ 真并发下"锁等待→唤醒→串行化"的跨进程阻塞路径（`PG_LOCK_WAIT_OK=1` 用例、示例 04 场景 1）
  在本沙箱跳过；其**终态正确性**已由顺序化等价用例与 `reading_slots` 唯一约束覆盖，
  锁等待/唤醒本身为 PostgreSQL 既有机制。

## 设计取舍与限制

- 约束用延迟触发器而非排他约束：覆盖检查需要"观测必须有归属"这类跨表规则，
  且要求事务中途可违规、提交时统一校验；
- 提交时校验按站点去重（会话临时表 `pending_station_checks`，`ON COMMIT DELETE ROWS`），
  一笔事务改 N 行只对该站检一次；站点行再次被修改时"已检"标记失效（兼容 `SET CONSTRAINTS IMMEDIATE`）；
- 抄报的"互斥"用独立锚表 `reading_slots` 而非仅靠应用检查：唯一索引由数据库在任何并发时序下强制，
  不依赖锁等待唤醒；自动读数优先级通过槽位 `kind` 改派 + 抄报 `withdrawn` 联动实现；
- 顶替触发器（`aa_` 前缀）刻意命名在完整性兜底检查（`zz_` 前缀）之前：同一事件的约束触发器按名字
  字母序执行，保证先顶替、后由 `zz_` 对受影响站点重新强检，避免"每站一轮"去重导致漏检；
- 超级用户/表主仍可 `DISABLE TRIGGER` 或 `session_replication_role=replica` 绕过——
  数据库层面无法防止拥有者自毁，生产上应收回应用账号的 DDL 权限；
- 校验为每站 `O(时段数² → 索引下近似线性)` + 一次反连接扫描，适合站点级并发负载；
  超大批量导入建议走 `/changesets` 单事务提交。
