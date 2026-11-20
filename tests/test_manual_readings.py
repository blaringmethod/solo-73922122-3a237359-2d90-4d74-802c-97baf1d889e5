"""人工抄报(水尺抄报)集成测试: 需要运行中的 API 与数据库.

运行: API_URL=... DATABASE_URL=... python -m pytest tests/test_manual_readings.py -v
"""
from __future__ import annotations

import os
import uuid

import httpx
import psycopg
import pytest

API = os.environ.get("API_URL", "http://localhost:8000")
DSN = os.environ.get("DATABASE_URL", "postgresql://watershed:watershed@localhost:5432/watershed")


def new_sid() -> str:
    return "M-" + uuid.uuid4().hex[:12]


@pytest.fixture()
def client():
    with httpx.Client(base_url=API, timeout=30) as c:
        yield c


def mk_station_with_period(client, sid, vfrom="2026-01-01T00:00:00Z", vto=None, ws="WS-A"):
    r = client.post("/stations", json={"station_id": sid, "name": "manual-test"})
    assert r.status_code == 201, r.text
    r = client.post("/changesets", json={"operations": [
        {"op": "insert_period", "station_id": sid, "watershed": ws,
         "valid_from": vfrom, "valid_to": vto},
    ]})
    assert r.status_code == 201, r.text


def register(client, **kw):
    return client.post("/manual-readings", json=kw)


def readings(client, sid, **params):
    return client.get("/readings", params={"station_id": sid, **params})


def ledger(client, sid, **params):
    return client.get("/manual-readings", params={"station_id": sid, **params})


# ---------------------------------------------------------------------------
# 登记接收条件
# ---------------------------------------------------------------------------

def test_register_manual_reading_ok_and_feeds(client):
    sid = new_sid()
    mk_station_with_period(client, sid)
    r = register(client, station_id=sid, observed_at="2026-03-01T00:00:00Z",
                 value=10.5, report_no="RPT-1")
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["withdrawn"] is False
    assert body["report_no"] == "RPT-1"

    feed = readings(client, sid).json()
    assert len(feed) == 1
    assert feed[0]["source"] == "manual"
    assert feed[0]["watershed"] == "WS-A"
    assert feed[0]["report_no"] == "RPT-1"
    assert feed[0]["value"] == 10.5


def test_duplicate_same_station_same_time_rejected(client):
    sid = new_sid()
    mk_station_with_period(client, sid)
    kw = dict(station_id=sid, observed_at="2026-03-01T00:00:00Z", value=1.0, report_no="R1")
    assert register(client, **kw).status_code == 201
    r = register(client, **{**kw, "report_no": "R2", "value": 2.0})
    assert r.status_code == 409, r.text
    # 只有一条, 且保留首次登记内容
    rows = ledger(client, sid).json()
    assert len(rows) == 1 and rows[0]["report_no"] == "R1"


def test_register_without_attribution_rejected(client):
    sid = new_sid()
    mk_station_with_period(client, sid, vto="2026-02-01T00:00:00Z")
    r = register(client, station_id=sid, observed_at="2026-03-01T00:00:00Z",
                 value=1.0, report_no="R1")
    assert r.status_code == 409 and "no covering attribution period" in r.json()["detail"]
    assert ledger(client, sid).json() == []


def test_register_when_automatic_exists_rejected(client):
    sid = new_sid()
    mk_station_with_period(client, sid)
    r = client.post("/observations", json={
        "station_id": sid, "observed_at": "2026-03-01T00:00:00Z", "value": 20.0})
    assert r.status_code == 201, r.text
    r = register(client, station_id=sid, observed_at="2026-03-01T00:00:00Z",
                 value=19.0, report_no="R1")
    assert r.status_code == 409 and "automatic observation already exists" in r.json()["detail"]
    assert ledger(client, sid).json() == []


def test_register_unknown_station_404(client):
    r = register(client, station_id=new_sid(), observed_at="2026-03-01T00:00:00Z",
                 value=1.0, report_no="R1")
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# 暂停供数 / 恢复
# ---------------------------------------------------------------------------

def test_manual_reading_pauses_without_attribution_and_resumes(client):
    sid = new_sid()
    mk_station_with_period(client, sid, vto="2026-06-01T00:00:00Z")
    register(client, station_id=sid, observed_at="2026-03-01T00:00:00Z",
             value=10.5, report_no="R1")
    assert len(readings(client, sid).json()) == 1

    # 缩短时段使采样时刻失去归属 (该站无自动读数, 撤段合法)
    ps = client.get(f"/stations/{sid}/periods").json()
    r = client.post("/changesets", json={"operations": [
        {"op": "update_period", "period_id": ps[0]["period_id"],
         "valid_to": "2026-02-01T00:00:00Z"},
    ]})
    assert r.status_code == 201, r.text

    # 供数暂停
    assert readings(client, sid).json() == []
    # 台账仍在, watershed 为空, 未退出
    led = ledger(client, sid).json()
    assert len(led) == 1
    assert led[0]["watershed"] is None and led[0]["withdrawn"] is False

    # 重新覆盖 -> 恢复供数, 流域按采样时新归属
    r = client.post("/changesets", json={"operations": [
        {"op": "insert_period", "station_id": sid, "watershed": "WS-NEW",
         "valid_from": "2026-02-01T00:00:00Z", "valid_to": None},
    ]})
    assert r.status_code == 201, r.text
    feed = readings(client, sid).json()
    assert len(feed) == 1 and feed[0]["watershed"] == "WS-NEW" and feed[0]["source"] == "manual"


# ---------------------------------------------------------------------------
# 自动读数顶替: 永久退出 + 删除不恢复
# ---------------------------------------------------------------------------

def test_automatic_backfill_withdraws_manual_permanently(client):
    sid = new_sid()
    mk_station_with_period(client, sid)
    register(client, station_id=sid, observed_at="2026-03-01T00:00:00Z",
             value=10.5, report_no="R1")

    # 补传同刻自动读数 -> 成功, 抄报退出
    r = client.post("/observations", json={
        "station_id": sid, "observed_at": "2026-03-01T00:00:00Z", "value": 10.9})
    assert r.status_code == 201, r.text

    feed = readings(client, sid).json()
    assert len(feed) == 1 and feed[0]["source"] == "automatic" and feed[0]["value"] == 10.9

    led = ledger(client, sid).json()
    assert len(led) == 1
    row = led[0]
    assert row["withdrawn"] is True
    assert row["value"] == 10.5 and row["report_no"] == "R1"  # 原值/单号可查
    assert row["withdrawn_at"] is not None

    # 删掉自动读数 -> 抄报不恢复
    with psycopg.connect(DSN) as c:
        c.execute("DELETE FROM observations WHERE station_id=%s AND observed_at=%s",
                  (sid, "2026-03-01 00:00+00"))
        c.commit()
    assert readings(client, sid).json() == []
    row = ledger(client, sid).json()[0]
    assert row["withdrawn"] is True

    # include_withdrawn=false 时台账也过滤掉
    assert ledger(client, sid, include_withdrawn="false").json() == []


def test_withdrawn_manual_remains_queryable_with_time_window(client):
    sid = new_sid()
    mk_station_with_period(client, sid)
    register(client, station_id=sid, observed_at="2026-03-01T00:00:00Z",
             value=10.5, report_no="R1")
    client.post("/observations", json={
        "station_id": sid, "observed_at": "2026-03-01T00:00:00Z", "value": 10.9})
    # 时段窗口查询
    rows = ledger(client, sid, **{"from": "2026-01-01T00:00:00Z",
                                  "to": "2026-04-01T00:00:00Z"}).json()
    assert len(rows) == 1 and rows[0]["withdrawn"] is True
    # 窗口外
    assert ledger(client, sid, **{"from": "2027-01-01T00:00:00Z"}).json() == []


# ---------------------------------------------------------------------------
# 回滚 / 提交前撤销 -> 不退出 (直连 SQL 路径, 验证库级约束对 SQL 同样生效)
# ---------------------------------------------------------------------------

def test_backfill_rollback_does_not_withdraw(client):
    sid = new_sid()
    # 有限上界, 便于制造无归属自动读数触发整笔回滚
    mk_station_with_period(client, sid, vto="2027-01-01T00:00:00Z")
    register(client, station_id=sid, observed_at="2026-05-01T00:00:00Z",
             value=29.0, report_no="R9")
    with psycopg.connect(DSN) as c:
        c.execute("INSERT INTO observations (station_id, observed_at, value)"
                  " VALUES (%s, %s, %s)", (sid, "2026-05-01 00:00+00", 30.0))
        c.execute("INSERT INTO observations (station_id, observed_at, value)"
                  " VALUES (%s, %s, %s)", (sid, "2030-05-01 00:00+00", 1.0))
        with pytest.raises(psycopg.errors.CheckViolation):
            c.commit()
    row = ledger(client, sid).json()[0]
    assert row["withdrawn"] is False
    feed = readings(client, sid).json()
    assert len(feed) == 1 and feed[0]["source"] == "manual"


def test_insert_then_delete_before_commit_does_not_withdraw(client):
    sid = new_sid()
    mk_station_with_period(client, sid)
    register(client, station_id=sid, observed_at="2026-05-15T00:00:00Z",
             value=40.0, report_no="R10")
    with psycopg.connect(DSN) as c:
        c.execute("INSERT INTO observations (station_id, observed_at, value)"
                  " VALUES (%s, %s, %s)", (sid, "2026-05-15 00:00+00", 41.0))
        c.execute("DELETE FROM observations WHERE station_id=%s AND observed_at=%s",
                  (sid, "2026-05-15 00:00+00"))
        c.commit()  # 提交成功, 但自动读数已撤销
    row = ledger(client, sid).json()[0]
    assert row["withdrawn"] is False
    feed = readings(client, sid).json()
    assert len(feed) == 1 and feed[0]["source"] == "manual"


# ---------------------------------------------------------------------------
# 直连 SQL 登记同样受库级规则约束
# ---------------------------------------------------------------------------

def test_direct_sql_register_obey_rules():
    sid = new_sid()
    try:
        with psycopg.connect(DSN, autocommit=True) as c:
            c.execute("INSERT INTO stations (station_id) VALUES (%s)", (sid,))
            c.execute("INSERT INTO attribution_periods (station_id, watershed, valid_from, valid_to)"
                      " VALUES (%s, 'WS-A', '2026-01-01 00:00+00', '2027-01-01 00:00+00')", (sid,))

        # 直连 SQL 登记成功
        with psycopg.connect(DSN, autocommit=True) as c:
            c.execute("INSERT INTO manual_readings (station_id, observed_at, value, report_no)"
                      " VALUES (%s, %s, %s, %s)", (sid, "2026-03-01 00:00+00", 5.0, "SQL-1"))

        # 直连 SQL 无归属登记 -> 即时持锁检查即在语句期拒绝(autocommit 无延迟阶段)
        with psycopg.connect(DSN, autocommit=True) as c:
            with pytest.raises(psycopg.errors.CheckViolation):
                c.execute("INSERT INTO manual_readings (station_id, observed_at, value, report_no)"
                          " VALUES (%s, %s, %s, %s)", (sid, "2028-03-01 00:00+00", 5.0, "SQL-2"))

        # 直连 SQL 同刻自动读数 -> 抄报退出
        with psycopg.connect(DSN, autocommit=True) as c:
            c.execute("INSERT INTO observations (station_id, observed_at, value)"
                      " VALUES (%s, %s, %s)", (sid, "2026-03-01 00:00+00", 6.0))
        with psycopg.connect(DSN) as c:
            w = c.execute(
                "SELECT withdrawn FROM manual_readings WHERE station_id=%s", (sid,)
            ).fetchone()[0]
            assert w is True
    finally:
        # 清理(独立事务, 保证即使中途断言失败也执行)
        with psycopg.connect(DSN) as c:
            c.execute("DELETE FROM manual_readings WHERE station_id=%s", (sid,))
            c.execute("DELETE FROM observations WHERE station_id=%s", (sid,))
            c.execute("DELETE FROM attribution_periods WHERE station_id=%s", (sid,))
            c.execute("DELETE FROM stations WHERE station_id=%s", (sid,))
            c.commit()


# ---------------------------------------------------------------------------
# API 与直连 SQL 并发写同站同刻: 站点行锁 + 持锁即时检查 + 唯一约束三重保证
# ---------------------------------------------------------------------------

def test_manual_then_automatic_sequential_equivalent(client):
    """顺序化等价(不依赖跨进程锁等待唤醒, 任何环境可跑):

    模拟并发被站点行锁串行化后的两种终态——
      时序A: 抄报先提交, 自动读数后到 -> 自动读数成功且抄报退出;
      时序B: 自动读数先提交, 抄报后到 -> 抄报被拒.
    两种时序都不允许两类读数并存且都"生效".
    """
    # 时序 A: 抄报先, 自动读数补传 -> 顶替
    sid_a = new_sid()
    mk_station_with_period(client, sid_a)
    register(client, station_id=sid_a, observed_at="2026-03-01T00:00:00Z",
             value=1.0, report_no="A1")
    with psycopg.connect(DSN, autocommit=True) as c:
        c.execute("INSERT INTO observations (station_id, observed_at, value)"
                  " VALUES (%s, %s, %s)", (sid_a, "2026-03-01 00:00+00", 2.0))
    feed = readings(client, sid_a).json()
    assert len(feed) == 1 and feed[0]["source"] == "automatic"
    assert ledger(client, sid_a).json()[0]["withdrawn"] is True

    # 时序 B: 自动读数先, 抄报后到 -> 拒绝
    sid_b = new_sid()
    mk_station_with_period(client, sid_b)
    client.post("/observations", json={
        "station_id": sid_b, "observed_at": "2026-03-01T00:00:00Z", "value": 2.0})
    r = register(client, station_id=sid_b, observed_at="2026-03-01T00:00:00Z",
                 value=1.0, report_no="B1")
    assert r.status_code == 409
    assert ledger(client, sid_b).json() == []


def test_concurrent_api_manual_vs_sql_automatic_only_one_wins(client):
    """快速并发(线程, 不主动持锁 sleep, 不依赖锁等待唤醒): 多轮下绝不双成功.

    锁等待/唤醒的跨进程路径在部分沙箱不可用, 本用例靠持锁即时检查与
    (station_id, observed_at) 唯一约束保证; 真正的锁阻塞唤醒由 PostgreSQL
    既有机制负责, 顺序化等价用例已覆盖其终态.
    """
    import threading

    for n in range(10):
        sid = new_sid()
        mk_station_with_period(client, sid)
        slot = "2026-03-01T00:00:00Z"
        outcome = {}
        go = threading.Event()

        def via_api():
            go.wait()
            try:
                r = register(client, station_id=sid, observed_at=slot,
                             value=1.0, report_no=f"C{n}")
                outcome["api"] = r.status_code
            except Exception as exc:  # noqa: BLE001
                outcome["api"] = f"exc: {exc}"

        def via_sql():
            go.wait()
            try:
                with psycopg.connect(DSN, autocommit=True) as c:
                    c.execute("INSERT INTO observations (station_id, observed_at, value)"
                              " VALUES (%s, %s, %s)", (sid, "2026-03-01 00:00+00", 2.0))
                outcome["sql"] = 201
            except Exception as exc:  # noqa: BLE001
                outcome["sql"] = type(exc).__name__

        t1 = threading.Thread(target=via_api)
        t2 = threading.Thread(target=via_sql)
        t1.start(); t2.start(); go.set(); t1.join(timeout=30); t2.join(timeout=30)

        with psycopg.connect(DSN) as c:
            n_auto = c.execute(
                "SELECT count(*) FROM observations WHERE station_id=%s AND observed_at=%s",
                (sid, "2026-03-01 00:00+00")).fetchone()[0]
            # 只统计"仍生效(未退出)"的抄报; 已退出抄报作为历史台账允许与自动读数并存
            n_manual_active = c.execute(
                "SELECT count(*) FROM manual_readings WHERE station_id=%s AND observed_at=%s"
                " AND withdrawn=false",
                (sid, "2026-03-01 00:00+00")).fetchone()[0]
        # 绝不允许"生效抄报与同刻自动读数并存": 有效来源合计至多一条
        assert n_auto + n_manual_active <= 1, (n, outcome, n_auto, n_manual_active)
        if n_manual_active == 1:
            # 抄报赢则其必然未退出, 且自动读数插入失败
            assert ledger(client, sid).json()[0]["withdrawn"] is False
            assert n_auto == 0
        elif n_auto == 1:
            # 自动读数赢: 若抄报行存在则必然已退出, 槽位归 automatic
            with psycopg.connect(DSN) as c:
                slot = c.execute(
                    "SELECT kind FROM reading_slots WHERE station_id=%s AND observed_at=%s",
                    (sid, "2026-03-01 00:00+00")).fetchone()
            assert slot is not None and slot[0] == "automatic"


def test_concurrent_two_manual_registrations_one_rejected(client):
    import threading

    sid = new_sid()
    mk_station_with_period(client, sid)
    slot = "2026-04-01T00:00:00Z"
    codes = []
    go = threading.Event()

    def worker(report):
        go.wait()
        r = register(client, station_id=sid, observed_at=slot, value=1.0, report_no=report)
        codes.append(r.status_code)

    threads = [threading.Thread(target=worker, args=(f"R{i}",)) for i in range(2)]
    for t in threads:
        t.start()
    go.set()
    for t in threads:
        t.join(timeout=30)
    assert sorted(codes) == [201, 409], codes
    assert len(ledger(client, sid).json()) == 1
