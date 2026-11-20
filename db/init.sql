-- ============================================================================
-- 流域归属与观测完整性约束
--
--   * 归属时段: [valid_from, valid_to) 左闭右开, valid_to 可空(=至今有效)
--   * 规则1: 同一站点任意两个时段不得重叠
--   * 规则2: 每条观测在采样时刻必须恰好落入本站的某一个归属时段
--            (不重叠保证"至多一段", 覆盖检查保证"至少一段", 合起来即"恰好一段")
--   * 约束以 DEFERRABLE INITIALLY DEFERRED 触发器在提交时统一校验:
--       事务中途允许暂时违规(便于拆分/合并/更正时段并补改观测),
--       提交时必须全部满足, 否则整笔回滚
--   * 修改时段/观测前按站点取行锁(SELECT ... FOR UPDATE), 使同站并发写
--     串行化; 不同站持不同锁行, 互不阻塞
--   * 约束保存在数据库目录中, 对 API 与任意直连 SQL 会话(READ COMMITTED)
--     同样生效, 重启后仍然有效
-- ============================================================================

BEGIN;

CREATE EXTENSION IF NOT EXISTS btree_gist;

CREATE TABLE stations (
    station_id  text PRIMARY KEY,
    name        text,
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE attribution_periods (
    period_id   bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    station_id  text NOT NULL REFERENCES stations (station_id),
    watershed   text NOT NULL,
    valid_from  timestamptz NOT NULL,
    valid_to    timestamptz,                       -- NULL = 至今有效
    CONSTRAINT period_bounds_ok CHECK (valid_to IS NULL OR valid_to > valid_from)
);

CREATE TABLE observations (
    observation_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    station_id  text NOT NULL REFERENCES stations (station_id),
    observed_at timestamptz NOT NULL,
    value       double precision NOT NULL
);

CREATE INDEX periods_station_range_idx
    ON attribution_periods USING gist (station_id, tstzrange(valid_from, valid_to, '[)'));
CREATE INDEX observations_station_time_idx
    ON observations (station_id, observed_at);

-- ----------------------------------------------------------------------------
-- 单站完整性强检: 无重叠时段 + 每条观测都有归属
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION check_station_integrity(p_station_id text) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    v_bad bigint;
BEGIN
    SELECT a.period_id INTO v_bad
    FROM attribution_periods a
    JOIN attribution_periods b
      ON b.station_id = a.station_id
     AND a.period_id < b.period_id
     AND tstzrange(a.valid_from, a.valid_to, '[)') && tstzrange(b.valid_from, b.valid_to, '[)')
    WHERE a.station_id = p_station_id
    LIMIT 1;
    IF FOUND THEN
        RAISE EXCEPTION 'station %: attribution periods overlap (period_id=%)', p_station_id, v_bad
            USING ERRCODE = '23514';
    END IF;

    SELECT o.observation_id INTO v_bad
    FROM observations o
    WHERE o.station_id = p_station_id
      AND NOT EXISTS (
            SELECT 1
            FROM attribution_periods p
            WHERE p.station_id = o.station_id
              AND tstzrange(p.valid_from, p.valid_to, '[)') @> o.observed_at
          )
    LIMIT 1;
    IF FOUND THEN
        RAISE EXCEPTION 'station %: observation % has no covering attribution period', p_station_id, v_bad
            USING ERRCODE = '23514';
    END IF;
END;
$$;

-- ----------------------------------------------------------------------------
-- BEFORE 行触发器: 按站点取排他行锁(升序加锁降低死锁概率),
-- 并使该站本事务内的"已检"标记失效(支持 SET CONSTRAINTS ... IMMEDIATE 后继续改)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION before_change_station() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    v_stations text[];
    s text;
BEGIN
    IF to_regclass('pg_temp.pending_station_checks') IS NULL THEN
        CREATE TEMP TABLE pending_station_checks (station_id text PRIMARY KEY) ON COMMIT DELETE ROWS;
    END IF;

    IF TG_OP = 'DELETE' THEN
        v_stations := ARRAY[OLD.station_id];
    ELSIF TG_OP = 'INSERT' THEN
        v_stations := ARRAY[NEW.station_id];
    ELSIF NEW.station_id IS DISTINCT FROM OLD.station_id THEN
        v_stations := ARRAY[OLD.station_id, NEW.station_id];
    ELSE
        v_stations := ARRAY[NEW.station_id];
    END IF;

    SELECT array_agg(x ORDER BY x) INTO v_stations FROM unnest(v_stations) AS x;

    FOREACH s IN ARRAY v_stations LOOP
        PERFORM 1 FROM stations WHERE station_id = s FOR UPDATE;
        DELETE FROM pg_temp.pending_station_checks WHERE station_id = s;
    END LOOP;

    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    ELSE
        RETURN NEW;
    END IF;
END;
$$;

-- ----------------------------------------------------------------------------
-- 提交时(DEFERRABLE INITIALLY DEFERRED)对受影响站点跑强检; 同一轮触发内去重
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION deferred_station_check() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    v_stations text[];
    s text;
BEGIN
    IF to_regclass('pg_temp.pending_station_checks') IS NULL THEN
        CREATE TEMP TABLE pending_station_checks (station_id text PRIMARY KEY) ON COMMIT DELETE ROWS;
    END IF;

    IF TG_OP = 'DELETE' THEN
        v_stations := ARRAY[OLD.station_id];
    ELSIF TG_OP = 'INSERT' THEN
        v_stations := ARRAY[NEW.station_id];
    ELSIF NEW.station_id IS DISTINCT FROM OLD.station_id THEN
        v_stations := ARRAY[OLD.station_id, NEW.station_id];
    ELSE
        v_stations := ARRAY[NEW.station_id];
    END IF;

    SELECT array_agg(x ORDER BY x) INTO v_stations FROM unnest(v_stations) AS x;

    FOREACH s IN ARRAY v_stations LOOP
        INSERT INTO pg_temp.pending_station_checks (station_id) VALUES (s) ON CONFLICT DO NOTHING;
        IF FOUND THEN
            PERFORM check_station_integrity(s);
        END IF;
    END LOOP;
    RETURN NULL;
END;
$$;

CREATE TRIGGER periods_lock_station
    BEFORE INSERT OR UPDATE OR DELETE ON attribution_periods
    FOR EACH ROW EXECUTE FUNCTION before_change_station();
CREATE TRIGGER observations_lock_station
    BEFORE INSERT OR UPDATE OR DELETE ON observations
    FOR EACH ROW EXECUTE FUNCTION before_change_station();

CREATE CONSTRAINT TRIGGER zz_periods_check_at_commit
    AFTER INSERT OR UPDATE OR DELETE ON attribution_periods
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION deferred_station_check();
CREATE CONSTRAINT TRIGGER zz_observations_check_at_commit
    AFTER INSERT OR UPDATE OR DELETE ON observations
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION deferred_station_check();

-- ----------------------------------------------------------------------------
-- 禁止 TRUNCATE (它绕过行触发器, 会留下无归属观测)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION forbid_truncate() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'TRUNCATE is not allowed on %: integrity triggers must run per row', TG_TABLE_NAME;
END;
$$;

CREATE TRIGGER periods_no_truncate
    BEFORE TRUNCATE ON attribution_periods
    FOR EACH STATEMENT EXECUTE FUNCTION forbid_truncate();
CREATE TRIGGER observations_no_truncate
    BEFORE TRUNCATE ON observations
    FOR EACH STATEMENT EXECUTE FUNCTION forbid_truncate();

-- ----------------------------------------------------------------------------
-- 人工抄报(水尺抄报): 断网期间值班员手工登记的临时读数
--
--   * 同站同刻只登记一次: UNIQUE (station_id, observed_at)
--   * 接收条件(提交时延迟校验, API 与直连 SQL 同规则):
--       (a) 采样时刻必须落入本站某归属时段(有流域归属);
--       (b) 同站同刻不得已存在自动读数 observations
--   * 抄报可在失去归属时保留: 提交后时段被删/改导致无归属时不报错,
--     仅在供数视图中"暂停供数", 重新有归属后自动恢复
--   * 同站同刻自动读数一旦提交成功 -> 抄报置 withdrawn=true 永久退出供数;
--     之后即使删除自动读数也不恢复(删除事件不触发任何恢复动作)
--   * 补传回滚 / 提交前撤掉自动读数: 约束触发器对"插入后又删除"的行不触发,
--     抄报不会退出
-- ----------------------------------------------------------------------------
CREATE TABLE manual_readings (
    manual_reading_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    station_id   text NOT NULL REFERENCES stations (station_id),
    observed_at  timestamptz NOT NULL,
    value        double precision NOT NULL,
    report_no    text NOT NULL,                      -- 抄报单号(一票可含多条, 不设唯一)
    withdrawn    boolean NOT NULL DEFAULT false,     -- true=已被自动读数顶替, 永久退出供数
    withdrawn_at timestamptz,
    created_at   timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT manual_readings_one_per_slot UNIQUE (station_id, observed_at)
);

CREATE INDEX manual_readings_station_time_idx
    ON manual_readings (station_id, observed_at);

-- ----------------------------------------------------------------------------
-- 读数槽位锚表: 同站同刻全库至多一个"有效读数来源"(自动 or 人工)
--
-- 这是不依赖锁等待唤醒时序的硬保证: 无论 API 与直连 SQL 如何并发,
-- 两个事务在同一 (station_id, observed_at) 上 INSERT 同一槽位时,
-- 唯一索引只会放行一个, 另一个在提交/语句期收到 23505 唯一冲突.
--   * 抄报登记  -> 占 kind='manual' 槽位
--   * 自动读数  -> 占 kind='automatic' 槽位; 顶替时把已存在的 manual 槽位改为 automatic
--   * 自动读数删除 -> 槽位保留(kind 仍 automatic), 旧抄报不恢复;
--                     槽位行随显式清理/级联维护, 不影响历史台账查询
-- ----------------------------------------------------------------------------
CREATE TABLE reading_slots (
    station_id  text NOT NULL REFERENCES stations (station_id) ON DELETE CASCADE,
    observed_at timestamptz NOT NULL,
    kind        text NOT NULL CHECK (kind IN ('manual', 'automatic')),
    PRIMARY KEY (station_id, observed_at)
);

CREATE INDEX reading_slots_station_idx ON reading_slots (station_id, observed_at);

-- ----------------------------------------------------------------------------
-- 抄报登记的提交时校验: 有归属 + 同刻无自动读数
-- (仅 INSERT/UPDATE; 行在触发前被删除则事件作废, 与补传回滚场景天然一致)
-- ----------------------------------------------------------------------------

-- 持锁即时校验: 站点行锁把同站写串行化, 拿到锁后此处读到的必然是并发方
-- 已提交的最新状态(READ COMMITTED 每条语句新快照), 用于堵住"双方都通过"的竞态;
-- 提交时的延迟约束触发器再做一次兜底, 覆盖本事务后续又写入自动读数等情况.
-- 注意: 只统计"本事务之外已提交"的自动读数(xmin 不等于当前事务 id),
-- 同事务内后写的自动读数由提交时延迟检查负责, 不在此拦截
CREATE OR REPLACE FUNCTION manual_reading_locked_check() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    v_my_xid xid;
BEGIN
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;

    v_my_xid := pg_current_xact_id()::text::xid;

    IF NOT EXISTS (
        SELECT 1
        FROM attribution_periods p
        WHERE p.station_id = NEW.station_id
          AND tstzrange(p.valid_from, p.valid_to, '[)') @> NEW.observed_at
    ) THEN
        RAISE EXCEPTION
            'station %: manual reading at % rejected: no covering attribution period',
            NEW.station_id, NEW.observed_at
            USING ERRCODE = '23514';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM observations o
        WHERE o.station_id = NEW.station_id
          AND o.observed_at = NEW.observed_at
          AND o.xmin <> v_my_xid
    ) THEN
        RAISE EXCEPTION
            'station %: manual reading at % rejected: automatic observation already exists',
            NEW.station_id, NEW.observed_at
            USING ERRCODE = '23514';
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER manual_readings_locked_check
    BEFORE INSERT OR UPDATE ON manual_readings
    FOR EACH ROW EXECUTE FUNCTION manual_reading_locked_check();

-- 抄报槽位维护: 插入生效抄报即占用 manual 槽位(同站同刻唯一索引硬保证);
-- 删除未退出抄报时释放槽位(已退出抄报的槽位已属 automatic, 不动)
CREATE OR REPLACE FUNCTION manual_reading_slot_sync() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO reading_slots (station_id, observed_at, kind)
        VALUES (NEW.station_id, NEW.observed_at, 'manual');
        RETURN NEW;
    END IF;

    IF TG_OP = 'UPDATE' THEN
        -- 被顶替(withdrawn false->true)时槽位改归 automatic; 其余情况槽位不变
        IF NEW.withdrawn AND NOT OLD.withdrawn THEN
            UPDATE reading_slots SET kind = 'automatic'
             WHERE station_id = NEW.station_id AND observed_at = NEW.observed_at;
        END IF;
        RETURN NEW;
    END IF;

    -- DELETE: 仅当槽位仍属 manual 才释放(automatic 槽位留给自动读数, 不恢复抄报)
    DELETE FROM reading_slots
     WHERE station_id = OLD.station_id
       AND observed_at = OLD.observed_at
       AND kind = 'manual'
       AND NOT OLD.withdrawn;
    RETURN OLD;
END;
$$;

CREATE TRIGGER manual_readings_slot_sync
    AFTER INSERT OR UPDATE OR DELETE ON manual_readings
    FOR EACH ROW EXECUTE FUNCTION manual_reading_slot_sync();

CREATE OR REPLACE FUNCTION deferred_manual_reading_check() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;

    -- 行可能在触发事件入队后、提交前被删除(登记撤销): 以提交时仍存在为准
    IF NOT EXISTS (
        SELECT 1 FROM manual_readings WHERE manual_reading_id = NEW.manual_reading_id
    ) THEN
        RETURN NULL;
    END IF;

    -- 已退出(withdrawn)的抄报是历史台账: 同刻本就存在自动读数, 不再适用接收条件;
    -- 也允许其在退出后因时段调整而失去归属(仅暂停供数, 记录保留).
    -- 但该 UPDATE 由 observations 的顶替触发器在提交阶段引发, 此时同站自动读数的
    -- 完整性检查可能已按"每站一轮"去重提前跑完, 必须在此对该站再兜底复检一次,
    -- 防止"补传 + 另一无归属自动读数"同事务提交时漏检
    IF NEW.withdrawn THEN
        PERFORM check_station_integrity(NEW.station_id);
        RETURN NULL;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM attribution_periods p
        WHERE p.station_id = NEW.station_id
          AND tstzrange(p.valid_from, p.valid_to, '[)') @> NEW.observed_at
    ) THEN
        RAISE EXCEPTION
            'station %: manual reading at % rejected: no covering attribution period',
            NEW.station_id, NEW.observed_at
            USING ERRCODE = '23514';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM observations o
        WHERE o.station_id = NEW.station_id
          AND o.observed_at = NEW.observed_at
    ) THEN
        RAISE EXCEPTION
            'station %: manual reading at % rejected: automatic observation already exists',
            NEW.station_id, NEW.observed_at
            USING ERRCODE = '23514';
    END IF;

    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER manual_readings_check_at_commit
    AFTER INSERT OR UPDATE ON manual_readings
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION deferred_manual_reading_check();

-- 写抄报前同样按站点取行锁: 与自动读数/时段写串行化, 复用既有锁函数
CREATE TRIGGER manual_readings_lock_station
    BEFORE INSERT OR UPDATE OR DELETE ON manual_readings
    FOR EACH ROW EXECUTE FUNCTION before_change_station();

-- ----------------------------------------------------------------------------
-- 自动读数提交成功 -> 同站同刻抄报永久退出供数
--   * 延迟约束触发器: 仅在事务真正提交时生效; 回滚则整体消失
--   * 行在触发前被 INSERT 后又 DELETE(提交前撤掉) -> 提交时行已不存在, 不顶替
--   * DELETE 事件显式不做任何事: 删除自动读数绝不恢复旧抄报
--   * 命名 aa_ 前缀: 同一事件的约束触发器按名字字母序执行,
--     顶替必须先于 zz_ 完整性兜底检查(它对 manual_readings 的 UPDATE 会让
--     该站"已检"标记失效, 由随后的 zz_ 检查重新强检)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION withdraw_manual_readings() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RETURN NULL;
    END IF;

    -- 关键: 行级 AFTER 事件不会因同事务内"插入后又删除"而合并,
    -- 因此提交时必须确认该自动读数行仍然存在; 提交前撤掉(INSERT->DELETE)
    -- 或补传整笔回滚(触发器随事务一起消失)都不会顶替抄报
    IF NOT EXISTS (
        SELECT 1 FROM observations WHERE observation_id = NEW.observation_id
    ) THEN
        RETURN NULL;
    END IF;

    -- 占用/改派槽位为 automatic: 若该刻是 manual 槽位则改派(同时下面退出抄报);
    -- 若已存在 automatic 槽位(同事务另一条同刻自动读数)则幂等无操作.
    -- 唯一约束在此兜底: 并发的 manual 槽位插入若先提交, 本 UPDATE 会等待其行锁,
    -- 锁释放后把槽位改派为 automatic —— 自动读数优先, 抄报退出.
    INSERT INTO reading_slots (station_id, observed_at, kind)
    VALUES (NEW.station_id, NEW.observed_at, 'automatic')
    ON CONFLICT (station_id, observed_at)
    DO UPDATE SET kind = 'automatic'
        WHERE reading_slots.kind = 'manual';

    UPDATE manual_readings
       SET withdrawn = true,
           withdrawn_at = COALESCE(withdrawn_at, now())
     WHERE station_id = NEW.station_id
       AND observed_at = NEW.observed_at
       AND withdrawn = false;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER aa_observations_withdraw_manual
    AFTER INSERT OR UPDATE ON observations
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION withdraw_manual_readings();

-- 自动读数删除/改时刻: 释放不再被任何自动读数占用的旧槽位(旧抄报不恢复).
-- 命名 zz_ 且为延迟触发: 在 aa_ 顶替(占用新槽位)之后执行, 避免误删同事务新占的槽位
CREATE OR REPLACE FUNCTION observation_slot_release() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'UPDATE' THEN
        -- 时刻未变: 槽位已由顶替函数维护, 无需释放
        IF NEW.observed_at IS NOT DISTINCT FROM OLD.observed_at
           AND NEW.station_id IS NOT DISTINCT FROM OLD.station_id THEN
            RETURN NULL;
        END IF;
    ELSIF TG_OP <> 'DELETE' THEN
        RETURN NULL;
    END IF;

    DELETE FROM reading_slots rs
     WHERE rs.station_id = OLD.station_id
       AND rs.observed_at = OLD.observed_at
       AND rs.kind = 'automatic'
       AND NOT EXISTS (
            SELECT 1 FROM observations o
             WHERE o.station_id = OLD.station_id
               AND o.observed_at = OLD.observed_at
       );
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER zz_observations_release_slot
    AFTER DELETE OR UPDATE ON observations
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION observation_slot_release();

CREATE TRIGGER manual_readings_no_truncate
    BEFORE TRUNCATE ON manual_readings
    FOR EACH STATEMENT EXECUTE FUNCTION forbid_truncate();

CREATE TRIGGER reading_slots_no_truncate
    BEFORE TRUNCATE ON reading_slots
    FOR EACH STATEMENT EXECUTE FUNCTION forbid_truncate();

-- ----------------------------------------------------------------------------
-- 供数视图: 按采样时刻关联流域 (READ COMMITTED 下只见完整已提交状态)
--   * 自动读数: 完整性约束保证必有归属, 恒可供数
--   * 人工抄报: INNER JOIN 时段 -> 缺归属时自动"暂停供数";
--               withdrawn=true 的抄报永久剔除(即使后来删了自动读数)
-- ----------------------------------------------------------------------------
CREATE VIEW observation_attribution AS
SELECT o.observation_id,
       o.station_id,
       o.observed_at,
       o.value,
       p.period_id,
       p.watershed,
       p.valid_from AS period_valid_from,
       p.valid_to   AS period_valid_to
FROM observations o
JOIN attribution_periods p
  ON p.station_id = o.station_id
 AND tstzrange(p.valid_from, p.valid_to, '[)') @> o.observed_at;

-- 统一供数视图: 自动读数 + 生效中的人工抄报, 带来源标记
CREATE VIEW reading_feed AS
SELECT o.observation_id AS reading_id,
       o.station_id,
       o.observed_at,
       o.value,
       p.watershed,
       'automatic'::text AS source,
       NULL::text AS report_no,
       p.period_id
FROM observations o
JOIN attribution_periods p
  ON p.station_id = o.station_id
 AND tstzrange(p.valid_from, p.valid_to, '[)') @> o.observed_at
UNION ALL
SELECT m.manual_reading_id,
       m.station_id,
       m.observed_at,
       m.value,
       p.watershed,
       'manual'::text,
       m.report_no,
       p.period_id
FROM manual_readings m
JOIN attribution_periods p
  ON p.station_id = m.station_id
 AND tstzrange(p.valid_from, p.valid_to, '[)') @> m.observed_at
WHERE m.withdrawn = false;

-- 抄报登记台账视图: 含已退出( withdrawn )抄报的原值/单号/退出状态;
-- LEFT JOIN: 失去归属时 watershed/period_id 为空(供数已暂停, 记录仍可查)
CREATE VIEW manual_reading_status AS
SELECT m.manual_reading_id,
       m.station_id,
       m.observed_at,
       m.value,
       m.report_no,
       m.withdrawn,
       m.withdrawn_at,
       m.created_at,
       p.watershed,
       p.period_id
FROM manual_readings m
LEFT JOIN attribution_periods p
  ON p.station_id = m.station_id
 AND tstzrange(p.valid_from, p.valid_to, '[)') @> m.observed_at;

COMMIT;
