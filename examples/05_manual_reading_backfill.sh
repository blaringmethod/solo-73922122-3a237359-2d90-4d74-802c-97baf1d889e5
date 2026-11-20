#!/usr/bin/env bash
# 示例5: 断网水尺抄报登记 -> 自动读数补传顶替 -> 删除不恢复 的完整取数对比
#
# 演示:
#   1. 断网期间值班员登记抄报(同站同刻仅一条, 须有流域归属且无自动读数)
#   2. 补传前 /readings 取数: 来源 manual, 带抄报单号与采样时流域
#   3. 同刻自动读数补传成功 -> 抄报永久退出供数(withdrawn=true)
#   4. 补传后 /readings 取数: 只剩 automatic; 台账仍可查抄报原值/单号/退出状态
#   5. 删掉自动读数 -> 旧抄报不恢复
#   6. 无归属时刻登记被拒; 同站同刻重复登记被拒
set -euo pipefail

API=${API_URL:-http://localhost:8000}
SID=${SID:-ST-MANUAL-$(date +%Y%m%d%H%M%S)}
SLOT="2026-03-01T00:00:00Z"

echo "== 站点: $SID =="

echo "-- 1. 建站 + 归属时段 [2026-01-01, 至今) 属 WS-HEADWATER"
curl -sS -X POST "$API/stations" -H 'Content-Type: application/json' \
  -d "{\"station_id\":\"$SID\",\"name\":\"断网演示站\"}" | jq .
curl -sS -X POST "$API/changesets" -H 'Content-Type: application/json' -d "{
  \"operations\": [
    {\"op\":\"insert_period\",\"station_id\":\"$SID\",\"watershed\":\"WS-HEADWATER\",
     \"valid_from\":\"2026-01-01T00:00:00Z\",\"valid_to\":null}
  ]
}" | jq .

echo
echo "-- 2. 断网: 登记水尺抄报(单号 RPT-20260301-01)"
curl -sS -X POST "$API/manual-readings" -H 'Content-Type: application/json' -d "{
  \"station_id\":\"$SID\",\"observed_at\":\"$SLOT\",\"value\":10.5,\"report_no\":\"RPT-20260301-01\"
}" | jq .

echo
echo "-- 3a. [补传前] 统一供数 /readings: 来源 manual, 含单号与采样时流域"
curl -sS "$API/readings?station_id=$SID" | jq .

echo "-- 3b. 抄报台账 /manual-readings: withdrawn=false(生效中)"
curl -sS "$API/manual-readings?station_id=$SID" | jq .

echo
echo "-- 4. 异常登记被拒:"
echo "   4a. 同站同刻重复登记(预期 409 duplicate)"
curl -sS -o /dev/null -w "HTTP %{http_code}\n" -X POST "$API/manual-readings" \
  -H 'Content-Type: application/json' -d "{
    \"station_id\":\"$SID\",\"observed_at\":\"$SLOT\",\"value\":11.0,\"report_no\":\"RPT-DUP\"}"
echo "   4b. 无流域归属时刻登记(预期 409 no covering attribution period)"
curl -sS -o /dev/null -w "HTTP %{http_code}\n" -X POST "$API/manual-readings" \
  -H 'Content-Type: application/json' -d "{
    \"station_id\":\"$SID\",\"observed_at\":\"2019-01-01T00:00:00Z\",\"value\":1.0,\"report_no\":\"RPT-NOCOV\"}"

echo
echo "-- 5. 网络恢复: 同刻自动读数补传(预期 201)"
curl -sS -X POST "$API/observations" -H 'Content-Type: application/json' -d "{
  \"station_id\":\"$SID\",\"observed_at\":\"$SLOT\",\"value\":10.9
}" | jq .

echo
echo "-- 6a. [补传后] 统一供数 /readings: 只剩 automatic, 抄报已退出"
curl -sS "$API/readings?station_id=$SID" | jq .

echo "-- 6b. 抄报台账: withdrawn=true, 原值 10.5 与单号仍可查(永久留痕)"
curl -sS "$API/manual-readings?station_id=$SID" | jq .

echo
echo "-- 7. 删掉自动读数 -> 旧抄报不恢复(/readings 该刻为空, 台账 withdrawn 仍为 true)"
OBS_ID=$(curl -sS "$API/attributed-observations?station_id=$SID" | jq '.[0].observation_id')
echo "   删除 observation_id=$OBS_ID"
DSN=${DATABASE_URL:-postgresql://watershed:watershed@localhost:5432/watershed}
if command -v docker >/dev/null 2>&1; then
  docker compose exec -T db psql "$DSN" -c "DELETE FROM observations WHERE observation_id=$OBS_ID;"
else
  psql "$DSN" -c "DELETE FROM observations WHERE observation_id=$OBS_ID;"
fi
echo "   /readings(预期空数组):"
curl -sS "$API/readings?station_id=$SID" | jq .
echo "   台账(预期 withdrawn 仍为 true):"
curl -sS "$API/manual-readings?station_id=$SID" | jq '.[] | {report_no, value, withdrawn}'

echo
echo "== 演示完成 =="
