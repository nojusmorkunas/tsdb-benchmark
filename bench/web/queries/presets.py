PRESET_QUERIES = {
    "Row count": {
        "sql": "SELECT COUNT(*) AS row_count FROM energy_data",
        "ch": "SELECT COUNT(*) AS row_count FROM energy_data",
        "qdb": "SELECT COUNT(*) AS row_count FROM energy_data",
        "flux": 'from(bucket: "energy") |> range(start: 0) |> filter(fn: (r) => r._measurement == "energy") |> group() |> count()',
        "influx3": "SELECT COUNT(*) AS row_count FROM energy",
    },
    "Total consumption per meter (top 20)": {
        "sql": "SELECT ean, SUM(value) AS total FROM energy_data WHERE direction = 'E17' GROUP BY ean ORDER BY total DESC LIMIT 20",
        "ch": "SELECT ean, SUM(value) AS total FROM energy_data WHERE direction = 'E17' GROUP BY ean ORDER BY total DESC LIMIT 20",
        "qdb": "SELECT ean, SUM(value) AS total FROM energy_data WHERE direction = 'E17' GROUP BY ean ORDER BY total DESC LIMIT 20",
        "flux": 'from(bucket: "energy") |> range(start: 0) |> filter(fn: (r) => r._measurement == "energy" and r.direction == "E17") |> group(columns: ["ean"]) |> sum() |> group() |> sort(columns: ["_value"], desc: true) |> limit(n: 20)',
        "influx3": "SELECT ean, SUM(value) AS total FROM energy WHERE direction = 'E17' GROUP BY ean ORDER BY total DESC LIMIT 20",
    },
    "Daily aggregation by direction": {
        "sql": "SELECT date_trunc('day', ts) AS day, direction, SUM(value) AS total FROM energy_data GROUP BY 1, 2 ORDER BY 1, 2",
        "ch": "SELECT toStartOfDay(ts) AS day, direction, SUM(value) AS total FROM energy_data GROUP BY 1, 2 ORDER BY 1, 2",
        "qdb": "SELECT timestamp, direction, SUM(value) AS total FROM energy_data SAMPLE BY 1d ALIGN TO CALENDAR ORDER BY timestamp",
        "flux": 'from(bucket: "energy") |> range(start: 0) |> aggregateWindow(every: 1d, fn: sum, createEmpty: false) |> group(columns: ["direction", "_time"]) |> sum() |> sort(columns: ["_time"])',
        "influx3": "SELECT date_trunc('day', time) AS day, direction, SUM(value) AS total FROM energy GROUP BY 1, 2 ORDER BY 1, 2",
    },
    "Hourly aggregation (first 3 months)": {
        "sql": "SELECT date_trunc('hour', ts) AS hour, direction, SUM(value) AS total FROM energy_data WHERE ts >= (SELECT MIN(ts) FROM energy_data) AND ts < (SELECT MIN(ts) FROM energy_data) + INTERVAL '3 months' GROUP BY 1, 2 ORDER BY 1, 2",
        "ch": "SELECT toStartOfHour(ts) AS hour, direction, SUM(value) AS total FROM energy_data WHERE ts >= (SELECT MIN(ts) FROM energy_data) AND ts < (SELECT MIN(ts) FROM energy_data) + INTERVAL 3 MONTH GROUP BY 1, 2 ORDER BY 1, 2",
        "qdb": "SELECT timestamp, direction, SUM(value) AS total FROM energy_data WHERE timestamp >= '{QDB_START}' AND timestamp < '{QDB_END_3M}' SAMPLE BY 1h ALIGN TO CALENDAR ORDER BY timestamp",
        "flux": 'from(bucket: "energy") |> range(start: {INFLUX_START}, stop: {INFLUX_END_3M}) |> filter(fn: (r) => r._measurement == "energy" and r._field == "value") |> group(columns: ["direction"]) |> aggregateWindow(every: 1h, fn: sum, createEmpty: false) |> sort(columns: ["_time"])',
        "influx3": "SELECT date_trunc('hour', time) AS hour, direction, SUM(value) AS total FROM energy WHERE time >= (SELECT MIN(time) FROM energy) AND time < (SELECT MIN(time) FROM energy) + INTERVAL '3 months' GROUP BY 1, 2 ORDER BY 1, 2",
    },
    "Peak consumption hours": {
        "sql": "SELECT EXTRACT(hour FROM ts) AS hour_of_day, AVG(value) AS avg_val, MAX(value) AS peak FROM energy_data WHERE direction = 'E17' GROUP BY 1 ORDER BY 1",
        "ch": "SELECT toHour(ts) AS hour_of_day, AVG(value) AS avg_val, MAX(value) AS peak FROM energy_data WHERE direction = 'E17' GROUP BY 1 ORDER BY 1",
        "qdb": "SELECT hour(timestamp) AS hour_of_day, AVG(value) AS avg_val, MAX(value) AS peak FROM energy_data WHERE direction = 'E17' GROUP BY hour(timestamp) ORDER BY hour_of_day",
        "flux": 'from(bucket: "energy") |> range(start: 0) |> filter(fn: (r) => r.direction == "E17") |> aggregateWindow(every: 1h, fn: mean, createEmpty: false) |> limit(n: 24)',
        "influx3": "SELECT EXTRACT(hour FROM time) AS hour_of_day, AVG(value) AS avg_val, MAX(value) AS peak FROM energy WHERE direction = 'E17' GROUP BY 1 ORDER BY 1",
    },
    "Prosumer detection (ratio)": {
        "sql": "WITH t AS (SELECT ean, SUM(CASE WHEN direction='E17' THEN value ELSE 0 END) AS cons, SUM(CASE WHEN direction='E18' THEN value ELSE 0 END) AS inj FROM energy_data GROUP BY 1) SELECT *, ROUND((inj/NULLIF(cons,0)*100)::numeric,1) AS ratio FROM t WHERE cons>0 ORDER BY ratio DESC LIMIT 20",
        "ch": "WITH t AS (SELECT ean, SUM(CASE WHEN direction='E17' THEN value ELSE 0 END) AS cons, SUM(CASE WHEN direction='E18' THEN value ELSE 0 END) AS inj FROM energy_data GROUP BY 1) SELECT *, ROUND(inj/nullIf(cons,0)*100,1) AS ratio FROM t WHERE cons>0 ORDER BY ratio DESC LIMIT 20",
        "qdb": "WITH t AS (SELECT ean, SUM(CASE WHEN direction='E17' THEN value ELSE 0 END) AS cons, SUM(CASE WHEN direction='E18' THEN value ELSE 0 END) AS inj FROM energy_data GROUP BY ean) SELECT ean, cons, inj, round(inj/cons*100, 1) AS ratio FROM t WHERE cons > 0 ORDER BY ratio DESC LIMIT 20",
        "flux": 'from(bucket: "energy") |> range(start: 0) |> group(columns: ["ean", "direction"]) |> sum() |> sort(columns: ["_value"], desc: true) |> limit(n: 40)',
        "influx3": "WITH t AS (SELECT ean, SUM(CASE WHEN direction='E17' THEN value ELSE 0 END) AS cons, SUM(CASE WHEN direction='E18' THEN value ELSE 0 END) AS inj FROM energy GROUP BY 1) SELECT ean, cons, inj, ROUND(inj/nullif(cons,0)*100,1) AS ratio FROM t WHERE cons>0 ORDER BY ratio DESC LIMIT 20",
    },
    "Active meters per day": {
        "sql": "SELECT date_trunc('day', ts) AS day, COUNT(DISTINCT ean) AS active FROM energy_data WHERE value > 0 GROUP BY 1 ORDER BY 1",
        "ch": "SELECT toStartOfDay(ts) AS day, COUNT(DISTINCT ean) AS active FROM energy_data WHERE value > 0 GROUP BY 1 ORDER BY 1",
        "qdb": "SELECT timestamp, COUNT_DISTINCT(ean) AS active FROM energy_data WHERE value > 0 SAMPLE BY 1d ALIGN TO CALENDAR ORDER BY timestamp",
        "flux": 'from(bucket: "energy") |> range(start: 0) |> filter(fn: (r) => r._value > 0) |> aggregateWindow(every: 1d, fn: count, createEmpty: false) |> group(columns: ["_time"]) |> count() |> sort(columns: ["_time"])',
        "influx3": "SELECT date_trunc('day', time) AS day, COUNT(DISTINCT ean) AS active FROM energy WHERE value > 0 GROUP BY 1 ORDER BY 1",
    },
    "Single meter: 1 day hourly": {
        "sql": "SELECT date_trunc('hour', ts) AS bucket, direction, SUM(value) AS total FROM energy_data WHERE ean = (SELECT MIN(ean) FROM energy_data) AND ts >= (SELECT MIN(ts) FROM energy_data) AND ts < (SELECT MIN(ts) FROM energy_data) + INTERVAL '1 day' GROUP BY 1, 2 ORDER BY 1, 2",
        "ch": "SELECT toStartOfHour(ts) AS bucket, direction, SUM(value) AS total FROM energy_data WHERE ean = (SELECT MIN(ean) FROM energy_data) AND ts >= (SELECT MIN(ts) FROM energy_data) AND ts < (SELECT MIN(ts) FROM energy_data) + INTERVAL 1 DAY GROUP BY 1, 2 ORDER BY 1, 2",
        "qdb": "SELECT timestamp, direction, SUM(value) AS total FROM energy_data WHERE ean = '{QDB_EAN}' AND timestamp >= '{QDB_START}' AND timestamp < '{QDB_END_1D}' SAMPLE BY 1h ALIGN TO CALENDAR",
        "flux": 'from(bucket: "energy") |> range(start: {INFLUX_START}, stop: {INFLUX_END_1D}) |> filter(fn: (r) => r.ean == "{INFLUX_EAN}") |> aggregateWindow(every: 1h, fn: sum, createEmpty: false) |> sort(columns: ["_time"])',
        "influx3": "SELECT date_trunc('hour', time) AS bucket, direction, SUM(value) AS total FROM energy WHERE ean = '{INFLUX3_EAN}' AND time >= '{INFLUX3_START}' AND time < '{INFLUX3_END_1D}' GROUP BY 1, 2 ORDER BY 1, 2",
    },
    "Single meter: 1 month daily": {
        "sql": "SELECT date_trunc('day', ts) AS bucket, direction, SUM(value) AS total FROM energy_data WHERE ean = (SELECT MIN(ean) FROM energy_data) AND ts >= (SELECT MIN(ts) FROM energy_data) AND ts < (SELECT MIN(ts) FROM energy_data) + INTERVAL '1 month' GROUP BY 1, 2 ORDER BY 1, 2",
        "ch": "SELECT toStartOfDay(ts) AS bucket, direction, SUM(value) AS total FROM energy_data WHERE ean = (SELECT MIN(ean) FROM energy_data) AND ts >= (SELECT MIN(ts) FROM energy_data) AND ts < (SELECT MIN(ts) FROM energy_data) + INTERVAL 1 MONTH GROUP BY 1, 2 ORDER BY 1, 2",
        "qdb": "SELECT timestamp, direction, SUM(value) AS total FROM energy_data WHERE ean = '{QDB_EAN}' AND timestamp >= '{QDB_START}' AND timestamp < '{QDB_END_1M}' SAMPLE BY 1d ALIGN TO CALENDAR",
        "flux": 'from(bucket: "energy") |> range(start: {INFLUX_START}, stop: {INFLUX_END_1M}) |> filter(fn: (r) => r.ean == "{INFLUX_EAN}") |> aggregateWindow(every: 1d, fn: sum, createEmpty: false) |> sort(columns: ["_time"])',
        "influx3": "SELECT date_trunc('day', time) AS bucket, direction, SUM(value) AS total FROM energy WHERE ean = '{INFLUX3_EAN}' AND time >= '{INFLUX3_START}' AND time < '{INFLUX3_END_1M}' GROUP BY 1, 2 ORDER BY 1, 2",
    },
    "Single meter: 1 year monthly": {
        "sql": "SELECT date_trunc('month', ts) AS bucket, direction, SUM(value) AS total FROM energy_data WHERE ean = (SELECT MIN(ean) FROM energy_data) AND ts >= (SELECT MIN(ts) FROM energy_data) AND ts < (SELECT MIN(ts) FROM energy_data) + INTERVAL '1 year' GROUP BY 1, 2 ORDER BY 1, 2",
        "ch": "SELECT toStartOfMonth(ts) AS bucket, direction, SUM(value) AS total FROM energy_data WHERE ean = (SELECT MIN(ean) FROM energy_data) AND ts >= (SELECT MIN(ts) FROM energy_data) AND ts < (SELECT MIN(ts) FROM energy_data) + INTERVAL 1 YEAR GROUP BY 1, 2 ORDER BY 1, 2",
        "qdb": "SELECT timestamp, direction, SUM(value) AS total FROM energy_data WHERE ean = '{QDB_EAN}' AND timestamp >= '{QDB_START}' AND timestamp < '{QDB_END_1Y}' SAMPLE BY 1M ALIGN TO CALENDAR",
        "flux": 'from(bucket: "energy") |> range(start: {INFLUX_START}, stop: {INFLUX_END_1Y}) |> filter(fn: (r) => r.ean == "{INFLUX_EAN}") |> aggregateWindow(every: 1mo, fn: sum, createEmpty: false) |> sort(columns: ["_time"])',
        "influx3": "SELECT date_trunc('month', time) AS bucket, direction, SUM(value) AS total FROM energy WHERE ean = '{INFLUX3_EAN}' AND time >= '{INFLUX3_START}' AND time < '{INFLUX3_END_1Y}' GROUP BY 1, 2 ORDER BY 1, 2",
    },
    "Monthly E17 vs E18 balance": {
        "sql": "SELECT date_trunc('month', ts) AS month, direction, SUM(value) AS total FROM energy_data GROUP BY 1, 2 ORDER BY 1, 2",
        "ch": "SELECT toStartOfMonth(ts) AS month, direction, SUM(value) AS total FROM energy_data GROUP BY 1, 2 ORDER BY 1, 2",
        "qdb": "SELECT timestamp, direction, SUM(value) AS total FROM energy_data SAMPLE BY 1M ALIGN TO CALENDAR ORDER BY timestamp",
        "flux": 'from(bucket: "energy") |> range(start: 0) |> aggregateWindow(every: 1mo, fn: sum, createEmpty: false) |> group(columns: ["direction", "_time"]) |> sum() |> sort(columns: ["_time"])',
        "influx3": "SELECT date_trunc('month', time) AS month, direction, SUM(value) AS total FROM energy GROUP BY 1, 2 ORDER BY 1, 2",
    },
    "Net energy balance per meter": {
        "sql": "SELECT ean, SUM(CASE WHEN direction='E17' THEN value ELSE 0 END) AS consumed, SUM(CASE WHEN direction='E18' THEN value ELSE 0 END) AS injected, SUM(CASE WHEN direction='E17' THEN value ELSE 0 END) - SUM(CASE WHEN direction='E18' THEN value ELSE 0 END) AS net FROM energy_data GROUP BY ean ORDER BY net DESC LIMIT 20",
        "ch": "SELECT ean, SUM(if(direction='E17', value, 0)) AS consumed, SUM(if(direction='E18', value, 0)) AS injected, SUM(if(direction='E17', value, 0)) - SUM(if(direction='E18', value, 0)) AS net FROM energy_data GROUP BY ean ORDER BY net DESC LIMIT 20",
        "qdb": "SELECT ean, SUM(CASE WHEN direction='E17' THEN value ELSE 0 END) AS consumed, SUM(CASE WHEN direction='E18' THEN value ELSE 0 END) AS injected, SUM(CASE WHEN direction='E17' THEN value ELSE 0 END) - SUM(CASE WHEN direction='E18' THEN value ELSE 0 END) AS net FROM energy_data GROUP BY ean ORDER BY net DESC LIMIT 20",
        "flux": 'from(bucket: "energy") |> range(start: 0) |> group(columns: ["ean", "direction"]) |> sum() |> pivot(rowKey: ["ean"], columnKey: ["direction"], valueColumn: "_value") |> map(fn: (r) => ({r with net: r.E17 - r.E18})) |> sort(columns: ["net"], desc: true) |> limit(n: 20)',
        "influx3": "SELECT ean, SUM(CASE WHEN direction='E17' THEN value ELSE 0 END) AS consumed, SUM(CASE WHEN direction='E18' THEN value ELSE 0 END) AS injected, SUM(CASE WHEN direction='E17' THEN value ELSE 0 END) - SUM(CASE WHEN direction='E18' THEN value ELSE 0 END) AS net FROM energy GROUP BY ean ORDER BY net DESC LIMIT 20",
    },
    "Hierarchy: supplier total daily": {
        "sql": "SELECT date_trunc('day', e.ts) AS day, h.supplier, e.direction, SUM(e.value) AS total FROM energy_data e JOIN meter_hierarchy h ON e.ean = h.ean AND e.ts >= h.valid_from AND (h.valid_to IS NULL OR e.ts < h.valid_to) GROUP BY 1, 2, 3 ORDER BY 1, 2, 3",
        "ch": "SELECT d.day, h.supplier, d.direction, SUM(d.total) AS total FROM (SELECT toStartOfDay(ts) AS day, ean, direction, SUM(value) AS total FROM energy_data GROUP BY 1, 2, 3) d JOIN meter_hierarchy h ON d.ean = h.ean AND d.day >= h.valid_from AND (h.valid_to IS NULL OR d.day < h.valid_to) GROUP BY 1, 2, 3 ORDER BY 1, 2, 3",
        # Previously: ASOF JOIN meter_hierarchy h ON (e.ean = h.ean) — timed out after 2h (ASOF JOIN not viable at 1.4B rows; also semantically wrong: picks nearest preceding row, not validity window)
        # Note: in QuestDB, valid_from is the designated timestamp column (named 'timestamp'); valid_to is stored as nanosecond integer
        # Pre-aggregate to daily totals first (~8.5M rows) then join with hierarchy — avoids risking a 1.4B-row intermediate if QuestDB materialises the cross-product before filtering
        "qdb": "SELECT d.day, h.supplier, d.direction, SUM(d.total) AS total FROM (SELECT date_trunc('day', timestamp) AS day, ean, direction, SUM(value) AS total FROM energy_data GROUP BY 1, 2, 3) d JOIN meter_hierarchy h ON d.ean = h.ean AND d.day >= h.timestamp AND (h.valid_to = 0 OR cast(d.day AS long) * 1000 < h.valid_to) GROUP BY 1, 2, 3 ORDER BY 1, 2, 3",
        "flux": '''import "join"
energy_daily = from(bucket: "energy") |> range(start: 0) |> filter(fn: (r) => r._measurement == "energy" and r._field == "value") |> truncateTimeColumn(unit: 1d) |> group(columns: ["_time", "ean", "direction"]) |> sum() |> map(fn: (r) => ({r with ts_ns: int(v: r._time)}))
hierarchy = from(bucket: "energy") |> range(start: 0) |> filter(fn: (r) => r._measurement == "meter_hierarchy" and r._field == "valid_to_ns") |> map(fn: (r) => ({r with valid_from_ns: int(v: r._time), valid_to_ns: r._value}))
join.inner(left: energy_daily, right: hierarchy, on: (l, r) => l.ean == r.ean, as: (l, r) => ({_time: l._time, supplier: r.supplier, direction: l.direction, value: l._value, ts_ns: l.ts_ns, valid_from_ns: r.valid_from_ns, valid_to_ns: r.valid_to_ns}))
  |> filter(fn: (r) => r.ts_ns >= r.valid_from_ns and (r.valid_to_ns == 0 or r.ts_ns < r.valid_to_ns))
  |> group(columns: ["_time", "supplier", "direction"])
  |> sum(column: "value")''',
        "influx3": "SELECT date_trunc('day', e.time) AS day, h.supplier, e.direction, SUM(e.value) AS total FROM energy e JOIN meter_hierarchy h ON e.ean = h.ean AND e.time >= h.time AND (h.valid_to_ns = 0 OR CAST(e.time AS BIGINT) < h.valid_to_ns) GROUP BY 1, 2, 3 ORDER BY 1, 2, 3",
    },
    "Hierarchy: by category (PRF/SMA) daily": {
        "sql": "SELECT date_trunc('day', e.ts) AS day, h.supplier, h.category, e.direction, SUM(e.value) AS total FROM energy_data e JOIN meter_hierarchy h ON e.ean = h.ean AND e.ts >= h.valid_from AND (h.valid_to IS NULL OR e.ts < h.valid_to) GROUP BY 1, 2, 3, 4 ORDER BY 1, 2, 3, 4",
        "ch": "SELECT d.day, h.supplier, h.category, d.direction, SUM(d.total) AS total FROM (SELECT toStartOfDay(ts) AS day, ean, direction, SUM(value) AS total FROM energy_data GROUP BY 1, 2, 3) d JOIN meter_hierarchy h ON d.ean = h.ean AND d.day >= h.valid_from AND (h.valid_to IS NULL OR d.day < h.valid_to) GROUP BY 1, 2, 3, 4 ORDER BY 1, 2, 3, 4",
        # Previously: ASOF JOIN meter_hierarchy h ON (e.ean = h.ean) — timed out after 2h (see Q12 note)
        # Pre-aggregate approach: same rationale as Q12
        "qdb": "SELECT d.day, h.supplier, h.category, d.direction, SUM(d.total) AS total FROM (SELECT date_trunc('day', timestamp) AS day, ean, direction, SUM(value) AS total FROM energy_data GROUP BY 1, 2, 3) d JOIN meter_hierarchy h ON d.ean = h.ean AND d.day >= h.timestamp AND (h.valid_to = 0 OR cast(d.day AS long) * 1000 < h.valid_to) GROUP BY 1, 2, 3, 4 ORDER BY 1, 2, 3, 4",
        "flux": '''import "join"
energy_daily = from(bucket: "energy") |> range(start: 0) |> filter(fn: (r) => r._measurement == "energy" and r._field == "value") |> truncateTimeColumn(unit: 1d) |> group(columns: ["_time", "ean", "direction"]) |> sum() |> map(fn: (r) => ({r with ts_ns: int(v: r._time)}))
hierarchy = from(bucket: "energy") |> range(start: 0) |> filter(fn: (r) => r._measurement == "meter_hierarchy" and r._field == "valid_to_ns") |> map(fn: (r) => ({r with valid_from_ns: int(v: r._time), valid_to_ns: r._value}))
join.inner(left: energy_daily, right: hierarchy, on: (l, r) => l.ean == r.ean, as: (l, r) => ({_time: l._time, supplier: r.supplier, category: r.category, direction: l.direction, value: l._value, ts_ns: l.ts_ns, valid_from_ns: r.valid_from_ns, valid_to_ns: r.valid_to_ns}))
  |> filter(fn: (r) => r.ts_ns >= r.valid_from_ns and (r.valid_to_ns == 0 or r.ts_ns < r.valid_to_ns))
  |> group(columns: ["_time", "supplier", "category", "direction"])
  |> sum(column: "value")''',
        "influx3": "SELECT date_trunc('day', e.time) AS day, h.supplier, h.category, e.direction, SUM(e.value) AS total FROM energy e JOIN meter_hierarchy h ON e.ean = h.ean AND e.time >= h.time AND (h.valid_to_ns = 0 OR CAST(e.time AS BIGINT) < h.valid_to_ns) GROUP BY 1, 2, 3, 4 ORDER BY 1, 2, 3, 4",
    },
    "Hierarchy: supplier total (all time)": {
        "sql": "SELECT h.supplier, e.direction, SUM(e.value) AS total FROM energy_data e JOIN meter_hierarchy h ON e.ean = h.ean AND e.ts >= h.valid_from AND (h.valid_to IS NULL OR e.ts < h.valid_to) GROUP BY 1, 2 ORDER BY 1, 2",
        "ch": "SELECT h.supplier, d.direction, SUM(d.total) AS total FROM (SELECT toStartOfDay(ts) AS day, ean, direction, SUM(value) AS total FROM energy_data GROUP BY 1, 2, 3) d JOIN meter_hierarchy h ON d.ean = h.ean AND d.day >= h.valid_from AND (h.valid_to IS NULL OR d.day < h.valid_to) GROUP BY 1, 2 ORDER BY 1, 2",
        "qdb": "SELECT h.supplier, d.direction, SUM(d.total) AS total FROM (SELECT date_trunc('day', timestamp) AS day, ean, direction, SUM(value) AS total FROM energy_data GROUP BY 1, 2, 3) d JOIN meter_hierarchy h ON d.ean = h.ean AND d.day >= h.timestamp AND (h.valid_to = 0 OR cast(d.day AS long) * 1000 < h.valid_to) GROUP BY 1, 2 ORDER BY 1, 2",
        "flux": '''import "join"
energy_daily = from(bucket: "energy") |> range(start: 0) |> filter(fn: (r) => r._measurement == "energy" and r._field == "value") |> truncateTimeColumn(unit: 1d) |> group(columns: ["_time", "ean", "direction"]) |> sum() |> map(fn: (r) => ({r with ts_ns: int(v: r._time)}))
hierarchy = from(bucket: "energy") |> range(start: 0) |> filter(fn: (r) => r._measurement == "meter_hierarchy" and r._field == "valid_to_ns") |> map(fn: (r) => ({r with valid_from_ns: int(v: r._time), valid_to_ns: r._value}))
join.inner(left: energy_daily, right: hierarchy, on: (l, r) => l.ean == r.ean, as: (l, r) => ({supplier: r.supplier, direction: l.direction, value: l._value, ts_ns: l.ts_ns, valid_from_ns: r.valid_from_ns, valid_to_ns: r.valid_to_ns}))
  |> filter(fn: (r) => r.ts_ns >= r.valid_from_ns and (r.valid_to_ns == 0 or r.ts_ns < r.valid_to_ns))
  |> group(columns: ["supplier", "direction"])
  |> sum(column: "value")''',
        "influx3": "SELECT h.supplier, e.direction, SUM(e.value) AS total FROM energy e JOIN meter_hierarchy h ON e.ean = h.ean AND e.time >= h.time AND (h.valid_to_ns = 0 OR CAST(e.time AS BIGINT) < h.valid_to_ns) GROUP BY 1, 2 ORDER BY 1, 2",
    },
    "Hierarchy: by category all time (PRF/SMA)": {
        "sql": "SELECT h.supplier, h.category, e.direction, SUM(e.value) AS total FROM energy_data e JOIN meter_hierarchy h ON e.ean = h.ean AND e.ts >= h.valid_from AND (h.valid_to IS NULL OR e.ts < h.valid_to) GROUP BY 1, 2, 3 ORDER BY 1, 2, 3",
        "ch": "SELECT h.supplier, h.category, d.direction, SUM(d.total) AS total FROM (SELECT toStartOfDay(ts) AS day, ean, direction, SUM(value) AS total FROM energy_data GROUP BY 1, 2, 3) d JOIN meter_hierarchy h ON d.ean = h.ean AND d.day >= h.valid_from AND (h.valid_to IS NULL OR d.day < h.valid_to) GROUP BY 1, 2, 3 ORDER BY 1, 2, 3",
        "qdb": "SELECT h.supplier, h.category, d.direction, SUM(d.total) AS total FROM (SELECT date_trunc('day', timestamp) AS day, ean, direction, SUM(value) AS total FROM energy_data GROUP BY 1, 2, 3) d JOIN meter_hierarchy h ON d.ean = h.ean AND d.day >= h.timestamp AND (h.valid_to = 0 OR cast(d.day AS long) * 1000 < h.valid_to) GROUP BY 1, 2, 3 ORDER BY 1, 2, 3",
        "flux": '''import "join"
energy_daily = from(bucket: "energy") |> range(start: 0) |> filter(fn: (r) => r._measurement == "energy" and r._field == "value") |> truncateTimeColumn(unit: 1d) |> group(columns: ["_time", "ean", "direction"]) |> sum() |> map(fn: (r) => ({r with ts_ns: int(v: r._time)}))
hierarchy = from(bucket: "energy") |> range(start: 0) |> filter(fn: (r) => r._measurement == "meter_hierarchy" and r._field == "valid_to_ns") |> map(fn: (r) => ({r with valid_from_ns: int(v: r._time), valid_to_ns: r._value}))
join.inner(left: energy_daily, right: hierarchy, on: (l, r) => l.ean == r.ean, as: (l, r) => ({supplier: r.supplier, category: r.category, direction: l.direction, value: l._value, ts_ns: l.ts_ns, valid_from_ns: r.valid_from_ns, valid_to_ns: r.valid_to_ns}))
  |> filter(fn: (r) => r.ts_ns >= r.valid_from_ns and (r.valid_to_ns == 0 or r.ts_ns < r.valid_to_ns))
  |> group(columns: ["supplier", "category", "direction"])
  |> sum(column: "value")''',
        "influx3": "SELECT h.supplier, h.category, e.direction, SUM(e.value) AS total FROM energy e JOIN meter_hierarchy h ON e.ean = h.ean AND e.time >= h.time AND (h.valid_to_ns = 0 OR CAST(e.time AS BIGINT) < h.valid_to_ns) GROUP BY 1, 2, 3 ORDER BY 1, 2, 3",
    },
    "Hierarchy: sub-category all time (PRF only)": {
        "sql": "SELECT h.supplier, h.category, h.sub_category, e.direction, SUM(e.value) AS total FROM energy_data e JOIN meter_hierarchy h ON e.ean = h.ean AND e.ts >= h.valid_from AND (h.valid_to IS NULL OR e.ts < h.valid_to) WHERE h.category = 'PRF' GROUP BY 1, 2, 3, 4 ORDER BY 1, 2, 3, 4",
        "ch": "SELECT h.supplier, h.category, h.sub_category, d.direction, SUM(d.total) AS total FROM (SELECT toStartOfDay(ts) AS day, ean, direction, SUM(value) AS total FROM energy_data GROUP BY 1, 2, 3) d JOIN meter_hierarchy h ON d.ean = h.ean AND d.day >= h.valid_from AND (h.valid_to IS NULL OR d.day < h.valid_to) WHERE h.category = 'PRF' GROUP BY 1, 2, 3, 4 ORDER BY 1, 2, 3, 4",
        "qdb": "SELECT h.supplier, h.category, h.sub_category, d.direction, SUM(d.total) AS total FROM (SELECT date_trunc('day', timestamp) AS day, ean, direction, SUM(value) AS total FROM energy_data GROUP BY 1, 2, 3) d JOIN meter_hierarchy h ON d.ean = h.ean AND d.day >= h.timestamp AND (h.valid_to = 0 OR cast(d.day AS long) * 1000 < h.valid_to) WHERE h.category = 'PRF' GROUP BY 1, 2, 3, 4 ORDER BY 1, 2, 3, 4",
        "flux": '''import "join"
energy_daily = from(bucket: "energy") |> range(start: 0) |> filter(fn: (r) => r._measurement == "energy" and r._field == "value") |> truncateTimeColumn(unit: 1d) |> group(columns: ["_time", "ean", "direction"]) |> sum() |> map(fn: (r) => ({r with ts_ns: int(v: r._time)}))
hierarchy = from(bucket: "energy") |> range(start: 0) |> filter(fn: (r) => r._measurement == "meter_hierarchy" and r._field == "valid_to_ns" and r.category == "PRF") |> map(fn: (r) => ({r with valid_from_ns: int(v: r._time), valid_to_ns: r._value}))
join.inner(left: energy_daily, right: hierarchy, on: (l, r) => l.ean == r.ean, as: (l, r) => ({supplier: r.supplier, category: r.category, sub_category: r.sub_category, direction: l.direction, value: l._value, ts_ns: l.ts_ns, valid_from_ns: r.valid_from_ns, valid_to_ns: r.valid_to_ns}))
  |> filter(fn: (r) => r.ts_ns >= r.valid_from_ns and (r.valid_to_ns == 0 or r.ts_ns < r.valid_to_ns))
  |> group(columns: ["supplier", "category", "sub_category", "direction"])
  |> sum(column: "value")''',
        "influx3": "SELECT h.supplier, h.category, h.sub_category, e.direction, SUM(e.value) AS total FROM energy e JOIN meter_hierarchy h ON e.ean = h.ean AND e.time >= h.time AND (h.valid_to_ns = 0 OR CAST(e.time AS BIGINT) < h.valid_to_ns) WHERE h.category = 'PRF' GROUP BY 1, 2, 3, 4 ORDER BY 1, 2, 3, 4",
    },
    "Hierarchy: sub-category monthly (PRF only)": {
        "sql": "SELECT date_trunc('month', e.ts) AS month, h.supplier, h.category, h.sub_category, e.direction, SUM(e.value) AS total FROM energy_data e JOIN meter_hierarchy h ON e.ean = h.ean AND e.ts >= h.valid_from AND (h.valid_to IS NULL OR e.ts < h.valid_to) WHERE h.category = 'PRF' GROUP BY 1, 2, 3, 4, 5 ORDER BY 1, 2, 3, 4, 5",
        "ch": "SELECT d.month, h.supplier, h.category, h.sub_category, d.direction, SUM(d.total) AS total FROM (SELECT toStartOfMonth(ts) AS month, ean, direction, SUM(value) AS total FROM energy_data GROUP BY 1, 2, 3) d JOIN meter_hierarchy h ON d.ean = h.ean AND d.month >= h.valid_from AND (h.valid_to IS NULL OR d.month < h.valid_to) WHERE h.category = 'PRF' GROUP BY 1, 2, 3, 4, 5 ORDER BY 1, 2, 3, 4, 5",
        # Previously: ASOF JOIN meter_hierarchy h ON (e.ean = h.ean) — timed out after 2h (see Q12 note)
        # Pre-aggregate approach: same rationale as Q12; WHERE h.category = 'PRF' applied after join
        "qdb": "SELECT d.month, h.supplier, h.category, h.sub_category, d.direction, SUM(d.total) AS total FROM (SELECT date_trunc('month', timestamp) AS month, ean, direction, SUM(value) AS total FROM energy_data GROUP BY 1, 2, 3) d JOIN meter_hierarchy h ON d.ean = h.ean AND d.month >= h.timestamp AND (h.valid_to = 0 OR cast(d.month AS long) * 1000 < h.valid_to) WHERE h.category = 'PRF' GROUP BY 1, 2, 3, 4, 5 ORDER BY 1, 2, 3, 4, 5",
        "flux": '''import "join"
energy_monthly = from(bucket: "energy") |> range(start: 0) |> filter(fn: (r) => r._measurement == "energy" and r._field == "value") |> truncateTimeColumn(unit: 1mo) |> group(columns: ["_time", "ean", "direction"]) |> sum() |> map(fn: (r) => ({r with ts_ns: int(v: r._time)}))
hierarchy = from(bucket: "energy") |> range(start: 0) |> filter(fn: (r) => r._measurement == "meter_hierarchy" and r._field == "valid_to_ns" and r.category == "PRF") |> map(fn: (r) => ({r with valid_from_ns: int(v: r._time), valid_to_ns: r._value}))
join.inner(left: energy_monthly, right: hierarchy, on: (l, r) => l.ean == r.ean, as: (l, r) => ({_time: l._time, supplier: r.supplier, category: r.category, sub_category: r.sub_category, direction: l.direction, value: l._value, ts_ns: l.ts_ns, valid_from_ns: r.valid_from_ns, valid_to_ns: r.valid_to_ns}))
  |> filter(fn: (r) => r.ts_ns >= r.valid_from_ns and (r.valid_to_ns == 0 or r.ts_ns < r.valid_to_ns))
  |> group(columns: ["_time", "supplier", "category", "sub_category", "direction"])
  |> sum(column: "value")''',
        "influx3": "SELECT date_trunc('month', e.time) AS month, h.supplier, h.category, h.sub_category, e.direction, SUM(e.value) AS total FROM energy e JOIN meter_hierarchy h ON e.ean = h.ean AND e.time >= h.time AND (h.valid_to_ns = 0 OR CAST(e.time AS BIGINT) < h.valid_to_ns) WHERE h.category = 'PRF' GROUP BY 1, 2, 3, 4, 5 ORDER BY 1, 2, 3, 4, 5",
    },
}


def get_query_for_db(preset_name: str, db_name: str, db_type: str) -> str | None:
    """Return the appropriate query string for a given preset and database."""
    pq = PRESET_QUERIES.get(preset_name)
    if not pq:
        return None
    if db_type == "influx":
        return pq.get("flux")
    elif db_type == "influx3":
        return pq.get("influx3")
    elif db_type == "ch":
        return pq.get("ch")
    elif db_type == "pg":
        return pq["qdb"] if db_name == "QuestDB" else pq["sql"]
    return None
