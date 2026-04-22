import re

_QTR_PG  = ("ts >= (SELECT MIN(ts) FROM energy_data)"
            " AND ts < (SELECT MIN(ts) FROM energy_data) + INTERVAL '3 months'")
_QTR_CH  = ("ts >= (SELECT MIN(ts) FROM energy_data)"
            " AND ts < (SELECT MIN(ts) FROM energy_data) + INTERVAL 3 MONTH")
_QTR_QDB = ("ts >= (SELECT MIN(ts) FROM energy_data)"
            " AND ts < (SELECT dateadd('M', 3, MIN(ts)) FROM energy_data)")


def _wrap_sql_quarter(sql: str, tf: str) -> str:
    """Wrap FROM energy_data (with optional alias) in a time-filtered subquery."""
    # aliased form: FROM energy_data e  or  FROM energy_data AS e
    result = re.sub(
        r'FROM energy_data(?: AS)? (\w+)\b',
        lambda m: f"FROM (SELECT * FROM energy_data WHERE {tf}) {m.group(1)}",
        sql,
    )
    if result != sql:
        return result
    # no alias
    return sql.replace(
        "FROM energy_data",
        f"FROM (SELECT * FROM energy_data WHERE {tf}) AS energy_data",
    )


def _wrap_qdb_quarter(sql: str) -> str:
    """For QuestDB inject WHERE before SAMPLE BY (SAMPLE BY can't live in outer subquery)."""
    tf = _QTR_QDB
    alias = "e." if re.search(r'\bJOIN\b', sql, re.I) else ""
    tf_a = tf.replace("ts >=", f"{alias}ts >=").replace("ts <", f"{alias}ts <")
    if re.search(r'\bWHERE\b', sql, re.I):
        return re.sub(r'(?i)( SAMPLE BY | ORDER BY )', f' AND {tf_a}\\1', sql, count=1)
    return re.sub(r'(?i)( SAMPLE BY | ORDER BY )', f' WHERE {tf_a}\\1', sql, count=1)


def apply_quarter_scope(q: dict) -> dict:
    """Return a copy of the preset query dict scoped to the first 3 months of data."""
    q = dict(q)
    if q.get("sql"):
        q["sql"] = _wrap_sql_quarter(q["sql"], _QTR_PG)
    if q.get("ch"):
        q["ch"] = _wrap_sql_quarter(q["ch"], _QTR_CH)
    if q.get("qdb"):
        q["qdb"] = _wrap_qdb_quarter(q["qdb"])
    # Flux: skip (InfluxDB 2 range scoping requires knowing the actual min timestamp)
    return q
