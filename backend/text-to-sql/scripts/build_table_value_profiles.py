from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any


ROOT_DIR = Path(__file__).resolve().parents[1]
BACKEND_DIR = ROOT_DIR / "backend"
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from app.services.oracle.connection import acquire_connection  # noqa: E402


_IDENT_RE = re.compile(r"^[A-Z0-9_$#]+$")
_VALUE_TYPES = {"VARCHAR2", "CHAR", "NVARCHAR2", "NCHAR", "NUMBER"}
_FALLBACK_NAME_HINTS = (
    "TYPE",
    "STATUS",
    "LOCATION",
    "RACE",
    "LANGUAGE",
    "SERVICE",
    "CATEGORY",
    "UNIT",
    "WARNING",
    "FLAG",
    "EVENT",
    "INTERPRETATION",
    "TEXT",
    "NAME",
)


def _safe_ident(name: str) -> str:
    value = str(name or "").strip().upper()
    if not _IDENT_RE.fullmatch(value):
        raise ValueError(f"Unsafe identifier: {name}")
    return value


def _load_schema(metadata_dir: Path) -> tuple[str, list[str]]:
    path = metadata_dir / "schema_catalog.json"
    data = json.loads(path.read_text(encoding="utf-8"))
    owner = str(data.get("owner") or "").strip().upper()
    tables = sorted(str(name).strip().upper() for name in (data.get("tables") or {}).keys() if str(name).strip())
    return owner, tables


def _fetch_table_stats(cur: Any, owner: str, table: str) -> tuple[int | None, list[dict[str, Any]]]:
    cur.execute(
        """
        SELECT num_rows
        FROM all_tables
        WHERE owner = :owner AND table_name = :table_name
        """,
        owner=owner,
        table_name=table,
    )
    row = cur.fetchone()
    row_count = int(row[0]) if row and row[0] is not None else None

    cur.execute(
        """
        SELECT
            c.column_name,
            c.data_type,
            c.nullable,
            s.num_distinct,
            s.num_nulls
        FROM all_tab_columns c
        LEFT JOIN all_tab_col_statistics s
          ON s.owner = c.owner
         AND s.table_name = c.table_name
         AND s.column_name = c.column_name
        WHERE c.owner = :owner
          AND c.table_name = :table_name
        ORDER BY c.column_id
        """,
        owner=owner,
        table_name=table,
    )
    cols: list[dict[str, Any]] = []
    for column_name, data_type, nullable, num_distinct, num_nulls in cur.fetchall():
        cols.append(
            {
                "column": str(column_name).upper(),
                "data_type": str(data_type).upper(),
                "nullable": str(nullable).upper() == "Y",
                "num_distinct": int(num_distinct) if num_distinct is not None else None,
                "num_nulls": int(num_nulls) if num_nulls is not None else None,
            }
        )
    return row_count, cols


def _fetch_top_values(
    cur: Any,
    owner: str,
    table: str,
    column: str,
    *,
    top_n: int,
    sample_rows: int,
    full_scan: bool,
) -> list[dict[str, Any]]:
    table_ref = f"{_safe_ident(owner)}.{_safe_ident(table)}"
    col = _safe_ident(column)
    if full_scan:
        sql = (
            "SELECT * FROM ("
            " SELECT CASE"
            "          WHEN {col} IS NULL THEN '__NULL__'"
            "          ELSE SUBSTR(TRIM(TO_CHAR({col})), 1, 120)"
            "        END AS value_text,"
            "        COUNT(*) AS value_count"
            " FROM {table_ref}"
            " GROUP BY {col}"
            " ORDER BY value_count DESC"
            ") WHERE ROWNUM <= :top_n"
        ).format(col=col, table_ref=table_ref)
        cur.execute(sql, top_n=int(top_n))
    else:
        sql = (
            "SELECT * FROM ("
            " SELECT CASE"
            "          WHEN {col} IS NULL THEN '__NULL__'"
            "          ELSE SUBSTR(TRIM(TO_CHAR({col})), 1, 120)"
            "        END AS value_text,"
            "        COUNT(*) AS value_count"
            " FROM (SELECT {col} FROM {table_ref} WHERE ROWNUM <= :sample_rows)"
            " GROUP BY {col}"
            " ORDER BY value_count DESC"
            ") WHERE ROWNUM <= :top_n"
        ).format(col=col, table_ref=table_ref)
        cur.execute(sql, top_n=int(top_n), sample_rows=max(1000, int(sample_rows)))
    rows = cur.fetchall()
    return [{"value": str(v), "count": int(c)} for v, c in rows]


def _estimate_distinct_from_sample(
    cur: Any,
    owner: str,
    table: str,
    column: str,
    *,
    sample_rows: int,
) -> tuple[int | None, int | None]:
    table_ref = f"{_safe_ident(owner)}.{_safe_ident(table)}"
    col = _safe_ident(column)
    sql = (
        "SELECT COUNT(DISTINCT {col}) AS num_distinct, "
        "SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) AS num_nulls "
        "FROM (SELECT {col} FROM {table_ref} WHERE ROWNUM <= :sample_rows)"
    ).format(col=col, table_ref=table_ref)
    cur.execute(sql, sample_rows=max(1000, int(sample_rows)))
    row = cur.fetchone()
    if not row:
        return None, None
    return (
        int(row[0]) if row[0] is not None else None,
        int(row[1]) if row[1] is not None else None,
    )


def _count_distinct_full(
    cur: Any,
    owner: str,
    table: str,
    column: str,
) -> tuple[int | None, int | None]:
    table_ref = f"{_safe_ident(owner)}.{_safe_ident(table)}"
    col = _safe_ident(column)
    sql = (
        "SELECT COUNT(DISTINCT {col}) AS num_distinct, "
        "SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) AS num_nulls "
        "FROM {table_ref}"
    ).format(col=col, table_ref=table_ref)
    cur.execute(sql)
    row = cur.fetchone()
    if not row:
        return None, None
    return (
        int(row[0]) if row[0] is not None else None,
        int(row[1]) if row[1] is not None else None,
    )


def _pick_fallback_column(columns: list[dict[str, Any]]) -> dict[str, Any] | None:
    value_cols = [
        col for col in columns
        if str(col.get("data_type") or "").upper() in _VALUE_TYPES
    ]
    if not value_cols:
        return None
    for hint in _FALLBACK_NAME_HINTS:
        for col in value_cols:
            name = str(col.get("column") or "").upper()
            if hint in name:
                return col
    return value_cols[0]


def build_profiles(
    *,
    metadata_dir: Path,
    output_path: Path,
    max_distinct: int,
    top_n: int,
    full_scan: bool = False,
    include_tables: set[str] | None = None,
) -> dict[str, Any]:
    owner, tables = _load_schema(metadata_dir)
    if not owner:
        owner = "SSO"
    if include_tables:
        tables = [table for table in tables if table in include_tables]

    conn = acquire_connection()
    try:
        cur = conn.cursor()
        out_rows: list[dict[str, Any]] = []
        for table in tables:
            row_count, columns = _fetch_table_stats(cur, owner, table)
            before_table_count = len(out_rows)
            for col in columns:
                dtype = str(col.get("data_type") or "").upper()
                if dtype not in _VALUE_TYPES:
                    continue

                num_distinct = col.get("num_distinct")
                num_nulls = col.get("num_nulls")
                row_count_num = int(row_count) if row_count is not None else 200000
                sample_rows = min(200000, max(20000, row_count_num))
                if full_scan:
                    num_distinct, num_nulls = _count_distinct_full(
                        cur,
                        owner,
                        table,
                        str(col["column"]),
                    )
                elif num_distinct is None:
                    num_distinct, num_nulls = _estimate_distinct_from_sample(
                        cur,
                        owner,
                        table,
                        str(col["column"]),
                        sample_rows=sample_rows,
                    )
                if num_distinct is None or num_distinct <= 0:
                    continue
                if int(num_distinct) > int(max_distinct):
                    continue

                top_values = _fetch_top_values(
                    cur,
                    owner,
                    table,
                    str(col["column"]),
                    top_n=top_n,
                    sample_rows=sample_rows,
                    full_scan=full_scan,
                )
                if not top_values:
                    continue

                out_rows.append(
                    {
                        "table": table,
                        "column": str(col["column"]).upper(),
                        "data_type": dtype,
                        "nullable": bool(col.get("nullable")),
                        "row_count": row_count,
                        "num_distinct": int(num_distinct),
                        "num_nulls": int(num_nulls) if num_nulls is not None else None,
                        "top_values": top_values,
                        "source": "oracle_full_scan" if full_scan else "oracle_scan",
                    }
                )

            # Ensure minimum coverage: if no low-cardinality profile was produced
            # for a table, add one fallback profile using a representative column.
            if len(out_rows) == before_table_count:
                fallback_col = _pick_fallback_column(columns)
                if fallback_col is not None:
                    fallback_name = str(fallback_col.get("column") or "").upper()
                    fallback_dtype = str(fallback_col.get("data_type") or "").upper()
                    row_count_num = int(row_count) if row_count is not None else 200000
                    sample_rows = min(100000, max(10000, row_count_num))
                    if full_scan:
                        num_distinct, num_nulls = _count_distinct_full(cur, owner, table, fallback_name)
                    else:
                        num_distinct, num_nulls = _estimate_distinct_from_sample(
                            cur,
                            owner,
                            table,
                            fallback_name,
                            sample_rows=sample_rows,
                        )
                    top_values = _fetch_top_values(
                        cur,
                        owner,
                        table,
                        fallback_name,
                        top_n=top_n,
                        sample_rows=sample_rows,
                        full_scan=full_scan,
                    )
                    if not top_values:
                        top_values = [{"value": "__NO_DATA__", "count": 0}]
                    out_rows.append(
                        {
                            "table": table,
                            "column": fallback_name,
                            "data_type": fallback_dtype,
                            "nullable": bool(fallback_col.get("nullable")),
                            "row_count": row_count,
                            "num_distinct": int(num_distinct) if num_distinct is not None else None,
                            "num_nulls": int(num_nulls) if num_nulls is not None else None,
                            "top_values": top_values,
                            "source": "oracle_full_scan_fallback" if full_scan else "oracle_scan_fallback",
                        }
                    )

        payload = "\n".join(json.dumps(row, ensure_ascii=True) for row in out_rows)
        if payload:
            payload += "\n"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(payload, encoding="utf-8")

        return {
            "owner": owner,
            "tables": len(tables),
            "profile_rows": len(out_rows),
            "output": str(output_path),
        }
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Build low-cardinality table value profiles from Oracle.")
    parser.add_argument("--metadata-dir", default="var/metadata")
    parser.add_argument("--output", default="var/metadata/table_value_profiles.jsonl")
    parser.add_argument("--max-distinct", type=int, default=40)
    parser.add_argument("--top-n", type=int, default=20)
    parser.add_argument("--full-scan", action="store_true", help="Use full table scan for distinct/top values.")
    parser.add_argument(
        "--tables",
        default="",
        help="Comma-separated table list (e.g., ADMISSIONS,ICUSTAYS). Empty means all tables.",
    )
    args = parser.parse_args()
    include_tables = {
        str(name).strip().upper()
        for name in str(args.tables or "").split(",")
        if str(name).strip()
    }

    report = build_profiles(
        metadata_dir=Path(args.metadata_dir),
        output_path=Path(args.output),
        max_distinct=max(1, int(args.max_distinct)),
        top_n=max(1, int(args.top_n)),
        full_scan=bool(args.full_scan),
        include_tables=include_tables or None,
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
