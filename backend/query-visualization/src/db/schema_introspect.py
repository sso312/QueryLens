"""Schema summarization helpers for DataFrame and Oracle metadata."""
from __future__ import annotations

from typing import Any, Dict, List, Optional

import pandas as pd
from pandas.api import types as pdt

from src.db.oracle_client import get_connection


def _safe_value(value: Any) -> Any:
    if isinstance(value, (int, float, str, bool)) or value is None:
        return value
    return str(value)


def _infer_time_by_name(name: str) -> bool:
    hints = ("date", "time", "day", "month", "year", "dt", "timestamp")
    return any(h in name.lower() for h in hints)


def _infer_time_by_sample(series: pd.Series, sample_size: int = 20) -> bool:
    sample = series.dropna().head(sample_size)
    if sample.empty:
        return False
    parsed = pd.to_datetime(sample, errors="coerce")
    return parsed.notna().any()


def _categorical_threshold(row_count: int) -> int:
    if row_count <= 0:
        return 0
    return max(10, min(50, int(row_count * 0.2)))


def _infer_column_role(
    name: str,
    series: pd.Series,
    unique_count: int,
    row_count: int,
) -> str:
    if pdt.is_datetime64_any_dtype(series):
        return "time"
    if _infer_time_by_name(name) and _infer_time_by_sample(series):
        return "time"
    if pdt.is_bool_dtype(series):
        return "boolean"
    if pdt.is_numeric_dtype(series):
        return "numeric"
    if pdt.is_object_dtype(series) or pdt.is_string_dtype(series):
        threshold = _categorical_threshold(row_count)
        return "categorical" if unique_count <= threshold else "text"
    return "other"


def _init_roles_dict() -> Dict[str, List[str]]:
    return {
        "time": [],
        "numeric": [],
        "categorical": [],
        "boolean": [],
        "text": [],
        "other": [],
    }


def summarize_dataframe_schema(df: pd.DataFrame, sample_size: int = 3) -> Dict[str, Any]:
    rows = int(len(df))
    columns = list(df.columns)
    dtypes = {col: str(dtype) for col, dtype in df.dtypes.items()}

    unique_counts: Dict[str, int] = {}
    null_counts: Dict[str, int] = {}
    examples: Dict[str, List[Any]] = {}
    inferred_types: Dict[str, str] = {}
    roles = _init_roles_dict()

    for col in columns:
        series = df[col]
        unique_count = int(series.nunique(dropna=True))
        unique_counts[col] = unique_count
        null_counts[col] = int(series.isna().sum())
        sample = series.dropna().head(sample_size).tolist()
        examples[col] = [_safe_value(v) for v in sample]

        role = _infer_column_role(col, series, unique_count, rows)
        inferred_types[col] = role
        roles[role].append(col)

    return {
        "source": "dataframe",
        "columns": columns,
        "dtypes": dtypes,
        "rows": rows,
        "unique_counts": unique_counts,
        "null_counts": null_counts,
        "examples": examples,
        "inferred_types": inferred_types,
        "column_roles": roles,
    }


def _format_oracle_type(
    data_type: str,
    length: Optional[int],
    precision: Optional[int],
    scale: Optional[int],
) -> str:
    if data_type in ("VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR") and length:
        return f"{data_type}({length})"
    if data_type == "NUMBER" and precision is not None:
        if scale is not None:
            return f"{data_type}({precision},{scale})"
        return f"{data_type}({precision})"
    return data_type


def _infer_oracle_role(name: str, data_type: str) -> str:
    upper = data_type.upper()
    if upper in ("DATE", "TIMESTAMP", "TIMESTAMP WITH TIME ZONE", "TIMESTAMP WITH LOCAL TIME ZONE"):
        return "time"
    if upper in ("NUMBER", "FLOAT", "BINARY_FLOAT", "BINARY_DOUBLE"):
        return "numeric"
    if upper in ("CHAR", "NCHAR", "VARCHAR2", "NVARCHAR2"):
        return "categorical"
    if _infer_time_by_name(name):
        return "time"
    return "other"


def summarize_oracle_schema(
    table_name: str,
    owner: Optional[str] = None,
) -> Dict[str, Any]:
    table = table_name.upper()
    owner_upper = owner.upper() if owner else None

    sql = """
        SELECT
            c.column_name,
            c.data_type,
            c.data_length,
            c.data_precision,
            c.data_scale,
            c.nullable,
            s.num_distinct,
            s.num_nulls
        FROM all_tab_columns c
        LEFT JOIN all_tab_col_statistics s
            ON s.owner = c.owner
           AND s.table_name = c.table_name
           AND s.column_name = c.column_name
        WHERE c.table_name = :table_name
          AND (:owner IS NULL OR c.owner = :owner)
        ORDER BY c.column_id
    """

    rows = None
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, {"table_name": table, "owner": owner_upper})
        results = cur.fetchall()

        cur.execute(
            """
            SELECT num_rows
            FROM all_tables
            WHERE table_name = :table_name
              AND (:owner IS NULL OR owner = :owner)
            """,
            {"table_name": table, "owner": owner_upper},
        )
        table_stats = cur.fetchone()
        if table_stats and table_stats[0] is not None:
            rows = int(table_stats[0])

    columns: List[str] = []
    dtypes: Dict[str, str] = {}
    unique_counts: Dict[str, int] = {}
    null_counts: Dict[str, int] = {}
    inferred_types: Dict[str, str] = {}
    roles = _init_roles_dict()

    for (
        column_name,
        data_type,
        data_length,
        data_precision,
        data_scale,
        _nullable,
        num_distinct,
        num_nulls,
    ) in results:
        name = column_name.lower()
        columns.append(name)

        dtype = _format_oracle_type(
            data_type,
            int(data_length) if data_length is not None else None,
            int(data_precision) if data_precision is not None else None,
            int(data_scale) if data_scale is not None else None,
        )
        dtypes[name] = dtype

        if num_distinct is not None:
            unique_counts[name] = int(num_distinct)
        if num_nulls is not None:
            null_counts[name] = int(num_nulls)

        role = _infer_oracle_role(name, data_type)
        inferred_types[name] = role
        roles[role].append(name)

    return {
        "source": "oracle",
        "columns": columns,
        "dtypes": dtypes,
        "rows": rows,
        "unique_counts": unique_counts,
        "null_counts": null_counts,
        "examples": {},
        "inferred_types": inferred_types,
        "column_roles": roles,
    }

