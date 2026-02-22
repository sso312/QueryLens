from __future__ import annotations

from pathlib import Path
from typing import Any
import json

from app.core.config import get_settings
from app.services.oracle.connection import acquire_connection
from app.services.runtime.settings_store import load_table_scope

_SYSTEM_OWNERS = {
    "ANONYMOUS",
    "APPQOSSYS",
    "AUDSYS",
    "CTXSYS",
    "DBSFWUSER",
    "DBSNMP",
    "DIP",
    "DVSYS",
    "GGSYS",
    "GSMADMIN_INTERNAL",
    "GSMCATUSER",
    "GSMROOTUSER",
    "GSMUSER",
    "LBACSYS",
    "MDDATA",
    "MDSYS",
    "OJVMSYS",
    "OLAPSYS",
    "ORDDATA",
    "ORDPLUGINS",
    "ORDSYS",
    "OUTLN",
    "REMOTE_SCHEDULER_AGENT",
    "SI_INFORMTN_SCHEMA",
    "SYS",
    "SYSTEM",
    "SYSBACKUP",
    "SYSDG",
    "SYSKM",
    "SYSRAC",
    "WMSYS",
    "XDB",
    "XS$NULL",
}

_SYSTEM_OWNER_PREFIXES = (
    "APEX_",
    "FLOWS_",
)


def _is_application_owner(owner: str) -> bool:
    upper = str(owner or "").strip().upper()
    if not upper:
        return False
    if upper in _SYSTEM_OWNERS:
        return False
    return not any(upper.startswith(prefix) for prefix in _SYSTEM_OWNER_PREFIXES)


def _build_table_pair_clause(
    pairs: list[tuple[str, str]],
    *,
    owner_col: str = "owner",
    table_col: str = "table_name",
) -> tuple[str, dict[str, str]]:
    clauses: list[str] = []
    binds: dict[str, str] = {}
    for idx, (table_owner, table_name) in enumerate(pairs):
        owner_key = f"owner_{idx}"
        table_key = f"table_{idx}"
        clauses.append(f"({owner_col} = :{owner_key} AND {table_col} = :{table_key})")
        binds[owner_key] = table_owner
        binds[table_key] = table_name
    return " OR ".join(clauses), binds


def _pick_best_owner(
    owner_tables: dict[str, list[str]],
    *,
    scoped_tables: set[str],
) -> str | None:
    if not owner_tables:
        return None
    ranked: list[tuple[int, int, str]] = []
    for owner, tables in owner_tables.items():
        table_set = {name.upper() for name in tables}
        overlap = len(table_set & scoped_tables) if scoped_tables else 0
        ranked.append((overlap, len(table_set), owner))
    ranked.sort(key=lambda item: (item[0], item[1], item[2]), reverse=True)
    return ranked[0][2]


def _resolve_target_tables(cur: Any, owner: str) -> tuple[list[tuple[str, str]], str, bool]:
    requested_owner = owner.upper()
    settings = get_settings()
    allow_owner_fallback = bool(getattr(settings, "metadata_owner_fallback_enabled", False))
    cur.execute(
        """
        SELECT owner, table_name
        FROM all_tables
        WHERE owner = :owner
        ORDER BY table_name
        """,
        owner=requested_owner,
    )
    owned_rows = [(str(row[0]), str(row[1])) for row in cur.fetchall()]
    if owned_rows:
        return owned_rows, requested_owner, False
    if not allow_owner_fallback:
        return [], requested_owner, False

    cur.execute(
        """
        SELECT owner, table_name
        FROM all_tables
        ORDER BY owner, table_name
        """
    )
    all_rows = [(str(row[0]), str(row[1])) for row in cur.fetchall()]
    if not all_rows:
        return [], requested_owner, False

    app_rows = [(table_owner, table_name) for table_owner, table_name in all_rows if _is_application_owner(table_owner)]
    candidate_rows = app_rows or all_rows

    owner_tables: dict[str, list[str]] = {}
    for table_owner, table_name in candidate_rows:
        owner_tables.setdefault(table_owner, []).append(table_name)

    scoped_tables = {str(name).strip().upper() for name in load_table_scope() if str(name).strip()}
    selected_owner = _pick_best_owner(owner_tables, scoped_tables=scoped_tables)
    if not selected_owner:
        return [], requested_owner, False

    selected_rows = [
        (table_owner, table_name)
        for table_owner, table_name in candidate_rows
        if table_owner == selected_owner
    ]
    return selected_rows, selected_owner, True


def extract_metadata(owner: str, output_dir: str = "var/metadata") -> dict[str, Any]:
    requested_owner = owner.upper()
    schema_catalog: dict[str, Any] = {
        "owner": requested_owner,
        "requested_owner": requested_owner,
        "owners": [],
        "tables": {},
    }
    join_graph: dict[str, Any] = {"owner": requested_owner, "requested_owner": requested_owner, "edges": []}

    conn = acquire_connection()
    cur = conn.cursor()

    target_pairs, effective_owner, fallback_from_owner_lookup = _resolve_target_tables(cur, requested_owner)
    schema_catalog["owner"] = effective_owner
    join_graph["owner"] = effective_owner

    for table_owner, table_name in target_pairs:
        schema_catalog["tables"].setdefault(
            table_name,
            {"owner": table_owner, "columns": [], "primary_keys": []},
        )
    schema_catalog["owners"] = sorted({table_owner for table_owner, _ in target_pairs})

    if target_pairs:
        pair_where, pair_binds = _build_table_pair_clause(target_pairs)
        cur.execute(
            f"""
            SELECT owner, table_name, column_name, data_type, nullable
            FROM all_tab_columns
            WHERE {pair_where}
            ORDER BY owner, table_name, column_id
            """,
            pair_binds,
        )
        for table_owner, table_name, column_name, data_type, nullable in cur.fetchall():
            table_entry = schema_catalog["tables"].setdefault(
                table_name,
                {"owner": table_owner, "columns": [], "primary_keys": []},
            )
            table_entry["columns"].append({
                "name": column_name,
                "type": data_type,
                "nullable": nullable == "Y",
            })

        cons_where, cons_binds = _build_table_pair_clause(
            target_pairs,
            owner_col="acc.owner",
            table_col="acc.table_name",
        )
        cur.execute(
            f"""
            SELECT acc.owner, acc.table_name, acc.column_name, ac.constraint_type, ac.constraint_name,
                   ac.r_owner, ac.r_constraint_name
            FROM all_cons_columns acc
            JOIN all_constraints ac
              ON acc.owner = ac.owner AND acc.constraint_name = ac.constraint_name
            WHERE ({cons_where})
              AND ac.constraint_type IN ('P', 'R')
            """,
            cons_binds,
        )

        pk_by_constraint: dict[tuple[str, str], list[tuple[str, str]]] = {}
        fk_rows: list[tuple[str, str, str, str, str]] = []

        for table_owner, table_name, column_name, ctype, cname, r_owner, r_cname in cur.fetchall():
            if ctype == "P":
                pk_by_constraint.setdefault((table_owner, cname), []).append((table_name, column_name))
                table_entry = schema_catalog["tables"].setdefault(
                    table_name,
                    {"owner": table_owner, "columns": [], "primary_keys": []},
                )
                table_entry["primary_keys"].append(column_name)
            elif ctype == "R":
                fk_rows.append((table_owner, table_name, column_name, str(r_owner), str(r_cname)))

        for fk_owner, fk_table, fk_column, r_owner, r_cname in fk_rows:
            pk_cols = pk_by_constraint.get((r_owner, r_cname), [])
            for pk_table, pk_column in pk_cols:
                join_graph["edges"].append({
                    "from_schema": fk_owner,
                    "from_table": fk_table,
                    "from_column": fk_column,
                    "to_schema": r_owner,
                    "to_table": pk_table,
                    "to_column": pk_column,
                    "type": "FK",
                })

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    (output_path / "schema_catalog.json").write_text(
        json.dumps(schema_catalog, ensure_ascii=True, indent=2),
        encoding="utf-8",
    )
    (output_path / "join_graph.json").write_text(
        json.dumps(join_graph, ensure_ascii=True, indent=2),
        encoding="utf-8",
    )

    cur.close()
    conn.close()

    return {
        "schema_catalog": schema_catalog,
        "join_graph": join_graph,
        "tables": len(schema_catalog["tables"]),
        "effective_owner": effective_owner,
        "fallback_from_owner_lookup": fallback_from_owner_lookup,
    }
