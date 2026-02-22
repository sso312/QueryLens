"""차트 코드 생성 + 실행 유틸.

- Plotly로 인터랙티브 차트를 생성한다.
- 결과는 figure JSON 형태로 반환한다.
"""
from __future__ import annotations

import os
import base64
import io
from typing import Any, Dict, List, Optional, Tuple
import json

import pandas as pd
from pandas.api import types as pdt
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio

from src.utils.logging import log_event

try:
    import seaborn as sns
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except Exception:  # pragma: no cover - optional dependency
    sns = None
    plt = None


def _read_bar_max_categories(default: int = 30) -> int:
    raw = str(os.getenv("QV_BAR_MAX_CATEGORIES", str(default))).strip()
    try:
        return int(raw)
    except Exception:
        return default


_BAR_MAX_CATEGORIES = _read_bar_max_categories(10)
_BAR_OTHER_LABEL = "기타"
_BAR_LONG_LABEL_LEN = 14
_BAR_DENSE_COUNT = 12
_DISCRETE_CATEGORY_MAX_UNIQUE = 16
_PYRAMID_LEFT_COLOR = "#1d4ed8"
_PYRAMID_RIGHT_COLOR = "#d97706"
_DEFAULT_TEMPLATE = str(os.getenv("QV_PLOT_TEMPLATE", "plotly_white")).strip() or "plotly_white"
_BASE_FONT_FAMILY = str(
    os.getenv(
        "QV_PLOT_FONT_FAMILY",
        "Pretendard, Noto Sans KR, Apple SD Gothic Neo, Segoe UI, sans-serif",
    )
).strip()
_GRID_COLOR = "rgba(226, 232, 240, 0.8)"
_ZERO_LINE_COLOR = "rgba(148, 163, 184, 0.5)"


def _to_hex(rgb: Tuple[float, float, float]) -> str:
    r = max(0, min(255, int(round(float(rgb[0]) * 255))))
    g = max(0, min(255, int(round(float(rgb[1]) * 255))))
    b = max(0, min(255, int(round(float(rgb[2]) * 255))))
    return f"#{r:02x}{g:02x}{b:02x}"


def _resolve_template_name() -> str:
    preferred = _DEFAULT_TEMPLATE
    if preferred in pio.templates:
        return preferred
    if "plotly_white" in pio.templates:
        return "plotly_white"
    if "seaborn" in pio.templates:
        return "seaborn"
    return "plotly"


def _qualitative_palette(n: int = 10) -> List[str]:
    # Balanced categorical palette with high contrast on light backgrounds.
    colors = [
        "#1d4ed8",
        "#0f766e",
        "#c2410c",
        "#be123c",
        "#6d28d9",
        "#0891b2",
        "#4d7c0f",
        "#7c3aed",
        "#ea580c",
        "#0369a1",
    ]
    if len(colors) >= n:
        return colors[:n]
    return [colors[i % len(colors)] for i in range(n)]


def _sequential_palette(n: int = 8) -> List[str]:
    if sns is not None:
        try:
            return [_to_hex(c) for c in sns.color_palette("mako", n)]
        except Exception:
            pass
    fallback = px.colors.sequential.YlGnBu
    if len(fallback) >= n:
        return fallback[:n]
    return [fallback[i % len(fallback)] for i in range(n)]


def _diverging_palette(n: int = 10) -> List[str]:
    if sns is not None:
        try:
            return [_to_hex(c) for c in sns.color_palette("icefire", n)]
        except Exception:
            pass
    fallback = px.colors.diverging.RdBu
    if len(fallback) >= n:
        return fallback[:n]
    return [fallback[i % len(fallback)] for i in range(n)]


def _hex_to_rgb(color: str) -> Tuple[int, int, int]:
    text = str(color or "").strip().lstrip("#")
    if len(text) == 3:
        text = "".join(ch * 2 for ch in text)
    if len(text) != 6:
        return (59, 130, 246)
    try:
        return (
            int(text[0:2], 16),
            int(text[2:4], 16),
            int(text[4:6], 16),
        )
    except Exception:
        return (59, 130, 246)


def _rgba(color: str, alpha: float) -> str:
    r, g, b = _hex_to_rgb(color)
    a = min(1.0, max(0.0, float(alpha)))
    return f"rgba({r},{g},{b},{a})"


def _build_code(chart_spec: Dict[str, Any]) -> str:
    # 시각화 코드를 문자열로 반환(로그/디버깅용)
    chart_type = chart_spec.get("chart_type")
    x = chart_spec.get("x")
    y = chart_spec.get("y")
    group = chart_spec.get("group")
    secondary_group = chart_spec.get("secondary_group")
    agg = chart_spec.get("agg")
    size = chart_spec.get("size")
    animation_frame = chart_spec.get("animation_frame")
    mode = chart_spec.get("mode")
    bar_mode = chart_spec.get("bar_mode")
    orientation = chart_spec.get("orientation")
    series_cols = chart_spec.get("series_cols")
    max_categories = chart_spec.get("max_categories")

    return (
        "# plotly 코드(요약)\n"
        f"# chart_type={chart_type}, x={x}, y={y}, group={group}, secondary_group={secondary_group}, agg={agg}, size={size}, animation_frame={animation_frame}, mode={mode}, "
        f"bar_mode={bar_mode}, orientation={orientation}, series_cols={series_cols}, max_categories={max_categories}\n"
    )


def _resolve_agg_func(agg: Optional[str]) -> Optional[str]:
    if not agg:
        return None
    agg_map = {
        "avg": "mean",
        "mean": "mean",
        "sum": "sum",
        "min": "min",
        "max": "max",
        "count": "count",
        "median": "median",
    }
    return agg_map.get(str(agg).lower())


def _aggregate_frame(
    df: pd.DataFrame,
    x: str,
    y: str,
    group: str | None,
    agg: str | None,
) -> pd.DataFrame:
    agg_func = _resolve_agg_func(agg)
    if not agg_func:
        return df

    by_cols = [x]
    if group:
        by_cols.append(group)
    return df.groupby(by_cols, dropna=False, as_index=False)[y].agg(agg_func)


def _is_color_group_usable(df: pd.DataFrame, col: Optional[str]) -> bool:
    if not col or col not in df.columns:
        return False
    try:
        if pdt.is_numeric_dtype(df[col]):
            return int(df[col].nunique(dropna=True)) <= 12
    except Exception:
        return False
    return True


def _aggregate_two_dimensional(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    *,
    value_col: str | None = None,
    agg: str | None = None,
) -> pd.DataFrame:
    cols = [x_col, y_col]
    if value_col and value_col in df.columns:
        cols.append(value_col)
    chart_df = df[cols].copy().dropna(subset=[x_col, y_col])
    if chart_df.empty:
        return chart_df

    result_value_col = "__value__"
    agg_map = {
        "avg": "mean",
        "mean": "mean",
        "sum": "sum",
        "min": "min",
        "max": "max",
        "count": "count",
        "median": "median",
    }

    if value_col and value_col in chart_df.columns:
        chart_df[value_col] = pd.to_numeric(chart_df[value_col], errors="coerce")
        chart_df = chart_df.dropna(subset=[value_col])
        if chart_df.empty:
            return chart_df
        agg_func = agg_map.get(str(agg).lower(), "sum") if agg else "sum"
        return (
            chart_df.groupby([x_col, y_col], dropna=False, as_index=False)[value_col]
            .agg(agg_func)
            .rename(columns={value_col: result_value_col})
        )

    return (
        chart_df.groupby([x_col, y_col], dropna=False, as_index=False)
        .size()
        .rename(columns={"size": result_value_col})
    )


def _figure_to_data_url(fig: Any) -> Optional[str]:
    if fig is None:
        return None
    try:
        buf = io.BytesIO()
        fig.tight_layout()
        fig.savefig(
            buf,
            format="png",
            dpi=160,
            bbox_inches="tight",
            facecolor="white",
        )
        buf.seek(0)
        encoded = base64.b64encode(buf.read()).decode("ascii")
        return f"data:image/png;base64,{encoded}"
    except Exception:
        return None


def _generate_seaborn_image(
    chart_spec: Dict[str, Any],
    df: pd.DataFrame,
) -> Optional[str]:
    if sns is None or plt is None or df is None or df.empty:
        return None

    chart_type = str(chart_spec.get("chart_type") or "").lower()
    x = chart_spec.get("x")
    y = chart_spec.get("y")
    group = chart_spec.get("group")
    agg = chart_spec.get("agg")
    max_categories_raw = chart_spec.get("max_categories")
    try:
        max_categories = int(max_categories_raw) if max_categories_raw is not None else _BAR_MAX_CATEGORIES
    except Exception:
        max_categories = _BAR_MAX_CATEGORIES
    max_categories = max(1, max_categories)
    orientation = str(chart_spec.get("orientation") or "").strip().lower()
    title = str(chart_spec.get("title") or "").strip()
    x_title = str(chart_spec.get("x_title") or "").strip()
    y_title = str(chart_spec.get("y_title") or "").strip()

    try:
        sns.set_theme(
            style="ticks",
            context="notebook",
            rc={
                "axes.facecolor": "#f8fafc",
                "figure.facecolor": "white",
                "axes.edgecolor": "#cbd5e1",
                "axes.grid": True,
                "grid.color": "#e2e8f0",
                "grid.linewidth": 0.8,
                "axes.titlesize": 15,
                "axes.titleweight": "semibold",
                "axes.labelsize": 12,
                "xtick.labelsize": 11,
                "ytick.labelsize": 11,
                "font.family": "sans-serif",
                "font.sans-serif": ["Noto Sans CJK JP", "DejaVu Sans", "Liberation Sans", "Arial"],
            },
        )
        if sns is not None:
            sns.set_palette("deep")
        fig, ax = plt.subplots(figsize=(10.5, 5.6), dpi=140)
    except Exception:
        return None

    rendered = False
    default_title = chart_type.upper() if chart_type else "CHART"

    try:
        if chart_type in {"line", "line_scatter", "area"} and x and y and x in df.columns and y in df.columns:
            chart_df = _aggregate_frame(df, x, y, group if group in df.columns else None, agg)
            chart_df = chart_df.dropna(subset=[x, y])
            if not chart_df.empty:
                chart_df[y] = pd.to_numeric(chart_df[y], errors="coerce")
                chart_df = chart_df.dropna(subset=[y])
                if not chart_df.empty:
                    chart_df = chart_df.sort_values(by=x, kind="stable")
                    line_kwargs: Dict[str, Any] = {
                        "data": chart_df,
                        "x": x,
                        "y": y,
                        "ax": ax,
                    }
                    if group and group in chart_df.columns:
                        line_kwargs["hue"] = group
                    if chart_type == "line_scatter":
                        line_kwargs["marker"] = "o"
                    sns.lineplot(**line_kwargs)
                    if chart_type == "area" and (not group or group not in chart_df.columns):
                        x_values = chart_df[x].tolist()
                        y_values = chart_df[y].astype(float).tolist()
                        ax.fill_between(x_values, y_values, color="#3b82f6", alpha=0.22)
                    rendered = True

        elif chart_type in {
            "bar",
            "bar_basic",
            "bar_grouped",
            "bar_stacked",
            "bar_hgroup",
            "bar_hstack",
            "bar_percent",
            "bar_hpercent",
        } and x and x in df.columns:
            if y and y in df.columns:
                chart_df = _aggregate_frame(df, x, y, group if group in df.columns else None, agg)
                chart_df[y] = pd.to_numeric(chart_df[y], errors="coerce")
                chart_df = chart_df.dropna(subset=[x, y])
                value_col = y
            else:
                chart_df = (
                    df[[x]]
                    .dropna()
                    .groupby([x], dropna=False, as_index=False)
                    .size()
                    .rename(columns={"size": "__value__"})
                )
                value_col = "__value__"

            if not chart_df.empty:
                chart_df = _limit_bar_categories(
                    chart_df,
                    category_col=x,
                    value_col=value_col,
                    group_col=group if group and group in chart_df.columns else None,
                    top_n=max_categories,
                    agg=agg,
                )
                draw_horizontal = orientation == "h" or chart_type in {"bar_hgroup", "bar_hstack", "bar_hpercent"}
                hue_col = group if group and group in chart_df.columns else None
                if chart_type in {"bar_stacked", "bar_hstack", "bar_percent", "bar_hpercent"} and hue_col:
                    pivot = chart_df.pivot_table(
                        index=x,
                        columns=hue_col,
                        values=value_col,
                        aggfunc="sum",
                        fill_value=0,
                    )
                    if chart_type in {"bar_percent", "bar_hpercent"}:
                        row_sum = pivot.sum(axis=1).replace(0, 1)
                        pivot = pivot.div(row_sum, axis=0) * 100.0
                    if draw_horizontal:
                        pivot.sort_values(by=pivot.columns.tolist()[0], ascending=True).plot(
                            kind="barh",
                            stacked=True,
                            ax=ax,
                            width=0.72,
                        )
                    else:
                        pivot.plot(kind="bar", stacked=True, ax=ax, width=0.72)
                else:
                    bar_kwargs: Dict[str, Any] = {
                        "data": chart_df,
                        "x": value_col if draw_horizontal else x,
                        "y": x if draw_horizontal else value_col,
                        "ax": ax,
                    }
                    if hue_col:
                        bar_kwargs["hue"] = hue_col
                    sns.barplot(**bar_kwargs)
                rendered = True

        elif chart_type == "lollipop" and x and y and x in df.columns and y in df.columns:
            chart_df = _aggregate_frame(df, x, y, None, agg)
            chart_df[y] = pd.to_numeric(chart_df[y], errors="coerce")
            chart_df = chart_df.dropna(subset=[x, y])
            if not chart_df.empty:
                chart_df = _limit_bar_categories(
                    chart_df,
                    category_col=x,
                    value_col=y,
                    group_col=None,
                    top_n=max_categories,
                    agg=agg,
                )
                chart_df = chart_df.sort_values(by=y, ascending=False).head(max(10, max_categories))
                draw_horizontal = orientation == "h" or _should_use_horizontal_bar(chart_df[x].astype(str))
                if draw_horizontal:
                    y_labels = chart_df[x].astype(str).tolist()
                    x_values = chart_df[y].astype(float).tolist()
                    ax.hlines(y=y_labels, xmin=0, xmax=x_values, color="#94a3b8", linewidth=2)
                    ax.scatter(x_values, y_labels, color="#2563eb", s=70, edgecolors="white", linewidths=0.9, zorder=3)
                else:
                    x_labels = chart_df[x].astype(str).tolist()
                    y_values = chart_df[y].astype(float).tolist()
                    ax.vlines(x=x_labels, ymin=0, ymax=y_values, color="#94a3b8", linewidth=2)
                    ax.scatter(x_labels, y_values, color="#2563eb", s=70, edgecolors="white", linewidths=0.9, zorder=3)
                    ax.tick_params(axis="x", rotation=35)
                rendered = True

        elif chart_type == "pie" and x and x in df.columns:
            if y and y in df.columns:
                pie_df = df[[x, y]].copy()
                pie_df[y] = pd.to_numeric(pie_df[y], errors="coerce")
                pie_df = pie_df.dropna(subset=[x, y])
                if not pie_df.empty:
                    pie_df = pie_df.groupby(x, dropna=False, as_index=False)[y].sum()
                    labels = pie_df[x].astype(str).tolist()
                    values = pie_df[y].astype(float).tolist()
                else:
                    labels, values = [], []
            else:
                pie_df = (
                    df[[x]]
                    .dropna()
                    .groupby(x, dropna=False)
                    .size()
                    .reset_index(name="count")
                )
                labels = pie_df[x].astype(str).tolist()
                values = pie_df["count"].astype(float).tolist()
            if values:
                ax.pie(
                    values,
                    labels=labels,
                    autopct="%1.1f%%",
                    startangle=90,
                    counterclock=False,
                    wedgeprops={"edgecolor": "white", "linewidth": 1.2},
                )
                ax.axis("equal")
                rendered = True

        elif chart_type == "hist" and x and x in df.columns:
            hist_df = df[[x] + ([group] if group and group in df.columns else [])].copy()
            hist_df[x] = pd.to_numeric(hist_df[x], errors="coerce")
            hist_df = hist_df.dropna(subset=[x])
            if not hist_df.empty:
                sns.histplot(
                    data=hist_df,
                    x=x,
                    hue=group if group and group in hist_df.columns else None,
                    kde=True,
                    ax=ax,
                    alpha=0.72,
                    edgecolor="white",
                )
                rendered = True

        elif chart_type in {"scatter", "dynamic_scatter"} and x and y and x in df.columns and y in df.columns:
            scatter_df = df[[x, y] + ([group] if group and group in df.columns else [])].copy()
            scatter_df[x] = pd.to_numeric(scatter_df[x], errors="coerce")
            scatter_df[y] = pd.to_numeric(scatter_df[y], errors="coerce")
            scatter_df = scatter_df.dropna(subset=[x, y])
            if not scatter_df.empty:
                sns.scatterplot(
                    data=scatter_df,
                    x=x,
                    y=y,
                    hue=group if group and group in scatter_df.columns else None,
                    ax=ax,
                    s=58,
                )
                rendered = True

        elif chart_type == "box" and y and y in df.columns:
            cols = [y] + ([x] if x and x in df.columns else [])
            box_df = df[cols].copy()
            box_df[y] = pd.to_numeric(box_df[y], errors="coerce")
            box_df = box_df.dropna(subset=[y])
            if not box_df.empty:
                if x and x in box_df.columns:
                    sns.boxplot(data=box_df, x=x, y=y, ax=ax)
                    ax.tick_params(axis="x", rotation=25)
                else:
                    sns.boxplot(data=box_df, y=y, ax=ax)
                rendered = True

        elif chart_type == "violin" and y and y in df.columns:
            cols = [y] + ([x] if x and x in df.columns else [])
            vio_df = df[cols].copy()
            vio_df[y] = pd.to_numeric(vio_df[y], errors="coerce")
            vio_df = vio_df.dropna(subset=[y])
            if not vio_df.empty:
                if x and x in vio_df.columns:
                    sns.violinplot(data=vio_df, x=x, y=y, ax=ax, inner="quartile", cut=0)
                    ax.tick_params(axis="x", rotation=25)
                else:
                    sns.violinplot(data=vio_df, y=y, ax=ax, inner="quartile", cut=0)
                rendered = True

        elif chart_type in {"heatmap", "confusion_matrix"} and x and y and x in df.columns and y in df.columns:
            numeric_group = group if group and group in df.columns and pdt.is_numeric_dtype(df[group]) else None
            hm_df = _aggregate_two_dimensional(df, x, y, value_col=numeric_group, agg=agg)
            if not hm_df.empty:
                pivot = hm_df.pivot_table(
                    index=y,
                    columns=x,
                    values="__value__",
                    aggfunc="sum",
                    fill_value=0,
                )
                if not pivot.empty:
                    row_cap = max(8, min(40, max_categories * 2))
                    col_cap = max(8, min(40, max_categories * 2))
                    if pivot.shape[0] > row_cap:
                        pivot = pivot.head(row_cap)
                    if pivot.shape[1] > col_cap:
                        pivot = pivot.iloc[:, :col_cap]
                    cm_mode = chart_type == "confusion_matrix"
                    sns.heatmap(
                        pivot,
                        cmap="Blues" if cm_mode else "crest",
                        annot=cm_mode,
                        fmt=".0f" if cm_mode else ".2g",
                        linewidths=0.35,
                        linecolor="white",
                        ax=ax,
                        cbar=True,
                    )
                    if cm_mode:
                        ax.set_xlabel(f"Predicted ({x})")
                        ax.set_ylabel(f"Actual ({y})")
                    rendered = True

        if not rendered:
            plt.close(fig)
            return None

        ax.set_title(title or default_title, loc="left", fontsize=15, fontweight="semibold")
        if x_title:
            ax.set_xlabel(x_title)
        elif x:
            ax.set_xlabel(str(x))
        if y_title:
            ax.set_ylabel(y_title)
        elif y:
            ax.set_ylabel(str(y))

        legend = ax.get_legend()
        if legend is not None:
            legend.set_title(None)
            legend.set_frame_on(False)

        if chart_type not in {"pie"}:
            ax.tick_params(axis="x", labelrotation=24 if chart_type in {"bar", "bar_basic", "bar_grouped", "bar_stacked"} else 0)
        try:
            sns.despine(ax=ax, top=True, right=True)
        except Exception:
            pass

        encoded = _figure_to_data_url(fig)
        plt.close(fig)
        return encoded
    except Exception:
        try:
            plt.close(fig)
        except Exception:
            pass
        return None


def _aggregate_pyramid_frame(
    df: pd.DataFrame,
    x: str,
    y: str,
    group: str,
    agg: str | None,
) -> pd.DataFrame:
    chart_df = df[[x, y, group]].copy()
    chart_df[y] = pd.to_numeric(chart_df[y], errors="coerce")
    chart_df = chart_df.dropna(subset=[x, y, group])
    if chart_df.empty:
        return chart_df
    agg_map = {
        "avg": "mean",
        "mean": "mean",
        "sum": "sum",
        "min": "min",
        "max": "max",
        "count": "count",
        "median": "median",
    }
    agg_func = agg_map.get(str(agg).lower(), "sum") if agg else "sum"
    return (
        chart_df.groupby([x, group], dropna=False, as_index=False)[y]
        .agg(agg_func)
    )


def _limit_bar_categories(
    chart_df: pd.DataFrame,
    category_col: str,
    value_col: str,
    group_col: Optional[str] = None,
    *,
    top_n: int = _BAR_MAX_CATEGORIES,
    agg: Optional[str] = None,
) -> pd.DataFrame:
    if category_col not in chart_df.columns or value_col not in chart_df.columns:
        return chart_df

    df2 = chart_df.copy()
    if top_n <= 0:
        return df2
    df2[category_col] = df2[category_col].astype(str)
    unique_count = int(df2[category_col].nunique(dropna=False))
    if unique_count <= top_n:
        return df2

    numeric_value = pd.to_numeric(df2[value_col], errors="coerce").fillna(0.0)
    agg_norm = str(agg or "").strip().lower()
    can_rollup_other = agg_norm in {"", "sum", "count"}
    reserve_other_slot = can_rollup_other and unique_count > top_n and top_n > 1
    head_n = max(1, top_n - 1) if reserve_other_slot else top_n
    score_by_category = (
        pd.DataFrame({category_col: df2[category_col], "__score__": numeric_value.abs()})
        .groupby(category_col, dropna=False)["__score__"]
        .sum()
        .sort_values(ascending=False)
    )
    top_categories = score_by_category.head(head_n).index.astype(str).tolist()
    top_set = set(top_categories)

    keep_df = df2[df2[category_col].isin(top_set)].copy()
    rest_df = df2[~df2[category_col].isin(top_set)].copy()

    if reserve_other_slot and not rest_df.empty:
        if group_col and group_col in rest_df.columns:
            other_rows = (
                rest_df.groupby(group_col, dropna=False, as_index=False)[value_col]
                .sum()
            )
            other_rows[category_col] = _BAR_OTHER_LABEL
            keep_df = pd.concat(
                [keep_df, other_rows[[category_col, group_col, value_col]]],
                ignore_index=True,
            )
        else:
            other_value = pd.to_numeric(rest_df[value_col], errors="coerce").fillna(0.0).sum()
            keep_df = pd.concat(
                [
                    keep_df,
                    pd.DataFrame([{category_col: _BAR_OTHER_LABEL, value_col: float(other_value)}]),
                ],
                ignore_index=True,
            )
        top_categories.append(_BAR_OTHER_LABEL)

    order_map = {cat: idx for idx, cat in enumerate(top_categories)}
    keep_df["__order__"] = keep_df[category_col].map(order_map).fillna(len(order_map))
    sort_cols = ["__order__"]
    if group_col and group_col in keep_df.columns:
        sort_cols.append(group_col)
    keep_df = keep_df.sort_values(sort_cols, kind="stable").drop(columns=["__order__"])

    log_event(
        "codegen.bar.capped_categories",
        {
            "category_col": category_col,
            "group_col": group_col,
            "before": unique_count,
            "after": int(keep_df[category_col].nunique(dropna=False)),
            "top_n": top_n,
        },
    )
    return keep_df


def _should_use_horizontal_bar(category_values: pd.Series) -> bool:
    if category_values.empty:
        return False
    labels = category_values.astype(str).tolist()
    max_len = max((len(text) for text in labels), default=0)
    return len(labels) >= _BAR_DENSE_COUNT or max_len >= _BAR_LONG_LABEL_LEN


def _looks_like_discrete_numeric_category(
    series: pd.Series,
    column_name: str,
    *,
    max_unique: int,
) -> bool:
    values = pd.to_numeric(series, errors="coerce").dropna()
    if values.empty:
        return False

    unique_count = int(values.nunique(dropna=True))
    if unique_count <= 1 or unique_count > max_unique:
        return False

    integer_like_ratio = float(((values - values.round()).abs() <= 1e-9).mean())
    if integer_like_ratio < 0.96:
        return False

    repeats_ratio = float(len(values) / max(unique_count, 1))
    col = str(column_name or "").strip().lower()
    category_hint = any(
        token in col
        for token in (
            "group",
            "category",
            "class",
            "bucket",
            "code",
            "band",
            "level",
            "type",
            "status",
            "stage",
            "grade",
            "range",
            "age_group",
        )
    )
    return bool(category_hint or repeats_ratio >= 2.2)


def _apply_axis_style(fig: go.Figure) -> None:
    fig.update_xaxes(
        showgrid=True,
        gridcolor=_GRID_COLOR,
        gridwidth=1,
        zeroline=False,
        zerolinecolor=_ZERO_LINE_COLOR,
        automargin=True,
        ticks="outside",
        ticklen=5,
        tickcolor="rgba(148, 163, 184, 0.75)",
        tickfont=dict(size=12, color="#64748b"),
        title_font=dict(size=13, color="#334155"),
        showline=True,
        linecolor="rgba(148, 163, 184, 0.45)",
    )
    fig.update_yaxes(
        showgrid=True,
        gridcolor=_GRID_COLOR,
        gridwidth=1,
        zeroline=False,
        zerolinecolor=_ZERO_LINE_COLOR,
        automargin=True,
        ticks="outside",
        ticklen=5,
        tickcolor="rgba(148, 163, 184, 0.75)",
        tickfont=dict(size=12, color="#64748b"),
        title_font=dict(size=13, color="#334155"),
        showline=True,
        linecolor="rgba(148, 163, 184, 0.45)",
    )


def _apply_visual_polish(fig: go.Figure, chart_type: str) -> None:
    ctype = str(chart_type or "").lower()
    trace_count = len(getattr(fig, "data", []) or [])
    legend_vertical = trace_count >= 8 and ctype.startswith("bar")
    if ctype in {"hist", "heatmap", "confusion_matrix", "treemap"}:
        colorway = _sequential_palette(8)
    elif ctype in {"scatter", "dynamic_scatter"}:
        colorway = _diverging_palette(10)
    else:
        colorway = _qualitative_palette(10)

    fig.update_layout(
        template=_resolve_template_name(),
        colorway=colorway,
        font=dict(family=_BASE_FONT_FAMILY, size=14, color="#1e293b"),
        hoverlabel=dict(
            bgcolor="#1e293b",
            bordercolor="#1e293b",
            font=dict(color="white", size=13, family=_BASE_FONT_FAMILY),
        ),
        legend=dict(
            orientation="v" if legend_vertical else "h",
            yanchor="top" if legend_vertical else "bottom",
            y=1.0 if legend_vertical else 1.03,
            xanchor="left",
            x=1.01 if legend_vertical else 0.0,
            bgcolor="rgba(255,255,255,0)",
            font=dict(size=12),
            borderwidth=0,
        ),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=56, r=46 if legend_vertical else 28, t=44, b=56),
        uniformtext=dict(minsize=10, mode="hide"),
        title=dict(
            font=dict(size=16, color="#0f172a"),
            x=0.0,
            xanchor="left",
            y=0.98,
        ),
    )

    if ctype in {"line", "line_scatter", "area"}:
        fig.update_layout(hovermode="x unified")
        fig.update_traces(
            selector=dict(type="scatter"),
            line=dict(shape="spline", width=2.8, smoothing=0.6),
            connectgaps=True,
        )
        if ctype == "line_scatter":
            fig.update_traces(
                selector=dict(type="scatter"),
                mode="lines+markers",
                marker=dict(size=8, symbol="circle", line=dict(width=2, color="white")),
            )
        if ctype == "area":
            fig.update_traces(fill="tozeroy", fillcolor="rgba(29, 78, 216, 0.22)")

    if ctype.startswith("bar") or ctype == "pyramid":
        fig.update_traces(
            selector=dict(type="bar"),
            marker=dict(opacity=0.92, line=dict(color="rgba(255,255,255,0.92)", width=1)),
            cliponaxis=False,
            textfont=dict(size=11, color="#0f172a"),
        )
        fig.update_layout(bargap=0.18, bargroupgap=0.08)

    if ctype == "hist":
        fig.update_layout(bargap=0.1)
        fig.update_traces(
            selector=dict(type="histogram"),
            opacity=0.85,
            marker=dict(line=dict(color="white", width=1)),
        )

    if ctype == "scatter":
        fig.update_traces(
            selector=dict(type="scatter"),
            marker=dict(size=10, opacity=0.85, line=dict(width=1, color="white")),
        )

    if ctype == "dynamic_scatter":
        fig.update_traces(
            selector=dict(type="scatter"),
            marker=dict(opacity=0.82, line=dict(color="white", width=0.7)),
        )

    if ctype == "lollipop":
        fig.update_layout(hovermode="closest")
        fig.update_traces(
            selector=dict(type="scatter"),
            marker=dict(size=9, opacity=0.92, line=dict(width=1, color="white")),
        )

    if ctype in {"heatmap", "confusion_matrix"}:
        fig.update_layout(hovermode="closest")
        fig.update_traces(
            selector=dict(type="heatmap"),
            xgap=1,
            ygap=1,
            hoverongaps=False,
        )
        if ctype == "confusion_matrix":
            fig.update_layout(coloraxis_colorbar=dict(title="count", thickness=14, len=0.78))
        else:
            fig.update_layout(coloraxis_colorbar=dict(title="value", thickness=14, len=0.78))

    if ctype == "treemap":
        fig.update_traces(
            selector=dict(type="treemap"),
            textinfo="label+percent entry",
            textfont=dict(size=12),
        )
        fig.update_layout(coloraxis_showscale=False)

    if ctype == "box":
        fig.update_traces(
            selector=dict(type="box"),
            boxmean=True,
            marker=dict(opacity=0.6, size=4, line=dict(width=1, color="white")),
            line=dict(width=2),
        )

    if ctype == "violin":
        fig.update_traces(
            selector=dict(type="violin"),
            box_visible=True,
            meanline_visible=True,
            points="suspectedoutliers",
        )

    if ctype in {"pie", "nested_pie", "sunburst"}:
        fig.update_traces(
            selector=dict(type="pie"),
            marker=dict(line=dict(color="white", width=2)),
            textinfo="label+percent",
            textposition="auto",
            hole=0.4 if ctype == "pie" else 0,
        )
        fig.update_layout(showlegend=True)

    if ctype not in {"pie", "nested_pie", "sunburst", "treemap"}:
        _apply_axis_style(fig)


def generate_chart(
    chart_spec: Dict[str, Any],
    df: pd.DataFrame,
) -> Dict[str, Any]:
    """차트를 생성하고 figure JSON과 코드를 반환."""
    chart_type = chart_spec.get("chart_type")
    x = chart_spec.get("x")
    y = chart_spec.get("y")
    group = chart_spec.get("group")
    secondary_group = chart_spec.get("secondary_group")
    agg = chart_spec.get("agg")
    size = chart_spec.get("size")
    animation_frame = chart_spec.get("animation_frame")
    mode = str(chart_spec.get("mode") or "").strip().lower()
    bar_mode = str(chart_spec.get("bar_mode") or "").strip().lower()
    orientation = str(chart_spec.get("orientation") or "").strip().lower()
    raw_series_cols = chart_spec.get("series_cols")
    title = chart_spec.get("title")
    x_title = chart_spec.get("x_title")
    y_title = chart_spec.get("y_title")
    max_categories_raw = chart_spec.get("max_categories")
    max_categories = _BAR_MAX_CATEGORIES
    if max_categories_raw is not None:
        try:
            max_categories = int(max_categories_raw)
        except Exception:
            max_categories = _BAR_MAX_CATEGORIES

    log_event(
        "codegen.start",
        {
            "chart_type": chart_type,
            "x": x,
            "y": y,
            "group": group,
            "secondary_group": secondary_group,
            "agg": agg,
            "size": size,
            "animation_frame": animation_frame,
            "mode": mode,
            "bar_mode": bar_mode,
            "orientation": orientation,
            "max_categories": max_categories,
        },
    )

    fig = None

    if chart_type == "line" and x and y:
        chart_df = _aggregate_frame(df, x, y, group, agg)
        fig = px.line(chart_df, x=x, y=y, color=group)
    elif chart_type == "line_scatter" and x and y:
        chart_df = _aggregate_frame(df, x, y, group, agg)
        fig = px.line(chart_df, x=x, y=y, color=group, markers=True)
        fig.update_traces(mode="lines+markers")
    elif chart_type == "area" and x and y:
        chart_df = _aggregate_frame(df, x, y, group, agg)
        fig = px.area(chart_df, x=x, y=y, color=group)
    elif chart_type in (
        "bar",
        "bar_basic",
        "bar_grouped",
        "bar_stacked",
        "bar_hgroup",
        "bar_hstack",
        "bar_percent",
        "bar_hpercent",
    ) and x:
        # Defaults by style (simple -> detailed bar variants)
        if chart_type == "bar_basic":
            default_mode = "group"
            default_orientation = "v"
        elif chart_type == "bar_grouped":
            default_mode = "group"
            default_orientation = "v"
        elif chart_type == "bar_stacked":
            default_mode = "stack"
            default_orientation = "v"
        elif chart_type == "bar_hgroup":
            default_mode = "group"
            default_orientation = "h"
        elif chart_type == "bar_hstack":
            default_mode = "stack"
            default_orientation = "h"
        elif chart_type == "bar_percent":
            default_mode = "stack"
            default_orientation = "v"
        elif chart_type == "bar_hpercent":
            default_mode = "stack"
            default_orientation = "h"
        else:
            default_mode = "group" if group else "group"
            default_orientation = "v"

        resolved_mode = bar_mode or default_mode
        resolved_orientation = orientation or default_orientation
        if resolved_mode not in {"group", "stack", "relative", "overlay"}:
            resolved_mode = default_mode
        if resolved_orientation not in {"h", "v"}:
            resolved_orientation = default_orientation
        auto_orientation = not bool(orientation)

        series_cols: list[str] = []
        if isinstance(raw_series_cols, list):
            for col in raw_series_cols:
                if isinstance(col, str) and col in df.columns and col != x:
                    series_cols.append(col)

        # Wide -> long transform for grouped/stacked multi-series bars.
        if series_cols:
            keep_cols = [x] + series_cols
            work_df = df[keep_cols].copy()
            for col in series_cols:
                work_df[col] = pd.to_numeric(work_df[col], errors="coerce")
            melt_df = (
                work_df.melt(
                    id_vars=[x],
                    value_vars=series_cols,
                    var_name="__series__",
                    value_name="__value__",
                )
                .dropna(subset=[x, "__value__"])
            )
            if not melt_df.empty:
                chart_df = (
                    melt_df.groupby([x, "__series__"], dropna=False, as_index=False)["__value__"]
                    .sum()
                )
                chart_df = _limit_bar_categories(
                    chart_df,
                    category_col=x,
                    value_col="__value__",
                    group_col="__series__",
                    top_n=max_categories,
                    agg="sum",
                )
                if auto_orientation and _should_use_horizontal_bar(chart_df[x]):
                    resolved_orientation = "h"
                if resolved_orientation == "h":
                    fig = px.bar(
                        chart_df,
                        x="__value__",
                        y=x,
                        color="__series__",
                        orientation="h",
                        barmode=resolved_mode,
                    )
                else:
                    fig = px.bar(
                        chart_df,
                        x=x,
                        y="__value__",
                        color="__series__",
                        orientation="v",
                        barmode=resolved_mode,
                    )
        elif y and y in df.columns:
            group_col = group if group and group in df.columns else None
            secondary_group_col = (
                secondary_group
                if secondary_group and secondary_group in df.columns and secondary_group != group_col
                else None
            )
            value_col = y

            y_numeric_series = pd.to_numeric(df[y], errors="coerce")
            y_numeric_ratio = float(y_numeric_series.notna().mean()) if len(df[y]) else 0.0
            y_is_metric = bool(pdt.is_numeric_dtype(df[y]) or y_numeric_ratio >= 0.6)
            agg_norm = str(agg or "").strip().lower()
            if y_is_metric and not agg_norm:
                discrete_like = _looks_like_discrete_numeric_category(
                    df[y],
                    str(y),
                    max_unique=max(_DISCRETE_CATEGORY_MAX_UNIQUE, max_categories * 2),
                )
                if discrete_like:
                    y_is_metric = False

            if not y_is_metric:
                fallback_group_col = group_col
                # y가 범주형이면 빈 차트 대신 count 기반 막대로 폴백한다.
                if not fallback_group_col and not secondary_group_col and y != x:
                    try:
                        y_card = int(df[y].nunique(dropna=True))
                    except Exception:
                        y_card = 999999
                    if 1 < y_card <= max(12, max_categories * 2):
                        fallback_group_col = y
                by_cols = [x]
                if fallback_group_col and fallback_group_col not in by_cols:
                    by_cols.append(fallback_group_col)
                if secondary_group_col and secondary_group_col not in by_cols:
                    by_cols.append(secondary_group_col)
                chart_df = (
                    df[by_cols]
                    .dropna()
                    .groupby(by_cols, dropna=False, as_index=False)
                    .size()
                    .rename(columns={"size": "__value__"})
                )
                value_col = "__value__"
                if group_col is None:
                    group_col = fallback_group_col
            elif secondary_group_col:
                use_cols = [x, y]
                if group_col:
                    use_cols.append(group_col)
                if secondary_group_col not in use_cols:
                    use_cols.append(secondary_group_col)
                chart_df = df[use_cols].copy()
                chart_df[y] = pd.to_numeric(chart_df[y], errors="coerce")
                drop_cols = [x, y]
                if group_col:
                    drop_cols.append(group_col)
                if secondary_group_col:
                    drop_cols.append(secondary_group_col)
                chart_df = chart_df.dropna(subset=drop_cols)
                agg_func = _resolve_agg_func(agg) or "sum"
                if not chart_df.empty:
                    by_cols = [x]
                    if group_col:
                        by_cols.append(group_col)
                    if secondary_group_col:
                        by_cols.append(secondary_group_col)
                    chart_df = (
                        chart_df.groupby(by_cols, dropna=False, as_index=False)[y]
                        .agg(agg_func)
                    )
            else:
                chart_df = _aggregate_frame(df, x, y, group_col, agg)
            if not chart_df.empty:
                if value_col in chart_df.columns:
                    chart_df[value_col] = pd.to_numeric(chart_df[value_col], errors="coerce")
                    chart_df = chart_df.dropna(subset=[value_col])
                if chart_df.empty:
                    fig = None
                else:
                    bar_group_col = None
                    if group_col and group_col in chart_df.columns:
                        bar_group_col = group_col
                    elif secondary_group_col and secondary_group_col in chart_df.columns:
                        bar_group_col = secondary_group_col
                    chart_df = _limit_bar_categories(
                        chart_df,
                        category_col=x,
                        value_col=value_col,
                        group_col=bar_group_col,
                        top_n=max_categories,
                        agg=agg if value_col == y else "count",
                    )
                    if auto_orientation and _should_use_horizontal_bar(chart_df[x]):
                        resolved_orientation = "h"
                    color_col = None
                    if group_col and group_col in chart_df.columns and _is_color_group_usable(df, group_col):
                        color_col = group_col
                    elif secondary_group_col and secondary_group_col in chart_df.columns and _is_color_group_usable(df, secondary_group_col):
                        color_col = secondary_group_col
                    pattern_col = (
                        secondary_group_col
                        if (
                            color_col == group_col
                            and secondary_group_col
                            and secondary_group_col in chart_df.columns
                            and _is_color_group_usable(df, secondary_group_col)
                        )
                        else None
                    )
                    if color_col and color_col in chart_df.columns and pdt.is_numeric_dtype(chart_df[color_col]):
                        chart_df[color_col] = chart_df[color_col].astype(str)
                    if pattern_col and pattern_col in chart_df.columns and pdt.is_numeric_dtype(chart_df[pattern_col]):
                        chart_df[pattern_col] = chart_df[pattern_col].astype(str)
                    if resolved_orientation == "h":
                        bar_kwargs: Dict[str, Any] = {
                            "x": value_col,
                            "y": x,
                            "orientation": "h",
                            "barmode": resolved_mode,
                        }
                        if color_col:
                            bar_kwargs["color"] = color_col
                        if pattern_col:
                            bar_kwargs["pattern_shape"] = pattern_col
                        fig = px.bar(chart_df, **bar_kwargs)
                    else:
                        bar_kwargs = {
                            "x": x,
                            "y": value_col,
                            "orientation": "v",
                            "barmode": resolved_mode,
                        }
                        if color_col:
                            bar_kwargs["color"] = color_col
                        if pattern_col:
                            bar_kwargs["pattern_shape"] = pattern_col
                        fig = px.bar(chart_df, **bar_kwargs)

        if fig is not None:
            category_count = int(chart_df[x].nunique(dropna=False)) if "chart_df" in locals() and x in chart_df.columns else 0
            if chart_type in {"bar_percent", "bar_hpercent"}:
                fig.update_layout(barnorm="percent")
            fig.update_traces(
                marker=dict(line=dict(color="white", width=0.6)),
                textposition="auto",
            )
            fig.update_layout(
                margin=dict(l=56, r=24, t=36, b=56),
                legend_title_text=None,
            )
            if resolved_orientation == "v" and category_count >= _BAR_DENSE_COUNT:
                fig.update_xaxes(tickangle=-35, automargin=True)
            if resolved_orientation == "h" and category_count >= _BAR_DENSE_COUNT:
                fig.update_layout(height=min(1400, max(480, 220 + category_count * 18)))
            if title:
                fig.update_layout(title=str(title))
            if x_title:
                fig.update_xaxes(title_text=str(x_title))
            if y_title:
                fig.update_yaxes(title_text=str(y_title))
            elif y:
                inferred_y_title = "count" if "value_col" in locals() and value_col == "__value__" else str(y)
                fig.update_yaxes(title_text=inferred_y_title)
    elif chart_type == "lollipop" and x and y:
        chart_df = _aggregate_frame(df, x, y, group, agg)
        if not chart_df.empty and y in chart_df.columns:
            chart_df[y] = pd.to_numeric(chart_df[y], errors="coerce")
            chart_df = chart_df.dropna(subset=[x, y])
        if not chart_df.empty:
            chart_df = _limit_bar_categories(
                chart_df,
                category_col=x,
                value_col=y,
                group_col=None,
                top_n=max_categories,
                agg=agg,
            )
        if not chart_df.empty:
            auto_horizontal = _should_use_horizontal_bar(chart_df[x])
            resolved_orientation = orientation if orientation in {"h", "v"} else ("h" if auto_horizontal else "v")
            lollipop_df = chart_df.sort_values(y, ascending=(resolved_orientation == "h"))
            categories = lollipop_df[x].astype(str).tolist()
            values = pd.to_numeric(lollipop_df[y], errors="coerce").fillna(0.0).tolist()
            base_color = _qualitative_palette(1)[0]

            if resolved_orientation == "h":
                stem_x: List[float | None] = []
                stem_y: List[str | None] = []
                for cat, val in zip(categories, values):
                    stem_x.extend([0.0, float(val), None])
                    stem_y.extend([cat, cat, None])
                fig = go.Figure()
                fig.add_trace(
                    go.Scatter(
                        x=stem_x,
                        y=stem_y,
                        mode="lines",
                        showlegend=False,
                        hoverinfo="skip",
                        line=dict(color="rgba(100,116,139,0.55)", width=2, shape="linear"),
                    )
                )
                fig.add_trace(
                    go.Scatter(
                        x=values,
                        y=categories,
                        mode="markers",
                        name=str(y),
                        marker=dict(
                            size=10,
                            color=values,
                            colorscale="Blues",
                            line=dict(color="white", width=1),
                            showscale=False,
                        ),
                    )
                )
                fig.update_layout(xaxis_title=str(y), yaxis_title=str(x))
            else:
                stem_x: List[str | None] = []
                stem_y: List[float | None] = []
                for cat, val in zip(categories, values):
                    stem_x.extend([cat, cat, None])
                    stem_y.extend([0.0, float(val), None])
                fig = go.Figure()
                fig.add_trace(
                    go.Scatter(
                        x=stem_x,
                        y=stem_y,
                        mode="lines",
                        showlegend=False,
                        hoverinfo="skip",
                        line=dict(color="rgba(100,116,139,0.55)", width=2, shape="linear"),
                    )
                )
                fig.add_trace(
                    go.Scatter(
                        x=categories,
                        y=values,
                        mode="markers",
                        name=str(y),
                        marker=dict(
                            size=10,
                            color=base_color,
                            line=dict(color="white", width=1),
                        ),
                    )
                )
                fig.update_layout(xaxis_title=str(x), yaxis_title=str(y))
    elif chart_type == "pie" and x:
        value_col = "__value__"
        chart_df = pd.DataFrame()
        if y and y in df.columns and pdt.is_numeric_dtype(df[y]):
            chart_df = df[[x, y]].copy()
            chart_df[y] = pd.to_numeric(chart_df[y], errors="coerce")
            chart_df = chart_df.dropna(subset=[x, y])
            if not chart_df.empty:
                agg_map = {
                    "avg": "mean",
                    "mean": "mean",
                    "sum": "sum",
                    "min": "min",
                    "max": "max",
                    "count": "count",
                    "median": "median",
                }
                agg_func = agg_map.get(str(agg).lower(), "sum") if agg else "sum"
                chart_df = (
                    chart_df.groupby([x], dropna=False, as_index=False)[y]
                    .agg(agg_func)
                    .rename(columns={y: value_col})
                )
        else:
            chart_df = df[[x]].dropna()
            if not chart_df.empty:
                chart_df = (
                    chart_df.groupby([x], dropna=False, as_index=False)
                    .size()
                    .rename(columns={"size": value_col})
                )
        if not chart_df.empty and value_col in chart_df.columns:
            fig = px.pie(chart_df, names=x, values=value_col)
            fig.update_traces(
                textinfo="label+percent",
                textposition="outside",
                automargin=True,
                marker=dict(line=dict(color="white", width=1.2)),
            )
            fig.update_layout(
                margin=dict(l=36, r=36, t=24, b=24),
            )
    elif chart_type == "treemap" and x:
        value_col = "__value__"
        if group and group in df.columns:
            chart_df = df[[x, group] + ([y] if y and y in df.columns else [])].copy()
            if y and y in chart_df.columns and pdt.is_numeric_dtype(chart_df[y]):
                chart_df[y] = pd.to_numeric(chart_df[y], errors="coerce")
                chart_df = chart_df.dropna(subset=[x, group, y])
                if not chart_df.empty:
                    agg_map = {
                        "avg": "mean",
                        "mean": "mean",
                        "sum": "sum",
                        "min": "min",
                        "max": "max",
                        "count": "count",
                        "median": "median",
                    }
                    agg_func = agg_map.get(str(agg).lower(), "sum") if agg else "sum"
                    chart_df = (
                        chart_df.groupby([x, group], dropna=False, as_index=False)[y]
                        .agg(agg_func)
                        .rename(columns={y: value_col})
                    )
            else:
                chart_df = chart_df.dropna(subset=[x, group])
                if not chart_df.empty:
                    chart_df = (
                        chart_df.groupby([x, group], dropna=False, as_index=False)
                        .size()
                        .rename(columns={"size": value_col})
                    )
            if not chart_df.empty and value_col in chart_df.columns:
                chart_df[x] = chart_df[x].astype(str)
                chart_df[group] = chart_df[group].astype(str)
                chart_df = chart_df.sort_values(value_col, ascending=False).head(80)
                fig = px.treemap(
                    chart_df,
                    path=[x, group],
                    values=value_col,
                    color=value_col,
                    color_continuous_scale="Blues",
                )
        else:
            chart_df = df[[x] + ([y] if y and y in df.columns else [])].copy()
            if y and y in chart_df.columns and pdt.is_numeric_dtype(chart_df[y]):
                chart_df[y] = pd.to_numeric(chart_df[y], errors="coerce")
                chart_df = chart_df.dropna(subset=[x, y])
                if not chart_df.empty:
                    agg_map = {
                        "avg": "mean",
                        "mean": "mean",
                        "sum": "sum",
                        "min": "min",
                        "max": "max",
                        "count": "count",
                        "median": "median",
                    }
                    agg_func = agg_map.get(str(agg).lower(), "sum") if agg else "sum"
                    chart_df = (
                        chart_df.groupby([x], dropna=False, as_index=False)[y]
                        .agg(agg_func)
                        .rename(columns={y: value_col})
                    )
            else:
                chart_df = chart_df.dropna(subset=[x])
                if not chart_df.empty:
                    chart_df = (
                        chart_df.groupby([x], dropna=False, as_index=False)
                        .size()
                        .rename(columns={"size": value_col})
                    )
            if not chart_df.empty and value_col in chart_df.columns:
                chart_df[x] = chart_df[x].astype(str)
                chart_df = chart_df.sort_values(value_col, ascending=False).head(50)
                fig = px.treemap(
                    chart_df,
                    path=[x],
                    values=value_col,
                    color=value_col,
                    color_continuous_scale="Blues",
                )
        if fig is not None:
            fig.update_layout(margin=dict(l=24, r=24, t=24, b=24))
    elif chart_type == "hist" and x:
        fig = px.histogram(df, x=x, color=group)
    elif chart_type == "heatmap" and x and y:
        numeric_group = group if group and group in df.columns and pdt.is_numeric_dtype(df[group]) else None
        chart_df = _aggregate_two_dimensional(
            df,
            x,
            y,
            value_col=numeric_group,
            agg=agg,
        )
        if not chart_df.empty and "__value__" in chart_df.columns:
            axis_cap = max(8, min(40, max_categories * 2))
            top_x = (
                chart_df.groupby(x, dropna=False)["__value__"]
                .sum()
                .sort_values(ascending=False)
                .head(axis_cap)
                .index
                .astype(str)
                .tolist()
            )
            top_y = (
                chart_df.groupby(y, dropna=False)["__value__"]
                .sum()
                .sort_values(ascending=False)
                .head(axis_cap)
                .index
                .astype(str)
                .tolist()
            )
            filtered = chart_df.copy()
            filtered[x] = filtered[x].astype(str)
            filtered[y] = filtered[y].astype(str)
            filtered = filtered[filtered[x].isin(top_x) & filtered[y].isin(top_y)]
            if not filtered.empty:
                pivot = filtered.pivot_table(
                    index=y,
                    columns=x,
                    values="__value__",
                    aggfunc="sum",
                    fill_value=0,
                )
                fig = go.Figure(
                    data=
                    go.Heatmap(
                        x=[str(v) for v in pivot.columns.tolist()],
                        y=[str(v) for v in pivot.index.tolist()],
                        z=pivot.values,
                        colorscale="YlGnBu",
                        hoverongaps=False,
                        colorbar=dict(title=str(numeric_group or "count")),
                    )
                )
                fig.update_layout(
                    xaxis_title=str(x),
                    yaxis_title=str(y),
                )
    elif chart_type == "confusion_matrix" and x and y:
        numeric_group = group if group and group in df.columns and pdt.is_numeric_dtype(df[group]) else None
        chart_df = _aggregate_two_dimensional(
            df,
            x,
            y,
            value_col=numeric_group,
            agg=agg,
        )
        if not chart_df.empty and "__value__" in chart_df.columns:
            axis_cap = max(8, min(40, max_categories * 2))
            top_x = (
                chart_df.groupby(x, dropna=False)["__value__"]
                .sum()
                .sort_values(ascending=False)
                .head(axis_cap)
                .index
                .astype(str)
                .tolist()
            )
            top_y = (
                chart_df.groupby(y, dropna=False)["__value__"]
                .sum()
                .sort_values(ascending=False)
                .head(axis_cap)
                .index
                .astype(str)
                .tolist()
            )
            filtered = chart_df.copy()
            filtered[x] = filtered[x].astype(str)
            filtered[y] = filtered[y].astype(str)
            filtered = filtered[filtered[x].isin(top_x) & filtered[y].isin(top_y)]
            if not filtered.empty:
                pivot = filtered.pivot_table(
                    index=y,
                    columns=x,
                    values="__value__",
                    aggfunc="sum",
                    fill_value=0,
                )
                if not pivot.empty:
                    fig = go.Figure(
                        data=
                        go.Heatmap(
                            x=[str(v) for v in pivot.columns.tolist()],
                            y=[str(v) for v in pivot.index.tolist()],
                            z=pivot.values,
                            colorscale="Blues",
                            hoverongaps=False,
                            colorbar=dict(title=str(numeric_group or "count")),
                            hovertemplate=(
                                "Predicted=%{x}<br>"
                                "Actual=%{y}<br>"
                                "Value=%{z}<extra></extra>"
                            ),
                        )
                    )
                    fig.update_layout(
                        xaxis_title=f"Predicted ({x})",
                        yaxis_title=f"Actual ({y})",
                    )
    elif chart_type == "scatter" and x and y:
        fig = px.scatter(df, x=x, y=y, color=group)
    elif chart_type == "dynamic_scatter" and x and y:
        use_cols = [x, y]
        if group and group in df.columns:
            use_cols.append(group)
        if size and size in df.columns:
            use_cols.append(size)
        if animation_frame and animation_frame in df.columns:
            use_cols.append(animation_frame)
        chart_df = df[use_cols].copy()
        chart_df[x] = pd.to_numeric(chart_df[x], errors="coerce")
        chart_df[y] = pd.to_numeric(chart_df[y], errors="coerce")
        if size and size in chart_df.columns:
            chart_df[size] = pd.to_numeric(chart_df[size], errors="coerce")
        drop_subset = [x, y]
        if animation_frame and animation_frame in chart_df.columns:
            drop_subset.append(animation_frame)
        chart_df = chart_df.dropna(subset=drop_subset)
        if not chart_df.empty:
            scatter_kwargs: Dict[str, Any] = {
                "x": x,
                "y": y,
                "color": group if group and group in chart_df.columns else None,
                "size": size if size and size in chart_df.columns else None,
                "animation_frame": animation_frame if animation_frame and animation_frame in chart_df.columns else None,
                "size_max": 48,
            }
            scatter_kwargs = {k: v for k, v in scatter_kwargs.items() if v is not None}
            fig = px.scatter(chart_df, **scatter_kwargs)
            fig.update_traces(marker=dict(opacity=0.8, line=dict(width=0.6, color="white")))
            fig.update_layout(margin=dict(l=56, r=24, t=36, b=56))
    elif chart_type == "box" and x and y:
        fig = px.box(df, x=x, y=y, color=group)
    elif chart_type == "violin" and y:
        use_cols = [y]
        if x and x in df.columns:
            use_cols.append(x)
        if group and group in df.columns and group not in use_cols:
            use_cols.append(group)
        chart_df = df[use_cols].copy()
        chart_df[y] = pd.to_numeric(chart_df[y], errors="coerce")
        chart_df = chart_df.dropna(subset=[y])
        if not chart_df.empty:
            if x and x in chart_df.columns:
                fig = px.violin(chart_df, x=x, y=y, color=group if group in chart_df.columns else None)
            else:
                fig = px.violin(chart_df, y=y, color=group if group in chart_df.columns else None)
    elif chart_type == "pyramid" and x and y and group:
        # Preferred: x(category) + y(numeric) + group(category) -> mirrored population pyramid.
        if not pdt.is_numeric_dtype(df[group]):
            pyr_df = _aggregate_pyramid_frame(df, x, y, group, agg)
            if not pyr_df.empty:
                g_order = (
                    pyr_df.groupby(group, dropna=False)[y]
                    .sum()
                    .sort_values(ascending=False)
                    .index.tolist()
                )
                if len(g_order) >= 2:
                    left_name = g_order[0]
                    right_name = g_order[1]
                    pivot = (
                        pyr_df[pyr_df[group].isin([left_name, right_name])]
                        .pivot_table(index=x, columns=group, values=y, fill_value=0)
                        .reset_index()
                    )
                    if left_name in pivot.columns and right_name in pivot.columns:
                        fig = go.Figure()
                        fig.add_trace(
                            go.Bar(
                                y=pivot[x],
                                x=-pivot[left_name],
                                name=str(left_name),
                                orientation="h",
                                marker=dict(color=_PYRAMID_LEFT_COLOR),
                                hovertemplate=f"{x}: %{{y}}<br>{left_name}: %{{x:,.0f}}<extra></extra>",
                            )
                        )
                        fig.add_trace(
                            go.Bar(
                                y=pivot[x],
                                x=pivot[right_name],
                                name=str(right_name),
                                orientation="h",
                                marker=dict(color=_PYRAMID_RIGHT_COLOR),
                                hovertemplate=f"{x}: %{{y}}<br>{right_name}: %{{x:,.0f}}<extra></extra>",
                            )
                        )
                        fig.update_layout(
                            barmode="relative",
                            bargap=0.18,
                            xaxis=dict(
                                tickformat="~s",
                                zeroline=True,
                                zerolinecolor="rgba(15, 23, 42, 0.65)",
                                zerolinewidth=1.2,
                            ),
                            xaxis_title=f"{left_name} vs {right_name}",
                            yaxis_title=str(x),
                            annotations=[
                                dict(
                                    x=0.5,
                                    y=1.07,
                                    xref="paper",
                                    yref="paper",
                                    text=f"{left_name} \u2190 mirrored comparison \u2192 {right_name}",
                                    showarrow=False,
                                    font=dict(size=11, color="#475569"),
                                )
                            ],
                        )

        # Legacy fallback: x(category) + y(numeric_left) + group(numeric_right)
        if fig is None and pdt.is_numeric_dtype(df[group]):
            chart_df = _aggregate_frame(df, x, y, None, agg)
            right_df = _aggregate_frame(df, x, group, None, agg)
            merged = chart_df.merge(
                right_df, on=x, how="inner", suffixes=("_left", "_right")
            )
            if not merged.empty:
                fig = go.Figure()
                fig.add_trace(
                    go.Bar(
                        y=merged[x],
                        x=-merged[f"{y}_left"],
                        name=str(y),
                        orientation="h",
                        marker=dict(color=_PYRAMID_LEFT_COLOR),
                        hovertemplate=f"{x}: %{{y}}<br>{y}: %{{x:,.0f}}<extra></extra>",
                    )
                )
                fig.add_trace(
                    go.Bar(
                        y=merged[x],
                        x=merged[f"{group}_right"],
                        name=str(group),
                        orientation="h",
                        marker=dict(color=_PYRAMID_RIGHT_COLOR),
                        hovertemplate=f"{x}: %{{y}}<br>{group}: %{{x:,.0f}}<extra></extra>",
                    )
                )
                fig.update_layout(
                    barmode="relative",
                    bargap=0.18,
                    xaxis=dict(
                        tickformat="~s",
                        zeroline=True,
                        zerolinecolor="rgba(15, 23, 42, 0.65)",
                        zerolinewidth=1.2,
                    ),
                    xaxis_title=f"{y} vs {group}",
                    yaxis_title=str(x),
                    annotations=[
                        dict(
                            x=0.5,
                            y=1.07,
                            xref="paper",
                            yref="paper",
                            text=f"{y} \u2190 mirrored comparison \u2192 {group}",
                            showarrow=False,
                            font=dict(size=11, color="#475569"),
                        )
                    ],
                )
    elif chart_type in ("nested_pie", "sunburst") and x and group:
        chart_df = df[[x, group]].copy()
        value_col = "__value__"
        if y and y in df.columns and pdt.is_numeric_dtype(df[y]):
            chart_df[y] = pd.to_numeric(df[y], errors="coerce")
            chart_df = chart_df.dropna(subset=[x, group, y])
            if not chart_df.empty:
                agg_map = {
                    "avg": "mean",
                    "mean": "mean",
                    "sum": "sum",
                    "min": "min",
                    "max": "max",
                    "count": "count",
                    "median": "median",
                }
                agg_func = agg_map.get(str(agg).lower(), "sum") if agg else "sum"
                chart_df = (
                    chart_df.groupby([x, group], dropna=False, as_index=False)[y]
                    .agg(agg_func)
                    .rename(columns={y: value_col})
                )
        else:
            chart_df = chart_df.dropna(subset=[x, group])
            if not chart_df.empty:
                chart_df = (
                    chart_df.groupby([x, group], dropna=False, as_index=False)
                    .size()
                    .rename(columns={"size": value_col})
                )
        if not chart_df.empty and value_col in chart_df.columns:
            # Donut-of-donut style:
            # inner ring: parent(x), outer ring: child(group) split by parent
            inner_df = (
                chart_df.groupby([x], dropna=False, as_index=False)[value_col]
                .sum()
                .sort_values(value_col, ascending=False)
                .reset_index(drop=True)
            )
            if not inner_df.empty:
                parent_order = inner_df[x].astype(str).tolist()
                parent_rank = {k: i for i, k in enumerate(parent_order)}
                outer_df = chart_df.copy()
                outer_df[x] = outer_df[x].astype(str)
                outer_df[group] = outer_df[group].astype(str)
                outer_df["__parent_rank__"] = outer_df[x].map(parent_rank).fillna(999999)
                outer_df = outer_df.sort_values(
                    by=["__parent_rank__", value_col],
                    ascending=[True, False],
                ).reset_index(drop=True)

                palette = px.colors.qualitative.Plotly
                parent_colors: Dict[str, str] = {}
                for idx, parent_name in enumerate(parent_order):
                    parent_colors[parent_name] = palette[idx % len(palette)]

                # Keep parent hue on outer slices and vary opacity by within-parent rank.
                outer_colors: list[str] = []
                for parent_name, grp in outer_df.groupby(x, sort=False):
                    base = parent_colors.get(str(parent_name), "#3b82f6")
                    n = max(len(grp), 1)
                    for i in range(len(grp)):
                        alpha = 0.95 - (0.55 * (i / max(n - 1, 1)))
                        outer_colors.append(_rgba(base, alpha))

                fig = go.Figure()
                fig.add_trace(
                    go.Pie(
                        labels=inner_df[x].astype(str),
                        values=inner_df[value_col],
                        # Shrink inner pie domain to avoid overlap with the outer ring.
                        domain=dict(x=[0.18, 0.82], y=[0.18, 0.82]),
                        hole=0.42,
                        sort=False,
                        textinfo="label+percent",
                        textposition="inside",
                        insidetextorientation="horizontal",
                        marker=dict(
                            colors=[parent_colors[name] for name in inner_df[x].astype(str)],
                            line=dict(color="white", width=2),
                        ),
                        showlegend=True,
                        name=str(x),
                        hovertemplate=f"{x}: %{{label}}<br>value=%{{value}}<br>share=%{{percent}}<extra></extra>",
                    )
                )
                fig.add_trace(
                    go.Pie(
                        labels=outer_df[group].astype(str),
                        values=outer_df[value_col],
                        hole=0.72,
                        sort=False,
                        textinfo="label+percent",
                        textposition="outside",
                        texttemplate="%{label}<br>%{percent}",
                        automargin=True,
                        marker=dict(
                            colors=outer_colors,
                            line=dict(color="white", width=1.5),
                        ),
                        outsidetextfont=dict(size=11),
                        showlegend=False,
                        name=str(group),
                        customdata=outer_df[[x, group]].values,
                        hovertemplate=(
                            f"{x}: %{{customdata[0]}}<br>"
                            f"{group}: %{{customdata[1]}}<br>"
                            "value=%{value}<br>"
                            "share=%{percent}<extra></extra>"
                        ),
                    )
                )
                total_value = float(inner_df[value_col].sum())
                center_text = f"{x}<br>{total_value:,.0f}"
                fig.update_layout(
                    annotations=[
                        dict(
                            text=center_text,
                            x=0.5,
                            y=0.5,
                            showarrow=False,
                            font=dict(size=13),
                            align="center",
                        )
                    ],
                    uniformtext=dict(minsize=9, mode="hide"),
                    margin=dict(l=56, r=56, t=28, b=28),
                )

    if fig is None:
        log_event("codegen.noop", {"chart_type": chart_type})
        return {"figure_json": None, "code": _build_code(chart_spec), "image_data_url": None, "render_engine": None}

    _apply_visual_polish(fig, str(chart_type or ""))
    has_secondary_group = bool(
        isinstance(secondary_group, str)
        and secondary_group
        and secondary_group in df.columns
    )
    image_data_url = None if has_secondary_group else _generate_seaborn_image(chart_spec, df)
    render_engine = "seaborn" if image_data_url else "plotly"

    # Numpy types in figure JSON can break Pydantic serialization
    fig_json = json.loads(pio.to_json(fig))
    log_event(
        "codegen.success",
        {
            "chart_type": chart_type,
            "render_engine": render_engine,
            "has_image_data_url": bool(image_data_url),
        },
    )

    return {
        "figure_json": fig_json,
        "image_data_url": image_data_url,
        "render_engine": render_engine,
        "code": _build_code(chart_spec),
    }
