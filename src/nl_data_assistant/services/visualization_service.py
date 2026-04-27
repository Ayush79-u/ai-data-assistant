"""
visualization_service.py — Plotly chart generation from DataFrames.
"""
from __future__ import annotations

import logging

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

log = logging.getLogger(__name__)

_TEMPLATE = "plotly_white"
_PALETTE = px.colors.qualitative.Set2

# Map chart-type aliases to canonical names
_ALIASES: dict[str, str] = {
    "bar": "bar", "column": "bar",
    "line": "line", "trend": "line",
    "pie": "pie", "donut": "pie",
    "scatter": "scatter", "dot": "scatter",
    "hist": "histogram", "histogram": "histogram",
    "dashboard": "dashboard",
}


class VisualizationService:
    def plot(
        self,
        df: pd.DataFrame,
        chart_type: str = "bar",
        title: str = "",
    ) -> go.Figure:
        """
        Auto-pick X / Y axes from the DataFrame and return a Plotly figure.
        """
        if df is None or df.empty:
            return self._empty_fig("No data to display.")

        chart = _ALIASES.get(chart_type.lower(), "bar")
        x_col, y_col = self._pick_axes(df)

        try:
            if chart == "bar":
                fig = px.bar(df, x=x_col, y=y_col, title=title,
                             color=x_col, color_discrete_sequence=_PALETTE)
            elif chart == "line":
                fig = px.line(df, x=x_col, y=y_col, title=title, markers=True)
            elif chart == "pie":
                fig = px.pie(df, names=x_col, values=y_col, title=title,
                             color_discrete_sequence=_PALETTE)
            elif chart == "scatter":
                fig = px.scatter(df, x=x_col, y=y_col, title=title,
                                 trendline="ols" if len(df) > 5 else None)
            elif chart == "histogram":
                fig = px.histogram(df, x=y_col, title=title,
                                   nbins=min(30, max(5, len(df) // 5)))
            elif chart == "dashboard":
                return self.dashboard(df, title)
            else:
                fig = px.bar(df, x=x_col, y=y_col, title=title)

        except Exception as exc:
            log.warning("Chart error (%s), falling back to table view.", exc)
            return self._table_fig(df, title)

        fig.update_layout(
            template=_TEMPLATE,
            title_font_size=16,
            legend_title_text="",
            margin={"t": 60, "b": 40, "l": 40, "r": 20},
        )
        return fig

    def dashboard(self, df: pd.DataFrame, title: str = "Dashboard") -> go.Figure:
        """Multi-panel dashboard: bar + line + histogram of all numeric cols."""
        num_cols = df.select_dtypes("number").columns.tolist()
        cat_cols = df.select_dtypes(exclude="number").columns.tolist()

        if not num_cols:
            return self._empty_fig("No numeric columns for dashboard.")

        rows, cols_count = 2, 2
        fig = make_subplots(
            rows=rows, cols=cols_count,
            subplot_titles=["Distribution", "Trend", "Histogram", "Summary"],
        )

        x = cat_cols[0] if cat_cols else df.index.astype(str)
        y = num_cols[0]

        fig.add_trace(go.Bar(x=df[x], y=df[y], name=y, marker_color="#4C78A8"), row=1, col=1)
        fig.add_trace(go.Scatter(x=df[x], y=df[y], mode="lines+markers", name=y), row=1, col=2)
        fig.add_trace(go.Histogram(x=df[y], name=y, marker_color="#72B7B2"), row=2, col=1)

        if len(num_cols) >= 2:
            fig.add_trace(go.Scatter(
                x=df[num_cols[0]], y=df[num_cols[1]],
                mode="markers", name=f"{num_cols[0]} vs {num_cols[1]}",
            ), row=2, col=2)

        fig.update_layout(
            title_text=title,
            template=_TEMPLATE,
            showlegend=False,
            height=600,
        )
        return fig

    # ── Helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _pick_axes(df: pd.DataFrame) -> tuple[str, str]:
        """Heuristically pick the best X (categorical) and Y (numeric) columns."""
        num_cols = df.select_dtypes("number").columns.tolist()
        cat_cols = df.select_dtypes(exclude="number").columns.tolist()

        y_col = num_cols[0] if num_cols else df.columns[-1]
        x_col = cat_cols[0] if cat_cols else (
            num_cols[1] if len(num_cols) > 1 else df.columns[0]
        )
        return x_col, y_col

    @staticmethod
    def _empty_fig(message: str) -> go.Figure:
        fig = go.Figure()
        fig.add_annotation(text=message, x=0.5, y=0.5, showarrow=False,
                           font={"size": 16}, xref="paper", yref="paper")
        fig.update_layout(template=_TEMPLATE)
        return fig

    @staticmethod
    def _table_fig(df: pd.DataFrame, title: str) -> go.Figure:
        fig = go.Figure(data=[go.Table(
            header={"values": list(df.columns), "fill_color": "#4C78A8",
                    "font": {"color": "white"}, "align": "left"},
            cells={"values": [df[c].tolist() for c in df.columns],
                   "fill_color": "lavender", "align": "left"},
        )])
        fig.update_layout(title_text=title, template=_TEMPLATE)
        return fig
