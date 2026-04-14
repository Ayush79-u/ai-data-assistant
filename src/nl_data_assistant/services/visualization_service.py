from __future__ import annotations

from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from nl_data_assistant.utils.cleaning import normalize_identifier


class VisualizationService:
    def create_chart(
        self,
        dataframe: pd.DataFrame,
        chart_type: str,
        output_dir: str | Path,
        x_column: str | None = None,
        y_column: str | None = None,
        title: str | None = None,
    ) -> tuple[go.Figure, Path, dict[str, str | None]]:
        if dataframe.empty:
            raise ValueError("There is no data available to visualize.")

        chart_type = (chart_type or "bar").lower()
        x_column, y_column = self._infer_axes(dataframe, chart_type, x_column, y_column)

        if chart_type == "bar":
            figure = px.bar(dataframe, x=x_column, y=y_column, title=title or "Bar Chart")
        elif chart_type == "line":
            figure = px.line(dataframe, x=x_column, y=y_column, title=title or "Line Chart")
        elif chart_type == "pie":
            figure = px.pie(dataframe, names=x_column, values=y_column, title=title or "Pie Chart")
        elif chart_type == "scatter":
            figure = px.scatter(dataframe, x=x_column, y=y_column, title=title or "Scatter Plot")
        elif chart_type == "histogram":
            figure = px.histogram(dataframe, x=x_column, title=title or "Histogram")
        elif chart_type == "dashboard":
            figure = self._build_dashboard(dataframe, title=title or "Dashboard")
        else:
            figure = px.bar(dataframe, x=x_column, y=y_column, title=title or "Chart")

        output_path = Path(output_dir).resolve() / f"{normalize_identifier(title or chart_type)}.html"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        figure.write_html(output_path)
        return figure, output_path, {"x_column": x_column, "y_column": y_column}

    def _infer_axes(
        self,
        dataframe: pd.DataFrame,
        chart_type: str,
        x_column: str | None,
        y_column: str | None,
    ) -> tuple[str | None, str | None]:
        numeric_columns = dataframe.select_dtypes(include="number").columns.tolist()
        non_numeric_columns = [column for column in dataframe.columns if column not in numeric_columns]

        if chart_type == "histogram":
            return x_column or (numeric_columns[0] if numeric_columns else dataframe.columns[0]), y_column

        inferred_x = x_column or (non_numeric_columns[0] if non_numeric_columns else dataframe.columns[0])
        inferred_y = y_column or (numeric_columns[0] if numeric_columns else None)

        if inferred_y is None and len(dataframe.columns) > 1:
            inferred_y = dataframe.columns[1]

        return inferred_x, inferred_y

    def _build_dashboard(self, dataframe: pd.DataFrame, title: str) -> go.Figure:
        numeric_columns = dataframe.select_dtypes(include="number").columns.tolist()
        category_columns = [column for column in dataframe.columns if column not in numeric_columns]

        figure = make_subplots(
            rows=2,
            cols=2,
            specs=[[{"type": "indicator"}, {"type": "histogram"}], [{"type": "bar"}, {"type": "box"}]],
            subplot_titles=("Row Count", "Distribution", "Top Categories", "Spread"),
        )

        figure.add_trace(
            go.Indicator(mode="number", value=len(dataframe), title={"text": "Rows"}),
            row=1,
            col=1,
        )

        if numeric_columns:
            main_numeric = numeric_columns[0]
            figure.add_trace(go.Histogram(x=dataframe[main_numeric], name=main_numeric), row=1, col=2)
            figure.add_trace(go.Box(y=dataframe[main_numeric], name=main_numeric), row=2, col=2)

        if category_columns:
            main_category = category_columns[0]
            counts = dataframe[main_category].astype(str).value_counts().head(10)
            figure.add_trace(go.Bar(x=counts.index, y=counts.values, name=main_category), row=2, col=1)

        figure.update_layout(height=700, title=title, showlegend=False)
        return figure

