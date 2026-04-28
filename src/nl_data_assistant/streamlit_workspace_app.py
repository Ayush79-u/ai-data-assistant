"""
streamlit_workspace_app.py

What changed from the old version:
- SQL Console pane is GONE — users type English, not SQL
- Assistant Chat is now the main/only input area
- Light / Dark mode toggle in the header
- MySQL Explorer sidebar kept (databases, tables, schema)
- All features preserved: NL→SQL execution, table preview, query history,
  Excel import/export, destructive confirmation, save chat
"""
from __future__ import annotations

import io
import tempfile
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

from nl_data_assistant.models import ExecutionResult, Intent
from nl_data_assistant.services.engine import DataAssistantEngine


# ── Themes ────────────────────────────────────────────────────────────────────

_DARK = {
    "--bg":          "#1a1a2e",
    "--bg2":         "#16213e",
    "--bg3":         "#0f3460",
    "--card":        "#1e2a45",
    "--border":      "#2d3f5e",
    "--text":        "#e0e6f0",
    "--text2":       "#8b9dc3",
    "--accent":      "#e94560",
    "--accent2":     "#0f7df7",
    "--success":     "#2ecc71",
    "--warning":     "#f39c12",
    "--user-bubble": "#0f7df7",
    "--bot-bubble":  "#1e2a45",
    "--bot-text":    "#e0e6f0",
    "--shadow":      "rgba(0,0,0,0.4)",
}

_LIGHT = {
    "--bg":          "#f5f7fa",
    "--bg2":         "#ffffff",
    "--bg3":         "#e8edf5",
    "--card":        "#ffffff",
    "--border":      "#dde3ee",
    "--text":        "#1a2033",
    "--text2":       "#5a6580",
    "--accent":      "#d63031",
    "--accent2":     "#0984e3",
    "--success":     "#00b894",
    "--warning":     "#e17055",
    "--user-bubble": "#0984e3",
    "--bot-bubble":  "#f0f3f8",
    "--bot-text":    "#1a2033",
    "--shadow":      "rgba(0,0,0,0.08)",
}


def _css_vars(theme: dict) -> str:
    return ":root {" + "".join(f"{k}:{v};" for k, v in theme.items()) + "}"


def _inject_css(theme: dict) -> None:
    st.markdown(
        f"""
<style>
{_css_vars(theme)}

/* ── global ── */
html, body, [class*="css"] {{
    background-color: var(--bg) !important;
    color: var(--text) !important;
    font-family: 'Segoe UI', system-ui, sans-serif;
}}
#MainMenu, footer, header {{ visibility: hidden; }}

/* ── sidebar ── */
[data-testid="stSidebar"] {{
    background-color: var(--bg2) !important;
    border-right: 1px solid var(--border);
}}
[data-testid="stSidebar"] .block-container {{ padding-top: 1.2rem; }}

/* ── buttons ── */
.stButton > button {{
    background: var(--bg3);
    color: var(--text);
    border: 1px solid var(--border);
    border-radius: 8px;
    font-size: 13px;
    transition: all .15s;
}}
.stButton > button:hover {{
    background: var(--accent2);
    color: #fff;
    border-color: var(--accent2);
}}
[data-testid="stDownloadButton"] > button {{
    border-radius: 8px; font-size: 13px;
}}

/* ── chat bubbles ── */
.bubble-user {{
    background: var(--user-bubble);
    color: #fff;
    padding: 10px 16px;
    border-radius: 18px 18px 4px 18px;
    margin: 6px 0 6px auto;
    max-width: 78%;
    font-size: 14.5px;
    line-height: 1.5;
    box-shadow: 0 2px 8px var(--shadow);
    word-wrap: break-word;
}}
.bubble-bot {{
    background: var(--bot-bubble);
    color: var(--bot-text);
    padding: 10px 16px;
    border-radius: 18px 18px 18px 4px;
    margin: 6px auto 6px 0;
    max-width: 85%;
    font-size: 14.5px;
    line-height: 1.5;
    border: 1px solid var(--border);
    box-shadow: 0 2px 8px var(--shadow);
    word-wrap: break-word;
}}
.bubble-error {{
    background: #fff0f0;
    color: #c0392b;
    border: 1px solid #f5c6cb;
    padding: 10px 16px;
    border-radius: 10px;
    font-size: 13.5px;
    margin: 4px 0;
}}

/* ── cards ── */
.stat-card {{
    background: var(--card);
    border: 1px solid var(--border);
    border-radius: 12px;
    padding: 12px 16px;
    margin-bottom: 8px;
}}

/* ── inputs ── */
[data-testid="stTextInput"] input,
[data-testid="stSelectbox"] select,
textarea {{
    background: var(--bg3) !important;
    color: var(--text) !important;
    border-color: var(--border) !important;
    border-radius: 8px !important;
}}

/* ── expanders ── */
[data-testid="stExpander"] {{
    background: var(--card);
    border: 1px solid var(--border);
    border-radius: 10px;
}}

/* ── dataframe ── */
[data-testid="stDataFrame"] {{ border-radius: 10px; overflow: hidden; }}

/* ── chat input ── */
[data-testid="stChatInput"] textarea {{
    background: var(--bg3) !important;
    color: var(--text) !important;
    border: 1.5px solid var(--border) !important;
    border-radius: 12px !important;
}}
</style>
        """,
        unsafe_allow_html=True,
    )


# ── Entry point ───────────────────────────────────────────────────────────────

def run_streamlit_app() -> None:
    st.set_page_config(
        page_title="Data Assistant",
        page_icon="🧠",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    _init_session()
    _inject_css(_DARK if st.session_state.dark_mode else _LIGHT)
    _render_header()
    sidebar_col, main_col = st.columns([1, 3], gap="large")
    with sidebar_col:
        _render_sidebar()
    with main_col:
        _render_chat_area()
        _render_table_editor()
        _render_chat_input()


# ── Session ───────────────────────────────────────────────────────────────────

def _init_session() -> None:
    defaults: dict = {
        "engine":               None,
        "dark_mode":            True,
        "chat":                 [],       # {role, text, result, ts}
        "pending_plan":         None,
        "query_log":            [],
        "current_table":        "",
        "current_db":           "",
        "table_editor_df":      pd.DataFrame(),
        "table_editor_table":   "",
        "table_editor_version": 0,
        "prefill":              "",
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v

    # Lazy-init engine after config is done
    if st.session_state.engine is None:
        st.session_state.engine = DataAssistantEngine()


def _eng() -> DataAssistantEngine:
    return st.session_state.engine


# ── Header ────────────────────────────────────────────────────────────────────

def _render_header() -> None:
    c_title, c_gap, c_theme, c_save, c_clear = st.columns([5, 2, 1.2, 1.4, 1])

    with c_title:
        st.markdown(
            "<h2 style='margin:0;padding:0'>🧠 Data Assistant</h2>"
            "<p style='margin:0;font-size:13px;color:var(--text2)'>"
            "Type in plain English — I'll convert it to SQL and run it.</p>",
            unsafe_allow_html=True,
        )

    with c_gap:
        if _eng().mysql.ping():
            tables = _eng().mysql.get_table_names()
            st.success(
                f"✅ Connected · {len(tables)} table{'s' if len(tables) != 1 else ''}",
            )
        else:
            st.error("MySQL unreachable")
            st.stop()

    with c_theme:
        label = "☀️ Light" if st.session_state.dark_mode else "🌙 Dark"
        if st.button(label, use_container_width=True):
            st.session_state.dark_mode = not st.session_state.dark_mode
            st.rerun()

    with c_save:
        if st.session_state.chat:
            st.download_button(
                "💾 Save chat",
                data=_export_chat(),
                file_name=f"chat_{datetime.now().strftime('%Y%m%d_%H%M')}.txt",
                mime="text/plain",
                use_container_width=True,
            )

    with c_clear:
        if st.button("🗑️ Clear", use_container_width=True):
            _clear_all()
            st.rerun()

    st.divider()


# ── Sidebar ───────────────────────────────────────────────────────────────────

def _render_sidebar() -> None:
    # ── Connection status ─────────────────────────────────────────────────────
    st.markdown("### 🗄️ MySQL Explorer")

    # ── Tables list ───────────────────────────────────────────────────────────
    tables = _eng().mysql.get_table_names()
    st.markdown(
        f"<div class='stat-card'><b>Tables</b> &nbsp;"
        f"<span style='color:var(--text2)'>{len(tables)} found</span></div>",
        unsafe_allow_html=True,
    )

    if tables:
        for t in tables:
            active = t == st.session_state.current_table
            label = f"{'▶ ' if active else '  '}📁 {t}"
            if st.button(label, use_container_width=True, key=f"tbl_{t}"):
                st.session_state.current_table = t
                _load_table_editor(t)
                st.session_state.prefill = f"Show all {t}"
                st.rerun()
        if st.button("🔄 Refresh tables", use_container_width=True):
            st.rerun()
    else:
        st.caption("No tables yet — ask me to create one!")

    st.divider()

    # ── Quick example prompts ─────────────────────────────────────────────────
    st.markdown("#### 💡 Example commands")
    examples = [
        ("📋 List tables",            "Show all tables"),
        ("🏗️ Create table",          "Create a students table with name, cgpa, and branch"),
        ("➕ Insert rows",            "Insert 5 students with random data"),
        ("🔍 Query",                  "Show all students ordered by cgpa descending"),
        ("📊 Chart",                  "Show me a bar chart of students by cgpa"),
        ("🗑️ Delete",                "Delete students with cgpa less than 6"),
        ("📝 Describe",              "Describe the schema of students"),
        ("📤 Export to Excel",        "Export students to Excel"),
    ]
    for label, cmd in examples:
        if st.button(label, use_container_width=True, key=f"ex_{cmd}"):
            st.session_state.prefill = cmd
            st.rerun()

    st.divider()

    # ── Excel upload ──────────────────────────────────────────────────────────
    st.markdown("#### 📂 Excel")
    uploaded = st.file_uploader(
        "Upload Excel",
        type=["xlsx", "xls"],
        label_visibility="collapsed",
        key="excel_upload",
    )
    if uploaded:
        tmp = Path(tempfile.gettempdir()) / uploaded.name
        tmp.write_bytes(uploaded.read())
        excel_svc = _eng().excel
        sheets = excel_svc.list_sheets(tmp)
        sheet = st.selectbox("Sheet", sheets, key="xl_sheet")
        df = excel_svc.read_sheet(tmp, sheet)
        st.caption(f"{len(df)} rows · {len(df.columns)} cols")

        with st.expander("Preview", expanded=False):
            st.dataframe(df.head(6), hide_index=True, use_container_width=True)

        buf = io.BytesIO()
        df.to_excel(buf, index=False)
        st.download_button(
            "⬇️ Download",
            data=buf.getvalue(),
            file_name=f"{Path(uploaded.name).stem}_{sheet}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )
        tbl_name = st.text_input("Import to MySQL as", value=sheet.lower(), key="xl_tbl")
        if st.button("⬆️ Import to MySQL", type="primary", use_container_width=True):
            with st.spinner("Importing…"):
                n = _eng().sync.excel_to_mysql(tmp, tbl_name, sheet)
            st.success(f"Imported {n} rows → `{tbl_name}`")
            _add("assistant",
                 f"Imported **{n} rows** from `{uploaded.name}` into MySQL table `{tbl_name}`.")
            st.rerun()

    if tables:
        with st.expander("Export table → Excel", expanded=False):
            tbl = st.selectbox("Table", tables, key="exp_tbl")
            if st.button("⬇️ Export", use_container_width=True):
                out = Path(tempfile.gettempdir()) / f"{tbl}_export.xlsx"
                _eng().sync.mysql_to_excel(tbl, out)
                st.download_button(
                    f"Download {tbl}.xlsx",
                    data=out.read_bytes(),
                    file_name=f"{tbl}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                )

    st.divider()

    # ── Query history ─────────────────────────────────────────────────────────
    log = st.session_state.query_log
    if log:
        with st.expander(f"📜 History ({len(log)})", expanded=False):
            dump = "\n\n".join(
                f"-- {e['ts']} {'OK' if e['ok'] else 'FAIL'}\n{e['sql']}" for e in log
            )
            st.download_button(
                "⬇️ Download .sql",
                data=dump,
                file_name="queries.sql",
                mime="text/plain",
                use_container_width=True,
            )
            for e in reversed(log[-15:]):
                icon = "✅" if e["ok"] else "❌"
                with st.expander(f"{icon} {e['ts']} — {e['sql'][:40]}…", expanded=False):
                    st.code(e["sql"], language="sql")
                    ca, cb = st.columns(2)
                    if cb.button("▶️ Re-run", key=f"rr_{e['ts']}{e['sql'][:6]}",
                                 use_container_width=True):
                        r = _eng().execute_raw(e["sql"])
                        _log(r)
                        _add("assistant", _friendly(r), r)
                        st.rerun()


# ── Chat area ─────────────────────────────────────────────────────────────────

def _render_chat_area() -> None:
    import plotly.graph_objects as go

    if not st.session_state.chat:
        st.markdown(
            """
<div style="text-align:center;padding:60px 0 30px;">
    <div style="font-size:56px">🗄️</div>
    <div style="font-size:22px;font-weight:700;margin-top:12px">
        Ask me anything about your data
    </div>
    <div style="font-size:14px;margin-top:8px;color:var(--text2)">
        Type naturally below — "show all students" · "create a sales table" · "bar chart of expenses"
    </div>
</div>
            """,
            unsafe_allow_html=True,
        )
        return

    for turn in st.session_state.chat:
        role = turn["role"]
        text = turn["text"]
        result: ExecutionResult | None = turn.get("result")

        if role == "user":
            st.markdown(f'<div class="bubble-user">{text}</div>', unsafe_allow_html=True)
        else:
            if turn.get("error"):
                st.markdown(
                    f'<div class="bubble-error">⚠️ {turn["error"]}</div>',
                    unsafe_allow_html=True,
                )
            else:
                st.markdown(f'<div class="bubble-bot">{text}</div>', unsafe_allow_html=True)

            # SQL (collapsed — not scary for normal users)
            if result and result.sql_executed:
                with st.expander("🔍 See the SQL that ran", expanded=False):
                    st.code(result.sql_executed, language="sql")

            # Data / chart
            if result and result.data is not None:
                if isinstance(result.data, pd.DataFrame) and not result.data.empty:
                    st.dataframe(result.data, use_container_width=True, hide_index=True)
                    buf = io.BytesIO()
                    result.data.to_excel(buf, index=False)
                    st.download_button(
                        "⬇️ Download result as Excel",
                        data=buf.getvalue(),
                        file_name="result.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key=f"dl_{turn['ts']}_{id(result)}",
                    )
                elif isinstance(result.data, go.Figure):
                    st.plotly_chart(result.data, use_container_width=True)

            if result and result.error:
                st.error(result.error)

    # Destructive confirmation
    if st.session_state.pending_plan is not None:
        plan = st.session_state.pending_plan
        st.warning(
            f"⚠️ This will **{plan.intent.value.upper()}** on `{plan.table_name}`. "
            "This can't be undone — are you sure?",
            icon="⚠️",
        )
        cy, cn = st.columns(2)
        if cy.button("✅ Yes, go ahead", type="primary", use_container_width=True):
            result = _eng().execute(plan)
            _handle(plan, result)
            st.session_state.pending_plan = None
            st.rerun()
        if cn.button("❌ Cancel", use_container_width=True):
            _add("assistant", "OK, cancelled. Nothing was changed.")
            st.session_state.pending_plan = None
            st.rerun()


# ── Table editor ──────────────────────────────────────────────────────────────

def _render_table_editor() -> None:
    current = st.session_state.current_table
    with st.expander(
        f"📝 Table Editor{f'  —  `{current}`' if current else ''}",
        expanded=bool(current),
    ):
        if not current:
            st.info("Click a table on the left to open it here.")
            return

        if st.session_state.table_editor_table != current:
            _load_table_editor(current)

        cr, cs = st.columns(2)
        if cr.button("🔄 Reload", use_container_width=True):
            _load_table_editor(current)
            st.rerun()

        key = f"te_{current}_{st.session_state.table_editor_version}"
        edited = st.data_editor(
            st.session_state.table_editor_df,
            key=key,
            num_rows="dynamic",
            use_container_width=True,
        )
        st.session_state.table_editor_df = edited

        if cs.button("💾 Save to MySQL", type="primary", use_container_width=True):
            r = _eng().mysql.replace_table_data(current, edited)
            _log(r)
            if r.success:
                _add("assistant", f"Saved edits to `{current}` ✔", r)
                _load_table_editor(current)
                st.rerun()
            else:
                st.error(r.error or "Save failed.")


# ── Chat input ────────────────────────────────────────────────────────────────

def _render_chat_input() -> None:
    prefill = st.session_state.pop("prefill", "") or ""
    blocked = st.session_state.pending_plan is not None

    prompt = st.chat_input(
        "Type in plain English — e.g. 'show all students' or 'create a products table'",
        disabled=blocked,
    )

    if prefill and not blocked:
        _process(prefill)
        st.rerun()
    if prompt and not blocked:
        _process(prompt)
        st.rerun()


# ── Command processing ────────────────────────────────────────────────────────

def _process(command: str) -> None:
    _add("user", command)
    with st.spinner("Thinking…"):
        try:
            plan = _eng().parse(command, default_table=st.session_state.current_table)
        except TypeError:
            plan = _eng().parse(command)

    if plan.is_destructive:
        st.session_state.pending_plan = plan
        _add(
            "assistant",
            f"Just checking — this will **{plan.intent.value.replace('_', ' ')}** "
            f"on `{plan.table_name or st.session_state.current_table}`. "
            "Confirm below if you want to go ahead.",
        )
        return

    with st.spinner("Running…"):
        result = _eng().execute(plan)
    _handle(plan, result)


def _handle(plan, result: ExecutionResult) -> None:
    _log(result)
    _add("assistant", _friendly(result), result)

    if not result.success:
        return
    if plan.table_name:
        st.session_state.current_table = plan.table_name
    if plan.intent in {Intent.SELECT, Intent.INSERT, Intent.UPDATE,
                       Intent.DELETE, Intent.DESCRIBE, Intent.CREATE_TABLE}:
        if st.session_state.current_table:
            try:
                _load_table_editor(st.session_state.current_table)
            except Exception:
                pass


def _friendly(result: ExecutionResult) -> str:
    import plotly.graph_objects as go
    if not result.success:
        return f"Something went wrong: {result.error or result.message}"
    if isinstance(result.data, pd.DataFrame):
        n = len(result.data)
        if n == 0:
            return "Query ran OK, but no rows matched — try a different filter?"
        return f"Here you go — {n} row{'s' if n != 1 else ''} found."
    if isinstance(result.data, go.Figure):
        return "Here's your chart! 📊"
    if result.rows_affected:
        return f"Done! {result.rows_affected} row{'s' if result.rows_affected != 1 else ''} affected."
    return result.message or "Done! ✔"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _add(role: str, text: str, result: ExecutionResult | None = None,
         error: str = "") -> None:
    st.session_state.chat.append({
        "role": role,
        "text": text,
        "result": result,
        "error": error,
        "ts": datetime.now().strftime("%H:%M:%S"),
    })


def _log(result: ExecutionResult) -> None:
    if result.sql_executed:
        st.session_state.query_log.append({
            "ts": datetime.now().strftime("%H:%M:%S"),
            "sql": result.sql_executed,
            "ok": result.success,
        })


def _load_table_editor(table_name: str) -> None:
    df = _eng().mysql.fetch_table(table_name)
    st.session_state.table_editor_table = table_name
    st.session_state.table_editor_df = df.copy()
    st.session_state.table_editor_version += 1
    st.session_state.current_table = table_name


def _export_chat() -> str:
    lines = [f"Chat — {datetime.now().strftime('%Y-%m-%d %H:%M')}", "=" * 50, ""]
    for t in st.session_state.chat:
        who = "You" if t["role"] == "user" else "Assistant"
        lines.append(f"[{t.get('ts','')}] {who}: {t['text']}")
        r = t.get("result")
        if r and r.sql_executed:
            lines.append(f"  SQL: {r.sql_executed}")
        lines.append("")
    return "\n".join(lines)


def _clear_all() -> None:
    st.session_state.chat.clear()
    st.session_state.pending_plan = None
    st.session_state.query_log.clear()
    _eng().clear_history()