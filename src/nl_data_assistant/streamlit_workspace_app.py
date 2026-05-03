"""
Compatibility wrapper for the legacy workspace module.

The project previously had two separate Streamlit UIs:
- `streamlit_workspace_app.py`
- `streamlit_app.py`

That split caused the app entrypoint to launch an older interface, which made
newer options such as "Build a table", the SQL editor, and the restored table
editor appear to vanish.

To keep imports stable and ensure there is only one active UI implementation,
this module now delegates to `streamlit_app.py`.
"""

from __future__ import annotations

from nl_data_assistant.streamlit_app import run_streamlit_app

__all__ = ["run_streamlit_app"]