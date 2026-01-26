import streamlit as st
from core.db_connector import execute_query
from core.sql_agent import generate_sql

# ---------------------- PAGE CONFIG & SIDEBAR ---------------------- #

st.set_page_config(page_title="DataBuddy – NL→SQL", layout="wide")
st.title("DataBuddy – Databricks System Tables Assistant")


# --- Sample questions (plain text under heading) ---
st.markdown(
    """
**Sample questions:**
- Which workspaces are currently running?
- jobs related information like top 10
- dbu usage in last 7 days

"""
)


# Sidebar: per-user context (different in each browser -> different user)
st.sidebar.header("User context")
user_name = st.sidebar.text_input("User name", value="user_a")
workspace_id = st.sidebar.text_input("Workspace ID", value="ws_1")
st.sidebar.markdown(f"**Current user:** `{user_name}`")
st.sidebar.markdown(f"**Workspace:** `{workspace_id}`")

tab_read, = st.tabs(["Chat"])


# --------- Helper: render chart for a given df and base key --------- #
def render_quick_visualisation(df, base_key: str):
    """
    Render a 'Quick visualisation' block for a given DataFrame.
    Handles both numeric and non-numeric datasets.

    base_key must be unique per message (e.g. f"msg_{i}").
    """
    if df is None or df.empty:
        st.info("No data to plot.")
        return

    st.markdown("**Quick visualisation**")

    all_cols = df.columns.tolist()
    numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()

    # Case 1: at least one numeric column – let user pick X, Y
    if numeric_cols:
        x_col = st.selectbox(
            "X axis",
            all_cols,
            index=0,
            key=f"{base_key}_x",
        )
        y_col = st.selectbox(
            "Y axis (numeric)",
            numeric_cols,
            index=0,
            key=f"{base_key}_y",
        )
        chart_type = st.selectbox(
            "Chart type",
            ["Bar", "Line", "Area"],
            key=f"{base_key}_type",
        )

        # If X != Y, plot Y vs X
        if x_col != y_col:
            chart_df = df[[x_col, y_col]].dropna().copy()
            if chart_df.empty:
                st.info("Not enough data to plot a chart.")
                return
            chart_df = chart_df.set_index(x_col)
            series = chart_df[y_col]
        else:
            # X == Y → treat as distribution: count per value
            chart_df = (
                df[[x_col]]
                .dropna()
                .groupby(x_col)
                .size()
                .reset_index(name="count")
            )
            if chart_df.empty:
                st.info("Not enough data to plot a chart.")
                return
            chart_df = chart_df.set_index(x_col)
            series = chart_df["count"]

    else:
        # Case 2: no numeric columns – plot counts per chosen column
        st.info(
            "No numeric columns detected – plotting row counts per selected column."
        )
        x_col = st.selectbox(
            "X axis (category or date)",
            all_cols,
            index=0,
            key=f"{base_key}_x",
        )
        chart_type = st.selectbox(
            "Chart type",
            ["Bar", "Line", "Area"],
            key=f"{base_key}_type",
        )

        chart_df = (
            df[[x_col]]
            .dropna()
            .groupby(x_col)
            .size()
            .reset_index(name="count")
        )
        if chart_df.empty:
            st.info("Not enough data to plot a chart.")
            return
        chart_df = chart_df.set_index(x_col)
        series = chart_df["count"]

    # Plot
    if chart_type == "Bar":
        st.bar_chart(series, width="stretch")
    elif chart_type == "Line":
        st.line_chart(series, width="stretch")
    else:
        st.area_chart(series, width="stretch")


# ---------------------- READ TAB (CHAT) ---------------------- #

with tab_read:
    st.subheader("Chat with your system tables")

    # Initialise chat history (per browser session)
    if "chat" not in st.session_state:
        st.session_state["chat"] = []  # list of {"role": "user"/"assistant", ...}

    # Display chat history
    for i, msg in enumerate(st.session_state["chat"]):
        if msg["role"] == "user":
            # Show which user/workspace asked it
            uname = msg.get("user_name", "unknown")
            wsid = msg.get("workspace_id", "unknown")
            st.markdown(f"**You ({uname}, ws={wsid}):** {msg['question']}")
        else:
            st.markdown("**Assistant:**")
            st.code(msg["sql"], language="sql")
            st.dataframe(msg["df"], width="stretch")

            # Per-message quick visualisation
            render_quick_visualisation(msg["df"], base_key=f"msg_{i}")

            # Download this result as CSV
            csv_bytes = msg["df"].to_csv(index=False).encode("utf-8")
            st.download_button(
                "Download this result as CSV",
                data=csv_bytes,
                file_name=f"results_{i}.csv",
                mime="text/csv",
                key=f"download_{i}",
            )

    # Input for new question at the bottom
    question = st.text_input(
        "Ask a new question",
        placeholder="e.g. Which workspaces are currently running?",
        key="new_question",
    )

    if st.button("Send", key="send_question"):
        if not question.strip():
            st.warning("Please enter a question.")
        else:
            raw_question = question.strip()

            # Append user message WITH user/workspace context
            st.session_state["chat"].append(
                {
                    "role": "user",
                    "question": raw_question,
                    "user_name": user_name,
                    "workspace_id": workspace_id,
                }
            )

            # Augment question with context for the SQL agent / LLM
            aug_question = (
                f"For workspace {workspace_id} and user {user_name}, "
                f"{raw_question}"
            )

            # Generate SQL and run it
            with st.spinner("Generating SQL and running query..."):
                try:
                    sql_text = generate_sql(aug_question, mode="read")
                except Exception as e:
                    st.error(f"Error generating SQL: {e}")
                    st.stop()

                try:
                    df = execute_query(sql_text)
                except Exception as e:
                    st.error(f"Error executing SQL: {e}")
                    st.stop()

            # Append assistant message
            st.session_state["chat"].append(
                {
                    "role": "assistant",
                    "question": raw_question,
                    "sql": sql_text,
                    "df": df,
                }
            )

            # Rerun to show updated history without re-running LLM for old messages
            st.rerun()






# # ENHANCING UI : AYUSH

# import streamlit as st
# import altair as alt
# import pandas as pd

# from core.db_connector import execute_query
# from core.sql_agent import generate_sql


# # ---------------------- PAGE CONFIG ---------------------- #

# st.set_page_config(
#     page_title="DataBuddy – Databricks System Tables Assistant",
#     layout="wide",
# )

# st.title("📊 DataBuddy – Databricks System Tables Assistant")
# st.caption("Ask natural language questions over your Databricks system tables. Get SQL + data + instant charts.")


# # ---------------------- SIDEBAR: USER CONTEXT ---------------------- #

# st.sidebar.header("User context")

# user_name = st.sidebar.text_input(
#     "User name",
#     value="user_a",
#     key="sidebar_user_name",
# )

# workspace_id = st.sidebar.text_input(
#     "Workspace ID",
#     value="ws_1",
#     key="sidebar_workspace_id",
# )

# st.sidebar.markdown(f"**Current user:** `{user_name}`")
# st.sidebar.markdown(f"**Workspace:** `{workspace_id}`")

# if st.sidebar.button("🧹 Clear chat history", key="sidebar_clear_chat"):
#     st.session_state["chat"] = []


# # ---------------------- STATE INIT ---------------------- #

# if "chat" not in st.session_state:
#     # user message     -> {"role": "user", "question", "user_name", "workspace_id"}
#     # assistant message-> {"role": "assistant", "question", "sql", "df"}
#     st.session_state["chat"] = []


# # ---------------------- VISUALISATION HELPER ---------------------- #

# def render_advanced_visualisation(df: pd.DataFrame, base_key: str) -> None:
#     """
#     Render an advanced visualisation block for a given DataFrame.

#     - User can pick chart type: Bar / Line / Area / Scatter / Pie
#     - User can pick X, Y, color columns where relevant
#     - Uses Altair for nicer charts

#     base_key must be unique per message (e.g. f"msg_{i}").
#     """
#     if df is None or df.empty:
#         st.info("No data to visualise.")
#         return

#     all_cols = df.columns.tolist()
#     numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
#     non_numeric_cols = [c for c in all_cols if c not in numeric_cols]

#     chart_type = st.selectbox(
#         "Chart type",
#         ["Bar", "Line", "Area", "Scatter", "Pie"],
#         key=f"{base_key}_chart_type",
#     )

#     # ------------- Bar / Line / Area ---------------- #
#     if chart_type in ["Bar", "Line", "Area"]:
#         x_candidates = all_cols
#         y_candidates = numeric_cols or all_cols

#         x_col = st.selectbox(
#             "X axis",
#             x_candidates,
#             index=0,
#             key=f"{base_key}_x",
#         )
#         y_col = st.selectbox(
#             "Y axis (numeric preferred)",
#             y_candidates,
#             index=0,
#             key=f"{base_key}_y",
#         )

#         color_col = st.selectbox(
#             "Group / color by (optional)",
#             ["(none)"] + all_cols,
#             index=0,
#             key=f"{base_key}_color",
#         )

#         data = df[[x_col, y_col] + ([] if color_col == "(none)" else [color_col])].dropna()
#         if data.empty:
#             st.info("Not enough data to plot.")
#             return

#         # yaha fix hai: mark_* functions use karo
#         if chart_type == "Bar":
#             chart = alt.Chart(data).mark_bar()
#         elif chart_type == "Line":
#             chart = alt.Chart(data).mark_line()
#         else:  # Area
#             chart = alt.Chart(data).mark_area()

#         chart = chart.encode(
#             x=alt.X(x_col, title=x_col),
#             y=alt.Y(y_col, title=y_col),
#             tooltip=list(data.columns),
#         )

#         if color_col != "(none)":
#             chart = chart.encode(color=color_col)

#         st.altair_chart(chart, use_container_width=True)
#         return

#     # ------------- Scatter ---------------- #
#     if chart_type == "Scatter":
#         if len(numeric_cols) < 2:
#             st.info("Need at least two numeric columns for a scatter plot.")
#             return

#         x_col = st.selectbox(
#             "X axis (numeric)",
#             numeric_cols,
#             key=f"{base_key}_scatter_x",
#         )
#         y_col = st.selectbox(
#             "Y axis (numeric)",
#             numeric_cols,
#             index=min(1, len(numeric_cols) - 1),
#             key=f"{base_key}_scatter_y",
#         )
#         color_col = st.selectbox(
#             "Color / group by (optional)",
#             ["(none)"] + all_cols,
#             index=0,
#             key=f"{base_key}_scatter_color",
#         )

#         data = df[[x_col, y_col] + ([] if color_col == "(none)" else [color_col])].dropna()
#         if data.empty:
#             st.info("Not enough data to plot.")
#             return

#         chart = alt.Chart(data).mark_circle(size=60, opacity=0.7).encode(
#             x=alt.X(x_col, title=x_col),
#             y=alt.Y(y_col, title=y_col),
#             tooltip=list(data.columns),
#         )
#         if color_col != "(none)":
#             chart = chart.encode(color=color_col)

#         st.altair_chart(chart, use_container_width=True)
#         return

#     # ------------- Pie chart ---------------- #
#     if chart_type == "Pie":
#         if not numeric_cols or not non_numeric_cols:
#             st.info("Pie chart needs at least one numeric and one categorical column.")
#             return

#         cat_col = st.selectbox(
#             "Category",
#             non_numeric_cols,
#             key=f"{base_key}_pie_cat",
#         )
#         val_col = st.selectbox(
#             "Value (numeric)",
#             numeric_cols,
#             key=f"{base_key}_pie_val",
#         )

#         data = (
#             df[[cat_col, val_col]]
#             .dropna()
#             .groupby(cat_col, as_index=False)[val_col]
#             .sum()
#         )
#         if data.empty:
#             st.info("Not enough data to plot.")
#             return

#         chart = alt.Chart(data).mark_arc().encode(
#             theta=alt.Theta(field=val_col, type="quantitative"),
#             color=alt.Color(field=cat_col, type="nominal"),
#             tooltip=list(data.columns),
#         )

#         st.altair_chart(chart, use_container_width=True)
#         return


# # ---------------------- RENDER CHAT HISTORY ---------------------- #

# for i, msg in enumerate(st.session_state["chat"]):
#     if msg["role"] == "user":
#         with st.chat_message("user"):
#             uname = msg.get("user_name", "unknown")
#             wsid = msg.get("workspace_id", "unknown")
#             st.markdown(f"**{uname} (ws={wsid})**")
#             st.write(msg["question"])

#     else:  # assistant
#         with st.chat_message("assistant"):
#             df = msg["df"]
#             sql_text = msg["sql"]

#             n_rows = len(df) if df is not None else 0
#             n_cols = len(df.columns) if df is not None else 0

#             st.markdown(f"**Result:** {n_rows} rows × {n_cols} columns")

#             tab_data, tab_viz, tab_sql = st.tabs(["📄 Data", "📈 Visualisation", "💻 SQL"])

#             with tab_data:
#                 st.dataframe(df, use_container_width=True, height=400)

#                 csv_bytes = df.to_csv(index=False).encode("utf-8")
#                 # No explicit key → Streamlit auto-assigns unique IDs
#                 st.download_button(
#                     "⬇️ Download CSV",
#                     data=csv_bytes,
#                     file_name=f"results_{i}.csv",
#                     mime="text/csv",
#                 )

#             with tab_viz:
#                 render_advanced_visualisation(df, base_key=f"msg_{i}")

#             with tab_sql:
#                 st.code(sql_text, language="sql")


# # ---------------------- CHAT INPUT (BOTTOM) ---------------------- #

# prompt = st.chat_input(
#     "Ask a question about your Databricks system tables (jobs, queries, warehouses, ML, cost, etc.)"
# )

# if prompt:
#     raw_question = prompt.strip()

#     # 1) Append user message with context
#     st.session_state["chat"].append(
#         {
#             "role": "user",
#             "question": raw_question,
#             "user_name": user_name,
#             "workspace_id": workspace_id,
#         }
#     )

#     # 2) Augment question with context for SQL agent / LLM
#     aug_question = (
#         f"For workspace {workspace_id} and user {user_name}, "
#         f"{raw_question}"
#     )

#     # 3) Generate SQL and run it
#     with st.chat_message("assistant"):
#         with st.spinner("Generating SQL and running query..."):
#             try:
#                 sql_text = generate_sql(aug_question, mode="read")
#             except Exception as e:
#                 st.error(f"Error generating SQL: {e}")
#                 st.stop()

#             try:
#                 df = execute_query(sql_text)
#                 if not isinstance(df, pd.DataFrame):
#                     df = pd.DataFrame(df)
#             except Exception as e:
#                 st.error(f"Error executing SQL: {e}")
#                 st.stop()

#     # 4) Store assistant message in history
#     st.session_state["chat"].append(
#         {
#             "role": "assistant",
#             "question": raw_question,
#             "sql": sql_text,
#             "df": df,
#         }
#     )

