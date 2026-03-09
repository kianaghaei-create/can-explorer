"""
CAN Explorer — Interactive Dashboard
======================================
A Streamlit app that lets you explore 60 years of Swedish
substance use data across 7 CAN reports.

Run with: streamlit run app.py
"""

import os
import pandas as pd
import duckdb
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "can_data.duckdb")

st.set_page_config(
    page_title="CAN Explorer",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)


@st.cache_resource
def get_db():
    return duckdb.connect(DB_PATH, read_only=True)


@st.cache_data(ttl=600)
def query(sql):
    con = get_db()
    return con.execute(sql).fetchdf()


# ── Slicer chart renderer ──────────────────────────────────────

def _render_slicer_chart(df: pd.DataFrame, title: str, key_prefix: str):
    """
    Interactive chart for multi-category gender-split data (e.g. CAN-239 incident tables).
    Shows gender radio (Alla / Pojkar / Flickor) and view toggle (latest year bar / trend line).
    """
    if df is None or "variable" not in df.columns or "value" not in df.columns:
        return

    GENDER_MAP = {"_alla": "Alla", "_po": "Pojkar", "_fl": "Flickor"}
    SUFFIX_ORDER = ["_alla", "_po", "_fl"]

    # Parse each variable into (display_label, gender_label)
    var_info: dict = {}
    for var in df["variable"].unique():
        for sfx in SUFFIX_ORDER:
            if var.endswith(sfx):
                raw = var[: -len(sfx)].replace("_", " ")
                label = raw[0].upper() + raw[1:] if raw else var
                var_info[var] = (label, GENDER_MAP[sfx])
                break
        else:
            var_info[var] = (var.replace("_", " "), "Alla")

    # Which genders exist in the data?
    available_genders = [
        GENDER_MAP[sfx]
        for sfx in SUFFIX_ORDER
        if any(v.endswith(sfx) for v in df["variable"].unique())
    ]
    if not available_genders:
        return

    col_ctrl, col_chart = st.columns([1, 4])

    with col_ctrl:
        st.markdown("**Kön**")
        selected_gender = st.radio(
            "Kön", available_genders,
            key=f"{key_prefix}_gender",
            label_visibility="collapsed",
        )
        st.markdown("**Visa**")
        view_mode = st.radio(
            "Visa", ["Senaste år", "Trend över tid"],
            key=f"{key_prefix}_view",
            label_visibility="collapsed",
        )

    # Filter to the selected gender suffix
    gender_suffix = {v: k for k, v in GENDER_MAP.items()}[selected_gender]
    gender_vars = [v for v in df["variable"].unique() if v.endswith(gender_suffix)]
    filtered = df[df["variable"].isin(gender_vars)].copy()
    filtered["kategori"] = filtered["variable"].map(lambda v: var_info.get(v, (v, ""))[0])

    with col_chart:
        if view_mode == "Senaste år":
            latest_year = int(filtered["year"].max())
            bar_df = (
                filtered[filtered["year"] == latest_year]
                .groupby("kategori", as_index=False)["value"].mean()
                .sort_values("value", ascending=True)
            )
            fig = px.bar(
                bar_df,
                x="value", y="kategori",
                orientation="h",
                title=f"{title} — {selected_gender} ({latest_year})",
                labels={"value": "%", "kategori": ""},
                color="value",
                color_continuous_scale="Blues",
            )
            fig.update_layout(
                height=max(420, len(gender_vars) * 32),
                coloraxis_showscale=False,
                margin=dict(l=10, r=40, t=50, b=30),
            )
            fig.update_traces(texttemplate="%{x:.1f}%", textposition="outside")
            st.plotly_chart(fig, use_container_width=True)
        else:
            fig = px.line(
                filtered, x="year", y="value", color="kategori",
                title=f"{title} — {selected_gender}",
                markers=True,
                labels={"value": "%", "year": "År", "kategori": ""},
            )
            fig.update_layout(height=520)
            st.plotly_chart(fig, use_container_width=True)


# ── Report dialog ─────────────────────────────────────────────
@st.dialog("📄 Create Report", width="large")
def report_dialog():
    """Multi-phase dialog: fetch studies → configure → generate PDF."""
    info = st.session_state.get("last_chat_result", {})
    question = info.get("question", "")
    answer = info.get("answer", "")
    data = info.get("data")

    if not question:
        st.warning("No recent query found. Please ask a question first.")
        if st.button("Close"):
            st.session_state.pop("rg_trigger", None)
            st.rerun()
        return

    phase = st.session_state.get("rg_phase", "loading")

    # ── Phase 1: Fetch PubMed studies ────────────────────────
    if phase == "loading":
        st.markdown("**Searching PubMed for relevant studies…**")
        with st.spinner("Querying PubMed…"):
            try:
                from chat_engine import decompose_query
                topics = decompose_query(question)
            except Exception:
                topics = [question]
            answer_text = info.get("answer", "")
            from report_generator import search_pubmed_multi, filter_relevant_studies
            raw, pubmed_queries = search_pubmed_multi(
                topics, max_per_topic=8,
                question=question, answer=answer_text,
            )
            filtered = filter_relevant_studies(
                raw, question, anchor_texts=pubmed_queries
            )
            st.session_state.rg_studies = filtered
            st.session_state.rg_phase = "configure"
        st.rerun()

    # ── Phase 2: Configure ────────────────────────────────────
    elif phase == "configure":
        studies = st.session_state.get("rg_studies", [])

        st.markdown(f"**Question:** {question[:220]}")
        st.markdown("---")
        st.markdown(
            "**Report sections:**  \n"
            "Sammanfattning · Inledning · Resultat *(with data table)* · "
            "Diskussion · Avslutande kommentarer · Externa studier · Referenser"
        )
        st.markdown("---")

        st.markdown(f"### Relevant studies found: {len(studies)}")
        st.caption(
            "Relevance score (0–1): cosine similarity between each study's abstract "
            "and the search query, computed via OpenAI embeddings. "
            "Only studies scoring ≥ 0.30 are shown."
        )
        if not studies:
            st.info(
                "No highly relevant PubMed studies were found for this query. "
                "The report will be based on CAN data only."
            )
        selected = []
        for i, s in enumerate(studies):
            au = ", ".join(s.get("authors", [])[:2])
            score = s.get("relevance_score", 0)
            col1, col2 = st.columns([0.06, 0.94])
            with col1:
                inc = st.checkbox("", value=True, key=f"rg_inc_{i}")
            with col2:
                st.markdown(
                    f"**{s['title'][:115]}**  \n"
                    f"{au} ({s.get('year', 'n.d.')}) — *{s.get('journal', '')[:60]}*  \n"
                    f"Relevance score: `{score:.2f}`"
                )
            if inc:
                selected.append(s)

        st.markdown("---")
        lang = st.radio(
            "Report language", ["Swedish", "English"],
            horizontal=True, key="rg_lang_choice",
        )
        c1, c2 = st.columns(2)
        with c1:
            if st.button(
                "Generate PDF", type="primary",
                use_container_width=True, key="rg_gen",
            ):
                st.session_state.rg_selected_studies = selected
                st.session_state.rg_language = lang
                st.session_state.rg_phase = "generating"
                st.rerun()
        with c2:
            if st.button("Cancel", use_container_width=True, key="rg_cancel"):
                for k in [
                    "rg_phase", "rg_studies", "rg_selected_studies",
                    "rg_pdf", "rg_language", "rg_trigger",
                ]:
                    st.session_state.pop(k, None)
                st.rerun()

    # ── Phase 3: Generate ─────────────────────────────────────
    elif phase == "generating":
        st.info("Generating report… this may take 20–40 seconds.")
        with st.spinner("Writing sections and rendering PDF…"):
            from report_generator import generate_report_sections, build_pdf
            selected = st.session_state.get("rg_selected_studies", [])
            lang = st.session_state.get("rg_language", "Swedish")
            data_preview = (
                data.head(30).to_string(index=False)
                if data is not None and len(data) > 0
                else ""
            )
            sections = generate_report_sections(
                question=question,
                answer=answer,
                data_preview=data_preview,
                studies=selected,
                language=lang,
            )
            pdf_bytes = build_pdf(
                question=question,
                sections=sections,
                studies=selected,
                data=data if data is not None and len(data) > 0 else None,
                language=lang,
            )
            st.session_state.rg_pdf = pdf_bytes
            st.session_state.rg_phase = "done"
        st.rerun()

    # ── Phase 4: Done ─────────────────────────────────────────
    elif phase == "done":
        st.success("Your report is ready!")
        today = datetime.now().strftime("%Y%m%d_%H%M")
        st.download_button(
            "📥 Download PDF Report",
            data=st.session_state.rg_pdf,
            file_name=f"CAN_Report_{today}.pdf",
            mime="application/pdf",
            type="primary",
            use_container_width=True,
        )
        st.markdown("---")
        if st.button("Close", use_container_width=True, key="rg_close"):
            for k in [
                "rg_phase", "rg_studies", "rg_selected_studies",
                "rg_pdf", "rg_language", "rg_trigger",
            ]:
                st.session_state.pop(k, None)
            st.rerun()


# ── Sidebar ──────────────────────────────────────────────────
st.sidebar.title("CAN Explorer")
st.sidebar.markdown("*60 years of Swedish substance use data*")
st.sidebar.markdown("---")

page = st.sidebar.radio(
    "Navigate",
    [
        "💬 Ask the Data",
        "📡 Signal Board",
        "🔀 Compare Series",
        "🔗 Cross-Domain Correlations",
        "📉 Trend Breaks",
        "🏘️ Municipal Context (KOLADA)",
        "📚 Data Catalog",
    ],
    index=0,
)

st.sidebar.markdown("---")
st.sidebar.markdown("*Powered by DuckDB + GPT-4o + KOLADA API*")

# ── REPORT LABELS ────────────────────────────────────────────
REPORT_LABELS = {
    "CAN-233": "Drug Prices",
    "CAN-234": "Smoking & Snus Habits",
    "CAN-235": "Drug Trends (Seizures/Crime/Health)",
    "CAN-236": "Alcohol Consumption",
    "CAN-237": "Alcohol Habits (Self-reported)",
    "CAN-238": "Tobacco & Nicotine Consumption",
    "CAN-239": "Youth School Survey",
}


# ══════════════════════════════════════════════════════════════
# PAGE: Ask the Data (AI Chat)
# ══════════════════════════════════════════════════════════════
if page == "💬 Ask the Data":
    st.title("💬 Ask the Data")
    st.markdown(
        "Ask questions in plain English or Swedish. The AI will query the database, "
        "explain what it finds, and show you a chart."
    )

    # Example questions
    with st.expander("Example questions to try"):
        st.markdown("""
- How has youth alcohol consumption changed since 2000?
- Compare cocaine street prices with cocaine seizures over time
- What happened to smoking rates among young women (17-29)?
- Show me the trend in cannabis use among grade 9 students
- Has the gender gap in drug experimentation changed?
- What are the biggest changes in the youth school survey since 2010?
- How do amphetamine prices relate to seizure rates?
- Show me all alcohol consumption data from CAN-236
        """)

    # Initialize chat history
    if "messages" not in st.session_state:
        st.session_state.messages = []

    # Open dialog if triggered
    if st.session_state.get("rg_trigger"):
        report_dialog()

    # Display chat history
    for i, message in enumerate(st.session_state.messages):
        with st.chat_message(message["role"]):
            st.markdown(message["content"])
            if message.get("is_slicer") and message.get("slicer_data") is not None:
                _render_slicer_chart(
                    message["slicer_data"],
                    message.get("slicer_title", ""),
                    key_prefix=f"hist_{i}",
                )
            elif message.get("chart") is not None:
                st.plotly_chart(message["chart"], use_container_width=True)
            if message.get("sql"):
                with st.expander("View SQL"):
                    st.code(message["sql"], language="sql")
            if message.get("data") is not None and len(message.get("data", [])) > 0:
                with st.expander("View raw data"):
                    st.dataframe(message["data"], use_container_width=True)

    # "Create Report" button — visible whenever there is a previous result
    if "last_chat_result" in st.session_state:
        _, col_btn = st.columns([0.72, 0.28])
        with col_btn:
            if st.button(
                "📄 Create Report",
                use_container_width=True,
                help="Generate a CAN-styled PDF report from your last question",
            ):
                st.session_state.rg_trigger = True
                st.rerun()

    # Chat input
    if prompt := st.chat_input("Ask a question about Swedish substance use data..."):
        # Show user message
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        # Get AI response
        with st.chat_message("assistant"):
            with st.spinner("Thinking..."):
                try:
                    from chat_engine import ask_data

                    # Build conversation history for context
                    history = []
                    for msg in st.session_state.messages[:-1]:  # exclude current
                        history.append({"role": msg["role"], "content": msg["content"]})

                    result = ask_data(prompt, history)

                    # Display answer
                    st.markdown(result["answer"])

                    # Display source citations
                    if result.get("sources"):
                        st.markdown("---")
                        st.markdown("**📚 Sources:**")
                        st.markdown(result["sources"])

                    # ── Slicer chart (enumeration questions) ──────────────
                    chart_fig = None
                    is_slicer = (
                        result.get("is_enumeration", False)
                        and result["data"] is not None
                        and len(result["data"]) > 0
                    )
                    slicer_title = (result.get("chart_spec") or {}).get("title", "")

                    if is_slicer:
                        _render_slicer_chart(
                            result["data"], slicer_title,
                            key_prefix=f"new_{len(st.session_state.messages)}",
                        )

                    # Display chart if we have data
                    elif result["data"] is not None and len(result["data"]) > 0 and result.get("chart_spec"):
                        spec = result["chart_spec"]
                        df = result["data"]

                        try:
                            chart_type = spec.get("type", "line")
                            x_col = spec.get("x", "year")
                            y_col = spec.get("y", "value")
                            color_col = spec.get("color")
                            title = spec.get("title", "")

                            # Handle case where color column doesn't exist
                            if color_col and color_col not in df.columns:
                                color_col = "variable" if "variable" in df.columns else None

                            # Check if we have multiple series with very different scales
                            # (e.g. "never used" at 90% vs "used cannabis" at 5%)
                            use_dual_axis = False
                            primary_groups = []
                            secondary_groups = []
                            if color_col and color_col in df.columns:
                                groups = df.groupby(color_col)[y_col].agg(["mean"])
                                if len(groups) >= 2:
                                    means = groups["mean"]
                                    overall_median = means.median()
                                    # Split into two scale groups: high vs low
                                    for gname, gmean in means.items():
                                        if overall_median > 0 and gmean > overall_median * 3:
                                            secondary_groups.append(gname)
                                        else:
                                            primary_groups.append(gname)
                                    # Only use dual axis if we have groups on both sides
                                    if secondary_groups and primary_groups:
                                        use_dual_axis = True
                                    else:
                                        primary_groups = list(means.index)
                                        secondary_groups = []

                            if use_dual_axis and color_col:
                                # Dual Y-axis chart for comparing series with different scales
                                from plotly.subplots import make_subplots
                                all_colors = ["#636EFA", "#EF553B", "#00CC96", "#AB63FA", "#FFA15A"]
                                chart_fig = make_subplots(specs=[[{"secondary_y": True}]])
                                color_idx = 0
                                for gname in primary_groups:
                                    gdata = df[df[color_col] == gname].sort_values(x_col)
                                    chart_fig.add_trace(
                                        go.Scatter(
                                            x=gdata[x_col], y=gdata[y_col],
                                            name=str(gname)[:70],
                                            mode="lines+markers",
                                            line=dict(color=all_colors[color_idx % len(all_colors)]),
                                        ),
                                        secondary_y=False,
                                    )
                                    color_idx += 1
                                for gname in secondary_groups:
                                    gdata = df[df[color_col] == gname].sort_values(x_col)
                                    chart_fig.add_trace(
                                        go.Scatter(
                                            x=gdata[x_col], y=gdata[y_col],
                                            name=str(gname)[:70],
                                            mode="lines+markers",
                                            line=dict(color=all_colors[color_idx % len(all_colors)], dash="dash"),
                                        ),
                                        secondary_y=True,
                                    )
                                    color_idx += 1
                                chart_fig.update_layout(height=450, title=title)
                                primary_label = ", ".join(str(g)[:45] for g in primary_groups[:2])
                                secondary_label = ", ".join(str(g)[:45] for g in secondary_groups[:2])
                                chart_fig.update_yaxes(title_text=primary_label, secondary_y=False)
                                chart_fig.update_yaxes(title_text=secondary_label, secondary_y=True)
                                st.plotly_chart(chart_fig, use_container_width=True)

                            elif chart_type == "bar":
                                chart_fig = px.bar(df, x=x_col, y=y_col, color=color_col, title=title, barmode="group")
                                chart_fig.update_layout(height=450)
                                st.plotly_chart(chart_fig, use_container_width=True)
                            elif chart_type == "scatter":
                                chart_fig = px.scatter(df, x=x_col, y=y_col, color=color_col, title=title)
                                chart_fig.update_layout(height=450)
                                st.plotly_chart(chart_fig, use_container_width=True)
                            else:
                                chart_fig = px.line(df, x=x_col, y=y_col, color=color_col, title=title, markers=True)
                                chart_fig.update_layout(height=450)
                                st.plotly_chart(chart_fig, use_container_width=True)

                        except Exception as chart_err:
                            # Fallback: just plot whatever we can
                            if "year" in df.columns and "value" in df.columns:
                                color_col = "variable" if "variable" in df.columns else None
                                chart_fig = px.line(df, x="year", y="value", color=color_col, markers=True)
                                chart_fig.update_layout(height=450)
                                st.plotly_chart(chart_fig, use_container_width=True)

                    elif result["data"] is not None and len(result["data"]) > 0:
                        # No chart spec but we have data — try a default chart
                        df = result["data"]
                        if "year" in df.columns and "value" in df.columns:
                            color_col = "variable" if "variable" in df.columns else None
                            chart_fig = px.line(df, x="year", y="value", color=color_col, markers=True)
                            chart_fig.update_layout(height=450)
                            st.plotly_chart(chart_fig, use_container_width=True)

                    # Show semantic search hits
                    if result.get("semantic_hits"):
                        with st.expander(f"🔍 Variables found by semantic search ({len(result['semantic_hits'])})"):
                            hits_df = pd.DataFrame(result["semantic_hits"])
                            hits_df["score"] = hits_df["score"].round(3)
                            hits_df = hits_df.rename(columns={
                                "score": "Relevance",
                                "report": "Report",
                                "table_id": "Table",
                                "table_title": "Title",
                                "variable": "Variable",
                                "y_min": "From",
                                "y_max": "To",
                            })
                            st.dataframe(
                                hits_df[["Relevance", "Report", "Table", "Variable", "From", "To"]],
                                use_container_width=True,
                                hide_index=True,
                            )

                    # Show SQL
                    if result.get("sql"):
                        with st.expander("View SQL query"):
                            st.code(result["sql"], language="sql")

                    # Show raw data
                    if result["data"] is not None and len(result["data"]) > 0:
                        with st.expander("View raw data"):
                            st.dataframe(result["data"], use_container_width=True)

                    # Save to history
                    st.session_state.messages.append({
                        "role": "assistant",
                        "content": result["answer"],
                        "sql": result.get("sql"),
                        "data": result["data"] if result["data"] is not None else None,
                        "chart": chart_fig if not is_slicer else None,
                        "is_slicer": is_slicer,
                        "slicer_data": result["data"] if is_slicer else None,
                        "slicer_title": slicer_title if is_slicer else "",
                    })

                    # Store for report generation
                    st.session_state.last_chat_result = {
                        "question": prompt,
                        "answer": result["answer"],
                        "data": result["data"],
                    }

                    # Show "Create Report" button immediately in the response
                    st.markdown("---")
                    _, col_rpt = st.columns([0.65, 0.35])
                    with col_rpt:
                        if st.button(
                            "📄 Create Report",
                            key=f"cr_inline_{len(st.session_state.messages)}",
                            use_container_width=True,
                            help="Generate a CAN-styled PDF from this answer",
                        ):
                            st.session_state.rg_trigger = True
                            st.rerun()

                except Exception as e:
                    error_msg = f"Error: {str(e)}"
                    st.error(error_msg)
                    if "OPENAI_API_KEY" in str(e) or "api_key" in str(e).lower():
                        st.warning(
                            "Set your OpenAI API key: `export OPENAI_API_KEY=sk-...` "
                            "then restart Streamlit."
                        )
                    st.session_state.messages.append({"role": "assistant", "content": error_msg})


# ══════════════════════════════════════════════════════════════
# PAGE: Signal Board
# ══════════════════════════════════════════════════════════════
elif page == "📡 Signal Board":
    st.title("📡 Signal Board")
    st.markdown("**Automated discoveries across all 7 CAN reports.** These are patterns the AI found — not questions anyone asked.")

    # Top correlations
    st.header("Strongest Cross-Domain Connections")
    st.markdown("Time series from *different* CAN reports that move together (or opposite).")

    try:
        corr = query("SELECT * FROM insight_correlations ORDER BY ABS(correlation) DESC LIMIT 20")
        if len(corr) > 0:
            for i, row in corr.head(10).iterrows():
                strength = abs(row["correlation"])
                color = "🔴" if strength > 0.9 else "🟠" if strength > 0.8 else "🟡"

                with st.expander(
                    f"{color} r = {row['correlation']:+.3f} | "
                    f"{row['report_1']} vs {row['report_2']} | "
                    f"{row['year_min']}–{row['year_max']}"
                ):
                    col1, col2 = st.columns(2)
                    with col1:
                        st.markdown(f"**{REPORT_LABELS.get(row['report_1'], row['report_1'])}**")
                        st.markdown(f"Table {row['table_1']}: {row['title_1']}")
                        st.code(row["variable_1"])
                    with col2:
                        st.markdown(f"**{REPORT_LABELS.get(row['report_2'], row['report_2'])}**")
                        st.markdown(f"Table {row['table_2']}: {row['title_2']}")
                        st.code(row["variable_2"])

                    s1 = query(f"""
                        SELECT year, value FROM timeseries
                        WHERE report='{row['report_1']}' AND table_id='{row['table_1']}'
                        AND variable='{row['variable_1']}' ORDER BY year
                    """)
                    s2 = query(f"""
                        SELECT year, value FROM timeseries
                        WHERE report='{row['report_2']}' AND table_id='{row['table_2']}'
                        AND variable='{row['variable_2']}' ORDER BY year
                    """)

                    fig = make_subplots(specs=[[{"secondary_y": True}]])
                    fig.add_trace(
                        go.Scatter(x=s1["year"], y=s1["value"], name=row["variable_1"][:60], line=dict(color="#636EFA")),
                        secondary_y=False,
                    )
                    fig.add_trace(
                        go.Scatter(x=s2["year"], y=s2["value"], name=row["variable_2"][:60], line=dict(color="#EF553B")),
                        secondary_y=True,
                    )
                    fig.update_layout(height=350, margin=dict(t=30, b=30))
                    st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No cross-domain correlations computed yet. Run `python insights.py` first.")
    except Exception:
        st.info("No cross-domain correlations computed yet. Run `python insights.py` first.")

    # Trend breaks
    st.header("Biggest Trend Breaks")
    st.markdown("Points in time where something shifted significantly.")

    try:
        breaks = query("SELECT * FROM insight_trend_breaks ORDER BY ABS(t_statistic) DESC LIMIT 15")
        if len(breaks) > 0:
            for i, row in breaks.head(8).iterrows():
                arrow = "📈" if row["direction"] == "increase" else "📉"
                with st.expander(
                    f"{arrow} {row['break_year']} | {row['change_pct']:+.1f}% | "
                    f"{row['report']} / {row['variable'][:70]}"
                ):
                    st.markdown(f"**{REPORT_LABELS.get(row['report'], row['report'])}** — Table {row['table_id']}")
                    st.markdown(f"*{row['table_title']}*")
                    st.markdown(f"Before {row['break_year']}: **{row['mean_before']}** → After: **{row['mean_after']}** ({row['change_pct']:+.1f}%)")

                    ts = query(f"""
                        SELECT year, value FROM timeseries
                        WHERE report='{row['report']}' AND table_id='{row['table_id']}'
                        AND variable='{row['variable']}' ORDER BY year
                    """)

                    fig = go.Figure()
                    fig.add_trace(go.Scatter(x=ts["year"], y=ts["value"], mode="lines+markers", name=row["variable"][:60]))
                    fig.add_vline(x=row["break_year"], line_dash="dash", line_color="red", annotation_text=f"Break: {row['break_year']}")
                    fig.update_layout(height=300, margin=dict(t=30, b=30))
                    st.plotly_chart(fig, use_container_width=True)
    except Exception:
        st.info("No trend breaks computed yet. Run `python insights.py` first.")

    # Recent movers
    st.header("Biggest Recent Movers")
    st.markdown("Series where the last 5 years look very different from history.")

    try:
        movers = query("SELECT * FROM insight_movers ORDER BY ABS(z_score) DESC LIMIT 15")
        if len(movers) > 0:
            cols = st.columns(3)
            for i, (_, row) in enumerate(movers.head(9).iterrows()):
                with cols[i % 3]:
                    st.metric(
                        label=f"{row['report']} / {row['variable'][:55]}",
                        value=f"{row['recent_mean']:.1f}",
                        delta=f"{row['z_score']:+.1f}σ from historical",
                    )
    except Exception:
        st.info("No movers computed yet. Run `python insights.py` first.")


# ══════════════════════════════════════════════════════════════
# PAGE: Compare Series
# ══════════════════════════════════════════════════════════════
elif page == "🔀 Compare Series":
    st.title("🔀 Compare Any Series")
    st.markdown("Pick variables from any report and overlay them.")

    catalog = query("""
        SELECT report, table_id, table_title, variables, year_min, year_max
        FROM catalog ORDER BY report, table_id
    """)

    col1, col2 = st.columns(2)

    selections = []
    for idx, col in enumerate([col1, col2]):
        with col:
            st.subheader(f"Series {idx + 1}")
            report = st.selectbox(
                f"Report #{idx+1}",
                catalog["report"].unique(),
                key=f"report_{idx}",
                format_func=lambda x: f"{x} — {REPORT_LABELS.get(x, '')}",
            )

            tables = catalog[catalog["report"] == report]
            table_options = tables.apply(
                lambda r: f"Table {r['table_id']}: {str(r['table_title'])[:80]}" if r['table_title'] else f"Table {r['table_id']}",
                axis=1,
            ).tolist()
            table_ids = tables["table_id"].tolist()

            table_choice = st.selectbox(f"Table #{idx+1}", range(len(table_options)), format_func=lambda i: table_options[i], key=f"table_{idx}")
            selected_table = table_ids[table_choice]

            vars_df = query(f"""
                SELECT DISTINCT variable FROM timeseries
                WHERE report='{report}' AND table_id='{selected_table}'
                ORDER BY variable
            """)

            variable = st.selectbox(f"Variable #{idx+1}", vars_df["variable"].tolist(), key=f"var_{idx}")
            selections.append({"report": report, "table_id": selected_table, "variable": variable})

    if st.button("Compare", type="primary"):
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        colors = ["#636EFA", "#EF553B"]

        for i, sel in enumerate(selections):
            data = query(f"""
                SELECT year, value FROM timeseries
                WHERE report='{sel['report']}' AND table_id='{sel['table_id']}'
                AND variable='{sel['variable']}' ORDER BY year
            """)
            fig.add_trace(
                go.Scatter(
                    x=data["year"], y=data["value"],
                    name=f"{sel['report']} / {sel['variable'][:60]}",
                    mode="lines+markers",
                    line=dict(color=colors[i]),
                ),
                secondary_y=(i == 1),
            )

        fig.update_layout(height=500, title="Comparison")
        fig.update_xaxes(title_text="Year")
        st.plotly_chart(fig, use_container_width=True)

        d1 = query(f"""
            SELECT year, value as v1 FROM timeseries
            WHERE report='{selections[0]['report']}' AND table_id='{selections[0]['table_id']}'
            AND variable='{selections[0]['variable']}'
        """)
        d2 = query(f"""
            SELECT year, value as v2 FROM timeseries
            WHERE report='{selections[1]['report']}' AND table_id='{selections[1]['table_id']}'
            AND variable='{selections[1]['variable']}'
        """)
        merged = d1.merge(d2, on="year").dropna()
        if len(merged) >= 5:
            from scipy.stats import pearsonr
            r, p = pearsonr(merged["v1"], merged["v2"])
            st.metric("Pearson Correlation", f"r = {r:.3f}", delta=f"p = {p:.4f}")
        else:
            st.warning("Not enough overlapping years to compute correlation.")


# ══════════════════════════════════════════════════════════════
# PAGE: Cross-Domain Correlations
# ══════════════════════════════════════════════════════════════
elif page == "🔗 Cross-Domain Correlations":
    st.title("🔗 Cross-Domain Correlation Matrix")
    st.markdown("Which reports are connected? Filter and explore.")

    try:
        corr = query("SELECT * FROM insight_correlations ORDER BY ABS(correlation) DESC")

        if len(corr) > 0:
            col1, col2, col3 = st.columns(3)
            with col1:
                min_r = st.slider("Min |correlation|", 0.7, 1.0, 0.8, 0.05)
            with col2:
                direction = st.selectbox("Direction", ["All", "positive", "negative"])
            with col3:
                report_filter = st.multiselect("Reports", sorted(set(corr["report_1"].tolist() + corr["report_2"].tolist())))

            filtered = corr[corr["correlation"].abs() >= min_r]
            if direction != "All":
                filtered = filtered[filtered["direction"] == direction]
            if report_filter:
                filtered = filtered[filtered["report_1"].isin(report_filter) | filtered["report_2"].isin(report_filter)]

            st.dataframe(
                filtered[["report_1", "variable_1", "report_2", "variable_2", "correlation", "overlap_years", "year_min", "year_max"]],
                use_container_width=True,
                height=500,
            )

            st.subheader("Report-to-Report Average Correlation")
            cross = corr.groupby(["report_1", "report_2"])["correlation"].mean().reset_index()
            cross2 = cross.rename(columns={"report_1": "report_2", "report_2": "report_1"})
            cross = pd.concat([cross, cross2])
            heatmap_data = cross.pivot_table(index="report_1", columns="report_2", values="correlation", aggfunc="mean")

            fig = px.imshow(
                heatmap_data,
                text_auto=".2f",
                color_continuous_scale="RdBu_r",
                zmin=-1, zmax=1,
                labels={"color": "Avg Correlation"},
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No correlations found.")
    except Exception as e:
        st.info(f"No correlations computed yet. Run `python insights.py` first. ({e})")


# ══════════════════════════════════════════════════════════════
# PAGE: Trend Breaks
# ══════════════════════════════════════════════════════════════
elif page == "📉 Trend Breaks":
    st.title("📉 Trend Break Timeline")
    st.markdown("When did things change? Every significant structural break detected across all data.")

    try:
        breaks = query("SELECT * FROM insight_trend_breaks ORDER BY break_year, ABS(t_statistic) DESC")

        if len(breaks) > 0:
            fig = px.scatter(
                breaks,
                x="break_year",
                y="change_pct",
                color="report",
                size=breaks["t_statistic"].abs(),
                hover_data=["variable", "table_title", "mean_before", "mean_after"],
                labels={"break_year": "Year of Break", "change_pct": "Change %", "report": "Report"},
                color_discrete_sequence=px.colors.qualitative.Set2,
            )
            fig.add_hline(y=0, line_dash="dash", line_color="gray")
            fig.update_layout(height=500)
            st.plotly_chart(fig, use_container_width=True)

            report_filter = st.multiselect("Filter by report", breaks["report"].unique().tolist())
            if report_filter:
                breaks = breaks[breaks["report"].isin(report_filter)]

            st.dataframe(
                breaks[["report", "break_year", "variable", "change_pct", "mean_before", "mean_after", "direction", "year_range"]],
                use_container_width=True,
                height=400,
            )
        else:
            st.info("No trend breaks found.")
    except Exception as e:
        st.info(f"No trend breaks computed yet. Run `python insights.py` first. ({e})")


# ══════════════════════════════════════════════════════════════
# PAGE: Municipal Context (KOLADA)
# ══════════════════════════════════════════════════════════════
elif page == "🏘️ Municipal Context (KOLADA)":
    st.title("🏘️ Municipal Context — KOLADA")
    st.markdown(
        "CAN data is national. KOLADA adds the **municipal dimension** — "
        "drug offenses, youth mental health, unemployment, education outcomes "
        "across Sweden's largest cities."
    )

    try:
        kolada = query("SELECT * FROM kolada")

        if len(kolada) > 0:
            # Filter controls
            col1, col2, col3 = st.columns(3)
            with col1:
                kpi_options = kolada[["kpi_id", "kpi_title"]].drop_duplicates().sort_values("kpi_title")
                selected_kpi = st.selectbox(
                    "Indicator",
                    kpi_options["kpi_id"].tolist(),
                    format_func=lambda x: kpi_options[kpi_options["kpi_id"] == x]["kpi_title"].iloc[0],
                )
            with col2:
                gender_options = kolada["gender"].unique().tolist()
                gender_map = {"T": "Total", "M": "Male", "K": "Female"}
                selected_gender = st.selectbox(
                    "Gender",
                    gender_options,
                    format_func=lambda x: gender_map.get(x, x),
                    index=gender_options.index("T") if "T" in gender_options else 0,
                )
            with col3:
                muni_options = sorted(kolada["municipality_name"].unique().tolist())
                selected_munis = st.multiselect("Municipalities", muni_options, default=muni_options[:5])

            # Filter data
            filtered = kolada[
                (kolada["kpi_id"] == selected_kpi)
                & (kolada["gender"] == selected_gender)
            ]
            if selected_munis:
                filtered = filtered[filtered["municipality_name"].isin(selected_munis)]

            if len(filtered) > 0:
                kpi_title = filtered["kpi_title"].iloc[0]

                # Line chart: municipality comparison over time
                fig = px.line(
                    filtered,
                    x="year",
                    y="value",
                    color="municipality_name",
                    title=f"{kpi_title} — by Municipality",
                    markers=True,
                    labels={"value": kpi_title, "year": "Year", "municipality_name": "Municipality"},
                )
                fig.update_layout(height=500)
                st.plotly_chart(fig, use_container_width=True)

                # Latest year bar chart
                latest_year = filtered["year"].max()
                latest = filtered[filtered["year"] == latest_year].sort_values("value", ascending=True)

                fig2 = px.bar(
                    latest,
                    x="value",
                    y="municipality_name",
                    orientation="h",
                    title=f"{kpi_title} — {latest_year}",
                    labels={"value": kpi_title, "municipality_name": "Municipality"},
                    color="value",
                    color_continuous_scale="RdYlGn_r" if "offenses" in kpi_title.lower() or "unemployment" in kpi_title.lower() else "RdYlGn",
                )
                fig2.update_layout(height=400)
                st.plotly_chart(fig2, use_container_width=True)

                st.dataframe(filtered.sort_values(["municipality_name", "year"]), use_container_width=True)
            else:
                st.warning("No data for this selection.")

            # Cross-indicator view
            st.header("Compare Indicators Across Municipalities")
            st.markdown("How do different municipal indicators relate to each other?")

            col1, col2 = st.columns(2)
            with col1:
                kpi_x = st.selectbox(
                    "X-axis indicator",
                    kpi_options["kpi_id"].tolist(),
                    format_func=lambda x: kpi_options[kpi_options["kpi_id"] == x]["kpi_title"].iloc[0],
                    key="kpi_x",
                )
            with col2:
                kpi_y = st.selectbox(
                    "Y-axis indicator",
                    kpi_options["kpi_id"].tolist(),
                    format_func=lambda x: kpi_options[kpi_options["kpi_id"] == x]["kpi_title"].iloc[0],
                    key="kpi_y",
                    index=min(1, len(kpi_options) - 1),
                )

            if kpi_x != kpi_y:
                scatter_year = st.slider("Year", int(kolada["year"].min()), int(kolada["year"].max()), int(kolada["year"].max()))

                dx = kolada[(kolada["kpi_id"] == kpi_x) & (kolada["gender"] == "T") & (kolada["year"] == scatter_year)]
                dy = kolada[(kolada["kpi_id"] == kpi_y) & (kolada["gender"] == "T") & (kolada["year"] == scatter_year)]

                merged = dx[["municipality_name", "value"]].rename(columns={"value": "x_value"}).merge(
                    dy[["municipality_name", "value"]].rename(columns={"value": "y_value"}),
                    on="municipality_name",
                )

                if len(merged) >= 3:
                    x_title = kpi_options[kpi_options["kpi_id"] == kpi_x]["kpi_title"].iloc[0]
                    y_title = kpi_options[kpi_options["kpi_id"] == kpi_y]["kpi_title"].iloc[0]

                    fig3 = px.scatter(
                        merged,
                        x="x_value",
                        y="y_value",
                        text="municipality_name",
                        title=f"{x_title} vs {y_title} ({scatter_year})",
                        labels={"x_value": x_title, "y_value": y_title},
                        trendline="ols",
                    )
                    fig3.update_traces(textposition="top center")
                    fig3.update_layout(height=500)
                    st.plotly_chart(fig3, use_container_width=True)

                    from scipy.stats import pearsonr
                    r, p = pearsonr(merged["x_value"], merged["y_value"])
                    st.metric("Correlation", f"r = {r:.3f}", delta=f"p = {p:.4f}")
                else:
                    st.warning("Not enough overlapping data for this year.")

        else:
            st.info("No KOLADA data loaded. Run `python kolada.py` first.")
    except Exception as e:
        st.info(f"No KOLADA data available. Run `python kolada.py` first. ({e})")


# ══════════════════════════════════════════════════════════════
# PAGE: Data Catalog
# ══════════════════════════════════════════════════════════════
elif page == "📚 Data Catalog":
    st.title("📚 Data Catalog")
    st.markdown("Everything in the database — browse, search, explore.")

    catalog = query("SELECT * FROM catalog ORDER BY report, table_id")
    catalog["report_label"] = catalog["report"].map(REPORT_LABELS)

    col1, col2, col3, col4 = st.columns(4)
    total_records = query("SELECT COUNT(*) as n FROM timeseries")["n"].iloc[0]
    col1.metric("Total Records", f"{total_records:,}")
    col2.metric("Tables", len(catalog))
    col3.metric("Reports", catalog["report"].nunique())
    col4.metric("Year Range", f"{catalog['year_min'].min()} – {catalog['year_max'].max()}")

    search = st.text_input(
        "Search variables",
        placeholder="e.g. 'youth cannabis use', 'cocaine price', 'binge drinking women'",
    )
    if search:
        from chat_engine import semantic_search_results
        with st.spinner("Searching…"):
            hits = semantic_search_results(search, top_k=20)
        if hits:
            hits_df = pd.DataFrame(hits)
            hits_df["score"] = hits_df["score"].round(3)
            hits_df = hits_df.rename(columns={
                "score": "Relevance",
                "report": "Report",
                "table_id": "Table",
                "table_title": "Title",
                "variable": "Variable",
                "y_min": "From",
                "y_max": "To",
            })
            st.markdown(f"**{len(hits_df)} matches**")
            st.dataframe(
                hits_df[["Relevance", "Report", "Table", "Title", "Variable", "From", "To"]],
                use_container_width=True,
            )
        else:
            st.info("No matching variables found.")

    st.subheader("All Tables")
    st.dataframe(
        catalog[["report", "report_label", "table_id", "table_title", "variables", "year_min", "year_max", "records"]],
        use_container_width=True,
        height=600,
    )

    st.subheader("Quick Plot")
    selected_report = st.selectbox("Report", catalog["report"].unique(), format_func=lambda x: f"{x} — {REPORT_LABELS.get(x, '')}")
    report_tables = catalog[catalog["report"] == selected_report]
    table_options = report_tables.apply(
        lambda r: f"Table {r['table_id']}: {str(r['table_title'])[:80]}" if r['table_title'] else f"Table {r['table_id']}",
        axis=1,
    ).tolist()
    table_ids = report_tables["table_id"].tolist()

    table_choice = st.selectbox("Table", range(len(table_options)), format_func=lambda i: table_options[i], key="cat_table")
    selected_table_id = table_ids[table_choice]

    data = query(f"""
        SELECT year, variable, value FROM timeseries
        WHERE report='{selected_report}' AND table_id='{selected_table_id}'
        ORDER BY year
    """)

    if len(data) > 0:
        fig = px.line(data, x="year", y="value", color="variable", title=f"Table {selected_table_id}")
        fig.update_layout(height=500, showlegend=True, legend=dict(orientation="h", yanchor="top", y=-0.2))
        st.plotly_chart(fig, use_container_width=True)
