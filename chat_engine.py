"""
CAN Chat Engine — Ask Your Data Questions
===========================================
Uses GPT-4o to translate natural language questions into SQL,
runs them against DuckDB, and generates explanations + chart specs.

Two-step approach (with semantic search):
  1. User question → embedded → cosine similarity finds the most relevant variables
  2. LLM writes SQL using real variable names → executes → LLM explains results
"""

import os
import json
import duckdb
import numpy as np
from openai import OpenAI

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "can_data.duckdb")
EMBEDDINGS_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "variable_embeddings.npz")
CATALOG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "variable_catalog.json")

# Read API key from env or from file
_api_key = os.environ.get("OPENAI_API_KEY", "")
if not _api_key:
    _key_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "openai_key.txt")
    if os.path.exists(_key_file):
        with open(_key_file) as f:
            _api_key = f.read().strip()

client = OpenAI(api_key=_api_key)

# ── Semantic search infrastructure ──────────────────────────

_embeddings_matrix = None
_catalog = None


def _load_embeddings():
    """Load pre-computed embeddings and catalog. Cached after first call."""
    global _embeddings_matrix, _catalog
    if _embeddings_matrix is not None:
        return

    data = np.load(EMBEDDINGS_PATH)
    _embeddings_matrix = data["embeddings"]  # shape: (N, 1536)

    with open(CATALOG_PATH, "r", encoding="utf-8") as f:
        _catalog = json.load(f)


def semantic_search(query: str, top_k: int = 25) -> str:
    """
    Embed the user's question and find the most relevant variables
    via cosine similarity against pre-computed variable embeddings.
    Returns formatted list of matching variables for the LLM.
    """
    _load_embeddings()

    # Embed the query
    response = client.embeddings.create(
        input=[query],
        model="text-embedding-3-small",
    )
    query_vec = np.array(response.data[0].embedding, dtype=np.float32)

    # Cosine similarity (embeddings are normalized by OpenAI)
    scores = _embeddings_matrix @ query_vec
    top_indices = np.argsort(scores)[::-1][:top_k]

    # Build results, filtering out generic col_ variables
    lines = []
    seen = set()
    for idx in top_indices:
        entry = _catalog[idx]
        var = entry["variable"]

        # Skip generic column names
        if "__col_" in var:
            continue

        # Deduplicate same variable from same report
        key = (entry["report"], entry["table_id"], var)
        if key in seen:
            continue
        seen.add(key)

        score = scores[idx]
        title_short = (entry["table_title"] or "")[:70]
        lines.append(
            f"[{score:.3f}] {entry['report']} | table {entry['table_id']} | "
            f"{var} | {entry['y_min']}-{entry['y_max']} | {title_short}"
        )

    return "\n".join(lines) if lines else "No matching variables found."


# ── Report source mapping ───────────────────────────────────

REPORT_SOURCES = {
    "CAN-233": {
        "title": "Narkotikaprisutvecklingen 1988–2024",
        "description": "Drug price trends",
    },
    "CAN-234": {
        "title": "Självrapporterade rök- och snusvanor 2003–2024",
        "description": "Self-reported smoking & snus habits",
    },
    "CAN-235": {
        "title": "Narkotikautvecklingen i Sverige",
        "description": "Drug seizures, crime & health stats",
    },
    "CAN-236": {
        "title": "Alkoholkonsumtionen i Sverige 2001–2024",
        "description": "Total alcohol consumption",
    },
    "CAN-237": {
        "title": "Självrapporterade alkoholvanor 2004–2024",
        "description": "Self-reported alcohol habits",
    },
    "CAN-238": {
        "title": "Total konsumtion av tobaks- och nikotinprodukter 2003–2024",
        "description": "Tobacco & nicotine consumption",
    },
    "CAN-239": {
        "title": "CANs nationella skolundersökning 2025",
        "description": "Youth school survey",
    },
}


def get_source_citations(data) -> str:
    """Extract unique report sources from query results and format as citations with table-level detail."""
    if data is None or len(data) == 0:
        return ""

    reports_used = set()
    table_details = []

    if "report" in data.columns:
        reports_used = set(data["report"].dropna().unique())

    for col in data.columns:
        for report_id in REPORT_SOURCES:
            if report_id.lower() in str(data[col].values).lower():
                reports_used.add(report_id)

    if "table_id" in data.columns and "table_title" in data.columns:
        for _, row in data[["report", "table_id", "table_title"]].drop_duplicates().iterrows():
            if row["report"] and row["table_id"] and row["table_title"]:
                table_details.append((str(row["report"]), str(row["table_id"]), str(row["table_title"])))

    if "kpi_title" in data.columns or "municipality_name" in data.columns:
        reports_used.add("KOLADA")

    if not reports_used:
        return ""

    citations = []
    for report_id in sorted(reports_used):
        if report_id == "KOLADA":
            citations.append(f"📄 **KOLADA**: Swedish Municipal Statistics API (api.kolada.se)")
        elif report_id in REPORT_SOURCES:
            src = REPORT_SOURCES[report_id]
            citations.append(f"📄 **{report_id}**: {src['title']}")
            tables_for_report = [(tid, tt) for r, tid, tt in table_details if r == report_id]
            for tid, tt in sorted(set(tables_for_report)):
                title_short = tt[:100] if len(tt) > 100 else tt
                citations.append(f"   ↳ Tabell {tid}: {title_short}")

    return "\n".join(citations)


# ── Schema overview for LLM context ────────────────────────

SCHEMA_OVERVIEW = """DATABASE: CAN — 60 years of Swedish substance use data

Table: timeseries (LONG FORMAT — each row = one variable, one year, one value)
Columns: year (INT), variable (VARCHAR), value (DOUBLE), report (VARCHAR), table_id (VARCHAR), table_title (VARCHAR), topic (VARCHAR)

REPORTS:
- CAN-233: Drug street/wholesale PRICES in SEK. Variables prefixed with substance name: cocaine__realprisjusterad_median, marijuana__pris_median, etc. Years 1988-2024.
- CAN-234: Self-reported SMOKING & SNUS habits among adults 17-84. By age/gender. Unit: %. Years 2003-2024.
- CAN-235: Drug SEIZURES, crime stats, health/mortality. Unit: varies (counts, %, rates). Years 1965-2024.
- CAN-236: Total ALCOHOL CONSUMPTION in Sweden. Unit: liters pure alcohol per capita. Years 2001-2024.
- CAN-237: Self-reported ALCOHOL HABITS. Drinking frequency, risk consumption. Unit: %. Years 2002-2024.
- CAN-238: Total TOBACCO & NICOTINE consumption. Unit: per capita counts. Years 2003-2024.
- CAN-239: YOUTH SCHOOL SURVEY. Grade 9 + gymnasium year 2. Unit: %. Years 1971-2025.

Table: kolada (MUNICIPAL-LEVEL DATA from KOLADA API)
Columns: kpi_id (VARCHAR), municipality_id (VARCHAR), year (INT), gender (VARCHAR: T=total, K=women, M=men), value (DOUBLE), kpi_title (VARCHAR), municipality_name (VARCHAR)

KOLADA KPIs: N07544 (Drug offenses per 100k), N33820 (Youth mental ill-health %), N03921 (Youth unemployment %), N17441 (Gymnasium completion %), N00621 (Drug trafficking problems %), N00620 (Alcohol/drug-affected persons %), and more.
Municipalities: Stockholm, Malmö, Göteborg, Uppsala, Linköping, Örebro, Jönköping, Kalmar, Karlskrona, Halmstad. Years: 2015-2024."""


# ── Main chat function ──────────────────────────────────────

def ask_data(question: str, conversation_history: list = None) -> dict:
    """
    Two-step process with semantic search:
    Step 1: Semantic search finds relevant variables → LLM writes SQL
    Step 2: Execute SQL → send results back to LLM for natural language answer
    """

    try:
        # ── STEP 1: Semantic search + SQL generation ────────
        variable_matches = semantic_search(question, top_k=30)

        step1_prompt = f"""You are a data analyst for CAN (Swedish Council for Alcohol and Drug Information).

{SCHEMA_OVERVIEW}

User question: "{question}"

I used semantic search to find the most relevant variables in the database.
The results are ranked by relevance score (higher = more relevant):

{variable_matches}

Write a SQL query using the EXACT variable names from above.
IMPORTANT: Always include report, table_id, and table_title columns in your SELECT.
Choose the MOST relevant variables for the user's question — you don't need to use all of them.
For cross-domain questions, pick the best variable from EACH relevant report.

Respond in JSON:
{{
    "sql": "SELECT year, variable, value, report, table_id, table_title FROM timeseries WHERE ... ORDER BY year",
    "chart": {{
        "type": "line",
        "x": "year",
        "y": "value",
        "color": "variable",
        "title": "Chart title"
    }}
}}

SQL RULES:
- Use EXACT variable names from the search results. Do NOT invent names.
- Table is 'timeseries' with columns: year, variable, value, report, table_id, table_title
- For comparisons across reports, use CASE WHEN to give readable labels:
  SELECT year, CASE WHEN variable='x' THEN 'Readable Label' END as variable, value, report, table_id, table_title FROM timeseries WHERE (...) ORDER BY year
- Always include report AND table_id in WHERE clauses
- IMPORTANT: table_id is just the number, e.g. table_id='4', NOT 'table 4'
- ORDER BY year, LIMIT 500
- For the kolada table use: SELECT year, kpi_title as variable, value, 'KOLADA' as report, kpi_id as table_id, kpi_title as table_title FROM kolada WHERE ...
- For color grouping, ensure 'variable' column has distinct readable values
- UNLESS the user asks about a specific time period, focus on the LAST 15 years (year >= 2010). This makes charts readable and data more relevant.
- Avoid variables that represent "never used" or "ej svar" (no answer) — they dominate the scale (90%+) and make other trends invisible on charts. Prefer variables showing ACTUAL usage/consumption.
- Pick 2-4 variables max for the chart — too many lines makes it unreadable."""

        step1_resp = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": step1_prompt}],
            temperature=0.1,
            max_tokens=1500,
            response_format={"type": "json_object"},
        )
        step1_result = json.loads(step1_resp.choices[0].message.content)
        sql = step1_result.get("sql", "")
        chart_spec = step1_result.get("chart")

        if not sql:
            return {
                "answer": "I couldn't formulate a query. Try rephrasing your question.",
                "sql": None, "data": None, "chart_spec": None, "sources": "", "error": None,
            }

        # ── Execute SQL ─────────────────────────────────────
        con = duckdb.connect(DB_PATH, read_only=True)
        try:
            data = con.execute(sql).fetchdf()
        except Exception as e:
            return {
                "answer": f"SQL error: {str(e)}\n\nThe query was:\n```sql\n{sql}\n```\n\nTop matching variables:\n{variable_matches[:500]}",
                "sql": sql, "data": None, "chart_spec": None, "sources": "", "error": str(e),
            }
        finally:
            con.close()

        if len(data) == 0:
            return {
                "answer": f"No results found. The most relevant variables I found were:\n{variable_matches[:500]}\n\nTry rephrasing your question.",
                "sql": sql, "data": data, "chart_spec": None, "sources": "", "error": None,
            }

        # ── STEP 2: Generate answer from actual results ─────
        data_preview = data.head(50).to_markdown(index=False)

        step2_prompt = f"""You are a data analyst for CAN (Swedish Council for Alcohol and Drug Information).

User asked: "{question}"

SQL query:
```sql
{sql}
```

Results ({len(data)} rows):
{data_preview}

Write a clear, insightful answer. RULES:
- Use SPECIFIC numbers from the results — exact values, years, percentages
- Point out trends, peaks, troughs, and surprises
- Compare across time periods when relevant
- Keep it concise: 3-5 sentences
- Do NOT use placeholders — only real numbers from the data above
- When citing a number, add the report source in parentheses, e.g. "cocaine seizures reached 5,200 (CAN-235)"
- If data comes from multiple reports, cite each one where relevant

UNITS BY REPORT (use the correct unit when presenting numbers):
- CAN-233: Values are PRICES in SEK (Swedish kronor). E.g. "800 SEK per gram"
- CAN-234: Values are mostly PERCENTAGES (%). E.g. "9.7% smoked daily".
- CAN-235: MIXED — seizure counts (antal), percentages (andel), and rates per 100,000. Check variable name: "antal"=count, "andel"=percentage.
- CAN-236: Values are LITERS of pure alcohol per capita. E.g. "3.19 liters per capita"
- CAN-237: Values are PERCENTAGES (%). E.g. "4.8% report risk consumption"
- CAN-238: Values are PER CAPITA counts. Cigarettes per person, snus cans per person, etc.
- CAN-239: Values are PERCENTAGES (%). E.g. "11.8% have tried cannabis"
- KOLADA: Check the kpi_title for the unit. "per 100,000" = rate, "(%)" = percentage. Always mention which municipality.
- Round values to 1 decimal place for readability."""

        step2_resp = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": step2_prompt}],
            temperature=0.3,
            max_tokens=800,
        )

        answer = step2_resp.choices[0].message.content
        sources = get_source_citations(data)

        return {
            "answer": answer,
            "sql": sql,
            "data": data,
            "chart_spec": chart_spec,
            "sources": sources,
            "error": None,
        }

    except Exception as e:
        return {
            "answer": f"Error: {str(e)}",
            "sql": None, "data": None, "chart_spec": None, "sources": "", "error": str(e),
        }
