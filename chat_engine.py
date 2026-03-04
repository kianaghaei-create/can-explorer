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
import re
import json
import duckdb
import numpy as np
import pandas as pd
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


def semantic_search_results(query: str, top_k: int = 25) -> list:
    """
    Embed the user's question and find the most relevant variables
    via cosine similarity. Returns a list of dicts with structured data.
    """
    _load_embeddings()

    response = client.embeddings.create(
        input=[query],
        model="text-embedding-3-small",
    )
    query_vec = np.array(response.data[0].embedding, dtype=np.float32)

    scores = _embeddings_matrix @ query_vec
    top_indices = np.argsort(scores)[::-1][:top_k * 2]  # oversample before dedup

    results = []
    seen = set()
    for idx in top_indices:
        if len(results) >= top_k:
            break
        entry = _catalog[idx]
        var = entry["variable"]

        if "__col_" in var:
            continue

        key = (entry["report"], entry["table_id"], var)
        if key in seen:
            continue
        seen.add(key)

        results.append({
            "report": entry["report"],
            "table_id": entry["table_id"],
            "table_title": entry.get("table_title", ""),
            "variable": var,
            "y_min": entry.get("y_min"),
            "y_max": entry.get("y_max"),
            "score": float(scores[idx]),
        })

    return results


def semantic_search(query: str, top_k: int = 25) -> str:
    """
    Returns formatted string of top matching variables for the LLM prompt.
    """
    results = semantic_search_results(query, top_k=top_k)
    if not results:
        return "No matching variables found."

    lines = []
    for r in results:
        title_short = (r["table_title"] or "")[:70]
        lines.append(
            f"[{r['score']:.3f}] {r['report']} | table {r['table_id']} | "
            f"{r['variable']} | {r['y_min']}-{r['y_max']} | {title_short}"
        )
    return "\n".join(lines)


def decompose_query(question: str) -> list:
    """
    Use gpt-4o-mini to split a multi-topic question into per-topic sub-queries.
    Single-topic questions return a list with just the original question.
    """
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": f"""You are helping decompose a data question into focused search queries.

Extract the distinct topics/substances from this question and return one short search phrase per topic.
Each phrase should be self-contained and specific enough to retrieve relevant variables.

Question: "{question}"

Examples:
- "How has youth alcohol and narcotics consumption changed?" → ["youth alcohol consumption", "youth narcotics drug use"]
- "Compare cocaine prices with cocaine seizures" → ["cocaine street price SEK", "cocaine seizures arrests"]
- "What happened to smoking rates?" → ["smoking rates cigarettes"]
- "Show alcohol, cannabis and amphetamine trends" → ["alcohol consumption", "cannabis marijuana use", "amphetamine use"]

Return JSON: {{"topics": ["phrase1", "phrase2", ...]}}
Return a single topic if the question is about one thing.
always reply in swedish"""}],
        temperature=0,
        max_tokens=200,
        response_format={"type": "json_object"},
    )
    result = json.loads(response.choices[0].message.content)
    topics = result.get("topics", [question])
    return topics if topics else [question]


def multi_topic_search(question: str, top_k_per_topic: int = 15) -> list:
    """
    Decompose the question into topics, run semantic search per topic,
    and merge results — guaranteeing coverage of all topics.
    Falls back to a single search if decomposition fails.
    """
    try:
        topics = decompose_query(question)
    except Exception:
        topics = [question]

    seen = set()
    merged = []

    for topic in topics:
        hits = semantic_search_results(topic, top_k=top_k_per_topic)
        for hit in hits:
            key = (hit["report"], hit["table_id"], hit["variable"])
            if key not in seen:
                seen.add(key)
                hit["topic"] = topic  # tag which sub-query found it
                merged.append(hit)

    # Sort merged list by score descending
    merged.sort(key=lambda x: x["score"], reverse=True)
    return merged


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
- CAN-237: Self-reported ALCOHOL HABITS among ADULTS aged 17-84. Drinking frequency, risk consumption. Unit: %. Years 2002-2024. NOT for youth questions.
- CAN-238: Total TOBACCO & NICOTINE consumption. Unit: per capita counts. Years 2003-2024.
- CAN-239: YOUTH SCHOOL SURVEY (ungdomar/unga). Grade 9 + gymnasium year 2. Alcohol, drugs, smoking among young people. Unit: %. Years 1971-2025. USE THIS for all youth/teenager questions.

Table: kolada (MUNICIPAL-LEVEL DATA from KOLADA API)
Columns: kpi_id (VARCHAR), municipality_id (VARCHAR), year (INT), gender (VARCHAR: T=total, K=women, M=men), value (DOUBLE), kpi_title (VARCHAR), municipality_name (VARCHAR)

KOLADA KPIs: N07544 (Drug offenses per 100k), N33820 (Youth mental ill-health %), N03921 (Youth unemployment %), N17441 (Gymnasium completion %), N00621 (Drug trafficking problems %), N00620 (Alcohol/drug-affected persons %), and more.
Municipalities: Stockholm, Malmö, Göteborg, Uppsala, Linköping, Örebro, Jönköping, Kalmar, Karlskrona, Halmstad. Years: 2015-2024."""


# ── Fallback SQL builder (handles mangled/truncated variable names) ──────────

_REPORT_RE = re.compile(r"report\s*=\s*'([^']+)'", re.IGNORECASE)
_TABLE_RE  = re.compile(r"table_id\s*=\s*'([^']+)'", re.IGNORECASE)
_YEAR_RE   = re.compile(r"year\s*>=\s*(\d{4})", re.IGNORECASE)


def _build_fallback_sql(failed_sql: str) -> str | None:
    """
    When an exact-variable-name SQL returns 0 rows (usually because the LLM
    truncated a very long Swedish variable name), build a broader fallback:
    keep report + table_id + year filter but drop the variable constraint,
    limiting to '_alla' aggregate variables while excluding metadata variables.
    """
    report_m = _REPORT_RE.search(failed_sql)
    table_m  = _TABLE_RE.search(failed_sql)
    if not report_m or not table_m:
        return None
    report   = report_m.group(1)
    table_id = table_m.group(1)
    year_m   = _YEAR_RE.search(failed_sql)
    year     = int(year_m.group(1)) if year_m else 2000
    return (
        f"SELECT year, variable, value, report, table_id, table_title "
        f"FROM timeseries "
        f"WHERE report = '{report}' AND table_id = '{table_id}' "
        f"AND variable LIKE '%_alla' "
        f"AND variable NOT LIKE '%formulär%' "
        f"AND variable NOT LIKE '%deltagande%' "
        f"AND variable NOT LIKE '%bortsorterade%' "
        f"AND variable NOT LIKE '%bortfall%' "
        f"AND variable NOT LIKE '%svarsfrekvens%' "
        f"AND year >= {year} "
        f"ORDER BY year LIMIT 500"
    )


# ── Metadata variable filter ─────────────────────────────────
# These are administrative/survey-logistics variables, never substance-use data.

_METADATA_VAR_RE = re.compile(
    r'(formulär|deltagande|bortsorterade|bortfall|svarsfrekvens|bearbetade|'
    r'antal_elever|antal_skolor|antal_klasser|medverkande)',
    re.IGNORECASE,
)


def _filter_metadata_hits(hits: list) -> list:
    """Remove survey-administration variables — they are never meaningful answers."""
    return [h for h in hits if not _METADATA_VAR_RE.search(h["variable"])]


# ── Gender-aware hit filtering ───────────────────────────────

_GENDER_QUESTION_RE = re.compile(
    r'\b(pojkar|flickor|m[aä]n|kvinnor|boys|girls|men|women|gender|k[oö]n|'
    r'killar|tjejer)\b',
    re.IGNORECASE,
)

# Suffixes that mark gender-specific variables
_GENDER_VAR_SUFFIXES = ('_fl', '_po', '_men', '_kv')


def _filter_gender_hits(hits: list, question: str) -> list:
    """
    When the question does NOT mention gender, suppress gender-specific
    variables (_fl, _po, _men, _kv) for any concept that also has an
    '_alla' aggregate counterpart in the hit list.
    Keeps gender-specific variables only when no '_alla' twin exists.
    """
    if _GENDER_QUESTION_RE.search(question):
        return hits  # question asks about gender — show everything

    # Collect (report, table_id, base_name) for every '_alla' variable
    has_alla: set = set()
    for hit in hits:
        var = hit["variable"]
        if var.endswith("_alla"):
            base = var[:-5]  # strip '_alla'
            has_alla.add((hit["report"], hit["table_id"], base))

    if not has_alla:
        return hits  # no aggregates present, nothing to filter

    filtered = []
    for hit in hits:
        var = hit["variable"]
        skip = False
        for suffix in _GENDER_VAR_SUFFIXES:
            if var.endswith(suffix):
                base = var[: -len(suffix)]
                if (hit["report"], hit["table_id"], base) in has_alla:
                    skip = True  # drop — the '_alla' twin is present
                break
        if not skip:
            filtered.append(hit)

    return filtered


# ── Enumeration detection + table expansion ──────────────────

_ENUMERATION_RE = re.compile(
    r'\b(vilka\s+specifika|vilka\s+typer|vilken\s+typ|vilka\s+\w+\s+har|'
    r'which\s+specific|which\s+types?|list\s+all|alla\s+kategorier|'
    r'alla\s+incidenter|specifika\s+incidenter)\b',
    re.IGNORECASE,
)


def _expand_enumeration_hits(hits: list, question: str) -> tuple:
    """
    When the question asks 'which specific X', expand hits to include ALL
    variables from the top-matching table (all categories + all genders).
    Returns (hits, is_enumeration).
    """
    if not _ENUMERATION_RE.search(question) or not hits:
        return hits, False

    top = hits[0]
    report, table_id = top["report"], top["table_id"]

    try:
        con = duckdb.connect(DB_PATH, read_only=True)
        rows = con.execute(
            """SELECT DISTINCT variable,
                      MIN(year) AS y_min,
                      MAX(year) AS y_max
               FROM timeseries
               WHERE report = ? AND table_id = ?
               GROUP BY variable""",
            [report, table_id],
        ).fetchdf()
        con.close()
    except Exception:
        return hits, False

    if rows.empty or len(rows) < 5:
        return hits, False

    table_title = top.get("table_title", "")
    expanded = []
    for _, row in rows.iterrows():
        var = str(row["variable"])
        if any(skip in var for skip in ("__col_", "ej_svar", "n=", "procent")):
            continue
        try:
            y_min = int(row["y_min"]) if pd.notna(row["y_min"]) else None
            y_max = int(row["y_max"]) if pd.notna(row["y_max"]) else None
        except (ValueError, TypeError):
            y_min = y_max = None
        expanded.append({
            "report": report,
            "table_id": table_id,
            "table_title": table_title,
            "variable": var,
            "y_min": y_min,
            "y_max": y_max,
            "score": 1.0,
            "topic": top.get("topic", question),
        })

    return (expanded if expanded else hits), bool(expanded)


# ── Main chat function ──────────────────────────────────────

def ask_data(question: str, conversation_history: list = None) -> dict:
    """
    Two-step process with semantic search:
    Step 1: Semantic search finds relevant variables → LLM writes SQL
    Step 2: Execute SQL → send results back to LLM for natural language answer
    """

    try:
        # ── STEP 1: Multi-topic semantic search + SQL generation ────────
        hits = multi_topic_search(question, top_k_per_topic=15)
        hits = _filter_metadata_hits(hits)              # strip survey-admin variables
        hits, is_enumeration = _expand_enumeration_hits(hits, question)
        if not is_enumeration:
            hits = _filter_gender_hits(hits, question)  # prefer _alla unless gender asked
        variable_matches = "\n".join(
            f"[{r['score']:.3f}] {r['report']} | table {r['table_id']} | "
            f"{r['variable']} | {r['y_min']}-{r['y_max']} | {(r['table_title'] or '')[:70]}"
            for r in hits
        ) if hits else "No matching variables found."

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
        "title": "Chart title in Swedish"
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
- YEAR FILTER: If the user mentions a specific year or time period (e.g. "since 2000", "from 1990", "between 2005 and 2015"), use that year range exactly. Otherwise default to year >= 2010.
- GENDER: Unless the user explicitly asks about gender differences or a specific gender (men, women, boys, girls, män, kvinnor, pojkar, flickor), ALWAYS prefer aggregate/total variables that contain "alla" in their name over gender-specific variables. Only include gender-disaggregated variables when the question specifically asks for a gender comparison or breakdown.
- Avoid "ej svar" (no answer) variables — they are not useful.
- MULTI-TOPIC QUESTIONS: If the user asks about multiple substances or topics (e.g. "alcohol AND narcotics", "smoking and cannabis"), you MUST include at least 1-2 variables for EACH topic. Do not drop a topic entirely. Use up to 6 variables total when covering multiple topics.
- For single-topic questions, pick 2-4 variables max for readability.
- If including a "never used" variable (e.g. "ingen_gång", "dricker_inte") alongside usage variables, that's fine — the chart will auto-detect the scale difference and use dual Y-axes.
- ENUMERATION/SLICER: If the variable list above contains more than 10 variables all from the same table (same report + table_id), this is an enumeration question. In that case:
  * Include ALL listed variables in the SQL WHERE clause (do not drop any)
  * Do NOT apply a year filter — fetch ALL available years
  * Set chart type to "slicer" in the JSON response
  * Increase LIMIT to 2000
- YOUTH: For any question about youth, teenagers, ungdomar, unga, young people, grade 9 (åk 9/årskurs 9), gymnasium, or school surveys, ALWAYS prefer CAN-239 over any other report. CAN-237 covers adults aged 17-84 and is NOT appropriate for youth questions."""

        step1_resp = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": step1_prompt}],
            temperature=0,
            seed=42,
            max_tokens=1500,
            response_format={"type": "json_object"},
        )
        step1_result = json.loads(step1_resp.choices[0].message.content)
        sql = step1_result.get("sql", "")
        chart_spec = step1_result.get("chart")

        if not sql:
            return {
                "answer": "Kunde inte formulera en databasfråga. Försök omformulera din fråga.",
                "sql": None, "data": None, "chart_spec": None, "sources": "", "error": None,
                "semantic_hits": hits,
                "is_enumeration": is_enumeration,
            }

        # ── Execute SQL ─────────────────────────────────────
        con = duckdb.connect(DB_PATH, read_only=True)
        try:
            data = con.execute(sql).fetchdf()
        except Exception as e:
            return {
                "answer": f"SQL-fel: {str(e)}\n\nFrågan var:\n```sql\n{sql}\n```",
                "sql": sql, "data": None, "chart_spec": None, "sources": "", "error": str(e),
                "semantic_hits": hits,
                "is_enumeration": is_enumeration,
            }
        finally:
            con.close()

        # ── Fallback: retry without variable filter if 0 rows ────────────────
        # (handles cases where LLM truncated a very long variable name in SQL)
        if len(data) == 0:
            fallback_sql = _build_fallback_sql(sql)
            if fallback_sql:
                try:
                    con2 = duckdb.connect(DB_PATH, read_only=True)
                    data_fb = con2.execute(fallback_sql).fetchdf()
                    con2.close()
                    if len(data_fb) > 0:
                        data = data_fb
                        sql = fallback_sql  # show corrected SQL in expander
                except Exception:
                    pass

        if len(data) == 0:
            return {
                "answer": "Inga resultat hittades. De mest relevanta variablerna jag fann:\n"
                          f"{variable_matches[:500]}\n\nFörsök omformulera din fråga.",
                "sql": sql, "data": data, "chart_spec": None, "sources": "", "error": None,
                "semantic_hits": hits,
                "is_enumeration": is_enumeration,
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

Write a clear, insightful answer in SWEDISH. RULES:
- ALWAYS reply in Swedish regardless of the language of the question
- Use SPECIFIC numbers from the results — exact values, years, percentages
- Point out trends, peaks, troughs, and surprises
- Compare across time periods when relevant
- Keep it concise: 3-5 sentences
- Do NOT use placeholders — only real numbers from the data above
- When citing a number, add the report source in parentheses, e.g. "cocaine seizures reached 5,200 (CAN-235)"
- If data comes from multiple reports, cite each one where relevant
- GENDER: If the data contains gender-specific variables (pojkar/flickor/män/kvinnor), you MUST explicitly state which gender group(s) are shown. Do not present gender-specific results as if they represent all students or the general population.

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
            temperature=0,
            seed=42,
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
            "semantic_hits": hits,
            "is_enumeration": is_enumeration,
        }

    except Exception as e:
        return {
            "answer": f"Ett fel uppstod: {str(e)}",
            "sql": None, "data": None, "chart_spec": None, "sources": "", "error": str(e),
            "semantic_hits": [],
            "is_enumeration": False,
        }
