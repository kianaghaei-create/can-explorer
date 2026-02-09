"""
CAN Chat Engine — Ask Your Data Questions
===========================================
Uses GPT-4o to translate natural language questions into SQL,
runs them against DuckDB, and generates explanations + chart specs.

Three-step approach:
  1. LLM picks search keywords → we find REAL variable names from the database
  2. LLM writes SQL using the real variable names
  3. We run the SQL, then send results BACK to the LLM for a natural language answer
"""

import os
import json
import duckdb
from openai import OpenAI

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "can_data.duckdb")

client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY", ""))

# Map report IDs to source file names and descriptions
REPORT_SOURCES = {
    "CAN-233": {
        "title": "Narkotikaprisutvecklingen 1988–2024",
        "file": "can-rapport-233-narkotikaprisutvecklingen-1988-2024-tabellbilaga.xlsx",
        "description": "Drug price trends",
    },
    "CAN-234": {
        "title": "Självrapporterade rök- och snusvanor 2003–2024",
        "file": "can-rapport-234-sjalvrapporterade-rok-och-snusvanor-2003-2024-tabellbilaga.xlsx",
        "description": "Self-reported smoking & snus habits",
    },
    "CAN-235": {
        "title": "Narkotikautvecklingen i Sverige",
        "file": "can-rapport-235-narkotikautvecklingen-i-sverige-tabellbilaga.xlsx",
        "description": "Drug seizures, crime & health stats",
    },
    "CAN-236": {
        "title": "Alkoholkonsumtionen i Sverige 2001–2024",
        "file": "can-rapport-236-alkoholkonsumtionen-i-sverige-2001-2024-tabellbilaga.xlsx",
        "description": "Total alcohol consumption",
    },
    "CAN-237": {
        "title": "Självrapporterade alkoholvanor 2004–2024",
        "file": "can-rapport-237-sjalvrapporterade-alkoholvanor-i-sverige-2004-2024-tabellbilaga.xlsx",
        "description": "Self-reported alcohol habits",
    },
    "CAN-238": {
        "title": "Total konsumtion av tobaks- och nikotinprodukter 2003–2024",
        "file": "can-rapport-238-den-totala-konsumtionen-av-tobaks-och-nikotinprodukter-i-sverige-2003-2024-tabellbilaga.xlsx",
        "description": "Tobacco & nicotine consumption",
    },
    "CAN-239": {
        "title": "CANs nationella skolundersökning 2025",
        "file": "can-rapport-239-cans-nationella-skolundersokning-2025-tabellbilaga.xlsx",
        "description": "Youth school survey",
    },
}


def get_source_citations(data) -> str:
    """Extract unique report sources from query results and format as citations."""
    if data is None or len(data) == 0:
        return ""

    reports_used = set()
    # Check for 'report' column in the data
    if "report" in data.columns:
        reports_used = set(data["report"].dropna().unique())
    # Also check if report IDs appear in other columns (from CASE WHEN aliases)
    for col in data.columns:
        for report_id in REPORT_SOURCES:
            if report_id.lower() in str(data[col].values).lower():
                reports_used.add(report_id)

    if not reports_used:
        return ""

    # Also check if kolada table was used
    if "kpi_title" in data.columns or "municipality_name" in data.columns:
        reports_used.add("KOLADA")

    citations = []
    for report_id in sorted(reports_used):
        if report_id == "KOLADA":
            citations.append(f"📄 **KOLADA**: Swedish Municipal Statistics API (api.kolada.se)")
        elif report_id in REPORT_SOURCES:
            src = REPORT_SOURCES[report_id]
            citations.append(f"📄 **{report_id}**: {src['title']} ({src['description']})")

    return "\n".join(citations)


SCHEMA_OVERVIEW = """DATABASE: CAN — 60 years of Swedish substance use data

Table: timeseries (LONG FORMAT — each row = one variable, one year, one value)
Columns: year (INT), variable (VARCHAR), value (DOUBLE), report (VARCHAR), table_id (VARCHAR), table_title (VARCHAR), topic (VARCHAR)

REPORTS:
- CAN-233: Drug street/wholesale PRICES in SEK. Variables are prefixed with substance name: cocaine__realprisjusterad_median, marijuana__pris_median, hashish__ursprungligt_pris_median, etc. table_id: 1=hashish, 2=marijuana, 3=amphetamine, 4=cocaine, 5=white_heroin, 6=brown_heroin, 7=ecstasy, 8=LSD. Years 1988-2024.
- CAN-234: Self-reported SMOKING & SNUS habits among adults 17-84. By age/gender. Years 2003-2024.
- CAN-235: Drug SEIZURES, crime stats, health/mortality. table_id='1' has seizure counts by substance (kokain_antal, amfetamin_antal, cannabis_antal, etc). Years 1965-2024.
- CAN-236: Total ALCOHOL CONSUMPTION in Sweden (liters pure alcohol). Registered vs unregistered. Years 2001-2024.
- CAN-237: Self-reported ALCOHOL HABITS. Drinking frequency, risk consumption. By age/gender. Years 2002-2024.
- CAN-238: Total TOBACCO & NICOTINE consumption. Cigarettes, snus, e-liquid per capita. Years 2003-2024.
- CAN-239: YOUTH SCHOOL SURVEY. Grade 9 + gymnasium year 2. Alcohol, tobacco, drugs, gambling. By gender (_pojkar=boys, _flickor=girls, _alla=all). Years 1971-2025.

SWEDISH TERMS: kokain=cocaine, amfetamin=amphetamine, hasch=hashish, cannabis=cannabis, heroin=heroin, ecstasy=ecstasy, beslag=seizures, antal=count, pris=price, alkohol=alcohol, rökt/rokt=smoked, snusat=snus use, druckit=drank, narkotika=drugs, dagligen=daily, pojkar=boys, flickor=girls, alla=all, man/män=men, kvinnor=women

Table: kolada (MUNICIPAL-LEVEL DATA from KOLADA API — Swedish municipal statistics)
Columns: kpi_id (VARCHAR), municipality_id (VARCHAR), year (INT), gender (VARCHAR: T=total, K=women, M=men), value (DOUBLE), kpi_title (VARCHAR), municipality_name (VARCHAR)

KOLADA KPIs available:
- N07544: Drug offenses per 100,000 inhabitants
- N33820: Mental ill-health among children/youth 0-19 (%)
- N03921: Youth unemployment 16-24 (%)
- N03922: Youth openly unemployed 16-24 (%)
- N17441: Gymnasium completion rate within 3 years (%)
- N17473: University eligibility within 3 years (%)
- N00621: Few problems with drug trafficking (citizen survey %)
- N00620: Few problems with alcohol/drug-affected persons (%)
- N07628: Problems with substance-affected persons outdoors (%)
- N02280: Unemployment 20-64 (%)

Municipalities: Stockholm, Malmö, Göteborg, Uppsala, Linköping, Örebro, Jönköping, Kalmar, Karlskrona, Halmstad
Years: 2015-2024. Use gender='T' for totals unless user asks for gender breakdown."""


def search_variables(keywords: list) -> str:
    """Search the database for variables matching keywords. Returns formatted results."""
    con = duckdb.connect(DB_PATH, read_only=True)

    all_results = []
    for kw in keywords:
        kw_clean = kw.lower().strip()
        if not kw_clean:
            continue
        results = con.execute(f"""
            SELECT DISTINCT report, table_id, table_title, variable,
                   MIN(year) as y_min, MAX(year) as y_max
            FROM timeseries
            WHERE LOWER(variable) LIKE '%{kw_clean}%'
               OR LOWER(table_title) LIKE '%{kw_clean}%'
            GROUP BY report, table_id, table_title, variable
            ORDER BY report, table_id
            LIMIT 30
        """).fetchdf()
        all_results.append(results)

    con.close()

    if not all_results:
        return "No matching variables found."

    import pandas as pd
    combined = pd.concat(all_results).drop_duplicates()

    if len(combined) == 0:
        return "No matching variables found."

    lines = []
    for _, row in combined.iterrows():
        title_short = str(row['table_title'])[:60] if row['table_title'] else ""
        lines.append(f"{row['report']} | table {row['table_id']} | {row['variable']} | {row['y_min']}-{row['y_max']} | {title_short}")

    return "\n".join(lines[:60])


def ask_data(question: str, conversation_history: list = None) -> dict:
    """
    Three-step process:
    Step 1: LLM picks search keywords → we find real variable names
    Step 2: LLM writes SQL using the real names
    Step 3: We run SQL, send results back to LLM for final answer
    """

    # ── STEP 1: Extract search keywords ───────────────────────
    step1_prompt = f"""The user wants to explore Swedish substance use data.

{SCHEMA_OVERVIEW}

User question: "{question}"

Extract 2-5 Swedish search keywords to find the right variables in the database.
Think about what Swedish words would appear in variable names.

Respond in JSON:
{{"keywords": ["keyword1", "keyword2", ...]}}

Examples:
- "cocaine prices vs seizures" → {{"keywords": ["kokain", "pris", "beslag", "antal"]}}
- "youth alcohol consumption" → {{"keywords": ["alkohol", "druckit", "pojkar", "flickor"]}}
- "smoking trends among women" → {{"keywords": ["rökt", "dagligen", "kvinnor", "cigaretter"]}}
- "biggest changes in school survey" → {{"keywords": ["skolelever", "narkotika", "alkohol", "rökt", "snusat"]}}"""

    try:
        step1_resp = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": step1_prompt}],
            temperature=0.1,
            max_tokens=200,
            response_format={"type": "json_object"},
        )
        step1_result = json.loads(step1_resp.choices[0].message.content)
        keywords = step1_result.get("keywords", [])

        # Search the database for matching variables
        variable_matches = search_variables(keywords)

        # ── STEP 2: Generate SQL with real variable names ─────
        step2_prompt = f"""You are a data analyst for CAN (Swedish Council for Alcohol and Drug Information).

{SCHEMA_OVERVIEW}

User question: "{question}"

I searched the database and found these REAL variables (use ONLY these exact names):

{variable_matches}

Now write a SQL query using the EXACT variable names from above.
IMPORTANT: Always include the 'report' column in your SELECT so we can cite the source.

Respond in JSON:
{{
    "sql": "SELECT year, variable, value, report FROM timeseries WHERE ... ORDER BY year",
    "chart": {{
        "type": "line",
        "x": "year",
        "y": "value",
        "color": "variable",
        "title": "Chart title"
    }}
}}

SQL RULES:
- Use EXACT variable names from the search results above. Do NOT invent names.
- Table is 'timeseries' with columns: year, variable, value, report, table_id
- For comparisons, use OR conditions to pull from multiple reports/tables. Use CASE WHEN to give readable labels:
  SELECT year, CASE WHEN report='CAN-233' THEN 'cocaine_price' WHEN variable='kokain_antal' THEN 'cocaine_seizures' END as variable, value FROM timeseries WHERE (...) OR (...) ORDER BY year
- Always include report AND table_id in WHERE clauses (different tables can have same variable names)
- ORDER BY year, LIMIT 500
- For color grouping, make sure the 'variable' column has distinct readable values"""

        step2_resp = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": step2_prompt}],
            temperature=0.1,
            max_tokens=1500,
            response_format={"type": "json_object"},
        )
        step2_result = json.loads(step2_resp.choices[0].message.content)
        sql = step2_result.get("sql", "")
        chart_spec = step2_result.get("chart")

        if not sql:
            return {
                "answer": "I couldn't formulate a query. Try rephrasing your question.",
                "sql": None, "data": None, "chart_spec": None, "sources": "", "error": None,
            }

        # ── Execute SQL ───────────────────────────────────────
        con = duckdb.connect(DB_PATH, read_only=True)
        try:
            data = con.execute(sql).fetchdf()
        except Exception as e:
            return {
                "answer": f"SQL error: {str(e)}\n\nThe query was:\n```sql\n{sql}\n```\n\nAvailable variables I found:\n{variable_matches[:500]}",
                "sql": sql, "data": None, "chart_spec": None, "sources": "", "error": str(e),
            }
        finally:
            con.close()

        if len(data) == 0:
            return {
                "answer": f"No results. The variables I searched for:\n{variable_matches[:500]}\n\nTry a more specific question.",
                "sql": sql, "data": data, "chart_spec": None, "sources": "", "error": None,
            }

        # ── STEP 3: Generate answer from actual results ───────
        data_preview = data.head(50).to_markdown(index=False)

        step3_prompt = f"""You are a data analyst for CAN (Swedish Council for Alcohol and Drug Information).

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
- CAN-234: Values are mostly PERCENTAGES (%). E.g. "9.7% smoked daily". Some are annual counts (cigarettes/year).
- CAN-235: MIXED — seizure counts (antal), percentages (andel), and rates per 100,000. Check the variable name: "antal"=count, "andel"=percentage.
- CAN-236: Values are LITERS of pure alcohol per capita. E.g. "3.19 liters per capita"
- CAN-237: Values are PERCENTAGES (%). E.g. "4.8% report risk consumption"
- CAN-238: Values are PER CAPITA counts. Cigarettes per person, snus cans per person, etc.
- CAN-239: Values are PERCENTAGES (%). E.g. "11.8% have tried cannabis"
- KOLADA: Check the kpi_title for the unit. "per 100,000" = rate, "(%)" = percentage. Always mention which municipality.
- Round values to 1 decimal place for readability."""

        step3_resp = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": step3_prompt}],
            temperature=0.3,
            max_tokens=800,
        )

        answer = step3_resp.choices[0].message.content
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
