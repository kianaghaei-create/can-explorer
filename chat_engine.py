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

SWEDISH TERMS: kokain=cocaine, amfetamin=amphetamine, hasch=hashish, cannabis=cannabis, heroin=heroin, ecstasy=ecstasy, beslag=seizures, antal=count, pris=price, alkohol=alcohol, rökt/rokt=smoked, snusat=snus use, druckit=drank, narkotika=drugs, dagligen=daily, pojkar=boys, flickor=girls, alla=all, man/män=men, kvinnor=women"""


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

Respond in JSON:
{{
    "sql": "SELECT year, variable, value FROM timeseries WHERE ... ORDER BY year",
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
                "sql": None, "data": None, "chart_spec": None, "error": None,
            }

        # ── Execute SQL ───────────────────────────────────────
        con = duckdb.connect(DB_PATH, read_only=True)
        try:
            data = con.execute(sql).fetchdf()
        except Exception as e:
            return {
                "answer": f"SQL error: {str(e)}\n\nThe query was:\n```sql\n{sql}\n```\n\nAvailable variables I found:\n{variable_matches[:500]}",
                "sql": sql, "data": None, "chart_spec": None, "error": str(e),
            }
        finally:
            con.close()

        if len(data) == 0:
            return {
                "answer": f"No results. The variables I searched for:\n{variable_matches[:500]}\n\nTry a more specific question.",
                "sql": sql, "data": data, "chart_spec": None, "error": None,
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
- Do NOT use placeholders — only real numbers from the data above"""

        step3_resp = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": step3_prompt}],
            temperature=0.3,
            max_tokens=800,
        )

        answer = step3_resp.choices[0].message.content

        return {
            "answer": answer,
            "sql": sql,
            "data": data,
            "chart_spec": chart_spec,
            "error": None,
        }

    except Exception as e:
        return {
            "answer": f"Error: {str(e)}",
            "sql": None, "data": None, "chart_spec": None, "error": str(e),
        }
