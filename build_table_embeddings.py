"""
Build semantic embeddings for all CAN *tables* (one embedding per unique table).

This creates a table-level index used as Stage 1 of the two-stage retrieval:
  Stage 1: Query → table embeddings → top-N relevant tables
  Stage 2: Variable embeddings filtered to those tables → LLM picks variables

Run once (or after re-ingesting data):
  python build_table_embeddings.py

Output:
  table_embeddings.npz   — shape (N_tables, embedding_dim)
  table_catalog.json     — list of {report, table_id, table_title, description}
"""

import os
import json
import duckdb
import numpy as np
from openai import OpenAI

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "can_data.duckdb")
TABLE_EMBEDDINGS_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "table_embeddings.npz")
TABLE_CATALOG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "table_catalog.json")

_api_key = os.environ.get("OPENAI_API_KEY", "")
if not _api_key:
    _key_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "openai_key.txt")
    if os.path.exists(_key_file):
        with open(_key_file) as f:
            _api_key = f.read().strip()

client = OpenAI(api_key=_api_key)

# Same report context as in build_embeddings.py for consistency
REPORT_CONTEXT = {
    "CAN-233": "Drug street and wholesale prices in Sweden. Unit: SEK (Swedish kronor).",
    "CAN-234": "Self-reported smoking and snus habits among adults 17-84 years. Unit: percentage.",
    "CAN-235": "Drug seizures, drug-related crime, health and mortality statistics. Unit: varies.",
    "CAN-236": "Total alcohol consumption in Sweden. Unit: liters of pure alcohol per capita.",
    "CAN-237": "Self-reported alcohol habits and drinking patterns among adults. Unit: percentage.",
    "CAN-238": "Total tobacco and nicotine product consumption volumes. Unit: per capita counts.",
    "CAN-239": "Youth school survey grade 9 and gymnasium year 2. Alcohol, tobacco, drugs, gambling. Unit: percentage.",
}

# Swedish→English term glossary for enriching table descriptions
TERM_GLOSSARY = {
    "kokain": "cocaine", "amfetamin": "amphetamine", "hasch": "hashish",
    "cannabis": "cannabis", "heroin": "heroin", "ecstasy": "ecstasy",
    "marijuana": "marijuana", "tramadol": "tramadol",
    "beslag": "seizures/confiscations", "antal": "count/number", "andel": "proportion/share",
    "pris": "price", "median": "median", "medel": "average/mean",
    "alkohol": "alcohol", "rökt": "smoked cigarettes", "snusat": "used snus/snuff",
    "snus": "snus/Swedish snuff (smokeless tobacco)", "snusar": "uses snus",
    "druckit": "drank alcohol", "narkotika": "drugs/narcotics",
    "dagligen": "daily", "sporadiskt": "occasionally",
    "pojkar": "boys", "flickor": "girls", "alla": "all/everyone",
    "män": "men", "kvinnor": "women",
    "ungdomar": "youth/young people", "elever": "pupils/students",
    "årskurs": "school grade", "gymnasiet": "upper secondary school",
    "åk 9": "grade 9 (age 15-16)", "gymnasiets år 2": "gymnasium year 2 (age 17)",
    "intensivkonsumtion": "heavy episodic drinking/binge drinking",
    "riskkonsumtion": "risk consumption/hazardous use",
    "cigaretter": "cigarettes", "vejp": "vape/e-cigarette", "e-cigaretter": "e-cigarettes/vaping",
    "lustgas": "nitrous oxide/laughing gas", "dopning": "doping/anabolic steroids",
    "spel": "gambling", "dödsfall": "deaths", "dödlighet": "mortality",
    "rattfylleri": "drunk driving", "realprisjusterad": "inflation-adjusted price",
    "konsumtion": "consumption", "försäljning": "sales",
    "smuggl": "smuggled", "skolka": "truancy/skipping school",
    "trivs": "wellbeing/how they feel at school",
    "nicopods": "nicotine pouches/white snus",
    "systembolaget": "state alcohol monopoly",
}


def build_table_description(report: str, table_id: str, table_title: str,
                            y_min: int, y_max: int, variables: list) -> str:
    """
    Build a rich bilingual description for a table entry.
    Includes: report context, full table title, year range, key variable terms.
    """
    report_ctx = REPORT_CONTEXT.get(report, "")
    title_clean = table_title or ""

    # Find matching glossary terms in the title
    translations = []
    for sv, en in TERM_GLOSSARY.items():
        if sv.lower() in title_clean.lower():
            translations.append(f"{sv}={en}")

    # Also scan variable names for extra terms
    var_sample = " ".join(variables[:5])
    for sv, en in TERM_GLOSSARY.items():
        if sv.lower() in var_sample.lower() and f"{sv}=" not in " ".join(translations):
            translations.append(f"{sv}={en}")

    translation_str = ", ".join(translations) if translations else ""

    desc = (
        f"Report {report}: {report_ctx} "
        f"Table {table_id}: {title_clean}. "
        f"Years: {y_min}–{y_max}. "
    )
    if translation_str:
        desc += f"Key terms: {translation_str}."

    return desc


def embed_batch(texts: list, model: str = "text-embedding-3-small") -> list:
    """Embed a list of texts using OpenAI. Max ~2048 per call."""
    response = client.embeddings.create(input=texts, model=model)
    return [item.embedding for item in response.data]


def main():
    print("Loading table metadata from database...")
    con = duckdb.connect(DB_PATH, read_only=True)

    # One row per unique table, with variable list and year range
    df = con.execute("""
        SELECT report, table_id, table_title,
               MIN(year) as y_min, MAX(year) as y_max,
               LIST(DISTINCT variable ORDER BY variable) as variables
        FROM timeseries
        GROUP BY report, table_id, table_title
        ORDER BY report, CAST(table_id AS INTEGER)
    """).fetchdf()
    con.close()

    print(f"Found {len(df)} unique tables across all reports.")

    descriptions = []
    catalog = []

    for _, row in df.iterrows():
        variables = list(row["variables"]) if row["variables"] is not None else []
        desc = build_table_description(
            report=row["report"],
            table_id=str(row["table_id"]),
            table_title=row["table_title"] or "",
            y_min=int(row["y_min"]),
            y_max=int(row["y_max"]),
            variables=list(variables),
        )
        descriptions.append(desc)
        catalog.append({
            "report": row["report"],
            "table_id": str(row["table_id"]),
            "table_title": row["table_title"] or "",
            "y_min": int(row["y_min"]),
            "y_max": int(row["y_max"]),
            "n_variables": len(variables),
            "description": desc,
        })

    print("Generating table embeddings...")
    all_embeddings = []
    batch_size = 200
    for i in range(0, len(descriptions), batch_size):
        batch = descriptions[i:i + batch_size]
        print(f"  Batch {i // batch_size + 1}: {len(batch)} tables...")
        embeddings = embed_batch(batch)
        all_embeddings.extend(embeddings)

    matrix = np.array(all_embeddings, dtype=np.float32)
    np.savez_compressed(TABLE_EMBEDDINGS_PATH, embeddings=matrix)
    print(f"Saved table embeddings: {matrix.shape} → {TABLE_EMBEDDINGS_PATH}")

    with open(TABLE_CATALOG_PATH, "w", encoding="utf-8") as f:
        json.dump(catalog, f, ensure_ascii=False, indent=2)
    print(f"Saved table catalog: {len(catalog)} entries → {TABLE_CATALOG_PATH}")

    print("\nSample descriptions:")
    for entry in catalog[:3]:
        print(f"  [{entry['report']} t{entry['table_id']}] {entry['description'][:120]}...")

    print("\nDone! Run the app — chat_engine will use table-first retrieval automatically.")


if __name__ == "__main__":
    main()
