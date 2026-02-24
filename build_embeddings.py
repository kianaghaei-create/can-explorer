"""
Build semantic embeddings for all CAN variables.
Run once to create the embeddings file, then used by chat_engine for semantic search.

Usage: OPENAI_API_KEY="sk-..." python build_embeddings.py
"""

import os
import json
import duckdb
import numpy as np
from openai import OpenAI

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "can_data.duckdb")
EMBEDDINGS_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "variable_embeddings.npz")
CATALOG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "variable_catalog.json")

client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY", ""))

# Report descriptions for richer context
REPORT_CONTEXT = {
    "CAN-233": "Drug street and wholesale prices in Sweden. Unit: SEK (Swedish kronor).",
    "CAN-234": "Self-reported smoking and snus habits among adults 17-84. Unit: percentage.",
    "CAN-235": "Drug seizures, drug-related crime, health and mortality statistics. Unit: varies (counts, percentages, rates per 100,000).",
    "CAN-236": "Total alcohol consumption in Sweden. Unit: liters of pure alcohol per capita.",
    "CAN-237": "Self-reported alcohol habits and drinking patterns. Unit: percentage.",
    "CAN-238": "Total tobacco and nicotine product consumption. Unit: per capita counts.",
    "CAN-239": "Youth school survey — grade 9 and gymnasium year 2. Alcohol, tobacco, drugs, gambling among young people. Unit: percentage.",
}

# Swedish-English translations for common terms to enrich embeddings
TERM_GLOSSARY = {
    "kokain": "cocaine", "amfetamin": "amphetamine", "hasch": "hashish",
    "cannabis": "cannabis", "heroin": "heroin", "ecstasy": "ecstasy",
    "marijuana": "marijuana", "tramadol": "tramadol",
    "beslag": "seizures", "antal": "count/number", "andel": "proportion/share",
    "pris": "price", "median": "median", "medel": "average/mean",
    "alkohol": "alcohol", "rökt": "smoked", "snusat": "used snus",
    "druckit": "drank alcohol", "narkotika": "drugs/narcotics",
    "dagligen": "daily", "sporadiskt": "occasionally",
    "pojkar": "boys", "flickor": "girls", "alla": "all/everyone",
    "män": "men", "kvinnor": "women",
    "årskurs": "grade", "gymnasiet": "upper secondary school",
    "intensivkonsumtion": "heavy episodic drinking/binge drinking",
    "riskkonsumtion": "risk consumption",
    "cigaretter": "cigarettes", "snus": "snus/Swedish snuff",
    "vejp": "vape/e-cigarette", "lustgas": "nitrous oxide/laughing gas",
    "dopning": "doping/anabolic steroids",
    "spel": "gambling", "pengar": "money",
    "dödsfall": "deaths", "dödlighet": "mortality",
    "sjukvård": "healthcare", "rattfylleri": "drunk driving",
    "realprisjusterad": "inflation-adjusted price",
    "orosanmälan": "concern report to social services",
    "konsumtion": "consumption", "försäljning": "sales",
    "registrerad": "registered", "oregistrerad": "unregistered",
    "smuggl": "smuggled", "langare": "drug dealer/bootlegger",
    "resandeinförsel": "traveller import",
    "systembolaget": "state alcohol monopoly store",
}


def build_description(row):
    """Build a rich text description for a variable entry."""
    variable = row["variable"]
    report = row["report"]
    table_id = row["table_id"]
    table_title = row["table_title"] or ""
    y_min = row["y_min"]
    y_max = row["y_max"]

    # Start with report context
    report_ctx = REPORT_CONTEXT.get(report, "")

    # Translate Swedish terms in the variable name
    var_readable = variable.replace("__", " — ").replace("_", " ")
    translations = []
    for sv, en in TERM_GLOSSARY.items():
        if sv.lower() in variable.lower() or sv.lower() in table_title.lower():
            translations.append(f"{sv}={en}")

    translation_str = ", ".join(translations) if translations else ""

    description = (
        f"Report {report}: {report_ctx} "
        f"Table {table_id}: {table_title}. "
        f"Variable: {var_readable}. "
        f"Years: {y_min}-{y_max}. "
    )
    if translation_str:
        description += f"Terms: {translation_str}."

    return description


def embed_batch(texts, model="text-embedding-3-small"):
    """Embed a batch of texts using OpenAI API. Max 2048 per batch."""
    response = client.embeddings.create(input=texts, model=model)
    return [item.embedding for item in response.data]


def main():
    print("Loading variables from database...")
    con = duckdb.connect(DB_PATH, read_only=True)
    df = con.execute("""
        SELECT DISTINCT report, table_id, table_title, variable,
               MIN(year) as y_min, MAX(year) as y_max
        FROM timeseries
        GROUP BY report, table_id, table_title, variable
        ORDER BY report, table_id, variable
    """).fetchdf()
    con.close()

    print(f"Found {len(df)} variable entries to embed.")

    # Build descriptions
    descriptions = []
    catalog = []
    for idx, row in df.iterrows():
        desc = build_description(row)
        descriptions.append(desc)
        catalog.append({
            "report": row["report"],
            "table_id": str(row["table_id"]),
            "table_title": row["table_title"],
            "variable": row["variable"],
            "y_min": int(row["y_min"]),
            "y_max": int(row["y_max"]),
            "description": desc,
        })

    # Embed in batches of 2000
    print("Generating embeddings...")
    all_embeddings = []
    batch_size = 2000
    for i in range(0, len(descriptions), batch_size):
        batch = descriptions[i:i + batch_size]
        print(f"  Embedding batch {i // batch_size + 1} ({len(batch)} items)...")
        embeddings = embed_batch(batch)
        all_embeddings.extend(embeddings)

    # Save as numpy array
    embeddings_matrix = np.array(all_embeddings, dtype=np.float32)
    np.savez_compressed(EMBEDDINGS_PATH, embeddings=embeddings_matrix)
    print(f"Saved embeddings: {embeddings_matrix.shape} to {EMBEDDINGS_PATH}")

    # Save catalog as JSON
    with open(CATALOG_PATH, "w", encoding="utf-8") as f:
        json.dump(catalog, f, ensure_ascii=False, indent=2)
    print(f"Saved catalog: {len(catalog)} entries to {CATALOG_PATH}")

    print("Done! Embeddings are ready for semantic search.")


if __name__ == "__main__":
    main()
