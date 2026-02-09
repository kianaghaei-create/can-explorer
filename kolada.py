"""
KOLADA Integration — Swedish Municipal Statistics
===================================================
Fetches data from KOLADA API to complement CAN's national data
with municipal-level context (unemployment, mental health, education, etc.)
"""

import requests
import pandas as pd
import time

BASE_URL = "https://api.kolada.se/v2"

# Pre-selected KPIs relevant to substance use context
RELEVANT_KPIS = {
    "N07544": "Drug offenses per 100,000 inhabitants",
    "N33820": "Mental ill-health among children/youth 0-19 (%)",
    "N03921": "Youth unemployment 16-24 (%)",
    "N03922": "Youth openly unemployed 16-24 (%)",
    "N17441": "Gymnasium completion rate within 3 years (%)",
    "N17473": "University eligibility within 3 years (%)",
    "N00621": "Few problems with drug trafficking (citizen survey %)",
    "N00620": "Few problems with alcohol/drug-affected persons (%)",
    "N07628": "Problems with substance-affected persons outdoors (%)",
    "N02280": "Unemployment 20-64 (%)",
}

# Regional-level KPIs (need region ID, not municipality ID)
REGIONAL_KPIS = {
    "U01404": "Risky alcohol habits 16-84 (%)",
    "U79232": "Grade 9 students who used drugs in past 12 months (%)",
    "U79235": "Gymnasium year 2 students who used drugs past 12 months (%)",
    "U79231": "Grade 9 students drinking alcohol past 12 months (%)",
}

# Major municipalities
MAJOR_MUNICIPALITIES = {
    "0180": "Stockholm",
    "1280": "Malmö",
    "1480": "Göteborg",
    "0380": "Uppsala",
    "0580": "Linköping",
    "1880": "Örebro",
    "0680": "Jönköping",
    "0880": "Kalmar",
    "1080": "Karlskrona",
    "1380": "Halmstad",
}


def search_kpis(query: str) -> pd.DataFrame:
    """Search KOLADA for KPIs matching a Swedish keyword."""
    resp = requests.get(f"{BASE_URL}/kpi", params={"title": query}, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    if not data.get("values"):
        return pd.DataFrame()

    records = []
    for kpi in data["values"]:
        records.append({
            "kpi_id": kpi["id"],
            "title": kpi.get("title", ""),
            "description": kpi.get("description", "")[:100],
            "area": kpi.get("operating_area", ""),
        })

    return pd.DataFrame(records)


def fetch_kpi_data(kpi_id: str, municipality_ids: list, years: list) -> pd.DataFrame:
    """
    Fetch data for a specific KPI across municipalities and years.
    Returns a DataFrame with columns: kpi_id, municipality, year, gender, value
    """
    muni_str = ",".join(municipality_ids)
    year_str = ",".join(str(y) for y in years)

    url = f"{BASE_URL}/data/kpi/{kpi_id}/municipality/{muni_str}/year/{year_str}"

    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        return pd.DataFrame()

    records = []
    for entry in data.get("values", []):
        for val in entry.get("values", []):
            if val.get("value") is not None:
                records.append({
                    "kpi_id": kpi_id,
                    "municipality_id": entry["municipality"],
                    "year": entry["period"],
                    "gender": val["gender"],
                    "value": val["value"],
                })

    return pd.DataFrame(records)


def fetch_all_relevant_kpis(municipality_ids: list = None, years: list = None) -> pd.DataFrame:
    """
    Fetch all pre-selected relevant KPIs for given municipalities and years.
    Returns a combined DataFrame.
    """
    if municipality_ids is None:
        municipality_ids = list(MAJOR_MUNICIPALITIES.keys())
    if years is None:
        years = list(range(2015, 2025))

    all_frames = []

    for kpi_id, title in RELEVANT_KPIS.items():
        print(f"  Fetching {kpi_id}: {title}...")
        df = fetch_kpi_data(kpi_id, municipality_ids, years)
        if len(df) > 0:
            df["kpi_title"] = title
            all_frames.append(df)
        time.sleep(0.25)  # respect rate limit

    if all_frames:
        combined = pd.concat(all_frames, ignore_index=True)
        # Add municipality names
        combined["municipality_name"] = combined["municipality_id"].map(MAJOR_MUNICIPALITIES).fillna(combined["municipality_id"])
        return combined

    return pd.DataFrame()


def fetch_and_store_kolada():
    """Fetch KOLADA data and store in DuckDB alongside CAN data."""
    import duckdb
    import os

    DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "can_data.duckdb")

    print("Fetching KOLADA municipal data...")
    df = fetch_all_relevant_kpis()

    if len(df) == 0:
        print("No KOLADA data fetched.")
        return

    print(f"\nFetched {len(df):,} KOLADA records")
    print(f"  KPIs: {df['kpi_id'].nunique()}")
    print(f"  Municipalities: {df['municipality_name'].nunique()}")
    print(f"  Years: {df['year'].min()}-{df['year'].max()}")

    # Store in DuckDB
    con = duckdb.connect(DB_PATH)
    con.execute("DROP TABLE IF EXISTS kolada")
    con.execute("CREATE TABLE kolada AS SELECT * FROM df")

    count = con.execute("SELECT COUNT(*) FROM kolada").fetchone()[0]
    print(f"\nStored {count:,} rows in DuckDB table 'kolada'")
    con.close()


if __name__ == "__main__":
    fetch_and_store_kolada()
