"""
CAN Automated Insight Discovery
=================================
Scans all time series in the DuckDB database and finds:
1. Cross-domain correlations (strongest relationships between different reports)
2. Trend change points (when did something shift significantly?)
3. Notable extremes (biggest rises, falls, gender gaps)
"""

import os
import itertools
import numpy as np
import pandas as pd
import duckdb
from scipy import stats as scipy_stats

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "can_data.duckdb")


def load_all_series():
    """Load all time series from DuckDB, pivoted as year x series."""
    con = duckdb.connect(DB_PATH, read_only=True)

    df = con.execute("""
        SELECT report, table_id, table_title, variable, year, value
        FROM timeseries
        ORDER BY report, table_id, variable, year
    """).fetchdf()

    con.close()

    # Create a unique series identifier
    df["series_id"] = df["report"] + "|" + df["table_id"].astype(str) + "|" + df["variable"]
    return df


def find_cross_correlations(df, min_overlap=10, top_n=50):
    """
    Find the strongest correlations between time series from DIFFERENT reports.
    This is the "if drugs go up, does alcohol go down?" detector.
    """
    # Pivot to wide format: rows=year, columns=series_id
    pivot = df.pivot_table(index="year", columns="series_id", values="value", aggfunc="first")

    # Only keep series with enough data points
    valid_cols = pivot.columns[pivot.notna().sum() >= min_overlap]
    pivot = pivot[valid_cols]

    # Build metadata lookup
    meta = df.drop_duplicates("series_id").set_index("series_id")[["report", "table_id", "table_title", "variable"]]

    results = []

    # Group columns by report to only compare CROSS-report pairs
    report_groups = {}
    for col in valid_cols:
        report = col.split("|")[0]
        report_groups.setdefault(report, []).append(col)

    reports = list(report_groups.keys())

    for i, r1 in enumerate(reports):
        for r2 in reports[i + 1:]:
            cols1 = report_groups[r1]
            cols2 = report_groups[r2]

            for c1 in cols1:
                s1 = pivot[c1].dropna()
                for c2 in cols2:
                    s2 = pivot[c2].dropna()

                    # Find overlapping years
                    common_years = s1.index.intersection(s2.index)
                    if len(common_years) < min_overlap:
                        continue

                    v1 = s1.loc[common_years].values
                    v2 = s2.loc[common_years].values

                    # Skip constant series
                    if np.std(v1) < 1e-10 or np.std(v2) < 1e-10:
                        continue

                    r, p = scipy_stats.pearsonr(v1, v2)

                    if abs(r) > 0.7 and p < 0.05:
                        m1 = meta.loc[c1]
                        m2 = meta.loc[c2]
                        results.append({
                            "series_1": c1,
                            "report_1": m1["report"],
                            "table_1": m1["table_id"],
                            "title_1": m1["table_title"],
                            "variable_1": m1["variable"],
                            "series_2": c2,
                            "report_2": m2["report"],
                            "table_2": m2["table_id"],
                            "title_2": m2["table_title"],
                            "variable_2": m2["variable"],
                            "correlation": round(r, 3),
                            "p_value": round(p, 6),
                            "overlap_years": len(common_years),
                            "year_min": int(common_years.min()),
                            "year_max": int(common_years.max()),
                            "direction": "positive" if r > 0 else "negative",
                        })

    results_df = pd.DataFrame(results)
    if len(results_df) == 0:
        return results_df

    # Sort by absolute correlation strength
    results_df["abs_corr"] = results_df["correlation"].abs()
    results_df = results_df.sort_values("abs_corr", ascending=False).head(top_n)
    results_df = results_df.drop(columns=["abs_corr"])

    return results_df


def find_trend_changes(df, min_years=10):
    """
    Detect significant trend changes / structural breaks in each series.
    Uses a simple approach: split each series at every year and compare
    the mean before vs after. Report the split with the biggest difference.
    """
    results = []

    meta = df.drop_duplicates("series_id").set_index("series_id")[["report", "table_id", "table_title", "variable"]]

    for series_id, group in df.groupby("series_id"):
        ts = group.sort_values("year").drop_duplicates("year")
        if len(ts) < min_years:
            continue

        years = ts["year"].values
        values = ts["value"].values

        if np.std(values) < 1e-10:
            continue

        best_t = 0
        best_split_year = None

        # Try splitting at each point (need at least 4 on each side)
        for split_idx in range(4, len(values) - 4):
            before = values[:split_idx]
            after = values[split_idx:]

            if np.std(before) < 1e-10 and np.std(after) < 1e-10:
                continue

            t_stat, p_val = scipy_stats.ttest_ind(before, after, equal_var=False)

            if abs(t_stat) > abs(best_t):
                best_t = t_stat
                best_split_year = years[split_idx]
                best_p = p_val
                best_before_mean = np.mean(before)
                best_after_mean = np.mean(after)

        if best_split_year is not None and abs(best_t) > 3.0:
            m = meta.loc[series_id]
            change_pct = ((best_after_mean - best_before_mean) / abs(best_before_mean) * 100) if best_before_mean != 0 else 0
            results.append({
                "series_id": series_id,
                "report": m["report"],
                "table_id": m["table_id"],
                "table_title": m["table_title"],
                "variable": m["variable"],
                "break_year": int(best_split_year),
                "mean_before": round(best_before_mean, 2),
                "mean_after": round(best_after_mean, 2),
                "change_pct": round(change_pct, 1),
                "t_statistic": round(best_t, 2),
                "p_value": round(best_p, 6),
                "direction": "increase" if best_after_mean > best_before_mean else "decrease",
                "year_range": f"{int(years.min())}-{int(years.max())}",
            })

    results_df = pd.DataFrame(results)
    if len(results_df) == 0:
        return results_df

    results_df["abs_t"] = results_df["t_statistic"].abs()
    results_df = results_df.sort_values("abs_t", ascending=False)
    results_df = results_df.drop(columns=["abs_t"])

    return results_df


def find_biggest_movers(df, window=5):
    """
    Find the variables that changed the most in the last N years vs their historical average.
    """
    results = []
    meta = df.drop_duplicates("series_id").set_index("series_id")[["report", "table_id", "table_title", "variable"]]

    for series_id, group in df.groupby("series_id"):
        ts = group.sort_values("year").drop_duplicates("year")
        if len(ts) < window + 5:
            continue

        years = ts["year"].values
        values = ts["value"].values
        max_year = years.max()

        recent = values[-window:]
        historical = values[:-window]

        if np.std(historical) < 1e-10:
            continue

        z_score = (np.mean(recent) - np.mean(historical)) / np.std(historical)

        if abs(z_score) > 2.0:
            m = meta.loc[series_id]
            results.append({
                "series_id": series_id,
                "report": m["report"],
                "table_id": m["table_id"],
                "table_title": m["table_title"],
                "variable": m["variable"],
                "recent_mean": round(np.mean(recent), 2),
                "historical_mean": round(np.mean(historical), 2),
                "z_score": round(z_score, 2),
                "direction": "rising" if z_score > 0 else "falling",
                "latest_year": int(max_year),
            })

    results_df = pd.DataFrame(results)
    if len(results_df) == 0:
        return results_df

    results_df["abs_z"] = results_df["z_score"].abs()
    results_df = results_df.sort_values("abs_z", ascending=False)
    results_df = results_df.drop(columns=["abs_z"])

    return results_df


def run_all_insights():
    """Run all insight discovery and save results to DuckDB."""
    print("Loading data...")
    df = load_all_series()
    print(f"  {len(df):,} records, {df['series_id'].nunique()} unique series")

    print("\nFinding cross-domain correlations...")
    correlations = find_cross_correlations(df)
    print(f"  Found {len(correlations)} strong cross-report correlations")

    print("\nDetecting trend change points...")
    changes = find_trend_changes(df)
    print(f"  Found {len(changes)} significant trend breaks")

    print("\nFinding biggest recent movers...")
    movers = find_biggest_movers(df)
    print(f"  Found {len(movers)} series with unusual recent changes")

    # Save to DuckDB
    con = duckdb.connect(DB_PATH)

    if len(correlations) > 0:
        con.execute("DROP TABLE IF EXISTS insight_correlations")
        con.execute("CREATE TABLE insight_correlations AS SELECT * FROM correlations")

    if len(changes) > 0:
        con.execute("DROP TABLE IF EXISTS insight_trend_breaks")
        con.execute("CREATE TABLE insight_trend_breaks AS SELECT * FROM changes")

    if len(movers) > 0:
        con.execute("DROP TABLE IF EXISTS insight_movers")
        con.execute("CREATE TABLE insight_movers AS SELECT * FROM movers")

    con.close()
    print("\nInsights saved to DuckDB!")

    return correlations, changes, movers


if __name__ == "__main__":
    corr, changes, movers = run_all_insights()

    if len(corr) > 0:
        print(f"\n{'='*60}")
        print("TOP 10 CROSS-DOMAIN CORRELATIONS")
        print("='*60")
        for _, row in corr.head(10).iterrows():
            print(f"\n  r={row['correlation']:+.3f} ({row['direction']}) | {row['year_min']}-{row['year_max']}")
            print(f"    {row['report_1']} / {row['variable_1']}")
            print(f"    {row['report_2']} / {row['variable_2']}")

    if len(changes) > 0:
        print(f"\n{'='*60}")
        print("TOP 10 TREND BREAKS")
        print("='*60")
        for _, row in changes.head(10).iterrows():
            print(f"\n  Break at {row['break_year']} ({row['direction']} {row['change_pct']:+.1f}%)")
            print(f"    {row['report']} / {row['variable']}")
            print(f"    Before: {row['mean_before']} -> After: {row['mean_after']}")
