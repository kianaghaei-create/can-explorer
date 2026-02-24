"""
Fix generic numbered variable names in the database.
Maps _1, _2, _3 etc to actual descriptive names based on Excel row headers.

Run once: python fix_variable_names.py
"""

import duckdb
import os

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "can_data.duckdb")

# CAN-239 Tables 6 & 7: Alcohol consumption by beverage type
# Order in Excel: Sprit(base), Vin(_1), Blanddrycker(_2), Starköl(_3), Folköl(_4), Totalt(_5)
RENAME_MAP_239_6_7 = {
    # Percentage columns (andel)
    "pojkar": "sprit_andel_pojkar",
    "pojkar_1": "vin_andel_pojkar",
    "pojkar_2": "blanddrycker_andel_pojkar",
    "pojkar_3": "starköl_andel_pojkar",
    "pojkar_4": "folköl_andel_pojkar",
    "pojkar_5": "totalt_andel_pojkar",
    "flickor": "sprit_andel_flickor",
    "flickor_1": "vin_andel_flickor",
    "flickor_2": "blanddrycker_andel_flickor",
    "flickor_3": "starköl_andel_flickor",
    "flickor_4": "folköl_andel_flickor",
    "flickor_5": "totalt_andel_flickor",
    "alla": "sprit_andel_alla",
    "alla_1": "vin_andel_alla",
    "alla_2": "blanddrycker_andel_alla",
    "alla_3": "starköl_andel_alla",
    "alla_4": "folköl_andel_alla",
    "alla_5": "totalt_andel_alla",
    # Liter columns
    "pojkar_liter": "sprit_liter_pojkar",
    "pojkar_liter_1": "vin_liter_pojkar",
    "pojkar_liter_2": "blanddrycker_liter_pojkar",
    "pojkar_liter_3": "starköl_liter_pojkar",
    "pojkar_liter_4": "folköl_liter_pojkar",
    "pojkar_liter_5": "totalt_liter_pojkar",
    "flickor_liter": "sprit_liter_flickor",
    "flickor_liter_1": "vin_liter_flickor",
    "flickor_liter_2": "blanddrycker_liter_flickor",
    "flickor_liter_3": "starköl_liter_flickor",
    "flickor_liter_4": "folköl_liter_flickor",
    "flickor_liter_5": "totalt_liter_flickor",
    "alla_liter": "sprit_liter_alla",
    "alla_liter_1": "vin_liter_alla",
    "alla_liter_2": "blanddrycker_liter_alla",
    "alla_liter_3": "starköl_liter_alla",
    "alla_liter_4": "folköl_liter_alla",
    "alla_liter_5": "totalt_liter_alla",
}

# CAN-234 Tables with day/week suffixes
# Table 5: rökning dagligen/sporadiskt/ej rökt
# _dag = dagligen, _dag_1 = sporadiskt, _dag_2 = ej rökt senaste 30d
RENAME_MAP_234_5 = {
    "alla_dag": "dagligen_alla",
    "alla_dag_1": "sporadiskt_alla",
    "alla_dag_2": "ej_rökt_30d_alla",
    "män_dag": "dagligen_män",
    "män_dag_1": "sporadiskt_män",
    "män_dag_2": "ej_rökt_30d_män",
    "kvinnor_dag": "dagligen_kvinnor",
    "kvinnor_dag_1": "sporadiskt_kvinnor",
    "kvinnor_dag_2": "ej_rökt_30d_kvinnor",
}


def apply_renames(con, report, table_ids, rename_map):
    """Apply variable renames for specific report+table combinations."""
    count = 0
    for old_name, new_name in rename_map.items():
        for tid in table_ids:
            result = con.execute(f"""
                UPDATE timeseries
                SET variable = '{new_name}'
                WHERE report = '{report}'
                AND table_id = '{tid}'
                AND variable = '{old_name}'
            """)
            # Check how many will be affected first
            affected = con.execute(f"""
                SELECT COUNT(*) FROM timeseries
                WHERE report = '{report}' AND table_id = '{tid}' AND variable = '{old_name}'
            """).fetchone()[0]
            if affected > 0:
                count += affected
                print(f"  {report} table {tid}: {old_name} → {new_name} ({affected} rows)")
    return count


def main():
    con = duckdb.connect(DB_PATH)

    total = 0

    print("Fixing CAN-239 tables 6 & 7 (alcohol by beverage type)...")
    total += apply_renames(con, "CAN-239", ["6", "7"], RENAME_MAP_239_6_7)

    print("\nFixing CAN-234 table 5 (smoking frequency)...")
    total += apply_renames(con, "CAN-234", ["5"], RENAME_MAP_234_5)

    con.close()
    print(f"\nDone! Fixed {total} rows total.")


if __name__ == "__main__":
    main()
