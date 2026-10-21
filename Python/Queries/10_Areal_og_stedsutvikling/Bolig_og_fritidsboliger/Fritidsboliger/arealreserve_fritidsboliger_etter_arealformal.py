import os
import re
from urllib.parse import quote
import pandas as pd
from pyjstat import pyjstat

# Import the utility functions from the Helper_scripts folder
from Helper_scripts.utility_functions import fetch_data
from Helper_scripts.github_functions import handle_output_data, download_github_file

# Capture the name of the current script
script_name = os.path.basename(__file__)

# List to collect errors during execution
error_messages = []

# ============================================================
# Step 1: Find the current table id by searching SSB for the
#         table title (SSB may replace the table with a new id).
#         Labels look like "14650: <title> 2025", so the id prefix
#         and trailing period are stripped before matching.
# ============================================================

TABLE_TITLE = "Arealreserver til fritidsbebyggelse i gjeldende arealplaner, etter arealformål og arealklasse (dekar) (K)"

SEARCH_URL = (
    "https://data.ssb.no/api/pxwebapi/v2/tables?lang=no&pageSize=100"
    f"&query={quote(TABLE_TITLE)}"
)

try:
    search_result = fetch_data(
        url=SEARCH_URL,
        payload=None,
        error_messages=error_messages,
        query_name="Søk etter tabell (arealreserve fritidsboliger etter arealformål)",
        response_type="json",
    )
except Exception as e:
    print(f"Error occurred: {e}")
    raise RuntimeError(
        "A critical error occurred during table search, stopping execution."
    )


def strip_label(label):
    label = re.sub(r"^\d+:\s*", "", label)
    return re.sub(r"\s+\d{4}(?:-\d{4})?$", "", label).strip()


matches = [
    t for t in search_result["tables"]
    if strip_label(t["label"]) == TABLE_TITLE and not t.get("discontinued")
]
if not matches:
    raise RuntimeError(f"No active SSB table found with title '{TABLE_TITLE}'")

# If several tables share the title, use the one with the latest data
table = max(matches, key=lambda t: (t["lastPeriod"], t["updated"]))
table_id = table["id"]
print(f"Using table {table['label']} (updated {table['updated']})")

# ============================================================
# Step 2: Query arealreserver til fritidsbebyggelse for the
#         Telemark municipalities, by arealklasse (what the
#         reserved area is today). Unit: dekar.
# ============================================================

GET_URL = (
    f"https://data.ssb.no/api/pxwebapi/v2/tables/{table_id}/data?lang=no"
    "&valueCodes[ContentsCode]=Areal"
    "&valueCodes[Tid]=top(1)"
    "&valueCodes[Region]=4001,4003,4005,4010,4012,4014,4016,4018,4020,4022,4024,4026,4028,4030,4032,4034,4036"
    "&valueCodes[ArealFormaal]=G04"
    "&valueCodes[ArealKlasse]=01-14,15-16,17,18,19,Annet"
    "&codelist[Region]=vs_Kommuner2024"
    "&codelist[ArealFormaal]=vs_ArealFormaal25a"
)

try:
    df_raw = fetch_data(
        url=GET_URL,
        payload=None,
        error_messages=error_messages,
        query_name="Arealreserve fritidsboliger etter arealformål",
        response_type="json",
    )
except Exception as e:
    print(f"Error occurred: {e}")
    raise RuntimeError(
        "A critical error occurred during data fetching, stopping execution."
    )

print(f"Raw data: {len(df_raw)} rows")
print(df_raw.head(10))

# ============================================================
# Step 3: Pivot to one row per kommune, one column per
#         arealklasse (dekar)
# ============================================================

kommunenummer = {
    "Porsgrunn": "4001",
    "Skien": "4003",
    "Notodden": "4005",
    "Siljan": "4010",
    "Bamble": "4012",
    "Kragerø": "4014",
    "Drangedal": "4016",
    "Nome": "4018",
    "Midt-Telemark": "4020",
    "Seljord": "4022",
    "Hjartdal": "4024",
    "Tinn": "4026",
    "Kviteseid": "4028",
    "Nissedal": "4030",
    "Fyresdal": "4032",
    "Tokke": "4034",
    "Vinje": "4036",
}

arealklasse_order = [
    "Bebygd areal",
    "Jordbruksareal",
    "Skog",
    "Åpen fastmark",
    "Åpen myr",
    "Annet areal",
]

df = df_raw.copy()
df.columns = ["region", "arealformål", "arealklasse", "statistikkvariabel", "år", "value"]
# "." (ikke mulig å oppgi tall) comes through as NaN and is kept empty
df["value"] = pd.to_numeric(df["value"], errors="coerce")

# pivot (not pivot_table), so missing values stay NaN instead of being summed to 0
df_pivot = df.pivot(index=["region", "år"], columns="arealklasse", values="value").reset_index()
df_pivot.columns.name = None

df_final = pd.concat(
    [
        pd.DataFrame(
            {
                "Kommunenummer": df_pivot["region"].map(kommunenummer),
                "Kommune": df_pivot["region"],
                "År": df_pivot["år"].astype(int),
            }
        ),
        df_pivot[arealklasse_order],
    ],
    axis=1,
)

if df_final["Kommunenummer"].isna().any():
    raise ValueError(f"Unmapped municipalities: {df_final.loc[df_final['Kommunenummer'].isna(), 'Kommune'].tolist()}")

print(df_final)

# ============================================================
# Step 4: Add the latest year to the existing dataset on GitHub.
#         SSB publishes each year in a new table (e.g. "... (K) 2026"),
#         so earlier years are kept from the existing CSV. Rows for the
#         fetched year are replaced, in case SSB revises them.
# ============================================================

file_name = "arealreserve_fritidsboliger_etter_arealformal.csv"
task_name = "Fritidsboliger - Arealreserve fritidsboliger etter arealformal"
github_folder = "Data/10_Areal- og stedsutvikling/Fritidsboliger"
temp_folder = os.environ.get("TEMP_FOLDER")

df_existing = download_github_file(f"{github_folder}/{file_name}")

if df_existing is not None:
    df_existing["År"] = df_existing["År"].astype(int)
    df_existing[arealklasse_order] = df_existing[arealklasse_order].apply(pd.to_numeric, errors="coerce")
    df_existing = df_existing[~df_existing["År"].isin(df_final["År"])]
    print(f"Keeping {len(df_existing)} rows for earlier years: {sorted(df_existing['År'].unique().tolist())}")
    df_final = pd.concat([df_existing[df_final.columns], df_final], ignore_index=True)

df_final = df_final.sort_values(["Kommunenummer", "År"]).reset_index(drop=True)

print(df_final)

# ============================================================
# Step 5: Save to CSV, compare and upload to GitHub
# ============================================================

value_columns = arealklasse_order

# Call the function and get the "New Data" status
is_new_data = handle_output_data(
    df_final, file_name, github_folder, temp_folder, keepcsv=True, value_columns=value_columns
)

# Write the "New Data" status to a unique log file
log_dir = os.environ.get("LOG_FOLDER", os.getcwd())
task_name_safe = task_name.replace(".", "_").replace(" ", "_")
new_data_status_file = os.path.join(log_dir, f"new_data_status_{task_name_safe}.log")

# Write the result in a detailed format
with open(new_data_status_file, "w", encoding="utf-8") as log_file:
    log_file.write(f"{task_name_safe},{file_name},{'Yes' if is_new_data else 'No'}\n")

# Output results for debugging/testing
if is_new_data:
    print("New data detected and pushed to GitHub.")
else:
    print("No new data detected.")

print(f"New data status log written to {new_data_status_file}")
