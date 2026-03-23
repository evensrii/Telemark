import os
import re
import time
import requests
import pandas as pd
from pyjstat import pyjstat

from Helper_scripts.utility_functions import fetch_data
from Helper_scripts.github_functions import handle_output_data

script_name = os.path.basename(__file__)
error_messages = []

# Telemark municipality names used to dynamically find region codes in each table.
# Includes both current and historical names (pre-2020 reform).
TELEMARK_NAMES = [
    "Porsgrunn", "Skien", "Notodden", "Siljan", "Bamble", "Kragerø",
    "Drangedal", "Nome", "Midt-Telemark", "Seljord", "Hjartdal", "Tinn",
    "Kviteseid", "Nissedal", "Fyresdal", "Tokke", "Vinje",
    # Pre-2020 names (before mergers)
    "Bø (Telemark)", "Sauherad",
]

# ============================================================
# Step 1: Search SSB for all "Framskrevet folkemengde" tables
# ============================================================

base_url = "https://data.ssb.no/api/pxwebapi/v2/tables"
all_tables = []
page = 1

while True:
    params = {
        "query": "title:Framskrevet folkemengde",
        "lang": "no",
        "pagesize": 50,
        "pageNumber": page,
    }
    r = requests.get(base_url, params=params)
    r.raise_for_status()
    data = r.json()
    all_tables.extend(data["tables"])

    if page >= data["page"]["totalPages"]:
        break
    page += 1

print(f"Found {len(all_tables)} tables matching 'Framskrevet folkemengde'")

# ============================================================
# Step 2: Filter to regional (K) tables from 2005 onward
# ============================================================

# We skip tables before 2005 because they have one alternative per table
# (separate tables for MMMM, LLML, etc.) rather than 9 alternatives in one table.

regional_tables = []
for t in all_tables:
    has_region = "region" in t["variableNames"]
    has_k = "(K)" in t["label"]
    first_year = int(t["firstPeriod"]) if t["firstPeriod"].isdigit() else 0

    if first_year < 2005:
        continue

    if has_region and has_k:
        regional_tables.append(t)

# Sort by firstPeriod (oldest first)
regional_tables.sort(key=lambda t: t["firstPeriod"])

df_tables = pd.DataFrame([{
    "Tabell": t["id"],
    "Tittel": t["label"],
    "Første år": t["firstPeriod"],
    "Siste år": t["lastPeriod"],
    "Oppdatert": t["updated"][:10],
} for t in regional_tables])

print(f"\nFiltered to {len(regional_tables)} regional (K) tables from 2005+:\n")
print(df_tables.to_string(index=False))

# ============================================================
# Step 3: For each table, get metadata to find:
#         - Telemark region codes (dynamically by name matching)
#         - The correct kommune codelist (if available)
#         - Available ContentsCode values
#         - Available time periods
# ============================================================

# Note on API versions:
# The SSB PxWeb API v2 (GET) can serve data from ALL tables, including old ones
# originally published via v0 (POST). We use v2 GET for everything.
# However, codelists (e.g. vs_KommunFram2024Agg) only work for tables that
# explicitly define them. For older tables, we use direct region codes instead.


def get_telemark_info(table_id):
    """Fetch metadata and extract Telemark-relevant region codes and codelist."""
    meta_url = f"https://data.ssb.no/api/pxwebapi/v2/tables/{table_id}/metadata?lang=no"
    r = requests.get(meta_url)
    r.raise_for_status()
    meta = r.json()

    region_dim = meta["dimension"]["Region"]
    region_labels = region_dim["category"]["label"]

    # Find the kommune codelist (vs_KommunFram*) if available
    kommune_codelist = None
    if "extension" in region_dim and "codelists" in region_dim["extension"]:
        for cl in region_dim["extension"]["codelists"]:
            if "KommunFram" in cl.get("id", ""):
                kommune_codelist = cl["id"]
                break

    # Find Telemark municipality codes by matching names
    telemark_codes = []
    for code, label in region_labels.items():
        # Strip year suffixes like "(-2019)" or "(2020-2023)" from label
        clean_label = re.sub(r"\s*\(.*?\)\s*$", "", label).strip()
        if clean_label in TELEMARK_NAMES:
            telemark_codes.append(code)

    # Also find the Telemark fylke code
    telemark_fylke = None
    for code, label in region_labels.items():
        clean_label = re.sub(r"\s*\(.*?\)\s*$", "", label).strip()
        if clean_label in ["Telemark", "Vestfold og Telemark"]:
            telemark_fylke = code

    # Get available ContentsCode values
    contents_dim = meta["dimension"]["ContentsCode"]
    contents_codes = list(contents_dim["category"]["index"].keys())

    # Get available time periods
    time_dim = meta["dimension"]["Tid"]
    time_codes = list(time_dim["category"]["index"].keys())

    # Check if vs_AlleAldre00B codelist is available for Alder
    alder_codelist = None
    alder_dim = meta["dimension"].get("Alder", {})
    if "extension" in alder_dim and "codelists" in alder_dim["extension"]:
        for cl in alder_dim["extension"]["codelists"]:
            if cl.get("id") == "vs_AlleAldre00B":
                alder_codelist = "vs_AlleAldre00B"
                break

    return {
        "table_id": table_id,
        "telemark_codes": telemark_codes,
        "telemark_fylke": telemark_fylke,
        "kommune_codelist": kommune_codelist,
        "alder_codelist": alder_codelist,
        "contents_codes": contents_codes,
        "time_codes": time_codes,
        "first_period": time_codes[0] if time_codes else None,
        "last_period": time_codes[-1] if time_codes else None,
    }


table_info = {}
for t in regional_tables:
    tid = t["id"]
    print(f"Fetching metadata for table {tid}...")
    info = get_telemark_info(tid)
    table_info[tid] = info
    print(
        f"  -> Found {len(info['telemark_codes'])} Telemark municipalities, "
        f"codelist: {info['kommune_codelist']}, "
        f"alder_codelist: {info['alder_codelist']}, "
        f"period: {info['first_period']}-{info['last_period']}"
    )
    time.sleep(0.3)  # Be polite to the API

# ============================================================
# Step 4: Query data from each table for Telemark municipalities
# ============================================================

# The v2 GET API is used for all tables (old and new).
# - If a kommune codelist exists -> use codelist[Region]=...
# - If not -> use direct region codes in valuecodes[Region]=...
# - Kjønn is eliminated by not specifying it (default behavior in v2)
# - Alder codelist is used if available, otherwise Alder is also eliminated

all_dfs = []

for t in regional_tables:
    tid = t["id"]
    info = table_info[tid]

    if not info["telemark_codes"]:
        print(f"Skipping table {tid}: no Telemark municipalities found")
        continue

    region_values = ",".join(info["telemark_codes"])
    contents_values = ",".join(info["contents_codes"])

    # Build GET URL
    url_parts = [
        f"https://data.ssb.no/api/pxwebapi/v2/tables/{tid}/data?lang=no",
        "&outputFormat=json-stat2",
        f"&valuecodes[ContentsCode]={contents_values}",
        f"&valuecodes[Region]={region_values}",
        "&valuecodes[Tid]=*",
    ]

    # Add kommune codelist if available (only works for tables that define it)
    if info["kommune_codelist"]:
        url_parts.append(f"&codelist[Region]={info['kommune_codelist']}")

    # Add age codelist if available
    if info["alder_codelist"]:
        url_parts.append(f"&codelist[Alder]={info['alder_codelist']}")

    GET_URL = "".join(url_parts)

    print(f"\nQuerying table {tid} ({t['firstPeriod']}-framskrivingen)...")
    print(f"  URL: {GET_URL[:120]}...")

    try:
        df = fetch_data(
            url=GET_URL,
            payload=None,
            error_messages=error_messages,
            query_name=f"Framskriving {tid}",
            response_type="json",
        )

        if df is not None and not df.empty:
            # Add columns to identify which framskriving this data comes from
            df["Framskriving"] = t["firstPeriod"]
            df["Tabell"] = tid
            all_dfs.append(df)
            print(f"  -> Got {len(df)} rows")
        else:
            print(f"  -> No data returned")

    except Exception as e:
        print(f"  -> Error: {e}")
        error_messages.append(f"Table {tid}: {e}")

    time.sleep(0.5)  # Be polite to the API

print(f"\n--- Errors during fetching ---")
if error_messages:
    for msg in error_messages:
        print(f"  {msg}")
else:
    print("  No errors.")

# ============================================================
# Step 5: Inspect each table's data
# ============================================================

for i, df in enumerate(all_dfs):
    tid = df["Tabell"].iloc[0]
    framskriving = df["Framskriving"].iloc[0]
    print(f"\n--- Table {tid} ({framskriving}-framskrivingen) ---")
    print(f"  Shape: {df.shape}")
    print(f"  Columns: {list(df.columns)}")
    print(f"  Regions: {sorted(df['region'].unique())}")
    print(df.head(5))

# ============================================================
# Step 6: Save each table's data to separate CSV files
# ============================================================

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

github_folder = "Data/01_Befolkning/Befolkningsframskrivinger"
temp_folder = os.environ.get("TEMP_FOLDER")

for i, df in enumerate(all_dfs):
    tid = df["Tabell"].iloc[0]
    info = table_info[tid]
    first_year = info["first_period"]
    last_year = info["last_period"]

    file_name = f"Framskrevet_folkemengde_MMMM_{first_year}-{last_year}.csv"
    task_name = f"Befolkningsframskrivinger {first_year}-{last_year}"

    print(f"\nSaving {file_name}...")

    is_new_data = handle_output_data(
        df,
        file_name,
        github_folder,
        temp_folder,
        keepcsv=True,
    )

    # Write the "New Data" status to a unique log file
    log_dir = os.environ.get("LOG_FOLDER", os.getcwd())
    task_name_safe = task_name.replace(".", "_").replace(" ", "_")
    new_data_status_file = os.path.join(log_dir, f"new_data_status_{task_name_safe}.log")

    with open(new_data_status_file, "w", encoding="utf-8") as log_file:
        log_file.write(f"{task_name_safe},{file_name},{'Yes' if is_new_data else 'No'}\n")

    if is_new_data:
        print(f"  New data detected and pushed to GitHub: {file_name}")
    else:
        print(f"  No new data detected: {file_name}")
