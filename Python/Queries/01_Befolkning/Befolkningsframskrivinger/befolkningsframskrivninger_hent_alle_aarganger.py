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


# Region code prefixes by era for Telemark municipalities.
# The v2 metadata returns codes from ALL eras, so we must filter to the correct one.
ERA_PREFIXES = {
    "pre2020": ["08"],       # Telemark fylke 08, kommuner 08xx
    "2020-2023": ["38"],     # Vestfold og Telemark fylke 38, kommuner 38xx
    "2024+": ["40"],         # Telemark fylke 40, kommuner 40xx
}

# Telemark municipality codes (2024 numbering) for use with agg_KommSummer (K-prefix)
TELEMARK_KOMM_2024 = [
    "4001", "4003", "4005", "4010", "4012", "4014", "4016", "4018",
    "4020", "4022", "4024", "4026", "4028", "4030", "4032", "4034", "4036",
]


def get_era(first_period):
    """Determine which era a table belongs to based on its first year."""
    year = int(first_period)
    if year >= 2024:
        return "2024+"
    elif year >= 2020:
        return "2020-2023"
    else:
        return "pre2020"


def get_telemark_info(table_id, first_period):
    """Fetch metadata and extract Telemark-relevant region codes and codelist."""
    meta_url = f"https://data.ssb.no/api/pxwebapi/v2/tables/{table_id}/metadata?lang=no"
    r = requests.get(meta_url)
    r.raise_for_status()
    meta = r.json()

    era = get_era(first_period)
    valid_prefixes = ERA_PREFIXES[era]

    region_dim = meta["dimension"]["Region"]
    region_labels = region_dim["category"]["label"]

    # Find relevant region codelists
    kommune_codelist = None
    fylke_codelist = None
    all_region_codelists = []
    if "extension" in region_dim and "codelists" in region_dim["extension"]:
        for cl in region_dim["extension"]["codelists"]:
            cl_id = cl.get("id", "")
            all_region_codelists.append(cl_id)
            if "KommunFram" in cl_id and kommune_codelist is None:
                kommune_codelist = cl_id
            if cl_id == "agg_KommSummer" and kommune_codelist is None:
                kommune_codelist = cl_id
            # Fylke codelist priority: FylkerFram > vs_Fylker* > agg_KommFylker
            if "FylkerFram" in cl_id and fylke_codelist is None:
                fylke_codelist = cl_id

        # If no FylkerFram found, look for vs_Fylker* (e.g. vs_Fylker, vs_Fylker2018)
        if fylke_codelist is None:
            for cl in region_dim["extension"]["codelists"]:
                cl_id = cl.get("id", "")
                if cl_id.startswith("vs_Fylker") and cl_id != "vs_Fylker" and fylke_codelist is None:
                    fylke_codelist = cl_id
            # Try plain vs_Fylker as last vs_Fylker option
            if fylke_codelist is None:
                for cl in region_dim["extension"]["codelists"]:
                    if cl.get("id", "") == "vs_Fylker":
                        fylke_codelist = "vs_Fylker"
                        break

        # Final fallback: agg_KommFylker
        if fylke_codelist is None:
            for cl in region_dim["extension"]["codelists"]:
                if cl.get("id", "") == "agg_KommFylker":
                    fylke_codelist = "agg_KommFylker"
                    break

    # Find Telemark municipality codes.
    # For agg_KommSummer: use K-prefixed 2024 codes (aggregates across all eras)
    # For other codelists: filter raw metadata labels by era prefix
    telemark_codes = []
    if kommune_codelist == "agg_KommSummer":
        telemark_codes = [f"K-{c}" for c in TELEMARK_KOMM_2024]
    else:
        for code, label in region_labels.items():
            # Only consider codes that start with the correct era prefix
            if not any(code.startswith(p) for p in valid_prefixes):
                continue
            # Strip year suffixes like "(-2019)" or "(2020-2023)" from label
            clean_label = re.sub(r"\s*\(.*?\)\s*$", "", label).strip()
            if clean_label in TELEMARK_NAMES:
                telemark_codes.append(code)

    # Determine Telemark fylke code based on era and codelist
    # agg_KommFylker needs F-prefix: F-08 (pre2020), F-38 (2020-2023), F-40 (2024+)
    # vs_Fylker/vs_Fylker2018 use plain codes: 08, 38, 40
    # FylkerFram codelists use their own codes from metadata
    fylke_code_map = {
        "pre2020": {"f_prefix": "F-08", "plain": "08"},
        "2020-2023": {"f_prefix": "F-38", "plain": "38"},
        "2024+": {"f_prefix": "F-40", "plain": "40"},
    }

    telemark_fylke = None
    if fylke_codelist:
        if fylke_codelist == "agg_KommFylker":
            telemark_fylke = fylke_code_map[era]["f_prefix"]
        elif "FylkerFram" in fylke_codelist:
            # Search metadata labels for the actual code
            for code, label in region_labels.items():
                clean_label = re.sub(r"\s*\(.*?\)\s*$", "", label).strip()
                if clean_label in ["Telemark", "Vestfold og Telemark"]:
                    telemark_fylke = code
                    break
            # Fallback to plain code if not found in labels
            if telemark_fylke is None:
                telemark_fylke = fylke_code_map[era]["plain"]
        else:
            # vs_Fylker, vs_Fylker2018, etc. use plain codes 
            telemark_fylke = fylke_code_map[era]["plain"]

    # Check if table has a separate Framskriv dimension (e.g. table 08825)
    has_framskriv = "Framskriv" in meta["dimension"]
    framskriv_codes = []
    if has_framskriv:
        framskriv_dim = meta["dimension"]["Framskriv"]
        framskriv_labels = framskriv_dim["category"]["label"]
        wanted_patterns = ["MMMM", "HHMH", "LLML"]
        framskriv_codes = [
            code for code, label in framskriv_labels.items()
            if any(p in label for p in wanted_patterns)
        ]

    # Get ContentsCode values
    contents_dim = meta["dimension"]["ContentsCode"]
    contents_labels = contents_dim["category"]["label"]
    if has_framskriv:
        # Alternatives are in Framskriv, so use all ContentsCode values
        contents_codes = list(contents_labels.keys())
    else:
        # Alternatives are encoded in ContentsCode (MMMM, HHMH, LLML)
        wanted_patterns = ["MMMM", "HHMH", "LLML"]
        contents_codes = [
            code for code, label in contents_labels.items()
            if any(p in label for p in wanted_patterns)
        ]

    # Get available time periods
    time_dim = meta["dimension"]["Tid"]
    time_codes = list(time_dim["category"]["index"].keys())

    # Find best available age codelist: prefer agg_Funksjonell4, fallback to agg_Funksjonell3
    alder_codelist = None
    alder_dim = meta["dimension"].get("Alder", {})
    if "extension" in alder_dim and "codelists" in alder_dim["extension"]:
        available_ids = [cl["id"] for cl in alder_dim["extension"]["codelists"]]
        if "agg_Funksjonell4" in available_ids:
            alder_codelist = "agg_Funksjonell4"
        elif "agg_Funksjonell3" in available_ids:
            alder_codelist = "agg_Funksjonell3"

    return {
        "table_id": table_id,
        "era": era,
        "telemark_codes": telemark_codes,
        "telemark_fylke": telemark_fylke,
        "kommune_codelist": kommune_codelist,
        "fylke_codelist": fylke_codelist,
        "all_region_codelists": all_region_codelists,
        "alder_codelist": alder_codelist,
        "contents_codes": contents_codes,
        "has_framskriv": has_framskriv,
        "framskriv_codes": framskriv_codes,
        "time_codes": time_codes,
        "first_period": time_codes[0] if time_codes else None,
        "last_period": time_codes[-1] if time_codes else None,
    }


table_info = {}
for t in regional_tables:
    tid = t["id"]
    print(f"Fetching metadata for table {tid}...")
    info = get_telemark_info(tid, t["firstPeriod"])
    table_info[tid] = info
    print(
        f"  -> Era: {info['era']}, "
        f"found {len(info['telemark_codes'])} Telemark municipalities, "
        f"region codelist: {info['kommune_codelist']}, "
        f"fylke codelist: {info['fylke_codelist']}, "
        f"telemark_fylke code: {info['telemark_fylke']}, "
        f"alder codelist: {info['alder_codelist']}, "
        f"period: {info['first_period']}-{info['last_period']}"
    )
    print(f"  -> All region codelists: {info['all_region_codelists']}")
    if info["has_framskriv"]:
        print(f"  -> Has Framskriv dimension, codes: {info['framskriv_codes']}")
    print(f"  -> ContentsCode: {info['contents_codes']}")
    time.sleep(0.3)  # Be polite to the API

# ============================================================
# Step 4: Query data from each table for:
#         - Telemark municipalities
#         - Telemark fylke
#         - Norge / Landet
# ============================================================

# The v2 GET API is used for all tables (old and new).
# - If a kommune codelist exists -> use codelist[Region]=...
# - If not -> use direct region codes in valuecodes[Region]=...
# - Kjønn is eliminated by not specifying it (default behavior in v2)
# - Alder is broken down by functional age groups (agg_Funksjonell4 or agg_Funksjonell3)

all_dfs = []

for t in regional_tables:
    tid = t["id"]
    info = table_info[tid]

    if not info["telemark_codes"]:
        print(f"Skipping table {tid}: no Telemark municipalities found")
        continue

    region_values = ",".join(info["telemark_codes"])
    contents_values = ",".join(info["contents_codes"])
    alder_codelist = info["alder_codelist"]
    has_framskriv = info["has_framskriv"]
    framskriv_values = ",".join(info["framskriv_codes"]) if has_framskriv else ""

    # Build list of queries for this table
    table_queries = []

    # Common parameters for Framskriv-style tables (e.g. 08825)
    # vs normal tables where alternatives are in ContentsCode
    if has_framskriv:
        framskriv_part = f"&valuecodes[Framskriv]={framskriv_values}"
        heading = "ContentsCode,Tid,Alder"
        stub_kommun = "Framskriv,Region"
        stub_fylke = "Framskriv,Region"
        stub_norge = "Framskriv,Region"
    else:
        framskriv_part = ""
        heading = "ContentsCode,Tid,Alder"
        stub_kommun = "Region"
        stub_fylke = "Region"
        stub_norge = "Region"

    # Query 1: Telemark kommuner
    url_parts = [
        f"https://data.ssb.no/api/pxwebapi/v2/tables/{tid}/data?lang=no",
        "&outputFormat=json-stat2",
        f"&valuecodes[ContentsCode]={contents_values}",
        f"&valuecodes[Region]={region_values}",
        "&valuecodes[Tid]=*",
        "&valuecodes[Alder]=*",
        f"&codelist[Alder]={alder_codelist}",
        framskriv_part,
        f"&heading={heading}",
        f"&stub={stub_kommun}",
    ]
    if info["kommune_codelist"]:
        url_parts.append(f"&codelist[Region]={info['kommune_codelist']}")
    table_queries.append({"name": "kommuner", "url": "".join(url_parts)})

    # Query 2: Telemark fylke (if fylke codelist found)
    if info["fylke_codelist"] and info["telemark_fylke"]:
        fylke_code = info["telemark_fylke"]
        url_fylke = (
            f"https://data.ssb.no/api/pxwebapi/v2/tables/{tid}/data?lang=no"
            "&outputFormat=json-stat2"
            f"&valuecodes[ContentsCode]={contents_values}"
            f"&valuecodes[Region]={fylke_code}"
            f"&codelist[Region]={info['fylke_codelist']}"
            "&valuecodes[Alder]=*"
            f"&codelist[Alder]={alder_codelist}"
            "&valuecodes[Tid]=*"
            f"{framskriv_part}"
            f"&heading={heading}"
            f"&stub={stub_fylke}"
        )
        table_queries.append({"name": "Telemark fylke", "url": url_fylke})

    # Query 3: Norge / Landet
    url_norge = (
        f"https://data.ssb.no/api/pxwebapi/v2/tables/{tid}/data?lang=no"
        "&outputFormat=json-stat2"
        f"&valuecodes[ContentsCode]={contents_values}"
        "&valuecodes[Region]=*"
        "&codelist[Region]=vs_Landet"
        "&valuecodes[Alder]=*"
        f"&codelist[Alder]={alder_codelist}"
        "&valuecodes[Tid]=*"
        f"{framskriv_part}"
        f"&heading={heading}"
        f"&stub={stub_norge}"
    )
    table_queries.append({"name": "Norge", "url": url_norge})

    print(f"\nQuerying table {tid} ({t['firstPeriod']}-framskrivingen)...")

    table_dfs = []
    for q in table_queries:
        print(f"  {q['name']}: {q['url'][:100]}...")

        try:
            df = fetch_data(
                url=q["url"],
                payload=None,
                error_messages=error_messages,
                query_name=f"Framskriving {tid} - {q['name']}",
                response_type="json",
            )

            if df is not None and not df.empty:
                # For Framskriv-style tables, move 'alternativ' into 'statistikkvariabel'
                if has_framskriv and "alternativ" in df.columns:
                    df["statistikkvariabel"] = df["alternativ"]
                    df = df.drop(columns=["alternativ"])
                table_dfs.append(df)
                print(f"    -> Got {len(df)} rows")
            else:
                print(f"    -> No data returned")

        except Exception as e:
            print(f"    -> Error: {e}")
            error_messages.append(f"Table {tid} ({q['name']}): {e}")

        time.sleep(1.5)  # Avoid 429 rate limiting

    if table_dfs:
        df_table = pd.concat(table_dfs, ignore_index=True)
        df_table["Framskriving"] = t["firstPeriod"]
        df_table["Tabell"] = tid
        all_dfs.append(df_table)
        print(f"  -> Combined: {len(df_table)} rows")

    time.sleep(1.0)  # Be polite to the API

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

    file_name = f"Framskrevet_folkemengde_{first_year}-{last_year}.csv"
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
