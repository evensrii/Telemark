import os
import re
import requests
import pandas as pd
from pyjstat import pyjstat

from Helper_scripts.utility_functions import fetch_data
from Helper_scripts.email_functions import notify_errors
from Helper_scripts.github_functions import upload_github_file
from Helper_scripts.github_functions import download_github_file
from Helper_scripts.github_functions import handle_output_data

# Capture the name of the current script
script_name = os.path.basename(__file__)

# List to collect errors during execution
error_messages = []

# Telemark municipality names used to dynamically find region codes.
TELEMARK_NAMES = [
    "Porsgrunn", "Skien", "Notodden", "Siljan", "Bamble", "Kragerø",
    "Drangedal", "Nome", "Midt-Telemark", "Seljord", "Hjartdal", "Tinn",
    "Kviteseid", "Nissedal", "Fyresdal", "Tokke", "Vinje",
]

# ============================================================
# Step 1: Search SSB for all "Framskrevet folkemengde" tables
#         and find the most recent regional (K) table.
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

# Filter to regional (K) tables and pick the most recent one (highest firstPeriod)
regional_tables = []
for t in all_tables:
    has_region = "region" in t["variableNames"]
    has_k = "(K)" in t["label"]
    first_year = int(t["firstPeriod"]) if t["firstPeriod"].isdigit() else 0

    if has_region and has_k and first_year >= 2005:
        regional_tables.append(t)

# Sort by firstPeriod descending -> most recent first
regional_tables.sort(key=lambda t: t["firstPeriod"], reverse=True)
latest_table = regional_tables[0]

print(f"\nMost recent regional (K) table:")
print(f"  Table ID: {latest_table['id']}")
print(f"  Label: {latest_table['label']}")
print(f"  Period: {latest_table['firstPeriod']}-{latest_table['lastPeriod']}")
print(f"  Updated: {latest_table['updated'][:10]}")

# ============================================================
# Step 2: Fetch metadata for the most recent table
# ============================================================

table_id = latest_table["id"]
first_period = latest_table["firstPeriod"]

meta_url = f"https://data.ssb.no/api/pxwebapi/v2/tables/{table_id}/metadata?lang=no"
r = requests.get(meta_url)
r.raise_for_status()
meta = r.json()

# Determine era for region code prefix filtering
year = int(first_period)
if year >= 2024:
    valid_prefixes = ["40"]
elif year >= 2020:
    valid_prefixes = ["38"]
else:
    valid_prefixes = ["08"]

# Find Telemark municipality codes
region_dim = meta["dimension"]["Region"]
region_labels = region_dim["category"]["label"]

telemark_codes = []
for code, label in region_labels.items():
    if not any(code.startswith(p) for p in valid_prefixes):
        continue
    clean_label = re.sub(r"\s*\(.*?\)\s*$", "", label).strip()
    if clean_label in TELEMARK_NAMES:
        telemark_codes.append(code)

# Find kommune codelist (vs_KommunFram*) if available
kommune_codelist = None
if "extension" in region_dim and "codelists" in region_dim["extension"]:
    for cl in region_dim["extension"]["codelists"]:
        if "KommunFram" in cl.get("id", ""):
            kommune_codelist = cl["id"]
            break

# Find best available age codelist
alder_codelist = None
alder_dim = meta["dimension"].get("Alder", {})
if "extension" in alder_dim and "codelists" in alder_dim["extension"]:
    available_ids = [cl["id"] for cl in alder_dim["extension"]["codelists"]]
    if "agg_Funksjonell4" in available_ids:
        alder_codelist = "agg_Funksjonell4"
    elif "agg_Funksjonell3" in available_ids:
        alder_codelist = "agg_Funksjonell3"

# Get available ContentsCode values and time periods
contents_codes = list(meta["dimension"]["ContentsCode"]["category"]["index"].keys())
time_codes = list(meta["dimension"]["Tid"]["category"]["index"].keys())
first_year_data = time_codes[0] if time_codes else first_period
last_year_data = time_codes[-1] if time_codes else latest_table["lastPeriod"]

print(f"\nMetadata for table {table_id}:")
print(f"  Telemark municipalities: {len(telemark_codes)} found")
print(f"  Region codelist: {kommune_codelist}")
print(f"  Age codelist: {alder_codelist}")
print(f"  ContentsCode: {contents_codes}")
print(f"  Period: {first_year_data}-{last_year_data}")

# ============================================================
# Step 3: Query data from the most recent table
# ============================================================

region_values = ",".join(telemark_codes)
contents_values = ",".join(contents_codes)

url_parts = [
    f"https://data.ssb.no/api/pxwebapi/v2/tables/{table_id}/data?lang=no",
    "&outputFormat=json-stat2",
    f"&valuecodes[ContentsCode]={contents_values}",
    f"&valuecodes[Region]={region_values}",
    "&valuecodes[Tid]=*",
    "&valuecodes[Alder]=*",
    f"&codelist[Alder]={alder_codelist}",
    "&heading=ContentsCode,Tid,Alder",
    "&stub=Region",
]

if kommune_codelist:
    url_parts.append(f"&codelist[Region]={kommune_codelist}")

GET_URL = "".join(url_parts)

print(f"\nQuerying table {table_id}...")
print(f"  URL: {GET_URL[:120]}...")

try:
    df = fetch_data(
        url=GET_URL,
        payload=None,
        error_messages=error_messages,
        query_name=f"Siste framskriving {table_id}",
        response_type="json",
    )
except Exception as e:
    print(f"Error occurred: {e}")
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError(
        "A critical error occurred during data fetching, stopping execution."
    )

if df is None or df.empty:
    print("No data returned from query.")
    notify_errors(["No data returned from most recent framskriving table."], script_name=script_name)
    raise RuntimeError("No data returned.")

print(f"  -> Got {len(df)} rows")
print(f"  Columns: {list(df.columns)}")
print(df.head())

# ============================================================
# Step 4: Check if a new framskriving has been published.
#         If so, archive the old file before saving the new one.
# ============================================================

file_name = "befolkningsframskrivinger_siste_tabell.csv"
github_folder = "Data/01_Befolkning/Befolkningsframskrivinger"
temp_folder = os.environ.get("TEMP_FOLDER")

# Download the existing file from GitHub (if it exists)
existing_df = download_github_file(f"{github_folder}/{file_name}")

if existing_df is not None:
    # Determine the year range of the existing data
    existing_years = sorted(existing_df["år"].unique()) if "år" in existing_df.columns else []
    new_years = sorted(df["år"].unique()) if "år" in df.columns else []

    existing_range = f"{existing_years[0]}-{existing_years[-1]}" if existing_years else "unknown"
    new_range = f"{new_years[0]}-{new_years[-1]}" if new_years else "unknown"

    print(f"\nExisting data year range: {existing_range}")
    print(f"New data year range:      {new_range}")

    if existing_range != new_range and existing_range != "unknown":
        # A new framskriving has been published -> archive the old file
        archive_name = f"Framskrevet_folkemengde_{existing_range}.csv"
        print(f"\nNew framskriving detected! Archiving old data as: {archive_name}")

        # Save old data to temp folder, then upload as the archive file
        os.makedirs(temp_folder, exist_ok=True)
        archive_temp_path = os.path.join(temp_folder, archive_name)
        existing_df.to_csv(archive_temp_path, index=False, encoding="utf-8")

        upload_github_file(
            archive_temp_path,
            f"{github_folder}/{archive_name}",
            message=f"Archived previous framskriving: {archive_name}",
        )
        print(f"  Archived to GitHub: {github_folder}/{archive_name}")
    else:
        print("\nSame framskriving year range — no archiving needed.")
else:
    print("\nNo existing file on GitHub — first run, no archiving needed.")

# ============================================================
# Step 5: Save the new data as befolkningsframskrivinger_siste_tabell.csv
# ============================================================

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

task_name = "Befolkning - Befolkningsframskrivinger siste tabell"

# Call the function and get the "New Data" status
is_new_data = handle_output_data(df, file_name, github_folder, temp_folder, keepcsv=True)

# Write the "New Data" status to a unique log file
log_dir = os.environ.get("LOG_FOLDER", os.getcwd())
task_name_safe = task_name.replace(".", "_").replace(" ", "_")
new_data_status_file = os.path.join(log_dir, f"new_data_status_{task_name_safe}.log")

with open(new_data_status_file, "w", encoding="utf-8") as log_file:
    log_file.write(f"{task_name_safe},{file_name},{'Yes' if is_new_data else 'No'}\n")

if is_new_data:
    print("New data detected and pushed to GitHub.")
else:
    print("No new data detected.")

print(f"New data status log written to {new_data_status_file}")
