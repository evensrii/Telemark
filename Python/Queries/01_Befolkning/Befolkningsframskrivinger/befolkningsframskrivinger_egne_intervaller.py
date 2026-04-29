import os
import re
import requests
import pandas as pd

from Helper_scripts.utility_functions import fetch_data
from Helper_scripts.email_functions import notify_errors
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

print(f"\nMetadata for table {table_id}:")
print(f"  Telemark municipalities: {len(telemark_codes)} found")
print(f"  Municipality codes: {telemark_codes}")
print(f"  Region codelist: {kommune_codelist}")

# ============================================================
# Step 3: Query data (single-year ages, MMMM only, kommuner)
# ============================================================

region_values = ",".join(telemark_codes)

url_parts = [
    f"https://data.ssb.no/api/pxwebapi/v2/tables/{table_id}/data?lang=no",
    "&outputFormat=json-stat2",
    "&valuecodes[ContentsCode]=Personer",
    f"&valuecodes[Region]={region_values}",
    "&valuecodes[Tid]=*",
    "&valuecodes[Alder]=*",
    "&codelist[Alder]=vs_AlleAldre00B",
    "&heading=ContentsCode,Tid",
    "&stub=Region,Alder",
]
if kommune_codelist:
    url_parts.append(f"&codelist[Region]={kommune_codelist}")

query_url = "".join(url_parts)

print(f"\nQuerying Telemark kommuner from table {table_id}...")
print(f"  URL: {query_url[:120]}...")

try:
    df = fetch_data(
        url=query_url,
        payload=None,
        error_messages=error_messages,
        query_name=f"Framskriving egne intervaller - Telemark kommuner",
        response_type="json",
    )
except Exception as e:
    print(f"Error occurred: {e}")
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError(
        "A critical error occurred during data fetching, stopping execution."
    )

if df is None or df.empty:
    print("No data returned.")
    notify_errors(["No data returned from most recent framskriving table."], script_name=script_name)
    raise RuntimeError("No data returned.")

print(f"\nRaw data: {len(df)} rows")
print(f"  Columns: {list(df.columns)}")
print(f"  Regions: {sorted(df['region'].unique())}")
print(df.head())

# ============================================================
# Step 4: Parse age and aggregate into custom intervals
#         0-19, 20-39, 40-66, 67-79, 80+
# ============================================================

# Extract numeric age from the "alder" column (e.g. "0 år" -> 0, "105 år eller eldre" -> 105)
df["alder_num"] = df["alder"].str.extract(r"(\d+)").astype(int)

# Map to custom age groups
def age_group(age):
    if age <= 19:
        return "0-19 år"
    elif age <= 39:
        return "20-39 år"
    elif age <= 66:
        return "40-66 år"
    elif age <= 79:
        return "67-79 år"
    else:
        return "80+ år"

df["Aldersgruppe"] = df["alder_num"].apply(age_group)

# Aggregate: sum value per region, year, and age group
df_agg = (
    df.groupby(["region", "år", "Aldersgruppe"], as_index=False)["value"]
    .sum()
)

# Rename columns
df_agg = df_agg.rename(columns={
    "region": "Kommune",
    "år": "År",
    "value": "Personer",
})

print(f"\nAggregated data: {len(df_agg)} rows")
print(f"  Columns: {list(df_agg.columns)}")
print(f"  Age groups: {sorted(df_agg['Aldersgruppe'].unique())}")
print(df_agg.head(10))

# ============================================================
# Step 5: Add Kommunenummer and SortColumn, sort
# ============================================================

kommunenummer_map = {
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
df_agg["Kommunenummer"] = df_agg["Kommune"].map(kommunenummer_map)

# Gir sorteringsrekkefølge som passer best til tilhørende graf.
age_sort_order = {
    "0-19 år": 1,
    "20-39 år": 2,
    "40-66 år": 5,
    "67-79 år": 4,
    "80+ år": 3,
}
df_agg["SortColumn"] = df_agg["Aldersgruppe"].map(age_sort_order).astype(int)

# Sort by Kommune, age group, year
df_agg = df_agg.sort_values(
    by=["Kommune", "SortColumn", "År"],
    ignore_index=True,
)

# Convert År to datetime format (YYYY-01-01)
df_agg["År"] = pd.to_datetime(df_agg["År"], format="%Y").dt.strftime("%Y-%m-%d")

# Reorder columns
df_agg = df_agg[["Kommunenummer", "Kommune", "Aldersgruppe", "År", "Personer", "SortColumn"]]

# Ensure numeric columns have correct types
df_agg["Personer"] = pd.to_numeric(df_agg["Personer"], errors="coerce")

print(f"\nFinal data: {len(df_agg)} rows")
print(f"  Columns: {list(df_agg.columns)}")
print(df_agg.head(20))

# ============================================================
# Step 6: Save to CSV, compare and upload to GitHub
# ============================================================

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "befolkningsframskrivinger_siste_tabell_egendef_alder.csv"
task_name = "Befolkning - Befolkningsframskrivinger egne intervaller"
github_folder = "Data/01_Befolkning/Befolkningsframskrivinger"
temp_folder = os.environ.get("TEMP_FOLDER")

# Call the function and get the "New Data" status
is_new_data = handle_output_data(df_agg, file_name, github_folder, temp_folder, keepcsv=True)

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
