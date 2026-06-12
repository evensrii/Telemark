import os
import re
import requests
import pandas as pd
from pyjstat import pyjstat

from Helper_scripts.utility_functions import fetch_data
from Helper_scripts.email_functions import notify_errors
from Helper_scripts.github_functions import handle_output_data

# Capture the name of the current script
script_name = os.path.basename(__file__)

# List to collect errors during execution
error_messages = []

# ============================================================
# Region groups for aggregation
# ============================================================

GROUPS = {
    "Grenland": ["Porsgrunn", "Skien", "Siljan", "Bamble"],
    "Vest-Telemark": ["Seljord", "Hjartdal", "Kviteseid", "Nissedal", "Fyresdal", "Tokke", "Vinje"],
    "Øst- og Midt-Telemark": ["Notodden", "Kragerø", "Drangedal", "Nome", "Midt-Telemark", "Tinn"],
}

print("Region groups:")
for group, kommuner in GROUPS.items():
    print(f"  {group}: {', '.join(kommuner)}")

# ============================================================
# Step 1: Find the latest "Framskrevet folkemengde" (K) table
#         and derive the Folkevekst table (ID + 1)
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

print(f"\nFound {len(all_tables)} tables matching 'Framskrevet folkemengde'")

regional_tables = []
for t in all_tables:
    has_region = "region" in t["variableNames"]
    has_k = "(K)" in t["label"]
    first_year = int(t["firstPeriod"]) if t["firstPeriod"].isdigit() else 0

    if has_region and has_k and first_year >= 2005:
        regional_tables.append(t)

regional_tables.sort(key=lambda t: t["firstPeriod"], reverse=True)
latest_table = regional_tables[0]

befolkning_table_id = latest_table["id"]
folkevekst_table_id = str(int(befolkning_table_id) + 1)

print(f"\nBefolkningsframskrivinger table:")
print(f"  Table ID: {befolkning_table_id}")
print(f"  Label: {latest_table['label']}")
print(f"  Period: {latest_table['firstPeriod']}-{latest_table['lastPeriod']}")

print(f"\nFolkevekst table (ID + 1): {folkevekst_table_id}")

# ============================================================
# Step 2: Fetch metadata to find region codes and codelist
# ============================================================

meta_url = f"https://data.ssb.no/api/pxwebapi/v2/tables/{befolkning_table_id}/metadata?lang=no"
r = requests.get(meta_url)
r.raise_for_status()
meta = r.json()

first_period = latest_table["firstPeriod"]
year = int(first_period)
if year >= 2024:
    valid_prefixes = ["40"]
elif year >= 2020:
    valid_prefixes = ["38"]
else:
    valid_prefixes = ["08"]

region_dim = meta["dimension"]["Region"]
region_labels = region_dim["category"]["label"]

# Build a mapping: clean_name -> code
name_to_code = {}
for code, label in region_labels.items():
    if not any(code.startswith(p) for p in valid_prefixes):
        continue
    clean_label = re.sub(r"\s*\(.*?\)\s*$", "", label).strip()
    name_to_code[clean_label] = code

# Map group names to codes
group_codes = {}
all_codes = []
for group, names in GROUPS.items():
    codes = []
    for name in names:
        if name in name_to_code:
            codes.append(name_to_code[name])
        else:
            print(f"  WARNING: '{name}' not found in region labels!")
    group_codes[group] = codes
    all_codes.extend(codes)

print(f"\nTelemark municipality codes found: {len(all_codes)}")
for group, codes in group_codes.items():
    print(f"  {group}: {codes}")

# Find kommune codelist (vs_KommunFram*)
kommune_codelist = None
if "extension" in region_dim and "codelists" in region_dim["extension"]:
    for cl in region_dim["extension"]["codelists"]:
        if "KommunFram" in cl.get("id", ""):
            kommune_codelist = cl["id"]
            break

print(f"  Kommune codelist: {kommune_codelist}")

region_values = ",".join(all_codes)

# ============================================================
# Step 3: Query Befolkningsframskrivinger (population totals)
#         Get "Personer" (MMMM) for all years
# ============================================================

url_befolkning = (
    f"https://data.ssb.no/api/pxwebapi/v2/tables/{befolkning_table_id}/data?lang=no"
    "&outputFormat=json-stat2"
    "&valuecodes[ContentsCode]=Personer"
    "&valuecodes[Tid]=*"
    f"&valuecodes[Region]={region_values}"
)
if kommune_codelist:
    url_befolkning += f"&codelist[Region]={kommune_codelist}"

print(f"\nQuerying Befolkningsframskrivinger (table {befolkning_table_id})...")

try:
    df_befolkning = fetch_data(
        url=url_befolkning,
        payload=None,
        error_messages=error_messages,
        query_name=f"Befolkningsframskrivinger {befolkning_table_id}",
        response_type="json",
    )
except Exception as e:
    print(f"Error occurred: {e}")
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError("A critical error occurred during data fetching, stopping execution.")

print(f"  -> Got {len(df_befolkning)} rows")
print(df_befolkning.head())

# ============================================================
# Step 4: Query Folkevekst (births, deaths, migration)
#         Get all ContentsCode, all years, MMMM alternative
# ============================================================

url_folkevekst = (
    f"https://data.ssb.no/api/pxwebapi/v2/tables/{folkevekst_table_id}/data?lang=no"
    "&outputFormat=json-stat2"
    "&valuecodes[ContentsCode]=*"
    "&valuecodes[Tid]=*"
    f"&valuecodes[Region]={region_values}"
    "&valuecodes[Framskriv]=MMMM"
)
if kommune_codelist:
    url_folkevekst += f"&codelist[Region]={kommune_codelist}"

print(f"\nQuerying Folkevekst (table {folkevekst_table_id})...")

try:
    df_folkevekst = fetch_data(
        url=url_folkevekst,
        payload=None,
        error_messages=error_messages,
        query_name=f"Folkevekst {folkevekst_table_id}",
        response_type="json",
    )
except Exception as e:
    print(f"Error occurred: {e}")
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError("A critical error occurred during data fetching, stopping execution.")

print(f"  -> Got {len(df_folkevekst)} rows")
print(df_folkevekst.head())

# ============================================================
# Step 5: Process Befolkningsframskrivinger
#         Sum population per group for earliest and latest year
#         Calculate "Samlet vekst" as percentage change
# ============================================================

print("\nBefolkningsframskrivinger columns:", list(df_befolkning.columns))

# Identify key columns
value_col = df_befolkning.columns[-1]
region_col = [c for c in df_befolkning.columns if c.lower() == "region"][0]
year_col = [c for c in df_befolkning.columns if c.lower() in ("år", "year", "tid")][0]

df_befolkning[value_col] = pd.to_numeric(df_befolkning[value_col], errors="coerce")
df_befolkning[year_col] = df_befolkning[year_col].astype(str)

# Helper function to map region labels to groups
def get_group_from_region(region_label):
    clean = re.sub(r"\s*\(.*?\)\s*$", "", str(region_label)).strip()
    for group, names in GROUPS.items():
        if clean in names:
            return group
    return None

df_befolkning["Område"] = df_befolkning[region_col].apply(get_group_from_region)
df_befolkning = df_befolkning.dropna(subset=["Område"])

# Sum population per group per year (across kjønn, alder, etc.)
df_pop = df_befolkning.groupby(["Område", year_col])[value_col].sum().reset_index()

# Get earliest and latest year
all_years = sorted(df_pop[year_col].unique())
earliest_year = all_years[0]
latest_year = all_years[-1]
print(f"\nPopulation period: {earliest_year} - {latest_year}")

# Calculate percentage change per group
df_earliest = df_pop[df_pop[year_col] == earliest_year].set_index("Område")[value_col]
df_latest = df_pop[df_pop[year_col] == latest_year].set_index("Område")[value_col]

samlet_vekst = ((df_latest - df_earliest) / df_earliest * 100).round(1)
print("\nSamlet vekst (%):")
print(samlet_vekst)

# ============================================================
# Step 6: Process Folkevekst
#         Sum each component across all years per group
#         Calculate percentage relative to base population
# ============================================================

print("\nFolkevekst columns:", list(df_folkevekst.columns))

fv_value_col = df_folkevekst.columns[-1]
fv_region_col = [c for c in df_folkevekst.columns if c.lower() == "region"][0]
fv_contents_col = [c for c in df_folkevekst.columns if c.lower() in ("statistikkvariabel", "contentscode")][0]

df_folkevekst[fv_value_col] = pd.to_numeric(df_folkevekst[fv_value_col], errors="coerce")

df_folkevekst["Område"] = df_folkevekst[fv_region_col].apply(get_group_from_region)
df_folkevekst = df_folkevekst.dropna(subset=["Område"])

# Sum each component across all years per group
df_components = df_folkevekst.groupby(["Område", fv_contents_col])[fv_value_col].sum().reset_index()

# Pivot to get one column per component
df_pivot = df_components.pivot(index="Område", columns=fv_contents_col, values=fv_value_col)

print("\nPivoted components (absolute numbers):")
print(df_pivot)

# Find actual column names (from json-stat labels)
fodte_col = [c for c in df_pivot.columns if "fødte" in c.lower() or "fodt" in c.lower()][0]
dode_col = [c for c in df_pivot.columns if "døde" in c.lower() or "dode" in c.lower()][0]
innvandring_col = [c for c in df_pivot.columns if "innvandring" in c.lower()][0]
innflytting_col = [c for c in df_pivot.columns if "innflytting" in c.lower() or "nettoinnflytting" in c.lower()][0]

# Calculate components
naturlig_tilvekst = df_pivot[fodte_col] - df_pivot[dode_col]
nettoinnvandring = df_pivot[innvandring_col]
nettoinnflytting = df_pivot[innflytting_col]

# Calculate percentages relative to base population (earliest year)
pct_naturlig = (naturlig_tilvekst / df_earliest * 100).round(1)
pct_innvandring = (nettoinnvandring / df_earliest * 100).round(1)
pct_innflytting = (nettoinnflytting / df_earliest * 100).round(1)

# ============================================================
# Step 7: Combine into final DataFrame
# ============================================================

df_result = pd.DataFrame({
    "Område": samlet_vekst.index,
    "Naturlig tilvekst": pct_naturlig.values,
    "Vekst fra nettoinnvandring": pct_innvandring.values,
    "Vekst fra nettoinnflytting": pct_innflytting.values,
    "Samlet vekst": samlet_vekst.values,
})

# Order rows as in the screenshot
row_order = ["Grenland", "Øst- og Midt-Telemark", "Vest-Telemark"]
df_result = df_result.set_index("Område").loc[row_order].reset_index()

# Ensure numeric columns are float64 for github_functions compatibility
for col in ["Naturlig tilvekst", "Vekst fra nettoinnvandring", "Vekst fra nettoinnflytting", "Samlet vekst"]:
    df_result[col] = df_result[col].astype(float)

print("\n=== Final result ===")
print(df_result.to_string(index=False))

# Print summary for each area
print("\n=== Summary per area ===")
for area in row_order:
    pop_first = int(df_earliest[area])
    pop_last = int(df_latest[area])
    pct_total = samlet_vekst[area]
    abs_nat = int(naturlig_tilvekst[area])
    abs_inn = int(nettoinnvandring[area])
    abs_fly = int(nettoinnflytting[area])
    pct_nat = pct_naturlig[area]
    pct_inn = pct_innvandring[area]
    pct_fly = pct_innflytting[area]
    print(f"\n{area}:")
    print(f"  Befolkning {earliest_year}: {pop_first:,} -> {latest_year}: {pop_last:,} (Samlet vekst: {pct_total}%)")
    print(f"  Naturlig tilvekst: {abs_nat:,} / {pop_first:,} * 100 = {pct_nat}%")
    print(f"  Nettoinnvandring:  {abs_inn:,} / {pop_first:,} * 100 = {pct_inn}%")
    print(f"  Nettoinnflytting:  {abs_fly:,} / {pop_first:,} * 100 = {pct_fly}%")

# ============================================================
# Step 8: Save and upload to GitHub
# ============================================================

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "bidrag_til_samlet_befolkningsvekst.csv"
task_name = "Befolkning - Bidrag til samlet befolkningsvekst"
github_folder = "Data/01_Befolkning/Befolkningsframskrivinger"
temp_folder = os.environ.get("TEMP_FOLDER")

# Call the function and get the "New Data" status
is_new_data = handle_output_data(df_result, file_name, github_folder, temp_folder, keepcsv=True)

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
