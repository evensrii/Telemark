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

# Telemark municipality names used to dynamically find region codes.
TELEMARK_NAMES = [
    "Porsgrunn", "Skien", "Notodden", "Siljan", "Bamble", "Kragerø",
    "Drangedal", "Nome", "Midt-Telemark", "Seljord", "Hjartdal", "Tinn",
    "Kviteseid", "Nissedal", "Fyresdal", "Tokke", "Vinje",
]

# ============================================================
# Step 1: Find the latest "Framskrevet folkemengde" (K) table
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

regional_tables = []
for t in all_tables:
    has_region = "region" in t["variableNames"]
    has_k = "(K)" in t["label"]
    first_year = int(t["firstPeriod"]) if t["firstPeriod"].isdigit() else 0

    if has_region and has_k and first_year >= 2005:
        regional_tables.append(t)

regional_tables.sort(key=lambda t: t["firstPeriod"], reverse=True)
latest_table = regional_tables[0]

table_id = latest_table["id"]
first_period = latest_table["firstPeriod"]

print(f"\nMost recent regional (K) table:")
print(f"  Table ID: {table_id}")
print(f"  Label: {latest_table['label']}")
print(f"  Period: {latest_table['firstPeriod']}-{latest_table['lastPeriod']}")

# ============================================================
# Step 2: Fetch metadata to find region codes and codelists
# ============================================================

meta_url = f"https://data.ssb.no/api/pxwebapi/v2/tables/{table_id}/metadata?lang=no"
r = requests.get(meta_url)
r.raise_for_status()
meta = r.json()

year = int(first_period)
if year >= 2024:
    valid_prefixes = ["40"]
elif year >= 2020:
    valid_prefixes = ["38"]
else:
    valid_prefixes = ["08"]

region_dim = meta["dimension"]["Region"]
region_labels = region_dim["category"]["label"]

# Find Telemark municipality codes
telemark_codes = []
for code, label in region_labels.items():
    if not any(code.startswith(p) for p in valid_prefixes):
        continue
    clean_label = re.sub(r"\s*\(.*?\)\s*$", "", label).strip()
    if clean_label in TELEMARK_NAMES:
        telemark_codes.append(code)

# Find Telemark county code (e.g. "40" for 2024+)
telemark_county_code = valid_prefixes[0]

# All county codes (including Hele landet)
county_codes = ["0", "11", "15", "18", "31", "32", "33", "34", "39", "40", "42", "46", "50", "55", "56"]

# Build full region list: all counties + Telemark municipalities
all_region_codes = county_codes + telemark_codes

print(f"\nTelemark municipalities: {len(telemark_codes)} found")
print(f"Counties: {county_codes}")
print(f"Total region codes in query: {len(all_region_codes)}")

# Find the Funksjonell2a age codelist (0-5, 6-15, 16-19, 20-66, 67+)
alder_codelist = None
alder_dim = meta["dimension"].get("Alder", {})
if "extension" in alder_dim and "codelists" in alder_dim["extension"]:
    for cl in alder_dim["extension"]["codelists"]:
        if "Funksjonell2a" in cl.get("id", ""):
            alder_codelist = cl["id"]
            break

# Fetch the age codelist to find the code for 67+
alder_codelist_url = f"https://data.ssb.no/api/pxwebapi/v2/codeLists/{alder_codelist}?lang=no"
r = requests.get(alder_codelist_url)
r.raise_for_status()
alder_meta = r.json()

# Get all age group codes from the codelist
alder_codes = [v["code"] for v in alder_meta["values"]]
alder_labels = {v["code"]: v["label"] for v in alder_meta["values"]}

# Find the 67+ code
code_67_plus = None
for code, label in alder_labels.items():
    if "67" in label:
        code_67_plus = code
        break

print(f"  Age codelist: {alder_codelist}")
print(f"  Age groups: {alder_labels}")
print(f"  67+ code: {code_67_plus} ({alder_labels.get(code_67_plus, 'N/A')})")

region_values = ",".join(all_region_codes)
alder_values = ",".join(alder_codes)

# ============================================================
# Step 3: Query data — all age groups, latest year, MMMM
# ============================================================

url_query = (
    f"https://data.ssb.no/api/pxwebapi/v2/tables/{table_id}/data?lang=no"
    "&outputFormat=json-stat2"
    "&valuecodes[ContentsCode]=Personer"
    f"&valuecodes[Tid]={first_period},2040,2050"
    f"&valuecodes[Region]={region_values}"
    f"&valuecodes[Alder]={alder_values}"
    f"&codelist[Alder]={alder_codelist}"
    f"&outputValues[Alder]=aggregated"
)

print(f"\nQuerying table {table_id}...")

try:
    df = fetch_data(
        url=url_query,
        payload=None,
        error_messages=error_messages,
        query_name=f"Andel 67+ framskriving {table_id}",
        response_type="json",
    )
except Exception as e:
    print(f"Error occurred: {e}")
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError("A critical error occurred during data fetching, stopping execution.")

print(f"  -> Got {len(df)} rows")
print(df.head(10))

# ============================================================
# Step 4: Calculate percentage of 67+ per region
# ============================================================

print("\nColumns:", list(df.columns))

value_col = df.columns[-1]
region_col = [c for c in df.columns if c.lower() == "region"][0]
alder_col = [c for c in df.columns if c.lower() == "alder"][0]
tid_col = [c for c in df.columns if c.lower() in ("tid", "år")][0]

df[value_col] = pd.to_numeric(df[value_col], errors="coerce")

# Clean region names (remove code prefix, e.g. "4001 Porsgrunn" -> "Porsgrunn")
df["Region_clean"] = df[region_col].apply(
    lambda x: re.sub(r"^\d+\s+", "", str(x)).strip()
)

# Split data by year
df_2050 = df[df[tid_col].astype(str) == "2050"]
df_2040 = df[df[tid_col].astype(str) == "2040"]
df_earliest = df[df[tid_col].astype(str) == first_period]

label_67 = alder_labels[code_67_plus]
group_cols = ["Region_clean", alder_col]

# --- Helper function to calculate 67+ percentage ---
def calc_andel_67(df_year):
    df_agg = df_year.groupby(group_cols)[value_col].sum().reset_index()
    df_total = df_agg.groupby("Region_clean")[value_col].sum().reset_index()
    df_total = df_total.rename(columns={value_col: "Total"})
    df_67 = df_agg[df_agg[alder_col] == label_67][["Region_clean", value_col]]
    df_67 = df_67.rename(columns={value_col: "67+"})
    df_merged = df_total.merge(df_67, on="Region_clean", how="left")
    df_merged["Andel"] = (df_merged["67+"] / df_merged["Total"] * 100).round(1)
    return df_merged

df_andel_2050 = calc_andel_67(df_2050)
df_andel_2040 = calc_andel_67(df_2040)
df_andel_earliest = calc_andel_67(df_earliest)

# --- CSV output: Telemark kommuner + Telemark + Hele landet ---

# Identify which regions are Telemark kommuner
telemark_names = [re.sub(r"\s*\(.*?\)\s*$", "", region_labels[c]).strip() for c in telemark_codes]
csv_regions = telemark_names + ["Telemark", "Hele landet"]

df_csv = df_andel_2050[df_andel_2050["Region_clean"].isin(csv_regions)].copy()
df_csv["Kommune"] = df_csv["Region_clean"]
df_csv["Label"] = df_csv["Kommune"]
df_csv["Andel"] = df_csv["Andel"].round(0).astype(int)
df_csv = df_csv[["Kommune", "Andel", "Label"]]

# Sort kommuner alphabetically, then place Telemark and Hele landet at the end
df_kommuner = df_csv[~df_csv["Kommune"].isin(["Telemark", "Hele landet"])].sort_values("Kommune")
df_telemark = df_csv[df_csv["Kommune"] == "Telemark"]
df_landet = df_csv[df_csv["Kommune"] == "Hele landet"]
df_result = pd.concat([df_kommuner, df_telemark, df_landet], ignore_index=True)

# Ensure numeric column is integer
df_result["Andel"] = df_result["Andel"].astype(int)

print("\n=== Final result (CSV) ===")
print(df_result.to_string(index=False))

# --- Print: Telemark 2040 vs 2050 ---

andel_2050_telemark = df_andel_2050[df_andel_2050["Region_clean"] == "Telemark"]["Andel"].values[0]
andel_2040_telemark = df_andel_2040[df_andel_2040["Region_clean"] == "Telemark"]["Andel"].values[0]
print(f"\n=== Telemark: Andel 67+ ===")
print(f"  2040: {andel_2040_telemark}%")
print(f"  2050: {andel_2050_telemark}%")

# --- Print: All counties sorted by 67+ in earliest year ---

# Identify county names from data (exclude Telemark kommuner)
df_counties_earliest = df_andel_earliest[~df_andel_earliest["Region_clean"].isin(telemark_names)].copy()
df_counties_earliest = df_counties_earliest.sort_values("Andel", ascending=False).reset_index(drop=True)

print(f"\n=== Andel 67+ i {first_period}, alle fylker (sortert) ===")
for _, row in df_counties_earliest.iterrows():
    print(f"  {row['Region_clean']}: {row['Andel']}%")

# --- Print: Kommuner with population decline (2050 < earliest year) ---

# Total population per kommune in earliest year and 2050
df_pop_earliest = df_earliest.groupby("Region_clean")[value_col].sum().reset_index()
df_pop_earliest = df_pop_earliest.rename(columns={value_col: "Pop_earliest"})
df_pop_2050 = df_2050.groupby("Region_clean")[value_col].sum().reset_index()
df_pop_2050 = df_pop_2050.rename(columns={value_col: "Pop_2050"})

df_pop_compare = df_pop_earliest.merge(df_pop_2050, on="Region_clean", how="inner")
df_pop_compare = df_pop_compare[df_pop_compare["Region_clean"].isin(telemark_names)]
df_pop_compare["Endring"] = df_pop_compare["Pop_2050"] - df_pop_compare["Pop_earliest"]
df_decline = df_pop_compare[df_pop_compare["Endring"] < 0].sort_values("Endring")

print(f"\n=== Kommuner med befolkningsnedgang ({first_period} -> 2050) ===")
if len(df_decline) > 0:
    for _, row in df_decline.iterrows():
        print(f"  {row['Region_clean']}: {int(row['Pop_earliest'])} -> {int(row['Pop_2050'])} ({int(row['Endring'])})")
else:
    print("  Ingen kommuner med nedgang.")

# ============================================================
# Step 5: Save and upload to GitHub
# ============================================================

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "andel_67_pluss.csv"
task_name = "Befolkning - Andel 67 pluss"
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
