"""
FHI Query Script: Bor trangt.txt
================================

Auto-generated script for processing FHI query data.
Query file: Oppvekst og levekår/Levekår/Trangboddhet/Bor trangt.txt

This script:
1. Loads query from .txt file
2. Fetches data from FHI API
3. Processes data (EDITABLE SECTION - outside main() for Jupyter interactive use)
4. Compares with GitHub and uploads if changed
5. Saves to CSV output

NB: Only creates script if it doesn't exist already! Does not overwrite code in "EDITABLE SECTION". :)

Generated: 2026-06-26 14:00:45
"""

import json
import os
import sys
import pandas as pd
from pathlib import Path

# Get PYTHONPATH and add to sys.path
pythonpath = os.environ.get("PYTHONPATH")
if not pythonpath:
    # Navigate up from script location to find the Python folder
    current = Path(__file__).resolve()
    while current.name != "Python" and current != current.parent:
        current = current.parent
    pythonpath = str(current)
    os.environ["PYTHONPATH"] = pythonpath

sys.path.append(pythonpath)

from Helper_scripts.utility_functions import fetch_data
from Helper_scripts.github_functions import handle_output_data

# Capture the name of the current script
script_name = os.path.basename(__file__)

# Paths
query_file = os.path.join(
    pythonpath, 
    "Queries", 
    "08_Folkehelse_og_levekår", 
    "FHI", 
    "queries",
    "Oppvekst og levekår", "Levekår", "Trangboddhet", "Bor trangt.txt"
)

# Output configuration
output_filename = "bor_trangt.csv"
github_folder = "Data/08_Folkehelse og levekår/Oppvekst og levekår/Levekår/Trangboddhet"

output_filename_long = "trangboddhet.csv"
github_folder_long = "Data/08_Folkehelse og levekår/Oppvekst og levekår"

# Get temp folder
temp_folder = os.environ.get("TEMP_FOLDER")
if not temp_folder:
    temp_folder = os.path.join(pythonpath, "Temp")


def load_query_file(file_path):
    """
    Load URL and query from the query file.

    Returns:
        tuple: (url, query_dict)
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    url = lines[0].strip()
    query_lines = [line.strip() for line in lines[1:] if line.strip()]
    query = json.loads(''.join(query_lines))

    return url, query


# %%
print(f"\n{'=' * 70}")
print(f"FHI Query: Bor trangt.txt")
print(f"{'=' * 70}\n")

# Load query from file
print("Loading query from file...")
url, query = load_query_file(query_file)
print(f"  ✓ Query loaded")
print(f"  URL: {url}")

# %%
# Fetch data from FHI API
print("\nFetching data from FHI API...")
error_messages = []
df = fetch_data(
    url=url,
    payload=query,
    error_messages=error_messages,
    query_name="FHI Query",
    response_type="json"
)

if df is None or df.empty:
    print("  ✗ No data returned from API")
    if error_messages:
        for error in error_messages:
            print(f"    Error: {error}")
    sys.exit(1)

print(f"  ✓ Fetched {len(df)} rows and {len(df.columns)} columns")
print(f"  Columns: {', '.join(df.columns.tolist())}")
    
# %%
####################################################################
### EDITABLE SECTION START                                       ###
### Add your data transformations and processing here            ###
####################################################################

# Define mappings
kommunenummer_map = {
    "Hele landet": "00",
    "Telemark": "40",
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

telemark_kommuner = [
    "Porsgrunn", "Skien", "Notodden", "Siljan", "Bamble", "Kragerø",
    "Drangedal", "Nome", "Midt-Telemark", "Seljord", "Hjartdal",
    "Tinn", "Kviteseid", "Nissedal", "Fyresdal", "Tokke", "Vinje"
]

# Normalize column names: strip whitespace and create case-insensitive lookup
df.columns = [c.strip() for c in df.columns]
col_lookup = {c.lower(): c for c in df.columns}

def get_col(name):
    """Get actual column name matching name (case-insensitive)."""
    return col_lookup.get(name.lower(), name)

# Identify columns (tolerate different label casing / names)
geo_col = get_col('Geografi')
year_col = get_col('År')
alder_col = get_col('Alder')
innv_col = get_col('Innvandringsbakgrunn')
if innv_col not in df.columns:
    innv_col = get_col('Innvandringskategori')
if innv_col not in df.columns:
    innv_col = get_col('Innvkat')

# Find the value/measure column
value_col = None
for c in df.columns:
    if c.lower() in ['value', 'values', 'andel', 'rate']:
        value_col = c
        break
if value_col is None:
    numeric_cols = df.select_dtypes(include='number').columns.tolist()
    value_col = numeric_cols[-1] if numeric_cols else df.columns[-1]

print("\nDetected columns:")
print(f"  Geografi: {geo_col}")
print(f"  År:       {year_col}")
print(f"  Alder:    {alder_col}")
print(f"  Innv.kat: {innv_col}")
print(f"  Value:    {value_col}")
print(f"\nRaw data sample:")
print(df.head(10))
print(f"\nUnique Alder: {sorted(df[alder_col].dropna().unique())}")
print(f"Unique Innvandringsbakgrunn: {sorted(df[innv_col].dropna().unique())}")
print(f"Unique År: {sorted(df[year_col].dropna().unique())}")

# Ensure value is numeric
df[value_col] = pd.to_numeric(df[value_col], errors='coerce')

# Make filtering helpers that work regardless of exact labels
def filter_age(series, pattern):
    return series.astype(str).str.contains(pattern, case=False, na=False, regex=False)

# Determine latest year
latest_year = df[year_col].max()
print(f"\nLatest year in data: {latest_year}")

# ---------------------------------------------------------
# Output 1: bor_trangt.csv — latest year, 0-17 år, Totalt
# ---------------------------------------------------------
# Accept either exact "0-17 år" or any label containing "0-17"
df_bor_trangt = df[
    filter_age(df[alder_col], '0-17') &
    (df[innv_col].astype(str).str.lower() == 'totalt') &
    (df[year_col] == latest_year)
].copy()

if df_bor_trangt.empty:
    print(f"\n  Warning: No 0-17 data found for latest year {latest_year}")

df_bor_trangt = df_bor_trangt[[geo_col, value_col]].copy()
# API returns percentage (e.g. 15.01), round to nearest integer
df_bor_trangt[value_col] = pd.to_numeric(df_bor_trangt[value_col], errors='coerce').round(0).fillna(0).astype('int64')
df_bor_trangt = df_bor_trangt.rename(columns={
    geo_col: 'Kommune',
    value_col: f'Andel ({latest_year})'
})
df_bor_trangt['Label'] = df_bor_trangt['Kommune']

# Sort: kommuner alphabetically, then Telemark and Hele landet last
kommuner_df = df_bor_trangt[df_bor_trangt['Kommune'].isin(telemark_kommuner)].sort_values('Kommune')
aggregates_df = df_bor_trangt[df_bor_trangt['Kommune'].isin(["Telemark", "Hele landet"])]
if not aggregates_df.empty:
    aggregates_df = aggregates_df.set_index('Kommune').loc[["Telemark", "Hele landet"]].reset_index()
df_bor_trangt = pd.concat([kommuner_df, aggregates_df], ignore_index=True)

# ---------------------------------------------------------
# Output 2: trangboddhet.csv — alle aldre, all years, long format
# ---------------------------------------------------------
# Filter to "alle aldre" (case-insensitive)
df_trangboddhet = df[filter_age(df[alder_col], 'alle aldre')].copy()

if df_trangboddhet.empty:
    print("\n  Warning: No 'alle aldre' data found")

df_trangboddhet['Kommunenummer'] = df_trangboddhet[geo_col].map(kommunenummer_map)
df_trangboddhet['År'] = pd.to_datetime(df_trangboddhet[year_col].astype(str), errors='coerce').dt.strftime('%Y-%m-%d')
df_trangboddhet = df_trangboddhet.rename(columns={value_col: 'Andel'})

# Ensure Andel is numeric and between 0 and 1
df_trangboddhet['Andel'] = pd.to_numeric(df_trangboddhet['Andel'], errors='coerce')
if df_trangboddhet['Andel'].max() > 1:
    df_trangboddhet['Andel'] = df_trangboddhet['Andel'] / 100

# Keep only expected immigration categories (accept common variants)
innv_mask = df_trangboddhet[innv_col].astype(str).str.lower().isin(['totalt', 'innvandrere', 'alle'])
df_trangboddhet = df_trangboddhet[innv_mask]

# Order and sort columns
df_trangboddhet = df_trangboddhet[['Kommunenummer', geo_col, 'År', alder_col, innv_col, 'Andel']]
df_trangboddhet = df_trangboddhet.sort_values(
    by=['Kommunenummer', 'År', innv_col],
    ignore_index=True
)

print(f"\nbor_trangt.csv: {len(df_bor_trangt)} rows")
print(f"trangboddhet.csv: {len(df_trangboddhet)} rows")

####################################################################
### EDITABLE SECTION END                                         ###
####################################################################

print(f"\nAfter processing: {len(df)} rows and {len(df.columns)} columns")

# %%
# Compare bor_trangt.csv with GitHub and upload if changed
print("\nComparing bor_trangt.csv with GitHub...")
has_changes_bor_trangt = handle_output_data(
    df=df_bor_trangt,
    file_name=output_filename,
    github_folder=github_folder,
    temp_folder=temp_folder,
    keepcsv=True
)

if has_changes_bor_trangt:
    print("  ✓ New data detected in bor_trangt.csv and uploaded to GitHub")
else:
    print("  ✓ No changes detected in bor_trangt.csv")

# Compare trangboddhet.csv with GitHub and upload if changed
print("\nComparing trangboddhet.csv with GitHub...")
has_changes_trangboddhet = handle_output_data(
    df=df_trangboddhet,
    file_name=output_filename_long,
    github_folder=github_folder_long,
    temp_folder=temp_folder,
    keepcsv=True
)

if has_changes_trangboddhet:
    print("  ✓ New data detected in trangboddhet.csv and uploaded to GitHub")
else:
    print("  ✓ No changes detected in trangboddhet.csv")

if has_changes_bor_trangt or has_changes_trangboddhet:
    print("New data detected")  # For master_script.py parsing

# Save to temp folder
output_path = os.path.join(temp_folder, output_filename)
df_bor_trangt.to_csv(output_path, index=False, encoding='utf-8')
print(f"\n  ✓ Saved bor_trangt.csv to: {output_path}")

output_path_long = os.path.join(temp_folder, output_filename_long)
df_trangboddhet.to_csv(output_path_long, index=False, encoding='utf-8')
print(f"\n  ✓ Saved trangboddhet.csv to: {output_path_long}")

print(f"\n{'=' * 70}")
print("Processing complete")
print(f"{'=' * 70}\n")
