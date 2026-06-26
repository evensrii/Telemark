"""
FHI Query Script: Vedvarende lavinntekt etter innvandringskategori, kommunegrense.txt
=====================================================================================

Auto-generated script for processing FHI query data.
Query file: Oppvekst og levekår/Levekår/Inntekt og gjeld/Vedvarende lavinntekt etter innvandringskategori, kommunegrense.txt

This script:
1. Loads query from .txt file
2. Fetches data from FHI API
3. Processes data (EDITABLE SECTION - outside main() for Jupyter interactive use)
4. Compares with GitHub and uploads if changed
5. Saves to CSV output

Generated: 2026-06-26 12:37:29
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
    "Oppvekst og levekår", "Levekår", "Inntekt og gjeld", "Vedvarende lavinntekt etter innvandringskategori, kommunegrense.txt"
)

# Output configuration
output_filename = "vedvarende_lavinntekt_etter_innvandringskategori_kommunegrense.csv"
github_folder = "Data/08_Folkehelse og levekår/Oppvekst og levekår/Levekår/Inntekt og gjeld"

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
print(f"FHI Query: Vedvarende lavinntekt etter innvandringskategori, kommunegrense.txt")
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

# Define Telemark kommuner (exclude aggregates like "Hele landet" and "Telemark")
telemark_kommuner = [
    "Porsgrunn", "Skien", "Notodden", "Siljan", "Bamble", "Kragerø",
    "Drangedal", "Nome", "Midt-Telemark", "Seljord", "Hjartdal",
    "Tinn", "Kviteseid", "Nissedal", "Fyresdal", "Tokke", "Vinje"
]

# Filter: Telemark kommuner + aggregates, total
all_geographies = telemark_kommuner + ["Telemark", "Hele landet"]
df_base = df[
    (df['Geografi'].isin(all_geographies)) &
    (df['Innvandringsbakgrunn'] == 'total')
].copy()

# Find the latest period
latest_period = df_base['År'].max()
print(f"  Latest period: {latest_period}")

df_base = df_base[df_base['År'] == latest_period].copy()

def reshape_for_everviz(df_input, alder_filter):
    """Filter by alder and reshape to Everviz format."""
    df_out = df_input[df_input['Alder'] == alder_filter][['Geografi', 'value']].copy()
    df_out['value'] = pd.to_numeric(df_out['value'], errors='coerce').round(0).astype('Int64')
    df_out = df_out.rename(columns={
        'Geografi': 'Kommune',
        'value': f'Andel ({latest_period})'
    })
    df_out['Label'] = df_out['Kommune']
    # Sort: kommuner alphabetically, then Telemark and Hele landet last
    kommuner_part = df_out[df_out['Kommune'].isin(telemark_kommuner)].sort_values('Kommune')
    aggregates_part = df_out[df_out['Kommune'].isin(["Telemark", "Hele landet"])]
    aggregates_part = aggregates_part.set_index('Kommune').loc[["Telemark", "Hele landet"]].reset_index()
    return pd.concat([kommuner_part, aggregates_part], ignore_index=True)

# Create two output DataFrames
df_alle_aldre = reshape_for_everviz(df_base, 'alle aldre')
df_0_17 = reshape_for_everviz(df_base, '0-17 år')

####################################################################
### EDITABLE SECTION END                                         ###
####################################################################

print(f"\nAfter processing: {len(df_alle_aldre)} rows (alle aldre), {len(df_0_17)} rows (0-17 år)")

# %%
# Output file names
output_files = [
    ("vedvarende_lavinntekt_alle_aldre.csv", df_alle_aldre),
    ("vedvarende_lavinntekt_0-17.csv", df_0_17),
]

# Compare with GitHub and upload if changed
print("\nComparing with GitHub...")
for file_name, df_out in output_files:
    print(f"\n  Processing: {file_name}")
    has_changes = handle_output_data(
        df=df_out,
        file_name=file_name,
        github_folder=github_folder,
        temp_folder=temp_folder,
        keepcsv=True
    )
    if has_changes:
        print(f"    ✓ New data detected and uploaded to GitHub")
        print("New data detected")  # For master_script.py parsing
    else:
        print(f"    ✓ No changes detected")

# Save to local output directory
output_dir = os.path.join(pythonpath, "..", "Data", "08_Folkehelse og levekår", "Oppvekst og levekår", "Levekår", "Inntekt og gjeld")
os.makedirs(output_dir, exist_ok=True)
for file_name, df_out in output_files:
    output_path = os.path.join(output_dir, file_name)
    df_out.to_csv(output_path, index=False, encoding='utf-8')
    print(f"  ✓ Saved to: {output_path}")

print(f"\n{'=' * 70}")
print("Processing complete")
print(f"{'=' * 70}\n")
