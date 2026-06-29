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

# Define Telemark kommuner
telemark_kommuner = [
    "Porsgrunn", "Skien", "Notodden", "Siljan", "Bamble", "Kragerø",
    "Drangedal", "Nome", "Midt-Telemark", "Seljord", "Hjartdal",
    "Tinn", "Kviteseid", "Nissedal", "Fyresdal", "Tokke", "Vinje"
]

# Get the year from data
year = df['År'].iloc[0]

# Reshape for Everviz: Kommune, Andel (ÅR), Label
df = df[['Geografi', 'value']].copy()
df['value'] = pd.to_numeric(df['value'], errors='coerce').round(0).astype('Int64')
df = df.rename(columns={
    'Geografi': 'Kommune',
    'value': f'Andel ({year})'
})
df['Label'] = df['Kommune']

# Sort: kommuner alphabetically, then Telemark and Hele landet last
kommuner_df = df[df['Kommune'].isin(telemark_kommuner)].sort_values('Kommune')
aggregates_df = df[df['Kommune'].isin(["Telemark", "Hele landet"])]
aggregates_df = aggregates_df.set_index('Kommune').loc[["Telemark", "Hele landet"]].reset_index()
df = pd.concat([kommuner_df, aggregates_df], ignore_index=True)

####################################################################
### EDITABLE SECTION END                                         ###
####################################################################

print(f"\nAfter processing: {len(df)} rows and {len(df.columns)} columns")

# %%
# Compare with GitHub and upload if changed
print("\nComparing with GitHub...")
has_changes = handle_output_data(
    df=df,
    file_name=output_filename,
    github_folder=github_folder,
    temp_folder=temp_folder,
    keepcsv=True
)

if has_changes:
    print("  ✓ New data detected and uploaded to GitHub")
    print("New data detected")  # For master_script.py parsing
else:
    print("  ✓ No changes detected")

# Save to temp folder
output_path = os.path.join(temp_folder, output_filename)
df.to_csv(output_path, index=False, encoding='utf-8')
print(f"\n  ✓ Saved to: {output_path}")

print(f"\n{'=' * 70}")
print("Processing complete")
print(f"{'=' * 70}\n")
