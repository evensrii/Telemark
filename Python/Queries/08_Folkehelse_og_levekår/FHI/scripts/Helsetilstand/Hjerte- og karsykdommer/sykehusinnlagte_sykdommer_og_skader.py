"""
FHI Query Script: Sykehusinnlagte sykdommer og skader.txt
=========================================================

Auto-generated script for processing FHI query data.
Query file: Helsetilstand/Hjerte- og karsykdommer/Sykehusinnlagte sykdommer og skader.txt

This script:
1. Loads query from .txt file
2. Fetches data from FHI API
3. Processes data (EDITABLE SECTION - outside main() for Jupyter interactive use)
4. Compares with GitHub and uploads if changed
5. Saves to CSV output

Generated: 2026-06-29 10:34:41
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
    "Helsetilstand", "Hjerte- og karsykdommer", "Sykehusinnlagte sykdommer og skader.txt"
)

# Output configuration
output_filename = "sykehusinnlagte_sykdommer_og_skader.csv"
github_folder = "Data/08_Folkehelse og levekår/Helsetilstand/Hjerte- og karsykdommer"

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
print(f"FHI Query: Sykehusinnlagte sykdommer og skader.txt")
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

# Convert År to datetime (YYYY-01-01)
df['År'] = pd.to_datetime(df['År'].astype(str) + '-01-01').dt.strftime('%Y-%m-%d')

# Capitalize first letter in Kjønn and Alder
df['Kjønn'] = df['Kjønn'].str.capitalize()
df['Alder'] = df['Alder'].str.capitalize()

# Replace ":" with empty string
df['value'] = df['value'].replace(':', '')

# Rename value to Antall and round to 1 decimal
df['value'] = pd.to_numeric(df['value'], errors='coerce').round(1)
df = df.rename(columns={'value': 'Antall'})

# Create SortKjonn column
kjonn_sort = {"Kjønn samlet": 1, "Menn": 2, "Kvinner": 3}
df['SortKjonn'] = df['Kjønn'].map(kjonn_sort)

# Create SortAlder column
alder_sort = {
    "Alle aldre": 1,
    "0-74 år": 2,
    "0-14 år": 3,
    "0-24 år": 4,
    "0-44 år": 5,
    "15-24 år": 6,
    "25-44 år": 7,
    "45-64 år": 8,
    "45-74 år": 9,
    "65-74 år": 10,
    "75 år+": 11,
    "75-79 år": 12,
    "80 år+": 13,
}
df['SortAlder'] = df['Alder'].map(alder_sort)

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
