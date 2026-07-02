"""
FHI Query Script: Kreft, nye tilfeller, tiårige tall.txt
========================================================

Auto-generated script for processing FHI query data.
Query file: Helsetilstand/Sykdomsgrupper/Kreft, nye tilfeller, tiårige tall.txt

This script:
1. Loads query from .txt file
2. Fetches data from FHI API
3. Processes data (EDITABLE SECTION - outside main() for Jupyter interactive use)
4. Compares with GitHub and uploads if changed
5. Saves to CSV output

Generated: 2026-07-02 10:54:55
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
    "Helsetilstand", "Sykdomsgrupper", "Kreft, nye tilfeller, tiårige tall.txt"
)

# Output configuration
output_filename = "kreft_nye_tilfeller_tiaarige_tall.csv"
github_folder = "Data/08_Folkehelse og levekår/Helsetilstand/Sykdomsgrupper"

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
print(f"FHI Query: Kreft, nye tilfeller, tiårige tall.txt")
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

# --- Standard FHI transformations (auto-generated) ---

# Convert År to datetime (YYYY-01-01) if column contains single years
# If År contains intervals (e.g. "2013-2016"), rename to "År (intervall)" and create "År" from last year
if 'År' in df.columns:
    if df['År'].astype(str).str.match(r'^\d{4}[-/]\d{4}$').all():
        df = df.rename(columns={'År': 'År (intervall)'})
        df['År'] = pd.to_datetime(df['År (intervall)'].str.split(r'[-/]').str[1] + '-01-01').dt.strftime('%Y-%m-%d')
        # Place År directly after År (intervall)
        cols = df.columns.tolist()
        idx = cols.index('År (intervall)')
        cols.remove('År')
        cols.insert(idx + 1, 'År')
        df = df[cols]
    elif df['År'].astype(str).str.match(r'^\d{4}$').all():
        df['År'] = pd.to_datetime(df['År'].astype(str) + '-01-01').dt.strftime('%Y-%m-%d')

# Capitalize first letter in Kjønn if column exists
if 'Kjønn' in df.columns:
    df['Kjønn'] = df['Kjønn'].str.capitalize()

# Capitalize first letter in Alder if column exists
if 'Alder' in df.columns:
    df['Alder'] = df['Alder'].str.capitalize()

# Determine value column name based on Måltall content
value_col_name = 'Antall'
if 'Måltall' in df.columns:
    maaltall_str = df['Måltall'].astype(str).str.lower().str.cat(sep=' ')
    if any(term in maaltall_str for term in ['andel', 'prosent', 'percent']):
        value_col_name = 'Andel'

# Replace ":" with empty string and process value column
if 'value' in df.columns:
    df['value'] = df['value'].replace(':', '')
    df['value'] = pd.to_numeric(df['value'], errors='coerce').round(1)
    df = df.rename(columns={'value': value_col_name})

# Divide by 100 for Andel columns (named "Andel" or "Andel (YYYY)")
for col in df.columns:
    if col == 'Andel' or (col.startswith('Andel (') and col.endswith(')')):
        df[col] = df[col] / 100

# Create SortKjonn column if Kjønn exists and has more than one unique value
if 'Kjønn' in df.columns and df['Kjønn'].nunique() > 1:
    kjonn_sort = {"Kjønn samlet": 1, "Menn": 2, "Gutter": 2, "Kvinner": 3, "Jenter": 3}
    df['SortKjonn'] = df['Kjønn'].map(kjonn_sort)

# Create SortAlder column if Alder exists and has more than one unique value
if 'Alder' in df.columns and df['Alder'].nunique() > 1:
    unique_alder = df['Alder'].unique().tolist()
    alder_sort = {}
    sort_num = 1
    if 'Alle aldre' in unique_alder:
        alder_sort['Alle aldre'] = sort_num
        sort_num += 1
    if '0-74 år' in unique_alder:
        alder_sort['0-74 år'] = sort_num
        sort_num += 1
    remaining = sorted([a for a in unique_alder if a not in alder_sort],
                       key=lambda x: (int(x.split('-')[0].split(' ')[0]) if x[0].isdigit() else 999))
    for a in remaining:
        alder_sort[a] = sort_num
        sort_num += 1
    df['SortAlder'] = df['Alder'].map(alder_sort)

# Rename Geografi to Kommune
if 'Geografi' in df.columns:
    df = df.rename(columns={'Geografi': 'Kommune'})

# Create SortKommune column
if 'Kommune' in df.columns:
    unique_kommuner = df['Kommune'].unique().tolist()
    sort_kommune = {"Telemark": 1, "Hele landet": 2}
    # Alphabetically sorted kommuner (excluding fixed and Bø/Sauherad)
    regular = sorted([k for k in unique_kommuner if k not in ["Telemark", "Hele landet", "Bø", "Sauherad"]])
    sort_num = 3
    for k in regular:
        sort_kommune[k] = sort_num
        sort_num += 1
    # Bø and Sauherad last
    if "Bø" in unique_kommuner:
        sort_kommune["Bø"] = sort_num
        sort_num += 1
    if "Sauherad" in unique_kommuner:
        sort_kommune["Sauherad"] = sort_num
        sort_num += 1
    df['SortKommune'] = df['Kommune'].map(sort_kommune)

# --- End standard transformations ---
# Add script-specific transformations below:

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
