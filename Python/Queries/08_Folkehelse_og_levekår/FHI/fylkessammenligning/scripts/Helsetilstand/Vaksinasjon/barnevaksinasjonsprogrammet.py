"""
Fylkessammenligning Query Script: Barnevaksinasjonsprogrammet.txt
=============================================================

Auto-generated county-comparison variant of the FHI query
"Barnevaksinasjonsprogrammet.txt" (see FHI/scripts/ for the production, per-kommune version).

Queries Telemark + Hele landet + the 15 other Norwegian counties, most
recent year only, and saves the result LOCALLY to fylkessammenligning/data/
for the Telemark-vs-other-counties analysis. Nothing here is pushed to
GitHub Data/ - this is a separate, on-demand analysis, not part of the
production pipeline.
"""

import json
import os
import sys
import pandas as pd
from pathlib import Path

# Get PYTHONPATH and add to sys.path
pythonpath = os.environ.get("PYTHONPATH")
if not pythonpath:
    current = Path(__file__).resolve()
    while current.name != "Python" and current != current.parent:
        current = current.parent
    pythonpath = str(current)
    os.environ["PYTHONPATH"] = pythonpath

sys.path.append(pythonpath)

from Helper_scripts.utility_functions import fetch_data

# Paths
query_file = os.path.join(
    pythonpath,
    "Queries",
    "08_Folkehelse_og_levekår",
    "FHI",
    "fylkessammenligning",
    "queries",
    "Helsetilstand", "Vaksinasjon", "Barnevaksinasjonsprogrammet.txt"
)

# Output configuration (local only - not pushed to GitHub)
output_filename = "barnevaksinasjonsprogrammet.csv"
data_dir = os.path.join(
    pythonpath, "Queries", "08_Folkehelse_og_levekår", "FHI", "fylkessammenligning", "data"
)


def load_query_file(file_path):
    """Load URL and query from the query file."""
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    url = lines[0].strip()
    query_lines = [line.strip() for line in lines[1:] if line.strip()]
    query = json.loads(''.join(query_lines))

    return url, query


# %%
print(f"\n{'=' * 70}")
print(f"Fylkessammenligning Query: Barnevaksinasjonsprogrammet.txt")
print(f"{'=' * 70}\n")

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
    query_name="FHI Fylkessammenligning Query",
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

# Handle Skoleår column (e.g. "2022/23-2024/25"): keep as-is, create "År" and "Skoleår_slutt"
if 'Skoleår' in df.columns:
    df['År'] = pd.to_datetime(df['Skoleår'].str.split('-').str[-1].str[:4] + '-01-01').dt.strftime('%Y-%m-%d')
    df['Skoleår_slutt'] = df['Skoleår'].str.split('-').str[-1]
    cols = df.columns.tolist()
    idx = cols.index('Skoleår')
    cols.remove('År')
    cols.remove('Skoleår_slutt')
    cols.insert(idx + 1, 'År')
    cols.insert(idx + 2, 'Skoleår_slutt')
    df = df[cols]

# Convert År to datetime (YYYY-01-01) if column contains single years
# If År contains intervals (e.g. "2013-2016"), rename to "År (intervall)" and create "År" from last year
if 'År' in df.columns:
    if df['År'].astype(str).str.match(r'^\d{4}[-/]\d{4}$').all():
        df = df.rename(columns={'År': 'År (intervall)'})
        df['År'] = pd.to_datetime(df['År (intervall)'].str.split(r'[-/]').str[1] + '-01-01').dt.strftime('%Y-%m-%d')
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
    kjonn_sort = {"Kjønn samlet": 1, "Begge kjønn": 1, "Menn": 2, "Mann": 2, "Gutter": 2, "Kvinner": 3, "Kvinne": 3, "Jenter": 3}
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

# Rename Geografi to Fylke (county-level analysis, not kommune)
if 'Geografi' in df.columns:
    df = df.rename(columns={'Geografi': 'Fylke'})

# Create SortFylke column: Telemark first, Hele landet second, rest alphabetical
if 'Fylke' in df.columns:
    unique_fylker = df['Fylke'].unique().tolist()
    sort_fylke = {"Telemark": 1, "Hele landet": 2}
    regular = sorted([f for f in unique_fylker if f not in ["Telemark", "Hele landet"]])
    for i, f in enumerate(regular, start=3):
        sort_fylke[f] = i
    df['SortFylke'] = df['Fylke'].map(sort_fylke)

# --- End standard transformations ---
# Add script-specific transformations below:

####################################################################
### EDITABLE SECTION END                                         ###
####################################################################

print(f"\nAfter processing: {len(df)} rows and {len(df.columns)} columns")

# %%
# Save locally (NOT published to GitHub Data/ - this is a standalone analysis)
os.makedirs(data_dir, exist_ok=True)
output_path = os.path.join(data_dir, output_filename)
df.to_csv(output_path, index=False, encoding='utf-8')
print(f"\n  ✓ Saved to: {output_path}")

print(f"\n{'=' * 70}")
print("Processing complete")
print(f"{'=' * 70}\n")
