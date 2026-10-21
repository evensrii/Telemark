"""
FHI Query Script: Gjennomføring i videregående opplæring
========================================================

Query file: VGO/queries/Gjennomføring vgo.txt
FHI-tabell 360: Gjennomføring i videregående opplæring,
treårige glidende gjennomsnitt, standardisert andel (prosent).

The query uses {"filter":"bottom","values":["10"],"code":"AAR"} to fetch all available periods
(currently 4), so new periods are picked up automatically.

This script:
1. Loads query from .txt file
2. Fetches data from FHI API
3. Processes data (EDITABLE SECTION - outside main() for Jupyter interactive use)
4. Compares with GitHub and uploads if changed
5. Saves to CSV output
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
    "02_Opplæring_og_kompetanse",
    "VGO",
    "queries",
    "Gjennomføring vgo.txt"
)

# Output configuration
output_filename = "gjennomforing_vgo.csv"
github_folder = "Data/02_Opplæring og kompetanse/Videregående opplæring - Gjennomføring"

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
print(f"FHI Query: Gjennomføring vgo.txt")
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

# År comes as intervals (e.g. "2022-2024"): keep as "År (intervall)" and create "År" (YYYY-01-01) from the last year
df = df.rename(columns={'År': 'År (intervall)'})
df.insert(
    df.columns.get_loc('År (intervall)') + 1,
    'År',
    pd.to_datetime(df['År (intervall)'].str.split('-').str[1] + '-01-01').dt.strftime('%Y-%m-%d')
)

# Capitalize first letter in text columns
for col in ['Kjønn', 'Foreldrenes utdanningsnivå', 'Innvandringsbakgrunn', 'Måltall']:
    df[col] = df[col].str.capitalize()

# Replace ":" with empty string, round, rename value to Andel and convert from percent to fraction
df['value'] = df['value'].replace(':', '')
df['value'] = pd.to_numeric(df['value'], errors='coerce').round(1)
df = df.rename(columns={'value': 'Andel'})
df['Andel'] = df['Andel'] / 100

# Rename Geografi to Kommune
df = df.rename(columns={'Geografi': 'Kommune'})

# Create SortKommune column (Telemark, Hele landet, then kommuner alphabetically)
unique_kommuner = df['Kommune'].unique().tolist()
sort_kommune = {"Telemark": 1, "Hele landet": 2}
regular = sorted([k for k in unique_kommuner if k not in sort_kommune])
for i, k in enumerate(regular, start=3):
    sort_kommune[k] = i
df['SortKommune'] = df['Kommune'].map(sort_kommune)

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
    keepcsv=True,
    value_columns=['Andel']
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
