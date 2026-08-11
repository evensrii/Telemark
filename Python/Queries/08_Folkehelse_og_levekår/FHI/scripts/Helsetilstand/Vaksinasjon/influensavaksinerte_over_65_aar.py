"""
FHI Query Script: Influensavaksinerte over 65 år.txt
====================================================

Auto-generated script for processing FHI query data.
Query file: Helsetilstand/Vaksinasjon/Influensavaksinerte over 65 år.txt

This script:
1. Loads query from .txt file
2. Fetches data from FHI API
3. Processes data (EDITABLE SECTION - outside main() for Jupyter interactive use)
4. Compares with GitHub and uploads if changed
5. Saves to CSV output

Generated: 2026-07-01 14:40:33
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
    "Helsetilstand", "Vaksinasjon", "Influensavaksinerte over 65 år.txt"
)

# Output configuration
output_filename = "influensavaksinerte_over_65_aar.csv"
github_folder = "Data/08_Folkehelse og levekår/Helsetilstand/Vaksinasjon"

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
print(f"FHI Query: Influensavaksinerte over 65 år.txt")
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

# --- Everviz format: Kommune, Andel (YYYY), Label ---

# Rename Geografi to Kommune
if 'Geografi' in df.columns:
    df = df.rename(columns={'Geografi': 'Kommune'})

# Rename "Hele landet" to "Landet"
df.loc[df["Kommune"] == "Hele landet", "Kommune"] = "Landet"

# Get year dynamically from År column
if 'År' in df.columns:
    year = df['År'].astype(str).str[:4].iloc[0]

# Process value column: round to integer
if 'value' in df.columns:
    df['value'] = df['value'].replace(':', '')
    df['value'] = pd.to_numeric(df['value'], errors='coerce').round(0).astype('Int64')
    df = df.rename(columns={'value': f'Andel ({year})'})

# Drop unnecessary columns (keep only Kommune and Andel)
keep_cols = ['Kommune', f'Andel ({year})']
df = df[[c for c in keep_cols if c in df.columns]]

# Sort: Kommuner alphabetically, then Telemark, then Landet
unique_kommuner = df['Kommune'].unique().tolist()
sort_map = {}
regular = sorted([k for k in unique_kommuner if k not in ["Telemark", "Landet"]])
for i, k in enumerate(regular, start=1):
    sort_map[k] = i
sort_map["Telemark"] = len(regular) + 1
sort_map["Landet"] = len(regular) + 2

df["SortOrder"] = df["Kommune"].map(sort_map)
df = df.sort_values("SortOrder").reset_index(drop=True)
df = df.drop(columns=["SortOrder"])

# Add Label column
df["Label"] = df["Kommune"]

# Final column order
df = df[["Kommune", f"Andel ({year})", "Label"]]

# --- End Everviz transformations ---

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
    value_columns=[f"Andel ({year})"],
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
