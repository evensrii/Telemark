"""
FHI Query Script: Psykiske symptomer og lidelser - primærhelsetjenesten, ettårige tall.txt
==========================================================================================

Auto-generated script for processing FHI query data.
Query file: Helsetilstand/Psykiske symptomer og lidelser - primærhelsetjenesten, ettårige tall.txt

This script:
1. Loads query from .txt file
2. Fetches data from FHI API
3. Processes data (EDITABLE SECTION - outside main() for Jupyter interactive use)
4. Compares with GitHub and uploads if changed
5. Saves to CSV output

Generated: 2025-12-19 13:37:19
"""

import json
import os
import sys
import pandas as pd
from pathlib import Path

# Get PYTHONPATH and add to sys.path
pythonpath = os.environ.get("PYTHONPATH")
if not pythonpath:
    pythonpath = str(Path(__file__).parent.parent.parent.parent.parent)
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
    "Helsetilstand",
    "Psykiske symptomer og lidelser - primærhelsetjenesten, ettårige tall.txt"
)

# Output configuration
output_filename = "psykiske_symptomer_og_lidelser_primaerhelsetjenesten_ettaarige_tall.csv"
github_folder = "Data/08_Folkehelse og levekår/Helsetilstand"

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
print(f"FHI Query: Psykiske symptomer og lidelser - primærhelsetjenesten, ettårige tall.txt")
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

# Example transformations (uncomment and modify as needed):
# df = df[df['value'] > 0]  # Filter rows
# df['new_column'] = df['old_column'] * 2  # Create new column
# df = df.rename(columns={'old_name': 'new_name'})  # Rename columns
# df = df.sort_values('column_name')  # Sort data

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

# Save to local output directory
output_dir = os.path.join(pythonpath, "Data", "08_Folkehelse og levekår", "Helsetilstand")
os.makedirs(output_dir, exist_ok=True)
output_path = os.path.join(output_dir, output_filename)
df.to_csv(output_path, index=False, encoding='utf-8')
print(f"\n  ✓ Saved to: {output_path}")

print(f"\n{'=' * 70}")
print("Processing complete")
print(f"{'=' * 70}\n")
