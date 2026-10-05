"""
Fylkessammenligning Script Generator
=====================================
Generates the county-comparison variant of every FHI indicator query.

For each existing query file under FHI/queries/ (the production, per-kommune
queries), this script:
1. Copies the query, replacing the GEO dimension with the 15 other Norwegian
   counties (+ Telemark + Hele landet as reference), and forcing AAR to the
   single most recent period (bottom 1) since this analysis only needs the
   latest year.
2. Writes the new query to fylkessammenligning/queries/, mirroring the
   original topic subfolder tree.
3. Generates a matching fetch script under fylkessammenligning/scripts/,
   adapted from the production template (Kommune -> Fylke naming, and saving
   to a local data/ folder instead of pushing to GitHub).

This script never touches FHI/queries, FHI/scripts, or master_script.py -
the county comparison is a separate, on-demand analysis.

Usage:
    Run manually whenever a new indicator is added to FHI/queries/, or to
    regenerate query files after changing COUNTY_GEO_VALUES below.
"""

import json
import os
import re
from pathlib import Path

# Get PYTHONPATH (fall back to walking up from this file, like the generated
# scripts do, so this also works outside the conda env / without env vars set)
pythonpath = os.environ.get("PYTHONPATH")
if not pythonpath:
    current = Path(__file__).resolve()
    while current.name != "Python" and current != current.parent:
        current = current.parent
    pythonpath = str(current)

FHI_BASE = os.path.join(pythonpath, "Queries", "08_Folkehelse_og_levekår", "FHI")
QUERIES_DIR = os.path.join(FHI_BASE, "queries")

FYLKESSAMMENLIGNING_BASE = os.path.join(FHI_BASE, "fylkessammenligning")
NEW_QUERIES_DIR = os.path.join(FYLKESSAMMENLIGNING_BASE, "queries")
NEW_SCRIPTS_DIR = os.path.join(FYLKESSAMMENLIGNING_BASE, "scripts")

# GEO codes: Telemark (40) + Hele landet (0) as reference + the 15 other counties
COUNTY_GEO_VALUES = [
    "0", "40",
    "03", "31", "32", "33", "34", "39", "42", "11", "46", "15", "50", "18", "55", "56",
]

# One indicator (selvmord.txt) uses FHI's older "daar" API path, which spells
# geography/year differently ("FYLKE"/"DAAR" instead of "GEO"/"AAR") and uses
# "Total" instead of "0" for Hele landet.
FYLKE_GEO_VALUES = [
    "Total", "40",
    "03", "31", "32", "33", "34", "39", "42", "11", "46", "15", "50", "18", "55", "56",
]

GEO_VALUES_BY_CODE = {"GEO": COUNTY_GEO_VALUES, "FYLKE": FYLKE_GEO_VALUES}
YEAR_DIMENSION_BY_CODE = {
    "AAR": {"filter": "bottom", "values": ["1"], "code": "AAR"},
    "DAAR": {"filter": "bottom", "values": ["1"], "code": "DAAR"},
}


def sanitize_filename(filename):
    """Mirror Automatisering/Task scheduler/fhi_script_generator.py's sanitizer
    so generated script filenames match the existing per-kommune script names."""
    name = filename.lower()
    replacements = {"å": "aa", "æ": "ae", "ø": "oe", "é": "e", "è": "e", "ê": "e", "ü": "u", "ö": "o", "ä": "a"}
    for old, new in replacements.items():
        name = name.replace(old, new)
    name = re.sub(r"[^a-z0-9\s]", "", name)
    name = re.sub(r"\s+", " ", name)
    name = name.replace(" ", "_")
    return name.strip("_")


def find_all_query_files(queries_dir):
    """Recursively find all .txt query files. Returns (full_path, subfolder)."""
    query_files = []
    for root, _dirs, files in os.walk(queries_dir):
        for file in files:
            if file.endswith(".txt"):
                full_path = os.path.join(root, file)
                subfolder = os.path.relpath(root, queries_dir)
                if subfolder == ".":
                    subfolder = ""
                query_files.append((full_path, subfolder))
    return sorted(query_files)


def load_query_file(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        lines = f.readlines()
    url = lines[0].strip()
    query_lines = [line.strip() for line in lines[1:] if line.strip()]
    query = json.loads("".join(query_lines))
    return url, query


def transform_payload(query):
    """Replace the geography dimension's values with the county list and force
    the year dimension to the single most recent period (bottom-1)."""
    new_dimensions = []
    for dim in query["dimensions"]:
        code = dim.get("code")
        if code in GEO_VALUES_BY_CODE:
            new_dimensions.append({**dim, "values": GEO_VALUES_BY_CODE[code]})
        elif code in YEAR_DIMENSION_BY_CODE:
            new_dimensions.append(dict(YEAR_DIMENSION_BY_CODE[code]))
        else:
            new_dimensions.append(dim)
    return {**query, "dimensions": new_dimensions}


def write_query_file(url, query, dest_path):
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    with open(dest_path, "w", encoding="utf-8") as f:
        f.write(url + "\n")
        f.write(json.dumps(query, ensure_ascii=False))


def generate_script_content(query_filename, subfolder, sanitized_name, geo_code="GEO"):
    """Build the fetch script source for one indicator (county-comparison variant)."""
    if subfolder:
        subfolder_parts = Path(subfolder).parts
        query_path_args = ", ".join([f'"{part}"' for part in subfolder_parts] + [f'"{query_filename}"'])
    else:
        query_path_args = f'"{query_filename}"'

    output_filename = f"{sanitized_name}.csv"

    # One indicator (selvmord.txt) uses FHI's "daar" API path (FYLKE/DAAR
    # dimension codes) whose json-stat labels come back as "Bofylke"/"Dødsår"
    # instead of the usual "Geografi"/"År" - normalize those first so the
    # standard transformations below (which look for Geografi/År) still apply.
    non_standard_rename_block = ""
    if geo_code == "FYLKE":
        non_standard_rename_block = (
            "# This indicator's FHI table uses non-standard column names for geography/year\n"
            "if 'Bofylke' in df.columns:\n"
            "    df = df.rename(columns={'Bofylke': 'Geografi'})\n"
            "if 'Dødsår' in df.columns:\n"
            "    df = df.rename(columns={'Dødsår': 'År'})\n\n"
        )

    template = f'''"""
Fylkessammenligning Query Script: {query_filename}
{'=' * (30 + len(query_filename))}

Auto-generated county-comparison variant of the FHI query
"{query_filename}" (see FHI/scripts/ for the production, per-kommune version).

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
    {query_path_args}
)

# Output configuration (local only - not pushed to GitHub)
output_filename = "{output_filename}"
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
print(f"\\n{{'=' * 70}}")
print(f"Fylkessammenligning Query: {query_filename}")
print(f"{{'=' * 70}}\\n")

print("Loading query from file...")
url, query = load_query_file(query_file)
print(f"  ✓ Query loaded")
print(f"  URL: {{url}}")

# %%
# Fetch data from FHI API
print("\\nFetching data from FHI API...")
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
            print(f"    Error: {{error}}")
    sys.exit(1)

print(f"  ✓ Fetched {{len(df)}} rows and {{len(df.columns)}} columns")
print(f"  Columns: {{', '.join(df.columns.tolist())}}")

# %%
####################################################################
### EDITABLE SECTION START                                       ###
### Add your data transformations and processing here            ###
####################################################################

{non_standard_rename_block}# --- Standard FHI transformations (auto-generated) ---

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
    if df['År'].astype(str).str.match(r'^\\d{{4}}[-/]\\d{{4}}$').all():
        df = df.rename(columns={{'År': 'År (intervall)'}})
        df['År'] = pd.to_datetime(df['År (intervall)'].str.split(r'[-/]').str[1] + '-01-01').dt.strftime('%Y-%m-%d')
        cols = df.columns.tolist()
        idx = cols.index('År (intervall)')
        cols.remove('År')
        cols.insert(idx + 1, 'År')
        df = df[cols]
    elif df['År'].astype(str).str.match(r'^\\d{{4}}$').all():
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
    df = df.rename(columns={{'value': value_col_name}})

# Divide by 100 for Andel columns (named "Andel" or "Andel (YYYY)")
for col in df.columns:
    if col == 'Andel' or (col.startswith('Andel (') and col.endswith(')')):
        df[col] = df[col] / 100

# Create SortKjonn column if Kjønn exists and has more than one unique value
if 'Kjønn' in df.columns and df['Kjønn'].nunique() > 1:
    kjonn_sort = {{"Kjønn samlet": 1, "Begge kjønn": 1, "Menn": 2, "Mann": 2, "Gutter": 2, "Kvinner": 3, "Kvinne": 3, "Jenter": 3}}
    df['SortKjonn'] = df['Kjønn'].map(kjonn_sort)

# Create SortAlder column if Alder exists and has more than one unique value
if 'Alder' in df.columns and df['Alder'].nunique() > 1:
    unique_alder = df['Alder'].unique().tolist()
    alder_sort = {{}}
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
    df = df.rename(columns={{'Geografi': 'Fylke'}})

# Create SortFylke column: Telemark first, Hele landet second, rest alphabetical
if 'Fylke' in df.columns:
    unique_fylker = df['Fylke'].unique().tolist()
    sort_fylke = {{"Telemark": 1, "Hele landet": 2}}
    regular = sorted([f for f in unique_fylker if f not in ["Telemark", "Hele landet"]])
    for i, f in enumerate(regular, start=3):
        sort_fylke[f] = i
    df['SortFylke'] = df['Fylke'].map(sort_fylke)

# --- End standard transformations ---
# Add script-specific transformations below:

####################################################################
### EDITABLE SECTION END                                         ###
####################################################################

print(f"\\nAfter processing: {{len(df)}} rows and {{len(df.columns)}} columns")

# %%
# Save locally (NOT published to GitHub Data/ - this is a standalone analysis)
os.makedirs(data_dir, exist_ok=True)
output_path = os.path.join(data_dir, output_filename)
df.to_csv(output_path, index=False, encoding='utf-8')
print(f"\\n  ✓ Saved to: {{output_path}}")

print(f"\\n{{'=' * 70}}")
print("Processing complete")
print(f"{{'=' * 70}}\\n")
'''
    return template


def main():
    print("\n" + "=" * 70)
    print("Fylkessammenligning Script Generator")
    print("=" * 70)
    print(f"Source queries:      {QUERIES_DIR}")
    print(f"New queries output:  {NEW_QUERIES_DIR}")
    print(f"New scripts output:  {NEW_SCRIPTS_DIR}")
    print("=" * 70 + "\n")

    query_files = find_all_query_files(QUERIES_DIR)
    print(f"Found {len(query_files)} source query files\n")

    generated_queries = 0
    generated_scripts = 0
    skipped_scripts = 0

    for query_path, subfolder in query_files:
        query_filename = Path(query_path).name
        query_name = Path(query_path).stem
        sanitized_name = sanitize_filename(query_name)

        # --- Query file ---
        url, query = load_query_file(query_path)
        geo_code = next((d["code"] for d in query["dimensions"] if d["code"] in GEO_VALUES_BY_CODE), "GEO")
        new_query = transform_payload(query)
        if subfolder:
            new_query_path = os.path.join(NEW_QUERIES_DIR, subfolder, query_filename)
        else:
            new_query_path = os.path.join(NEW_QUERIES_DIR, query_filename)
        write_query_file(url, new_query, new_query_path)
        generated_queries += 1

        # --- Script file ---
        if subfolder:
            script_dir = os.path.join(NEW_SCRIPTS_DIR, subfolder)
        else:
            script_dir = NEW_SCRIPTS_DIR
        script_path = os.path.join(script_dir, f"{sanitized_name}.py")

        if os.path.exists(script_path):
            print(f"[EXISTS] {sanitized_name}.py")
            skipped_scripts += 1
            continue

        os.makedirs(script_dir, exist_ok=True)
        content = generate_script_content(query_filename, subfolder, sanitized_name, geo_code=geo_code)
        with open(script_path, "w", encoding="utf-8") as f:
            f.write(content)
        print(f"[NEW]    {sanitized_name}.py")
        generated_scripts += 1

    print("\n" + "=" * 70)
    print("Summary")
    print("=" * 70)
    print(f"Query files written:  {generated_queries}")
    print(f"Scripts generated:    {generated_scripts}")
    print(f"Scripts skipped (existing): {skipped_scripts}")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    main()
