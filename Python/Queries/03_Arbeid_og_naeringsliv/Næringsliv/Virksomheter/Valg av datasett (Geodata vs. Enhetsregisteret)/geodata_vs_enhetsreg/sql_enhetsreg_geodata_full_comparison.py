# %%
"""
Full Join Comparison of SQL-Filtered Enhetsregisteret and Geodata Datasets

This script performs a full outer join of the two filtered datasets based on
organization number ("Org. nr."). The resulting dataset includes all enterprises
from both datasets, with columns indicating the source and differences.

Filters applied:
- Keeps only rows where at least one dataset has ≥1 employee
- Removes rows where both datasets have null/0 employees
"""

import os
import pandas as pd

# %% [markdown]
# ## 1. Load Filtered Datasets

# %%
# Define file paths
data_folder = r"c:\Users\eve1509\OneDrive - Telemark fylkeskommune\Github\Telemark\Data\03_Arbeid og næringsliv\02_Næringsliv\Virksomheter"
enhetsreg_file = os.path.join(data_folder, "enhetsreg_kombi_sql_filtrerte_virksomheter.csv")
geodata_file = os.path.join(data_folder, "geodata_sql_filtrerte_virksomheter.csv")

# Load datasets
print("="*80)
print("LOADING FILTERED DATASETS")
print("="*80)

enhetsreg_df = pd.read_csv(enhetsreg_file)
geodata_df = pd.read_csv(geodata_file)

print(f"\n✓ Enhetsregisteret (SQL filtered): {len(enhetsreg_df):,} records")
print(f"✓ Geodata (SQL filtered): {len(geodata_df):,} records")

# Get employee counts
enhetsreg_employees = pd.to_numeric(enhetsreg_df['Antall ansatte'], errors='coerce').sum()
geodata_employees = pd.to_numeric(geodata_df['Antall ansatte'], errors='coerce').sum()

print(f"\nTotal employees:")
print(f"  Enhetsregisteret: {enhetsreg_employees:,.0f}")
print(f"  Geodata: {geodata_employees:,.0f}")

# %% [markdown]
# ## 2. Prepare DataFrames for Join

# %%
print("\n" + "="*80)
print("PREPARING DATAFRAMES FOR FULL JOIN")
print("="*80)

# Ensure Org. nr. is consistent format (string without .0)
enhetsreg_df['Org. nr.'] = pd.to_numeric(enhetsreg_df['Org. nr.'], errors='coerce').apply(
    lambda x: str(int(x)) if pd.notna(x) else None
)
geodata_df['Org. nr.'] = pd.to_numeric(geodata_df['Org. nr.'], errors='coerce').apply(
    lambda x: str(int(x)) if pd.notna(x) else None
)

print("✓ Standardized 'Org. nr.' format in both datasets")

# Add suffixes to column names (except Org. nr. which is the join key)
enhetsreg_renamed = enhetsreg_df.rename(columns={
    col: f"{col} ER" if col != 'Org. nr.' else col 
    for col in enhetsreg_df.columns
})

geodata_renamed = geodata_df.rename(columns={
    col: f"{col} GD" if col != 'Org. nr.' else col 
    for col in geodata_df.columns
})

print(f"✓ Added suffixes: 'ER' for Enhetsregisteret columns, 'GD' for Geodata columns")

# %% [markdown]
# ## 3. Perform Full Outer Join

# %%
print("\n" + "="*80)
print("PERFORMING FULL OUTER JOIN")
print("="*80)

# Full outer join on Org. nr.
df_joined = pd.merge(
    enhetsreg_renamed,
    geodata_renamed,
    on='Org. nr.',
    how='outer',
    indicator=True
)

print(f"\n✓ Full join completed: {len(df_joined):,} unique organizations")

# Analyze join results
join_stats = df_joined['_merge'].value_counts()
print(f"\nJoin statistics:")
print(f"  In both datasets: {join_stats.get('both', 0):,}")
print(f"  Only in Enhetsregisteret: {join_stats.get('left_only', 0):,}")
print(f"  Only in Geodata: {join_stats.get('right_only', 0):,}")

# %% [markdown]
# ## 4. Prepare Data and Filter

# %%
print("\n" + "="*80)
print("PREPARING DATA AND FILTERING")
print("="*80)

# Start with all records from the full outer join
df_filtered = df_joined.drop(columns=['_merge']).copy()

print(f"\nTotal records after join: {len(df_filtered):,}")

# Filter: Keep only rows where at least one dataset has ≥1 employee
er_employees_num = pd.to_numeric(df_filtered['Antall ansatte ER'], errors='coerce').fillna(0)
gd_employees_num = pd.to_numeric(df_filtered['Antall ansatte GD'], errors='coerce').fillna(0)

# Keep rows where at least one has 1 or more employees
at_least_one_employee = (er_employees_num >= 1) | (gd_employees_num >= 1)

records_before = len(df_filtered)
df_filtered = df_filtered[at_least_one_employee].copy()
records_removed = records_before - len(df_filtered)

print(f"\nFiltering: keeping only rows where at least one dataset has ≥1 employee:")
print(f"  Records removed (both null or 0): {records_removed:,}")
print(f"  Records remaining: {len(df_filtered):,}")

# Clean Overordnet enhet columns (remove trailing .0)
for col in ['Overordnet enhet ER', 'Overordnet enhet GD']:
    if col in df_filtered.columns:
        df_filtered[col] = pd.to_numeric(df_filtered[col], errors='coerce').apply(
            lambda x: str(int(x)) if pd.notna(x) else ''
        )

print(f"✓ Cleaned 'Overordnet enhet' columns (removed trailing .0)")

# Calculate employee difference (GD - ER)
df_filtered['Differanse antall ansatte'] = (
    pd.to_numeric(df_filtered['Antall ansatte GD'], errors='coerce').fillna(0) -
    pd.to_numeric(df_filtered['Antall ansatte ER'], errors='coerce').fillna(0)
)

print(f"✓ Added 'Differanse antall ansatte' column (GD - ER)")

# %% [markdown]
# ## 5. Reorder Columns

# %%
print("\n" + "="*80)
print("REORDERING COLUMNS")
print("="*80)

# Define key columns in desired order
key_columns = [
    'Org. nr.',
    'Navn ER',
    'Navn GD',
    'Overordnet enhet ER',
    'Overordnet enhet GD',
    'Antall ansatte ER',
    'Antall ansatte GD',
    'Differanse antall ansatte'
]

# Get all other columns
other_columns = [col for col in df_filtered.columns if col not in key_columns]

# Reorder
df_filtered = df_filtered[key_columns + other_columns]

print(f"✓ Columns reordered: {len(key_columns)} key columns first, then {len(other_columns)} others")

# %% [markdown]
# ## 6. Generate Comprehensive Statistics

# %%
print("\n" + "="*80)
print("COMPREHENSIVE STATISTICS")
print("="*80)

# Check which dataset each record appears in
has_er = df_filtered['Navn ER'].notna()
has_gd = df_filtered['Navn GD'].notna()

in_both = (has_er & has_gd).sum()
only_er = (has_er & ~has_gd).sum()
only_gd = (~has_er & has_gd).sum()

print(f"\nDataset presence:")
print(f"  In both datasets: {in_both:,} ({in_both/len(df_filtered)*100:.1f}%)")
print(f"  Only in Enhetsregisteret: {only_er:,} ({only_er/len(df_filtered)*100:.1f}%)")
print(f"  Only in Geodata: {only_gd:,} ({only_gd/len(df_filtered)*100:.1f}%)")

# Employee statistics for each category
er_employees = pd.to_numeric(df_filtered['Antall ansatte ER'], errors='coerce')
gd_employees = pd.to_numeric(df_filtered['Antall ansatte GD'], errors='coerce')

print(f"\nEmployee statistics:")
print(f"  Total employees in ER column: {er_employees.sum():,.0f}")
print(f"  Total employees in GD column: {gd_employees.sum():,.0f}")
print(f"  Average employees (ER): {er_employees.mean():,.1f}")
print(f"  Average employees (GD): {gd_employees.mean():,.1f}")
print(f"  Median employees (ER): {er_employees.median():,.0f}")
print(f"  Median employees (GD): {gd_employees.median():,.0f}")

# Records with employee count differences
both_have_data = has_er & has_gd
er_emp = pd.to_numeric(df_filtered.loc[both_have_data, 'Antall ansatte ER'], errors='coerce')
gd_emp = pd.to_numeric(df_filtered.loc[both_have_data, 'Antall ansatte GD'], errors='coerce')

differences = (er_emp != gd_emp).sum()
print(f"\nAmong {in_both:,} enterprises in both datasets:")
print(f"  Same employee count: {in_both - differences:,}")
print(f"  Different employee count: {differences:,}")

if differences > 0:
    abs_diff = (er_emp - gd_emp).abs()
    print(f"  Average absolute difference: {abs_diff.mean():,.1f} employees")
    print(f"  Max absolute difference: {abs_diff.max():,.0f} employees")

# %% [markdown]
# ## 7. Name Comparison Analysis

# %%
print("\n" + "="*80)
print("NAME COMPARISON ANALYSIS")
print("="*80)

# For records in both, compare names
both_df = df_filtered[both_have_data].copy()

if len(both_df) > 0:
    # Check for exact name matches (case-insensitive)
    both_df['name_match'] = (
        both_df['Navn ER'].str.lower().str.strip() == 
        both_df['Navn GD'].str.lower().str.strip()
    )
    
    exact_matches = both_df['name_match'].sum()
    print(f"\nAmong {len(both_df):,} enterprises in both datasets:")
    print(f"  Exact name match: {exact_matches:,} ({exact_matches/len(both_df)*100:.1f}%)")
    print(f"  Different names: {len(both_df) - exact_matches:,} ({(len(both_df) - exact_matches)/len(both_df)*100:.1f}%)")
    
    # Show sample of different names
    if len(both_df) - exact_matches > 0:
        print(f"\nSample of enterprises with different names (first 10):")
        diff_names = both_df[~both_df['name_match']][['Org. nr.', 'Navn ER', 'Navn GD', 'Antall ansatte ER', 'Antall ansatte GD']].head(10)
        print(diff_names.to_string(index=False))

# %% [markdown]
# ## 8. Save Output File

# %%
print("\n" + "="*80)
print("SAVING OUTPUT FILE")
print("="*80)

output_file = os.path.join(data_folder, "sql_enhetsreg_geodata_comparison.csv")
df_filtered.to_csv(output_file, index=False)

print(f"\n✓ Saved: sql_enhetsreg_geodata_comparison.csv")
print(f"  Total records: {len(df_filtered):,}")
print(f"  Total columns: {len(df_filtered.columns)}")

# %% [markdown]
# ## 9. Final Summary Report

# %%
print("\n" + "="*80)
print("FINAL SUMMARY REPORT")
print("="*80)

print(f"""
DATASET OVERVIEW:
-----------------
Source files:
  - Enhetsregisteret: enhetsreg_kombi_sql_filtrerte_virksomheter.csv
  - Geodata: geodata_sql_filtrerte_virksomheter.csv

Original record counts (after SQL filtering):
  - Enhetsregisteret: {len(enhetsreg_df):,}
  - Geodata: {len(geodata_df):,}

JOIN RESULTS:
-------------
Full outer join created: {len(df_joined):,} unique organization numbers
Filtered records (both datasets null/0 employees): {records_removed:,}
  - Final records: {len(df_filtered):,}
  - Filter: Keep only rows where at least one dataset has ≥1 employee

DATASET PRESENCE:
-----------------
In both datasets: {in_both:,} ({in_both/len(df_filtered)*100:.1f}%)
Only in Enhetsregisteret: {only_er:,} ({only_er/len(df_filtered)*100:.1f}%)
Only in Geodata: {only_gd:,} ({only_gd/len(df_filtered)*100:.1f}%)

EMPLOYEE DATA:
--------------
Total employees (ER column): {er_employees.sum():,.0f}
Total employees (GD column): {gd_employees.sum():,.0f}
Difference: {abs(er_employees.sum() - gd_employees.sum()):,.0f}

Average employees per enterprise:
  - Enhetsregisteret: {er_employees.mean():,.1f}
  - Geodata: {gd_employees.mean():,.1f}

DATA QUALITY INSIGHTS:
----------------------
Among enterprises in both datasets:
  - Same employee count: {in_both - differences if in_both > 0 else 0:,}
  - Different employee count: {differences if in_both > 0 else 0:,}
  - Exact name match: {exact_matches if len(both_df) > 0 else 0:,}
  - Different names: {len(both_df) - exact_matches if len(both_df) > 0 else 0:,}

OUTPUT FILE:
------------
File: sql_enhetsreg_geodata_comparison.csv
Location: {output_file}
Records: {len(df_filtered):,}
Columns: {len(df_filtered.columns)}

Key columns (in order):
  1. Org. nr.
  2. Navn ER (Enhetsregisteret name)
  3. Navn GD (Geodata name)
  4. Overordnet enhet ER
  5. Overordnet enhet GD
  6. Antall ansatte ER
  7. Antall ansatte GD
  8. Differanse antall ansatte (GD - ER)
  ... followed by all other columns from both datasets
""")

print("="*80)
print("PROCESSING COMPLETE")
print("="*80)

# %%
