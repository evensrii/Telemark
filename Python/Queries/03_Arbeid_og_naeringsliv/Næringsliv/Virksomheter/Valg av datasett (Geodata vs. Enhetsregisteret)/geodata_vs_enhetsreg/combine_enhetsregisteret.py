# %%
"""
Combine Enhetsregisteret Hovedenheter and Underenheter

This script loads the two separate Enhetsregisteret files (hovedenheter and underenheter),
aligns their column structures, and combines them into a single clean CSV file.

The two files have different column structures:
- Hovedenheter: Navn, Org. nr., Overordnet enhet, ...
- Underenheter: Org. nr., Navn, Overordnet enhet, ...

This script ensures all columns from both files are preserved.
"""

import os
import pandas as pd

# %% [markdown]
# ## 1. Load Both Datasets

# %%
# Define file paths
data_folder = r"c:\Users\eve1509\OneDrive - Telemark fylkeskommune\Github\Telemark\Data\03_Arbeid og næringsliv\02_Næringsliv\Virksomheter"
hovedenheter_file = os.path.join(data_folder, "enhetsregisteret_hovedenheter.csv")
underenheter_file = os.path.join(data_folder, "enhetsregisteret_underenheter.csv")
output_file = os.path.join(data_folder, "enhetsregisteret_kombinert_clean.csv")

# Load both datasets separately
print("="*80)
print("LOADING ENHETSREGISTERET DATA")
print("="*80)

# Load hovedenheter
df_hoved = pd.read_csv(hovedenheter_file)
print(f"\n✓ Loaded hovedenheter: {len(df_hoved):,} records")
print(f"  Columns: {list(df_hoved.columns[:5])}...")
print(f"  Total columns: {len(df_hoved.columns)}")

# Add source indicator
df_hoved['Dataset'] = 'Hovedenhet'

# Load underenheter  
df_under = pd.read_csv(underenheter_file)
print(f"\n✓ Loaded underenheter: {len(df_under):,} records")
print(f"  Columns: {list(df_under.columns[:5])}...")
print(f"  Total columns: {len(df_under.columns)}")

# Add source indicator
df_under['Dataset'] = 'Underenhet'

# %% [markdown]
# ## 2. Align Column Structures

# %%
print("\n" + "="*80)
print("ALIGNING COLUMN STRUCTURES")
print("="*80)

# Note: Keep all columns from both files separate - no renaming or merging
# Each file has its own column structure, and all columns will be preserved
print("\n✓ Keeping all columns separate (no merging)")

# Get all unique columns from both datasets
hoved_cols = set(df_hoved.columns)
under_cols = set(df_under.columns)

# Find columns unique to each dataset
hoved_only = hoved_cols - under_cols
under_only = under_cols - hoved_cols
common_cols = hoved_cols & under_cols

print(f"\nColumn analysis:")
print(f"  Common columns: {len(common_cols)}")
print(f"  Only in hovedenheter: {len(hoved_only)}")
print(f"  Only in underenheter: {len(under_only)}")

if hoved_only:
    print(f"\n  Columns only in hovedenheter: {sorted(hoved_only)}")
if under_only:
    print(f"  Columns only in underenheter: {sorted(under_only)}")

# Create unified column list (preserve order from hovedenheter, add underenheter-specific columns at end)
# But ensure 'Dataset' comes right after 'Antall ansatte'
base_columns = [col for col in df_hoved.columns if col != 'Dataset']
all_columns_without_dataset = base_columns + [col for col in df_under.columns if col not in df_hoved.columns and col != 'Dataset']

# Insert 'Dataset' after 'Antall ansatte'
if 'Antall ansatte' in all_columns_without_dataset:
    ansatte_index = all_columns_without_dataset.index('Antall ansatte')
    all_columns = all_columns_without_dataset[:ansatte_index+1] + ['Dataset'] + all_columns_without_dataset[ansatte_index+1:]
else:
    all_columns = all_columns_without_dataset + ['Dataset']

print(f"\nTotal columns in combined dataset: {len(all_columns)}")
print(f"  'Dataset' column positioned after 'Antall ansatte'")

# Add missing columns to each dataframe with NaN values
for col in all_columns:
    if col not in df_hoved.columns:
        df_hoved[col] = None
    if col not in df_under.columns:
        df_under[col] = None

# Ensure both dataframes have the same column order
df_hoved = df_hoved[all_columns]
df_under = df_under[all_columns]

print("✓ Column structures aligned")

# %% [markdown]
# ## 3. Combine Datasets

# %%
print("\n" + "="*80)
print("COMBINING DATASETS")
print("="*80)

df_combined = pd.concat([df_hoved, df_under], ignore_index=True)
print(f"\n✓ Combined dataset created: {len(df_combined):,} records")
print(f"  Columns: {len(df_combined.columns)}")

# Verify totals
expected_total = len(df_hoved) + len(df_under)
if len(df_combined) == expected_total:
    print(f"  ✓ Record count verified: {len(df_hoved):,} + {len(df_under):,} = {len(df_combined):,}")
else:
    print(f"  ⚠ Warning: Expected {expected_total:,} records, got {len(df_combined):,}")

# Clean Overordnet enhet column (remove trailing .0)
if 'Overordnet enhet' in df_combined.columns:
    df_combined['Overordnet enhet'] = pd.to_numeric(df_combined['Overordnet enhet'], errors='coerce').apply(
        lambda x: str(int(x)) if pd.notna(x) else ''
    )
    print(f"  ✓ Cleaned 'Overordnet enhet' column (removed trailing .0)")

# %% [markdown]
# ## 4. Data Quality Check

# %%
print("\n" + "="*80)
print("DATA QUALITY CHECK")
print("="*80)

# Check for duplicate rows
duplicate_rows = df_combined.duplicated(keep=False)
num_duplicates = duplicate_rows.sum()
if num_duplicates > 0:
    print(f"\n⚠ Warning: Found {num_duplicates:,} duplicate rows (not removed)")
else:
    print(f"\n✓ No duplicate rows found")

# Check employee data
employees = pd.to_numeric(df_combined['Antall ansatte'], errors='coerce')
records_with_employee_data = employees.notna().sum()
records_missing_employee_data = employees.isna().sum()
records_with_zero_employees = (employees == 0).sum()
total_employees = employees.sum()

print(f"\nDataset distribution:")
dataset_counts = df_combined['Dataset'].value_counts()
for dataset, count in dataset_counts.items():
    print(f"  {dataset}: {count:,} ({count/len(df_combined)*100:.1f}%)")

print(f"\nEmployee data statistics:")
print(f"  Records with employee data: {records_with_employee_data:,} ({records_with_employee_data/len(df_combined)*100:.1f}%)")
print(f"  Records missing employee data: {records_missing_employee_data:,} ({records_missing_employee_data/len(df_combined)*100:.1f}%)")
print(f"  Records with 0 employees: {records_with_zero_employees:,}")
print(f"  Total employees: {total_employees:,.0f}")

# %% [markdown]
# ## 5. Save Combined File

# %%
print("\n" + "="*80)
print("SAVING COMBINED FILE")
print("="*80)

df_combined.to_csv(output_file, index=False)
print(f"\n✓ Saved: enhetsregisteret_kombinert_clean.csv")
print(f"  Location: {output_file}")
print(f"  Records: {len(df_combined):,}")
print(f"  Columns: {len(df_combined.columns)}")

# %% [markdown]
# ## 6. Summary

# %%
print("\n" + "="*80)
print("SUMMARY")
print("="*80)

print(f"""
Input files:
  - enhetsregisteret_hovedenheter.csv: {len(df_hoved):,} records
  - enhetsregisteret_underenheter.csv: {len(df_under):,} records

Output file:
  - enhetsregisteret_kombinert_clean.csv: {len(df_combined):,} records
  - Columns: {len(df_combined.columns)}
  
Data preserved:
  - All columns from both files maintained
  - No records removed
  - Column structures aligned

Ready for SQL filtering!
""")

print("="*80)

# %%
