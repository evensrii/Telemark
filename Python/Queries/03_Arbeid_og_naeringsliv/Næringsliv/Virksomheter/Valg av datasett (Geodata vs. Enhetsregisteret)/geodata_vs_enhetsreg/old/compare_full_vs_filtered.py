# %%
"""
Comparison of Geodata API datasets: Full vs Filtered
This script compares two versions of the Geodata API extract:
1. geodata_bedrifter_api.csv (filtered - specific fields only)
2. geodata_bedrifter_api_full.csv (full - all fields)

Goal: Verify that the enterprises are identical despite different column counts
"""

import pandas as pd
import os

# %% [markdown]
# ## 1. Load Data

# %%
# Define file paths
data_folder = r"c:\Users\eve1509\OneDrive - Telemark fylkeskommune\Github\Telemark\Data\03_Arbeid og næringsliv\02_Næringsliv\Virksomheter\geodata_vs_enhetsregisteret"
filtered_file = os.path.join(data_folder, "geodata_bedrifter_api.csv")
full_file = os.path.join(data_folder, "geodata_bedrifter_api_full.csv")

# Load datasets
print("Loading datasets...")
filtered_df = pd.read_csv(filtered_file)
full_df = pd.read_csv(full_file)

print(f"✓ Filtered dataset: {len(filtered_df):,} records")
print(f"✓ Full dataset: {len(full_df):,} records")

# %% [markdown]
# ## 2. Basic Dataset Overview

# %%
print("\n" + "="*80)
print("DATASET OVERVIEW")
print("="*80)

print("\n--- FILTERED DATASET (Specific Fields) ---")
print(f"Total records: {len(filtered_df):,}")
print(f"Columns: {len(filtered_df.columns)}")
print(f"Column names: {', '.join(filtered_df.columns.tolist()[:10])}...")

print("\n--- FULL DATASET (All Fields) ---")
print(f"Total records: {len(full_df):,}")
print(f"Columns: {len(full_df.columns)}")
print(f"Column names: {', '.join(full_df.columns.tolist()[:10])}...")

# %% [markdown]
# ## 3. Column Comparison

# %%
print("\n" + "="*80)
print("COLUMN COMPARISON")
print("="*80)

# Get column sets
filtered_cols = set(filtered_df.columns)
full_cols = set(full_df.columns)

# Find common and unique columns
common_cols = filtered_cols & full_cols
only_filtered = filtered_cols - full_cols
only_full = full_cols - filtered_cols

print(f"\nColumn analysis:")
print(f"  Columns in both: {len(common_cols)}")
print(f"  Only in filtered: {len(only_filtered)}")
print(f"  Only in full: {len(only_full)}")

if only_filtered:
    print(f"\n--- Columns only in FILTERED dataset ---")
    for col in sorted(only_filtered):
        print(f"  - {col}")

if only_full:
    print(f"\n--- Columns only in FULL dataset (showing first 20) ---")
    for col in sorted(only_full)[:20]:
        print(f"  - {col}")
    if len(only_full) > 20:
        print(f"  ... and {len(only_full) - 20} more columns")

# %% [markdown]
# ## 4. Record Count Analysis

# %%
print("\n" + "="*80)
print("RECORD COUNT ANALYSIS")
print("="*80)

print(f"\nTotal records:")
print(f"  Filtered dataset: {len(filtered_df):,}")
print(f"  Full dataset: {len(full_df):,}")
print(f"  Difference: {abs(len(filtered_df) - len(full_df)):,}")

# Check for full duplicates based on all common columns
if len(common_cols) > 0:
    # Convert common_cols set to sorted list for consistent ordering
    common_cols_list = sorted(list(common_cols))
    
    # Check for full duplicates in filtered dataset
    filtered_dups_full = filtered_df[common_cols_list].duplicated().sum()
    
    # Check for full duplicates in full dataset
    full_dups_full = full_df[common_cols_list].duplicated().sum()
    
    print(f"\nFull duplicates (based on {len(common_cols_list)} common columns):")
    print(f"  Filtered dataset: {filtered_dups_full:,}")
    print(f"  Full dataset: {full_dups_full:,}")
    
    # Show which columns are used for duplicate detection
    print(f"\n  Common columns used for duplicate detection:")
    for i, col in enumerate(common_cols_list[:10], 1):
        print(f"    {i}. {col}")
    if len(common_cols_list) > 10:
        print(f"    ... and {len(common_cols_list) - 10} more columns")
else:
    print("\n⚠ No common columns found between datasets - cannot check for duplicates")

# %% [markdown]
# ## 4b. Detailed Duplicate Investigation (geodata_bedrifter_api.csv)

# %%
print("\n" + "="*80)
print("DETAILED DUPLICATE INVESTIGATION - geodata_bedrifter_api.csv")
print("="*80)

if len(common_cols) > 0 and filtered_dups_full > 0:
    common_cols_list = sorted(list(common_cols))
    
    # Get all duplicate rows (including first occurrence)
    dup_mask = filtered_df[common_cols_list].duplicated(keep=False)
    duplicate_rows = filtered_df[dup_mask].copy()
    
    print(f"\n✓ Found {filtered_dups_full:,} duplicate records (excluding first occurrence)")
    print(f"✓ Total rows involved in duplicates: {len(duplicate_rows):,}")
    
    # Group by all common columns to see duplicate groups
    dup_groups = duplicate_rows.groupby(common_cols_list, dropna=False).size().reset_index(name='count')
    dup_groups = dup_groups[dup_groups['count'] > 1].sort_values('count', ascending=False)
    
    print(f"✓ Number of distinct duplicate groups: {len(dup_groups):,}")
    print(f"\nDuplicate group sizes:")
    print(f"  Max duplicates of same record: {dup_groups['count'].max()}")
    print(f"  Average duplicates per group: {dup_groups['count'].mean():.1f}")
    
    # Show examples of duplicate records
    print(f"\n--- Sample Duplicate Groups (first 3 groups) ---")
    display_cols = [col for col in ['firfirmaid', 'firorgnr', 'firfirmanavn1', 'objectid'] if col in filtered_df.columns]
    
    for idx, (group_idx, group_row) in enumerate(dup_groups.head(3).iterrows()):
        print(f"\nGroup {idx + 1}: {int(group_row['count'])} identical records")
        # Create filter condition for this group
        filter_condition = True
        for col in common_cols_list:
            filter_condition &= (filtered_df[col] == group_row[col]) | (filtered_df[col].isna() & pd.isna(group_row[col]))
        
        sample_dups = filtered_df[filter_condition][display_cols].head()
        print(sample_dups.to_string(index=False))
        
elif len(common_cols) > 0:
    print("\n✓ No duplicates found in geodata_bedrifter_api.csv")
else:
    print("\n⚠ Cannot analyze duplicates - no common columns")

# %% [markdown]
# ## 5. Enterprise ID Comparison (firfirmaid)

# %%
print("\n" + "="*80)
print("ENTERPRISE ID COMPARISON (firfirmaid)")
print("="*80)

# Check if firfirmaid exists in both datasets
if 'firfirmaid' in filtered_df.columns and 'firfirmaid' in full_df.columns:
    # Get unique firfirmaid from each dataset
    filtered_firmaid = set(filtered_df['firfirmaid'].dropna())
    full_firmaid = set(full_df['firfirmaid'].dropna())
    
    print(f"\nUnique firfirmaid:")
    print(f"  Filtered dataset: {len(filtered_firmaid):,}")
    print(f"  Full dataset: {len(full_firmaid):,}")
    
    # Find overlaps and differences
    in_both = filtered_firmaid & full_firmaid
    only_filtered = filtered_firmaid - full_firmaid
    only_full = full_firmaid - filtered_firmaid
    
    print(f"\nOverlap analysis:")
    print(f"  In both datasets: {len(in_both):,}")
    print(f"  Only in filtered: {len(only_filtered):,}")
    print(f"  Only in full: {len(only_full):,}")
    
    if len(filtered_firmaid) > 0:
        overlap_pct_filtered = (len(in_both) / len(filtered_firmaid) * 100)
        print(f"  {overlap_pct_filtered:.1f}% of filtered IDs are in full dataset")
    
    if len(full_firmaid) > 0:
        overlap_pct_full = (len(in_both) / len(full_firmaid) * 100)
        print(f"  {overlap_pct_full:.1f}% of full IDs are in filtered dataset")
    
    # Show examples if there are differences
    if only_filtered:
        print(f"\n--- Sample firfirmaid only in FILTERED (first 10) ---")
        for fid in sorted(only_filtered)[:10]:
            print(f"  {fid}")
    
    if only_full:
        print(f"\n--- Sample firfirmaid only in FULL (first 10) ---")
        for fid in sorted(only_full)[:10]:
            print(f"  {fid}")
else:
    print("firfirmaid column not found in one or both datasets")

# %% [markdown]
# ## 6. Organization Number Comparison (firorgnr)

# %%
print("\n" + "="*80)
print("ORGANIZATION NUMBER COMPARISON (firorgnr)")
print("="*80)

# Determine which column name to use
orgnr_col_filtered = None
orgnr_col_full = None

if 'firorgnr' in filtered_df.columns:
    orgnr_col_filtered = 'firorgnr'
elif 'Organisasjonsnummer' in filtered_df.columns:
    orgnr_col_filtered = 'Organisasjonsnummer'

if 'firorgnr' in full_df.columns:
    orgnr_col_full = 'firorgnr'
elif 'Organisasjonsnummer' in full_df.columns:
    orgnr_col_full = 'Organisasjonsnummer'

if orgnr_col_filtered and orgnr_col_full:
    # Get unique org numbers from each dataset
    filtered_orgnr = set(filtered_df[orgnr_col_filtered].dropna().astype(str))
    full_orgnr = set(full_df[orgnr_col_full].dropna().astype(str))
    
    print(f"\nUnique organization numbers:")
    print(f"  Filtered dataset: {len(filtered_orgnr):,}")
    print(f"  Full dataset: {len(full_orgnr):,}")
    
    # Find overlaps and differences
    in_both_orgnr = filtered_orgnr & full_orgnr
    only_filtered_orgnr = filtered_orgnr - full_orgnr
    only_full_orgnr = full_orgnr - filtered_orgnr
    
    print(f"\nOverlap analysis:")
    print(f"  In both datasets: {len(in_both_orgnr):,}")
    print(f"  Only in filtered: {len(only_filtered_orgnr):,}")
    print(f"  Only in full: {len(only_full_orgnr):,}")
    
    if len(filtered_orgnr) > 0:
        overlap_pct = (len(in_both_orgnr) / len(filtered_orgnr) * 100)
        print(f"  {overlap_pct:.1f}% of filtered org numbers are in full dataset")
    
    if len(full_orgnr) > 0:
        overlap_pct = (len(in_both_orgnr) / len(full_orgnr) * 100)
        print(f"  {overlap_pct:.1f}% of full org numbers are in filtered dataset")
    
    # Show examples if there are differences
    if only_filtered_orgnr:
        print(f"\n--- Sample org numbers only in FILTERED (first 10) ---")
        for org in sorted(only_filtered_orgnr)[:10]:
            print(f"  {org}")
    
    if only_full_orgnr:
        print(f"\n--- Sample org numbers only in FULL (first 10) ---")
        for org in sorted(only_full_orgnr)[:10]:
            print(f"  {org}")
else:
    print("Organization number column not found in one or both datasets")

# %% [markdown]
# ## 7. Data Quality Check on Common Columns

# %%
print("\n" + "="*80)
print("DATA QUALITY CHECK - COMMON COLUMNS")
print("="*80)

# Check for null values in key common columns
key_columns = ['firfirmaid', 'firorgnr', 'firfirmanavn1', 'objectid']
common_key_cols = [col for col in key_columns if col in common_cols]

if common_key_cols:
    print("\nNull values in key columns:")
    for col in common_key_cols:
        filtered_nulls = filtered_df[col].isna().sum()
        full_nulls = full_df[col].isna().sum()
        print(f"\n  {col}:")
        print(f"    Filtered: {filtered_nulls:,} ({filtered_nulls/len(filtered_df)*100:.2f}%)")
        print(f"    Full: {full_nulls:,} ({full_nulls/len(full_df)*100:.2f}%)")

# %% [markdown]
# ## 8. Sample Data Comparison

# %%
print("\n" + "="*80)
print("SAMPLE DATA COMPARISON")
print("="*80)

# Show first few records from each dataset (common columns only)
display_cols = [col for col in ['firfirmaid', 'objectid', 'firorgnr', 'firfirmanavn1'] if col in common_cols]

if display_cols:
    print("\n--- First 5 records from FILTERED dataset ---")
    print(filtered_df[display_cols].head(5).to_string(index=False))
    
    print("\n--- First 5 records from FULL dataset ---")
    print(full_df[display_cols].head(5).to_string(index=False))

# %% [markdown]
# ## 9. Summary Report

# %%
print("\n" + "="*80)
print("SUMMARY REPORT")
print("="*80)

# Calculate key metrics
record_diff = abs(len(filtered_df) - len(full_df))
record_match = len(filtered_df) == len(full_df)

# Check if all IDs match (if available)
if 'firfirmaid' in filtered_df.columns and 'firfirmaid' in full_df.columns:
    id_match = filtered_firmaid == full_firmaid
else:
    id_match = None

print(f"""
COMPARISON RESULTS:

1. RECORD COUNTS:
   - Filtered dataset: {len(filtered_df):,} records
   - Full dataset: {len(full_df):,} records
   - Match: {'✓ YES' if record_match else f'✗ NO (difference: {record_diff:,})'}

2. COLUMNS:
   - Filtered dataset: {len(filtered_df.columns)} columns
   - Full dataset: {len(full_df.columns)} columns
   - Common columns: {len(common_cols)}
   - Only in full: {len(only_full)} additional columns

3. ENTERPRISE IDs (firfirmaid):
   - {'✓ IDENTICAL' if id_match else '✗ DIFFERENT' if id_match is not None else 'N/A - Column not found'}
   {f'- Unique IDs in filtered: {len(filtered_firmaid):,}' if id_match is not None else ''}
   {f'- Unique IDs in full: {len(full_firmaid):,}' if id_match is not None else ''}

4. DATA QUALITY:
   - Full duplicate records (all common columns): {'Present - Filtered: ' + str(filtered_dups_full) + ', Full: ' + str(full_dups_full) if len(common_cols) > 0 and (filtered_dups_full > 0 or full_dups_full > 0) else 'None detected' if len(common_cols) > 0 else 'N/A'}
   - All key identifiers present: {'✓ YES' if all(col in common_cols for col in ['firfirmaid', 'firorgnr']) else '✗ NO'}

5. CONCLUSION:
   {'✓ Datasets appear to contain the SAME enterprises with different column counts' if record_match and (id_match if id_match is not None else True) else '⚠ Datasets differ - review details above'}
   
RECOMMENDATION:
   {'Use the filtered dataset for GitHub (smaller file size, same enterprises)' if record_match and (id_match if id_match is not None else True) else 'Investigate differences before proceeding'}
""")

print("="*80)
print("ANALYSIS COMPLETE")
print("="*80)
