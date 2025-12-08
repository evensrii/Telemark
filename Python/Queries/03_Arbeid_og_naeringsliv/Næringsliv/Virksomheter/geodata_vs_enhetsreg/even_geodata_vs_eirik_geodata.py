# %%
"""
Comparison of two Geodata extracts: Even's API data vs Eirik's export
This script analyzes the differences between two Geodata datasets:
1. geodata_bedrifter_api.csv (Even's data - from API)
2. Bedrifter_Telemark_Eirik.csv (Eirik's data - from export)
"""

import pandas as pd
import os

# %% [markdown]
# ## 1. Load Data

# %%
# Define file paths
data_folder = r"c:\Users\eve1509\OneDrive - Telemark fylkeskommune\Github\Telemark\Data\03_Arbeid og næringsliv\02_Næringsliv\Virksomheter\geodata_vs_enhetsregisteret"
even_file = os.path.join(data_folder, "geodata_bedrifter_api.csv")
eirik_file = os.path.join(data_folder, "Bedrifter_Telemark_Eirik.csv")

# Load datasets
print("Loading datasets...")
even_df = pd.read_csv(even_file)
eirik_df = pd.read_csv(eirik_file, sep=';')  # Eirik's file uses semicolon

print(f"✓ Even's Geodata (API): {len(even_df):,} records")
print(f"✓ Eirik's Geodata (Export): {len(eirik_df):,} records")

# Map renamed columns in Even's data back to raw column names for comparison
# This allows the script to work with both the processed API file and Eirik's raw export
column_mapping = {
    'Organisasjonsnummer': 'firorgnr',
    'Orgnr til overliggende enhet': 'firorgnrknytning',  # Parent organization
    'Bedriftsnavn': 'firfirmanavn1',
    'Kommunenummer': 'firkommnr',
    'Kommunenavn': 'kommunenavn',
    'Organisasjonsform': 'orfkode',
    'Intervallkode antall ansatte': 'ansatt_kd',
    'objectid': 'objectid',  # Keep as is
    'firfirmaid': 'firfirmaid'  # Keep as is
}

# Check which columns need to be mapped in Even's data
print("\n--- Column Mapping ---")
for new_name, raw_name in column_mapping.items():
    if new_name in even_df.columns and raw_name not in even_df.columns:
        even_df[raw_name] = even_df[new_name]
        print(f"  Mapped '{new_name}' → '{raw_name}'")

print("✓ Column names standardized for comparison")

# %% [markdown]
# ## 2. Basic Dataset Overview

# %%
print("\n" + "="*80)
print("DATASET OVERVIEW")
print("="*80)

print("\n--- EVEN'S GEODATA (API) ---")
print(f"Total records: {len(even_df):,}")
print(f"Columns: {len(even_df.columns)}")
key_cols_even = [col for col in ['objectid', 'firorgnr', 'firfirmanavn1', 'firkommnr', 'kommunenavn'] if col in even_df.columns]
print(f"Key columns: {', '.join(key_cols_even)}")

print("\n--- EIRIK'S GEODATA (EXPORT) ---")
print(f"Total records: {len(eirik_df):,}")
print(f"Columns: {len(eirik_df.columns)}")
print(f"Key columns: firfirmaid, firorgnr, firfirmanavn1, firkommnr, kommunenavn")

# Check for null organization numbers
print("\n--- Data Quality Check ---")
print(f"Even's data - Null org numbers: {even_df['firorgnr'].isna().sum():,}")
print(f"Eirik's data - Null org numbers: {eirik_df['firorgnr'].isna().sum():,}")

# %% [markdown]
# ## 3. Organization Number Comparison

# %%
print("\n" + "="*80)
print("ORGANIZATION NUMBER COMPARISON")
print("="*80)

# Get unique org numbers from each source
even_orgnr = set(even_df['firorgnr'].dropna().astype(str))
eirik_orgnr = set(eirik_df['firorgnr'].dropna().astype(str))

print(f"\nUnique organization numbers:")
print(f"  Even's Geodata: {len(even_orgnr):,}")
print(f"  Eirik's Geodata: {len(eirik_orgnr):,}")

# Find overlaps and differences
in_both = even_orgnr & eirik_orgnr
only_even = even_orgnr - eirik_orgnr
only_eirik = eirik_orgnr - even_orgnr

print(f"\nOverlap analysis:")
print(f"  In both datasets: {len(in_both):,}")
print(f"  Only in Even's data: {len(only_even):,}")
print(f"  Only in Eirik's data: {len(only_eirik):,}")

overlap_pct_even = (len(in_both) / len(even_orgnr) * 100) if even_orgnr else 0
overlap_pct_eirik = (len(in_both) / len(eirik_orgnr) * 100) if eirik_orgnr else 0

print(f"\nOverlap percentages:")
print(f"  {overlap_pct_even:.1f}% of Even's organizations are in Eirik's data")
print(f"  {overlap_pct_eirik:.1f}% of Eirik's organizations are in Even's data")

# %% [markdown]
# ## 4. Apply Eirik's SQL Filter
# Filter: Keep only organizations where firorgnr is NOT in the set of firorgnrknytning values
# (i.e., exclude organizations that are subsidiaries/underenhet of another organization)

# %%
print("\n" + "="*80)
print("APPLYING SQL FILTER: firorgnr NOT IN (firorgnrknytning)")
print("="*80)

# Apply filter to Even's data
# Convert floats to int first to remove .0 suffix, then to string
even_knytning = set(even_df['firorgnrknytning'].dropna().astype(int).astype(str))
print(f"\nEven's data:")
print(f"  Total unique 'firorgnrknytning' values: {len(even_knytning):,}")

# DEBUG: Check for overlap between firorgnr and firorgnrknytning
even_orgnr_set = set(even_df['firorgnr'].dropna().astype(str))
overlap = even_orgnr_set & even_knytning
print(f"  DEBUG: firorgnr values that appear in firorgnrknytning: {len(overlap):,}")
if len(overlap) > 0:
    print(f"  DEBUG: Sample overlapping values: {list(overlap)[:5]}")
else:
    print(f"  DEBUG: Sample firorgnr values: {list(even_orgnr_set)[:5]}")
    print(f"  DEBUG: Sample firorgnrknytning values: {list(even_knytning)[:5]}")
    print(f"  ⚠ WARNING: No overlap means parent organizations are NOT in the dataset!")
    print(f"  This is expected if parents are registered in other counties.")

even_filtered = even_df[~even_df['firorgnr'].astype(str).isin(even_knytning)].copy()
print(f"  Records before filter: {len(even_df):,}")
print(f"  Records after filter: {len(even_filtered):,}")
print(f"  Records removed: {len(even_df) - len(even_filtered):,}")

# Apply filter to Eirik's data
# Convert floats to int first to remove .0 suffix, then to string
eirik_knytning = set(eirik_df['firorgnrknytning'].dropna().astype(int).astype(str))
print(f"\nEirik's data:")
print(f"  Total unique 'firorgnrknytning' values: {len(eirik_knytning):,}")
eirik_filtered = eirik_df[~eirik_df['firorgnr'].astype(str).isin(eirik_knytning)].copy()
print(f"  Records before filter: {len(eirik_df):,}")
print(f"  Records after filter: {len(eirik_filtered):,}")
print(f"  Records removed: {len(eirik_df) - len(eirik_filtered):,}")

# Compare filtered datasets
even_filtered_orgnr = set(even_filtered['firorgnr'].dropna().astype(str))
eirik_filtered_orgnr = set(eirik_filtered['firorgnr'].dropna().astype(str))

in_both_filtered = even_filtered_orgnr & eirik_filtered_orgnr
only_even_filtered = even_filtered_orgnr - eirik_filtered_orgnr
only_eirik_filtered = eirik_filtered_orgnr - even_filtered_orgnr

print(f"\nFiltered data comparison:")
print(f"  Unique org numbers in Even's filtered data: {len(even_filtered_orgnr):,}")
print(f"  Unique org numbers in Eirik's filtered data: {len(eirik_filtered_orgnr):,}")
print(f"  In both filtered datasets: {len(in_both_filtered):,}")
print(f"  Only in Even's filtered data: {len(only_even_filtered):,}")
print(f"  Only in Eirik's filtered data: {len(only_eirik_filtered):,}")

overlap_pct_even_filtered = (len(in_both_filtered) / len(even_filtered_orgnr) * 100) if even_filtered_orgnr else 0
overlap_pct_eirik_filtered = (len(in_both_filtered) / len(eirik_filtered_orgnr) * 100) if eirik_filtered_orgnr else 0

print(f"\nFiltered overlap percentages:")
print(f"  {overlap_pct_even_filtered:.1f}% of Even's filtered organizations are in Eirik's filtered data")
print(f"  {overlap_pct_eirik_filtered:.1f}% of Eirik's filtered organizations are in Even's filtered data")

# %% [markdown]
# ## 5. Organizations Only in Even's Data

# %%
print("\n" + "="*80)
print("ORGANIZATIONS ONLY IN EVEN'S GEODATA (API)")
print("="*80)

only_even_df = even_df[even_df['firorgnr'].astype(str).isin(only_even)].copy()

print(f"\nTotal: {len(only_even_df):,} organizations")

if len(only_even_df) > 0:
    print("\n--- By Organization Type (orfkode) ---")
    if 'orfkode' in only_even_df.columns:
        org_type_counts = only_even_df['orfkode'].value_counts()
        for org_type, count in org_type_counts.head(10).items():
            print(f"  {org_type}: {count:,}")
    
    print("\n--- By Municipality ---")
    if 'kommunenavn' in only_even_df.columns:
        muni_counts = only_even_df['kommunenavn'].value_counts()
        for muni, count in muni_counts.items():
            print(f"  {muni}: {count:,}")
    
    print("\n--- By Employee Category ---")
    if 'ansatt_kd' in only_even_df.columns:
        emp_counts = only_even_df['ansatt_kd'].value_counts()
        for cat, count in emp_counts.head(10).items():
            print(f"  {cat}: {count:,}")
    
    print("\n--- Sample records (first 20) ---")
    sample_cols = ['objectid', 'firorgnr', 'firfirmanavn1', 'orfkode', 'kommunenavn']
    available_cols = [col for col in sample_cols if col in only_even_df.columns]
    print(only_even_df[available_cols].head(20).to_string(index=False))

# %% [markdown]
# ## 6. Organizations Only in Eirik's Data

# %%
print("\n" + "="*80)
print("ORGANIZATIONS ONLY IN EIRIK'S GEODATA (EXPORT)")
print("="*80)

only_eirik_df = eirik_df[eirik_df['firorgnr'].astype(str).isin(only_eirik)].copy()

print(f"\nTotal: {len(only_eirik_df):,} organizations")

if len(only_eirik_df) > 0:
    print("\n--- By Organization Type (orfkode) ---")
    if 'orfkode' in only_eirik_df.columns:
        org_type_counts = only_eirik_df['orfkode'].value_counts()
        for org_type, count in org_type_counts.head(10).items():
            print(f"  {org_type}: {count:,}")
    
    print("\n--- By Municipality ---")
    if 'kommunenavn' in only_eirik_df.columns or 'knrkommnavn' in only_eirik_df.columns:
        muni_col = 'kommunenavn' if 'kommunenavn' in only_eirik_df.columns else 'knrkommnavn'
        muni_counts = only_eirik_df[muni_col].value_counts()
        for muni, count in muni_counts.items():
            print(f"  {muni}: {count:,}")
    
    print("\n--- By Employee Category ---")
    if 'ansatt_kd' in only_eirik_df.columns:
        emp_counts = only_eirik_df['ansatt_kd'].value_counts()
        for cat, count in emp_counts.head(10).items():
            print(f"  {cat}: {count:,}")
    
    print("\n--- Sample records (first 20) ---")
    sample_cols = ['firfirmaid', 'firorgnr', 'firfirmanavn1', 'orfkode']
    available_cols = [col for col in sample_cols if col in only_eirik_df.columns]
    if 'kommunenavn' in only_eirik_df.columns:
        available_cols.append('kommunenavn')
    elif 'knrkommnavn' in only_eirik_df.columns:
        available_cols.append('knrkommnavn')
    print(only_eirik_df[available_cols].head(20).to_string(index=False))

# %% [markdown]
# ## 7. Matched Organizations - Data Comparison

# %%
print("\n" + "="*80)
print("MATCHED ORGANIZATIONS - DATA COMPARISON")
print("="*80)

# Get matching records
even_matched = even_df[even_df['firorgnr'].astype(str).isin(in_both)].copy()
eirik_matched = eirik_df[eirik_df['firorgnr'].astype(str).isin(in_both)].copy()

print(f"\nMatched organizations: {len(in_both):,}")
print(f"Even's records for matched orgs: {len(even_matched):,}")
print(f"Eirik's records for matched orgs: {len(eirik_matched):,}")

# Prepare for merge
merge_cols_even = ['firorgnr', 'firfirmanavn1']
if 'objectid' in even_matched.columns:
    merge_cols_even.append('objectid')
even_for_merge = even_matched[merge_cols_even].copy()

rename_dict_even = {'firfirmanavn1': 'Navn_Even'}
if 'objectid' in even_for_merge.columns:
    rename_dict_even['objectid'] = 'ID_Even'
even_for_merge.rename(columns=rename_dict_even, inplace=True)

eirik_for_merge = eirik_matched[['firorgnr', 'firfirmanavn1', 'firfirmaid']].copy() if 'firfirmaid' in eirik_matched.columns else eirik_matched[['firorgnr', 'firfirmanavn1']].copy()
eirik_for_merge.rename(columns={
    'firfirmanavn1': 'Navn_Eirik',
    'firfirmaid': 'ID_Eirik'
}, inplace=True)

# Merge
comparison_df = pd.merge(
    even_for_merge,
    eirik_for_merge,
    on='firorgnr'
)

print(f"\nMerged comparison records: {len(comparison_df):,}")

# Compare company names
comparison_df['Name_Match'] = comparison_df['Navn_Even'] == comparison_df['Navn_Eirik']
name_matches = comparison_df['Name_Match'].sum()
name_differences = (~comparison_df['Name_Match']).sum()

print(f"\nCompany name comparison:")
print(f"  Exact matches: {name_matches:,} ({name_matches/len(comparison_df)*100:.1f}%)")
print(f"  Differences: {name_differences:,} ({name_differences/len(comparison_df)*100:.1f}%)")

# Show examples of name differences
if name_differences > 0:
    print("\n--- Examples of name differences (first 20) ---")
    diff_examples = comparison_df[~comparison_df['Name_Match']].head(20)
    display_cols = ['firorgnr', 'Navn_Even', 'Navn_Eirik']
    available_cols = [col for col in display_cols if col in diff_examples.columns]
    print(diff_examples[available_cols].to_string(index=False))

# %% [markdown]
# ## 8. Record Count Analysis

# %%
print("\n" + "="*80)
print("RECORD COUNT ANALYSIS")
print("="*80)

# Check if there are duplicate org numbers in either dataset
even_duplicates = even_df['firorgnr'].duplicated().sum()
eirik_duplicates = eirik_df['firorgnr'].duplicated().sum()

print(f"\nDuplicate organization numbers:")
print(f"  Even's data: {even_duplicates:,} duplicates")
print(f"  Eirik's data: {eirik_duplicates:,} duplicates")

# Check for organizations appearing multiple times (if any)
if even_duplicates > 0:
    print("\n--- Organizations appearing multiple times in Even's data (top 10) ---")
    even_dup_orgs = even_df[even_df.duplicated(subset=['firorgnr'], keep=False)]['firorgnr'].value_counts().head(10)
    for orgnr, count in even_dup_orgs.items():
        print(f"  {orgnr}: {count} times")

if eirik_duplicates > 0:
    print("\n--- Organizations appearing multiple times in Eirik's data (top 10) ---")
    eirik_dup_orgs = eirik_df[eirik_df.duplicated(subset=['firorgnr'], keep=False)]['firorgnr'].value_counts().head(10)
    for orgnr, count in eirik_dup_orgs.items():
        print(f"  {orgnr}: {count} times")

# %% [markdown]
# ## 9. Total Employee Count Comparison

# %%
print("\n" + "="*80)
print("TOTAL EMPLOYEE COUNT COMPARISON")
print("="*80)

# Check if employee columns exist
even_emp_col = 'Antall ansatte' if 'Antall ansatte' in even_df.columns else None
eirik_emp_col = 'firantansatt' if 'firantansatt' in eirik_df.columns else None

if even_emp_col and eirik_emp_col:
    print("\n" + "-"*80)
    print("FULL DATASETS (All organizations)")
    print("-"*80)
    
    # Convert to numeric, handling any non-numeric values
    even_employees = pd.to_numeric(even_df[even_emp_col], errors='coerce')
    eirik_employees = pd.to_numeric(eirik_df[eirik_emp_col], errors='coerce')
    
    # Calculate totals
    even_total = even_employees.sum()
    eirik_total = eirik_employees.sum()
    
    # Count non-null values
    even_count = even_employees.notna().sum()
    eirik_count = eirik_employees.notna().sum()
    
    # Calculate averages
    even_avg = even_employees.mean()
    eirik_avg = eirik_employees.mean()
    
    print(f"\n--- Total Employees (Full Dataset) ---")
    print(f"  Even's Geodata (API): {even_total:,.0f} employees")
    print(f"  Eirik's Geodata (Export): {eirik_total:,.0f} employees")
    print(f"  Difference: {abs(even_total - eirik_total):,.0f} employees")
    if max(even_total, eirik_total) > 0:
        print(f"  Percentage difference: {abs(even_total - eirik_total) / max(even_total, eirik_total) * 100:.1f}%")
    
    print(f"\n--- Records with Employee Data ---")
    print(f"  Even's Geodata: {even_count:,} records ({even_count/len(even_df)*100:.1f}% of total)")
    print(f"  Eirik's Geodata: {eirik_count:,} records ({eirik_count/len(eirik_df)*100:.1f}% of total)")
    
    print(f"\n--- Average Employees per Organization ---")
    print(f"  Even's Geodata: {even_avg:.1f} employees")
    print(f"  Eirik's Geodata: {eirik_avg:.1f} employees")
    
    # Now analyze FILTERED datasets (after SQL filter)
    print("\n" + "-"*80)
    print("FILTERED DATASETS (SQL Filter: firorgnr NOT IN firorgnrknytning)")
    print("Excludes organizations that are subsidiaries/underenheter")
    print("-"*80)
    
    # Convert to numeric for filtered datasets
    even_employees_filtered = pd.to_numeric(even_filtered[even_emp_col], errors='coerce')
    eirik_employees_filtered = pd.to_numeric(eirik_filtered[eirik_emp_col], errors='coerce')
    
    # Calculate totals for filtered data
    even_total_filtered = even_employees_filtered.sum()
    eirik_total_filtered = eirik_employees_filtered.sum()
    
    # Count non-null values
    even_count_filtered = even_employees_filtered.notna().sum()
    eirik_count_filtered = eirik_employees_filtered.notna().sum()
    
    # Calculate averages
    even_avg_filtered = even_employees_filtered.mean()
    eirik_avg_filtered = eirik_employees_filtered.mean()
    
    print(f"\n--- Total Employees (Filtered Dataset) ---")
    print(f"  Even's Geodata (API): {even_total_filtered:,.0f} employees")
    print(f"  Eirik's Geodata (Export): {eirik_total_filtered:,.0f} employees")
    print(f"  Difference: {abs(even_total_filtered - eirik_total_filtered):,.0f} employees")
    if max(even_total_filtered, eirik_total_filtered) > 0:
        print(f"  Percentage difference: {abs(even_total_filtered - eirik_total_filtered) / max(even_total_filtered, eirik_total_filtered) * 100:.1f}%")
    
    print(f"\n--- Records with Employee Data (Filtered) ---")
    print(f"  Even's Geodata: {even_count_filtered:,} records ({even_count_filtered/len(even_filtered)*100:.1f}% of filtered total)")
    print(f"  Eirik's Geodata: {eirik_count_filtered:,} records ({eirik_count_filtered/len(eirik_filtered)*100:.1f}% of filtered total)")
    
    print(f"\n--- Average Employees per Organization (Filtered) ---")
    print(f"  Even's Geodata: {even_avg_filtered:.1f} employees")
    print(f"  Eirik's Geodata: {eirik_avg_filtered:.1f} employees")
    
    # Compare filtered vs full
    print("\n" + "-"*80)
    print("IMPACT OF SQL FILTER ON EMPLOYEE COUNTS")
    print("-"*80)
    
    print(f"\nEven's Geodata:")
    print(f"  Employees removed by filter: {even_total - even_total_filtered:,.0f} ({(even_total - even_total_filtered)/even_total*100:.1f}% of total)")
    print(f"  Organizations removed: {len(even_df) - len(even_filtered):,}")
    
    print(f"\nEirik's Geodata:")
    print(f"  Employees removed by filter: {eirik_total - eirik_total_filtered:,.0f} ({(eirik_total - eirik_total_filtered)/eirik_total*100:.1f}% of total)")
    print(f"  Organizations removed: {len(eirik_df) - len(eirik_filtered):,}")
    
    # Distribution analysis for filtered data
    print("\n" + "-"*80)
    print("EMPLOYEE DISTRIBUTION (Filtered Datasets Only)")
    print("-"*80)
    
    print(f"\n--- Even's Filtered Data ---")
    print(f"  Min: {even_employees_filtered.min():.0f}")
    print(f"  25th percentile: {even_employees_filtered.quantile(0.25):.0f}")
    print(f"  Median: {even_employees_filtered.median():.0f}")
    print(f"  75th percentile: {even_employees_filtered.quantile(0.75):.0f}")
    print(f"  Max: {even_employees_filtered.max():.0f}")
    
    print(f"\n--- Eirik's Filtered Data ---")
    print(f"  Min: {eirik_employees_filtered.min():.0f}")
    print(f"  25th percentile: {eirik_employees_filtered.quantile(0.25):.0f}")
    print(f"  Median: {eirik_employees_filtered.median():.0f}")
    print(f"  75th percentile: {eirik_employees_filtered.quantile(0.75):.0f}")
    print(f"  Max: {eirik_employees_filtered.max():.0f}")
    
else:
    print("\n⚠ Warning: Could not find employee columns in both datasets")
    if not even_emp_col:
        print(f"  'Antall ansatte' not found in Even's data. Available columns: {', '.join(even_df.columns)}")
    if not eirik_emp_col:
        print(f"  'firantansatt' not found in Eirik's data. Available columns: {', '.join(eirik_df.columns)}")

# %% [markdown]
# ## 10. Summary Statistics

# %%
print("\n" + "="*80)
print("SUMMARY STATISTICS")
print("="*80)

summary_stats = {
    'Metric': [
        'Total Records',
        'Unique Org Numbers',
        'Organizations in Both',
        'Only in Dataset',
        'Overlap Percentage',
        'Duplicate Org Numbers'
    ],
    "Even's Geodata (API)": [
        f"{len(even_df):,}",
        f"{len(even_orgnr):,}",
        f"{len(in_both):,}",
        f"{len(only_even):,}",
        f"{overlap_pct_even:.1f}%",
        f"{even_duplicates:,}"
    ],
    "Eirik's Geodata (Export)": [
        f"{len(eirik_df):,}",
        f"{len(eirik_orgnr):,}",
        f"{len(in_both):,}",
        f"{len(only_eirik):,}",
        f"{overlap_pct_eirik:.1f}%",
        f"{eirik_duplicates:,}"
    ]
}

summary_df = pd.DataFrame(summary_stats)
print("\n" + summary_df.to_string(index=False))

# %% [markdown]
# ## 11. Key Findings Summary

# %%
print("\n" + "="*80)
print("KEY FINDINGS")
print("="*80)

print(f"""
1. DATASET SIZES:
   - Even's Geodata (API) contains {len(even_df):,} records
   - Eirik's Geodata (Export) contains {len(eirik_df):,} records
   - Difference: {abs(len(even_df) - len(eirik_df)):,} records

2. ORGANIZATION OVERLAP:
   - {len(in_both):,} organizations appear in both datasets
   - {len(only_even):,} organizations only in Even's data ({len(only_even)/len(even_orgnr)*100:.1f}% of Even's)
   - {len(only_eirik):,} organizations only in Eirik's data ({len(only_eirik)/len(eirik_orgnr)*100:.1f}% of Eirik's)
   - Overall overlap: {overlap_pct_even:.1f}% (Even) / {overlap_pct_eirik:.1f}% (Eirik)

3. DATA QUALITY:
   - Duplicate org numbers in Even's data: {even_duplicates:,}
   - Duplicate org numbers in Eirik's data: {eirik_duplicates:,}
   - Company name matches (for matched orgs): {name_matches:,} ({name_matches/len(comparison_df)*100:.1f}%)
   - Company name differences: {name_differences:,} ({name_differences/len(comparison_df)*100:.1f}%)

4. POSSIBLE REASONS FOR DIFFERENCES:
   - Different extraction dates/times from Geodata
   - Different filtering criteria (e.g., active vs all organizations)
   - Different API endpoints or export methods
   - Updates to the Geodata database between extractions
   - Different handling of duplicate entries
   - Possible filtering on organization type or status

5. DATA SOURCE CHARACTERISTICS:
   Even's Geodata (API):
   - Extracted via API call
   - Comma-separated format
   - {len(even_df.columns)} columns
   - Load date: {even_df['load_date'].iloc[0] if 'load_date' in even_df.columns else 'Unknown'}
   
   Eirik's Geodata (Export):
   - Extracted via export/download
   - Semicolon-separated format
   - {len(eirik_df.columns)} columns
   - Load date: {eirik_df['load_date'].iloc[0] if 'load_date' in eirik_df.columns else 'Unknown'}

6. RECOMMENDATIONS:
   - Investigate the {len(only_even):,} organizations only in Even's data
   - Check why {len(only_eirik):,} organizations are only in Eirik's data
   - Verify company name differences for matched organizations
   - Consider using the most recent extract as the authoritative source
   - Document the extraction method and criteria for future reference
""")

print("\n" + "="*80)
print("ANALYSIS COMPLETE")
print("="*80)

# %%
# Optional: Export detailed comparison
# comparison_df.to_csv(os.path.join(data_folder, 'geodata_comparison_even_vs_eirik.csv'), index=False, sep=';')
# print("Detailed comparison exported to: geodata_comparison_even_vs_eirik.csv")
