# %%
"""
Comprehensive comparison of Geodata API vs Enhetsregisteret Laveste Nivå
This script analyzes the differences between business data from two sources:
1. Geodata API (geodata_bedrifter_api.csv)
2. Enhetsregisteret Laveste Nivå (enhetsregisteret_laveste_nivaa.csv)

Note: The "laveste nivå" dataset represents the most granular organizational level,
combining hovedenheter and their lowest-level underenheter.
"""

import pandas as pd
import os

# %% [markdown]
# ## 1. Load Data

# %%
# Define file paths
data_folder = r"c:\Users\eve1509\OneDrive - Telemark fylkeskommune\Github\Telemark\Data\03_Arbeid og næringsliv\02_Næringsliv\Virksomheter"
geodata_file = os.path.join(data_folder, "geodata_bedrifter_api.csv")
laveste_nivaa_file = os.path.join(data_folder, "enhetsregisteret_laveste_nivaa.csv")

# Load datasets
print("Loading datasets...")
geodata_df = pd.read_csv(geodata_file)
laveste_nivaa_df = pd.read_csv(laveste_nivaa_file)

print(f"✓ Geodata: {len(geodata_df):,} records")
print(f"✓ Enhetsregisteret Laveste Nivå: {len(laveste_nivaa_df):,} records")

# %% [markdown]
# ## 2. Basic Dataset Overview

# %%
print("\n" + "="*80)
print("DATASET OVERVIEW")
print("="*80)

print("\n--- GEODATA API ---")
print(f"Total records: {len(geodata_df):,}")
print(f"Columns: {len(geodata_df.columns)}")
print(f"Column names:\n{', '.join(geodata_df.columns.tolist())}")

print("\n--- ENHETSREGISTERET LAVESTE NIVÅ ---")
print(f"Total records: {len(laveste_nivaa_df):,}")
print(f"Columns: {len(laveste_nivaa_df.columns)}")
print(f"Column names:\n{', '.join(laveste_nivaa_df.columns.tolist())}")

# Distribution by level
print("\n--- Distribution by Organizational Level ---")
level_dist = laveste_nivaa_df['Nivå enhetsregisteret'].value_counts().sort_index()
for level, count in level_dist.items():
    print(f"  Nivå {level}: {count:,} ({count/len(laveste_nivaa_df)*100:.1f}%)")

# %% [markdown]
# ## 3. Organization Number Analysis

# %%
print("\n" + "="*80)
print("ORGANIZATION NUMBER COMPARISON")
print("="*80)

# Get unique org numbers from each source
geodata_orgnr = set(geodata_df['Organisasjonsnummer'].dropna().astype(str))
laveste_orgnr = set(laveste_nivaa_df['Organisasjonsnummer'].dropna().astype(str))

print(f"\nUnique organization numbers:")
print(f"  Geodata API: {len(geodata_orgnr):,}")
print(f"  Enhetsregisteret Laveste Nivå: {len(laveste_orgnr):,}")

# Find overlaps and differences
only_geodata = geodata_orgnr - laveste_orgnr
only_laveste = laveste_orgnr - geodata_orgnr
in_both = geodata_orgnr & laveste_orgnr

print(f"\nOverlap analysis:")
print(f"  In both datasets: {len(in_both):,}")
print(f"  Only in Geodata: {len(only_geodata):,}")
print(f"  Only in Laveste Nivå: {len(only_laveste):,}")

overlap_pct_geodata = (len(in_both) / len(geodata_orgnr) * 100) if geodata_orgnr else 0
overlap_pct_laveste = (len(in_both) / len(laveste_orgnr) * 100) if laveste_orgnr else 0

print(f"\nOverlap percentages:")
print(f"  {overlap_pct_geodata:.1f}% of Geodata organizations are in Laveste Nivå")
print(f"  {overlap_pct_laveste:.1f}% of Laveste Nivå organizations are in Geodata")

# %% [markdown]
# ## 4. Analyze Organizations Only in Geodata

# %%
print("\n" + "="*80)
print("ORGANIZATIONS ONLY IN GEODATA API")
print("="*80)

only_geodata_df = geodata_df[geodata_df['Organisasjonsnummer'].astype(str).isin(only_geodata)].copy()

print(f"\nTotal: {len(only_geodata_df):,} organizations")

if len(only_geodata_df) > 0:
    print("\n--- By Organizational Form ---")
    org_form_counts = only_geodata_df['Organisasjonsform'].value_counts()
    for form, count in org_form_counts.head(10).items():
        print(f"  {form}: {count:,}")
    
    print("\n--- By Employee Category ---")
    emp_counts = only_geodata_df['Intervallkode antall ansatte'].value_counts()
    for cat, count in emp_counts.items():
        print(f"  {cat}: {count:,}")
    
    print("\n--- By Sector ---")
    sector_counts = only_geodata_df['Privat eller offentlig'].value_counts()
    for sector, count in sector_counts.items():
        print(f"  {sector}: {count:,}")
    
    print("\n--- Sample records (first 10) ---")
    sample_cols = ['Bedriftsnavn', 'Organisasjonsnummer', 'Organisasjonsform', 
                   'Antall ansatte', 'NACE-kategori (To første siffer)', 'Nacetittel1']
    print(only_geodata_df[sample_cols].head(10).to_string(index=False))

# %% [markdown]
# ## 5. Analyze Organizations Only in Laveste Nivå

# %%
print("\n" + "="*80)
print("ORGANIZATIONS ONLY IN ENHETSREGISTERET LAVESTE NIVÅ")
print("="*80)

only_laveste_df = laveste_nivaa_df[
    laveste_nivaa_df['Organisasjonsnummer'].astype(str).isin(only_laveste)
].copy()

print(f"\nTotal: {len(only_laveste_df):,} organizations")

if len(only_laveste_df) > 0:
    print("\n--- By Organizational Level ---")
    level_counts = only_laveste_df['Nivå enhetsregisteret'].value_counts().sort_index()
    for level, count in level_counts.items():
        print(f"  Nivå {level}: {count:,}")
    
    print("\n--- By Employee Size (top 10) ---")
    # Categorize employees
    def categorize_employees(emp):
        if pd.isna(emp):
            return 'Unknown'
        elif emp == 0:
            return '0'
        elif emp <= 10:
            return '1-10'
        elif emp <= 50:
            return '11-50'
        elif emp <= 100:
            return '51-100'
        elif emp <= 250:
            return '101-250'
        else:
            return '250+'
    
    only_laveste_df['Emp_Category'] = only_laveste_df['Antall ansatte_x'].apply(categorize_employees)
    emp_cat_counts = only_laveste_df['Emp_Category'].value_counts()
    for cat, count in emp_cat_counts.items():
        print(f"  {cat}: {count:,}")
    
    print("\n--- Sample records (first 10) ---")
    sample_cols = ['Navn', 'Organisasjonsnummer', 'Nivå enhetsregisteret', 'Antall ansatte_x']
    print(only_laveste_df[sample_cols].head(10).to_string(index=False))

# %% [markdown]
# ## 6. Employee Count Comparison (for matched organizations)

# %%
print("\n" + "="*80)
print("EMPLOYEE COUNT COMPARISON (MATCHED ORGANIZATIONS)")
print("="*80)

# Get matching records
geodata_matched = geodata_df[geodata_df['Organisasjonsnummer'].astype(str).isin(in_both)].copy()
laveste_matched = laveste_nivaa_df[
    laveste_nivaa_df['Organisasjonsnummer'].astype(str).isin(in_both)
].copy()

print(f"\nMatched organizations: {len(in_both):,}")
print(f"Geodata records with employee data: {geodata_matched['Antall ansatte'].notna().sum():,}")
print(f"Laveste Nivå records with employee data: {laveste_matched['Antall ansatte_x'].notna().sum():,}")

# Rename columns before merge for clarity
geodata_for_merge = geodata_matched[['Organisasjonsnummer', 'Bedriftsnavn', 'Antall ansatte']].copy()
geodata_for_merge.rename(columns={'Antall ansatte': 'Antall_ansatte_geodata'}, inplace=True)

laveste_for_merge = laveste_matched[['Organisasjonsnummer', 'Navn', 'Antall ansatte_x', 'Nivå enhetsregisteret']].copy()
laveste_for_merge.rename(columns={'Antall ansatte_x': 'Antall_ansatte_laveste'}, inplace=True)

# Merge for comparison
merged_emp = pd.merge(
    geodata_for_merge,
    laveste_for_merge,
    on='Organisasjonsnummer'
)

print(f"\nMerged comparison records: {len(merged_emp):,}")

# Compare employee counts
merged_emp['Emp_Diff'] = merged_emp['Antall_ansatte_geodata'] - merged_emp['Antall_ansatte_laveste']
merged_emp['Emp_Match'] = merged_emp['Emp_Diff'].abs() < 1

print(f"\nEmployee count matches: {merged_emp['Emp_Match'].sum():,} ({merged_emp['Emp_Match'].sum()/len(merged_emp)*100:.1f}%)")
print(f"Employee count differences: {(~merged_emp['Emp_Match']).sum():,}")

# Show examples of differences
print("\n--- Examples of employee count differences (top 10 by absolute difference) ---")
diff_examples = merged_emp[~merged_emp['Emp_Match']].nlargest(10, merged_emp['Emp_Diff'].abs(), keep='first')
display_cols = ['Bedriftsnavn', 'Organisasjonsnummer', 'Antall_ansatte_geodata', 
                'Antall_ansatte_laveste', 'Emp_Diff', 'Nivå enhetsregisteret']
if len(diff_examples) > 0:
    print(diff_examples[display_cols].to_string(index=False))
else:
    print("  No significant differences found")

# %% [markdown]
# ## 7. Coverage Analysis

# %%
print("\n" + "="*80)
print("COVERAGE ANALYSIS")
print("="*80)

print("\n--- Geodata API Coverage ---")
print(f"Total organizations: {len(geodata_df):,}")
print(f"With coordinates (Lon/Lat): {geodata_df[['Lon', 'Lat']].notna().all(axis=1).sum():,}")
print(f"With employee data: {geodata_df['Antall ansatte'].notna().sum():,}")
print(f"With NACE code: {geodata_df['NACE-kode (numerisk)'].notna().sum():,}")
print(f"With revenue data: {(geodata_df['Omsetning i hele tusen'] > 0).sum():,}")

print("\n--- Enhetsregisteret Laveste Nivå Coverage ---")
print(f"Total organizations: {len(laveste_nivaa_df):,}")
print(f"With employee data (Antall ansatte_x): {laveste_nivaa_df['Antall ansatte_x'].notna().sum():,}")
print(f"With coordinates (from merged data): {laveste_nivaa_df[['Lon', 'Lat']].notna().all(axis=1).sum():,}")

# %% [markdown]
# ## 8. Summary Statistics

# %%
print("\n" + "="*80)
print("SUMMARY STATISTICS")
print("="*80)

summary_stats = {
    'Metric': [
        'Total Organizations',
        'Unique Org Numbers',
        'Organizations in Both',
        'Only in Geodata',
        'Only in Laveste Nivå',
        'Avg Employees',
        'Total Employees'
    ],
    'Geodata API': [
        f"{len(geodata_df):,}",
        f"{len(geodata_orgnr):,}",
        f"{len(in_both):,}",
        f"{len(only_geodata):,}",
        "-",
        f"{geodata_df['Antall ansatte'].mean():.1f}",
        f"{geodata_df['Antall ansatte'].sum():,.0f}"
    ],
    'Laveste Nivå': [
        f"{len(laveste_nivaa_df):,}",
        f"{len(laveste_orgnr):,}",
        f"{len(in_both):,}",
        "-",
        f"{len(only_laveste):,}",
        f"{laveste_nivaa_df['Antall ansatte_x'].mean():.1f}",
        f"{laveste_nivaa_df['Antall ansatte_x'].sum():,.0f}"
    ]
}

summary_df = pd.DataFrame(summary_stats)
print("\n" + summary_df.to_string(index=False))

# %% [markdown]
# ## 9. Key Findings Summary

# %%
print("\n" + "="*80)
print("KEY FINDINGS")
print("="*80)

print(f"""
1. DATASET SIZES:
   - Geodata API contains {len(geodata_df):,} records
   - Enhetsregisteret Laveste Nivå contains {len(laveste_nivaa_df):,} records
   - Laveste Nivå represents the most granular organizational level

2. ORGANIZATION OVERLAP:
   - {len(in_both):,} organizations appear in both datasets
   - {len(only_geodata):,} organizations only in Geodata API ({len(only_geodata)/len(geodata_orgnr)*100:.1f}% of Geodata)
   - {len(only_laveste):,} organizations only in Laveste Nivå ({len(only_laveste)/len(laveste_orgnr)*100:.1f}% of Laveste)

3. ORGANIZATIONAL LEVELS IN LAVESTE NIVÅ:
   Distribution by level:
""")

for level, count in level_dist.items():
    print(f"   - Nivå {level}: {count:,} ({count/len(laveste_nivaa_df)*100:.1f}%)")

print(f"""
4. DATA COMPLETENESS:
   - Geodata has geographic coordinates for all records
   - Geodata includes revenue and operational result data
   - Laveste Nivå has merged data from both Enhetsregisteret and Geodata
   - Employee counts may differ between sources for matched organizations

5. UNIQUE FEATURES:
   Geodata API:
   - Pure Geodata source data
   - More complete geographic coordinates
   - Financial data (revenue, profit)
   
   Enhetsregisteret Laveste Nivå:
   - Represents the most granular organizational level
   - Combines hovedenheter with their lowest-level underenheter
   - Level indicator (Nivå enhetsregisteret: 1, 2, or 3)
   - Already merged with some Geodata information

6. KEY DIFFERENCE:
   - Laveste Nivå focuses on the operational level of organizations
   - Geodata includes all organizational units regardless of hierarchy
   - This explains why overlap may not be 100%
""")

print("\n" + "="*80)
print("ANALYSIS COMPLETE")
print("="*80)
