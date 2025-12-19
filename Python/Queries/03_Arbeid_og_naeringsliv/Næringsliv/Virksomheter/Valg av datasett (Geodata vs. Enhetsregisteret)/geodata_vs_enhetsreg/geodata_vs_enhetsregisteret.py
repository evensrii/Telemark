"""
Script to compare enterprise data from two different sources:
1. Enhetsregisteret (enhetsregisteret_laveste_nivaa.csv)
2. Geodata (geodata_sql_filtrerte_virksomheter.csv)

The script analyzes:
- Enterprises unique to each dataset
- Employee count differences for shared enterprises
- Total employee counts in each dataset
"""

import pandas as pd
import numpy as np
import os

# File paths
data_base_path = r"c:\Users\eve1509\OneDrive - Telemark fylkeskommune\Github\Telemark\Data\03_Arbeid og næringsliv\02_Næringsliv\Virksomheter"
enhetsreg_file = os.path.join(data_base_path, "enhetsregisteret_laveste_nivaa.csv")
geodata_file = os.path.join(data_base_path, "geodata_sql_filtrerte_virksomheter.csv")
output_folder = os.path.join(data_base_path, "geodata_vs_enhetsregisteret", "output")

# Create output folder if it doesn't exist
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

print("="*80)
print("COMPARISON OF ENTERPRISE DATA: ENHETSREGISTERET VS GEODATA")
print("="*80)
print("\n")

# =============================================================================
# 1. LOAD DATA
# =============================================================================
print("Loading data...")
print("-" * 80)

# Load enhetsregisteret data
df_enhetsreg = pd.read_csv(enhetsreg_file, sep=',', encoding='utf-8', low_memory=False)
print(f"Enhetsregisteret: {len(df_enhetsreg):,} rows loaded")

# Load geodata
df_geodata = pd.read_csv(geodata_file, sep=',', encoding='utf-8', low_memory=False)
print(f"Geodata:          {len(df_geodata):,} rows loaded")
print(f"\nDifference:       {len(df_geodata) - len(df_enhetsreg):,} more enterprises in Geodata")
print("\n")

# =============================================================================
# 2. PREPARE DATA FOR COMPARISON
# =============================================================================
print("Preparing data for comparison...")
print("-" * 80)

# Create clean datasets with relevant columns
# Enhetsregisteret - use its own employee count for comparison
df_enh_clean = df_enhetsreg[['Organisasjonsnummer', 'Navn', 'Antall ansatte Enhetsreg']].copy()
df_enh_clean.rename(columns={
    'Organisasjonsnummer': 'orgnr',
    'Navn': 'navn_enhetsreg',
    'Antall ansatte Enhetsreg': 'ansatte_enhetsreg'
}, inplace=True)

# Convert org numbers to string, remove decimal points if present, and remove whitespace
df_enh_clean['orgnr'] = df_enh_clean['orgnr'].astype(str).str.replace('.0', '', regex=False).str.strip()

# Convert employee counts to integers
df_enh_clean['ansatte_enhetsreg'] = pd.to_numeric(df_enh_clean['ansatte_enhetsreg'], errors='coerce').fillna(0).astype(int)

# Geodata
df_geo_clean = df_geodata[['firorgnr', 'Bedriftsnavn', 'Antall ansatte']].copy()
df_geo_clean.rename(columns={
    'firorgnr': 'orgnr',
    'Bedriftsnavn': 'navn_geodata',
    'Antall ansatte': 'ansatte_geodata'
}, inplace=True)

# Convert org numbers to string, remove decimal points if present, and remove whitespace
df_geo_clean['orgnr'] = df_geo_clean['orgnr'].astype(str).str.replace('.0', '', regex=False).str.strip()

# Convert employee counts to integers
df_geo_clean['ansatte_geodata'] = pd.to_numeric(df_geo_clean['ansatte_geodata'], errors='coerce').fillna(0).astype(int)

print(f"Enhetsregisteret unique org numbers: {df_enh_clean['orgnr'].nunique():,}")
print(f"Geodata unique org numbers:          {df_geo_clean['orgnr'].nunique():,}")

# Data quality checks
print("\nDATA QUALITY CHECKS:")
print("-" * 80)
print("Enhetsregisteret:")
print(f"  - Total rows: {len(df_enh_clean):,}")
print(f"  - Unique org numbers: {df_enh_clean['orgnr'].nunique():,}")
print(f"  - Duplicates: {len(df_enh_clean) - df_enh_clean['orgnr'].nunique():,}")
print(f"  - Missing org numbers: {df_enh_clean['orgnr'].isna().sum():,}")
print(f"  - Org numbers containing 'nan': {df_enh_clean['orgnr'].str.contains('nan', case=False).sum():,}")

print("\nGeodata:")
print(f"  - Total rows: {len(df_geo_clean):,}")
print(f"  - Unique org numbers: {df_geo_clean['orgnr'].nunique():,}")
print(f"  - Duplicates: {len(df_geo_clean) - df_geo_clean['orgnr'].nunique():,}")
print(f"  - Missing org numbers: {df_geo_clean['orgnr'].isna().sum():,}")
print(f"  - Org numbers containing 'nan': {df_geo_clean['orgnr'].str.contains('nan', case=False).sum():,}")

# Sample org numbers from each dataset
print("\nSample org numbers from Enhetsregisteret:")
print(df_enh_clean['orgnr'].head(10).tolist())
print("\nSample org numbers from Geodata:")
print(df_geo_clean['orgnr'].head(10).tolist())
print("\n")

# =============================================================================
# 3. IDENTIFY UNIQUE AND SHARED ENTERPRISES
# =============================================================================
print("Identifying unique and shared enterprises...")
print("-" * 80)

enh_orgs = set(df_enh_clean['orgnr'])
geo_orgs = set(df_geo_clean['orgnr'])

# Enterprises only in enhetsregisteret
only_in_enh = enh_orgs - geo_orgs
only_in_geo = geo_orgs - enh_orgs
in_both = enh_orgs & geo_orgs

print(f"Enterprises only in Enhetsregisteret: {len(only_in_enh):,}")
print(f"Enterprises only in Geodata:          {len(only_in_geo):,}")
print(f"Enterprises in both datasets:         {len(in_both):,}")
print(f"\nTotal unique enterprises:             {len(enh_orgs | geo_orgs):,}")

# Show samples of unique enterprises
print("\nSample enterprises ONLY in Enhetsregisteret (first 10):")
sample_only_enh = list(only_in_enh)[:10]
for orgnr in sample_only_enh:
    row = df_enh_clean[df_enh_clean['orgnr'] == orgnr].iloc[0]
    print(f"  {orgnr}: {row['navn_enhetsreg']} ({row['ansatte_enhetsreg']} ansatte)")

print("\nSample enterprises ONLY in Geodata (first 10):")
sample_only_geo = list(only_in_geo)[:10]
for orgnr in sample_only_geo:
    row = df_geo_clean[df_geo_clean['orgnr'] == orgnr].iloc[0]
    print(f"  {orgnr}: {row['navn_geodata']} ({row['ansatte_geodata']} ansatte)")
print("\n")

# =============================================================================
# 4. ANALYZE EMPLOYEE COUNTS
# =============================================================================
print("Analyzing employee counts...")
print("-" * 80)

# Calculate total employees in each dataset
total_enh_employees = df_enh_clean['ansatte_enhetsreg'].sum()
total_geo_employees = df_geo_clean['ansatte_geodata'].sum()

print(f"Total employees in Enhetsregisteret: {total_enh_employees:,.0f}")
print(f"Total employees in Geodata:          {total_geo_employees:,.0f}")
print(f"Difference:                          {total_geo_employees - total_enh_employees:,.0f}")
print(f"Percentage difference:               {((total_geo_employees - total_enh_employees) / total_enh_employees * 100):.2f}%")
print("\n")

# =============================================================================
# 5. COMPARE SHARED ENTERPRISES
# =============================================================================
print("Comparing enterprises in both datasets...")
print("-" * 80)

# Merge datasets on org number
df_comparison = pd.merge(
    df_enh_clean[['orgnr', 'navn_enhetsreg', 'ansatte_enhetsreg']],
    df_geo_clean[['orgnr', 'navn_geodata', 'ansatte_geodata']],
    on='orgnr',
    how='inner'
)

# Calculate differences
df_comparison['ansatte_diff'] = df_comparison['ansatte_geodata'] - df_comparison['ansatte_enhetsreg']
df_comparison['ansatte_pct_diff'] = ((df_comparison['ansatte_geodata'] - df_comparison['ansatte_enhetsreg']) / 
                                     (df_comparison['ansatte_enhetsreg'] + 0.01)) * 100

# Count differences
enterprises_with_diff = (df_comparison['ansatte_diff'] != 0).sum()
enterprises_higher_in_geo = (df_comparison['ansatte_diff'] > 0).sum()
enterprises_lower_in_geo = (df_comparison['ansatte_diff'] < 0).sum()
enterprises_same = (df_comparison['ansatte_diff'] == 0).sum()

print(f"Enterprises with same employee count:      {enterprises_same:,}")
print(f"Enterprises with different employee count:  {enterprises_with_diff:,}")
print(f"  - Higher count in Geodata:                {enterprises_higher_in_geo:,}")
print(f"  - Lower count in Geodata:                 {enterprises_lower_in_geo:,}")
print("\n")

# Calculate employee differences for shared enterprises
total_enh_shared = df_comparison['ansatte_enhetsreg'].sum()
total_geo_shared = df_comparison['ansatte_geodata'].sum()

print(f"Employees in shared enterprises (Enhetsregisteret): {total_enh_shared:,.0f}")
print(f"Employees in shared enterprises (Geodata):          {total_geo_shared:,.0f}")
print(f"Difference in shared enterprises:                   {total_geo_shared - total_enh_shared:,.0f}")
print("\n")

# =============================================================================
# 6. ANALYZE ENTERPRISES UNIQUE TO EACH DATASET
# =============================================================================
print("Analyzing enterprises unique to each dataset...")
print("-" * 80)

# Enterprises only in enhetsregisteret
df_only_enh = df_enh_clean[df_enh_clean['orgnr'].isin(only_in_enh)].copy()
total_employees_only_enh = df_only_enh['ansatte_enhetsreg'].sum()

print(f"\nEnterprises ONLY in Enhetsregisteret:")
print(f"  - Count: {len(df_only_enh):,}")
print(f"  - Total employees: {total_employees_only_enh:,.0f}")
print(f"  - Average employees per enterprise: {total_employees_only_enh / len(df_only_enh):.1f}")

# Enterprises only in geodata
df_only_geo = df_geo_clean[df_geo_clean['orgnr'].isin(only_in_geo)].copy()
total_employees_only_geo = df_only_geo['ansatte_geodata'].sum()

print(f"\nEnterprises ONLY in Geodata:")
print(f"  - Count: {len(df_only_geo):,}")
print(f"  - Total employees: {total_employees_only_geo:,.0f}")
print(f"  - Average employees per enterprise: {total_employees_only_geo / len(df_only_geo):.1f}")
print("\n")

# =============================================================================
# 7. LARGEST DISCREPANCIES
# =============================================================================
print("Largest employee count discrepancies (Geodata - Enhetsregisteret)...")
print("-" * 80)

# Sort by absolute difference
df_comparison['ansatte_abs_diff'] = df_comparison['ansatte_diff'].abs()
df_top_diff = df_comparison.nlargest(20, 'ansatte_abs_diff')

print("\nTop 20 enterprises with largest absolute differences:")
print(f"{'Org.nr.':<12} {'Name':<40} {'Enh.reg':<10} {'Geodata':<10} {'Diff':<10} {'Abs.Diff':<10}")
print("-" * 100)
for idx, row in df_top_diff.iterrows():
    name = (row['navn_geodata'] if pd.notna(row['navn_geodata']) else row['navn_enhetsreg'])[:38]
    print(f"{row['orgnr']:<12} {name:<40} {row['ansatte_enhetsreg']:<10} "
          f"{row['ansatte_geodata']:<10} {row['ansatte_diff']:<10} {row['ansatte_abs_diff']:<10.0f}")

print("\n")

# =============================================================================
# 8. SAVE DETAILED COMPARISON FILES
# =============================================================================
print("Saving detailed comparison files...")
print("-" * 80)

# 1. Enterprises only in enhetsregisteret
df_only_enh_output = df_only_enh[['orgnr', 'navn_enhetsreg', 'ansatte_enhetsreg']].copy()
df_only_enh_output.columns = ['Organisasjonsnummer', 'Navn', 'Antall_ansatte']
df_only_enh_output = df_only_enh_output.sort_values('Antall_ansatte', ascending=False)
output_file_1 = os.path.join(output_folder, "only_in_enhetsregisteret.csv")
df_only_enh_output.to_csv(output_file_1, index=False, encoding='utf-8')
print(f"✓ Saved: only_in_enhetsregisteret.csv ({len(df_only_enh_output):,} rows)")

# 2. Enterprises only in geodata
df_only_geo_output = df_only_geo[['orgnr', 'navn_geodata', 'ansatte_geodata']].copy()
df_only_geo_output.columns = ['Organisasjonsnummer', 'Navn', 'Antall_ansatte']
df_only_geo_output = df_only_geo_output.sort_values('Antall_ansatte', ascending=False)
output_file_2 = os.path.join(output_folder, "only_in_geodata.csv")
df_only_geo_output.to_csv(output_file_2, index=False, encoding='utf-8')
print(f"✓ Saved: only_in_geodata.csv ({len(df_only_geo_output):,} rows)")

# 3. Enterprises in both with differences
df_comparison_output = df_comparison[['orgnr', 'navn_enhetsreg', 'navn_geodata', 
                                      'ansatte_enhetsreg', 'ansatte_geodata', 
                                      'ansatte_diff', 'ansatte_pct_diff']].copy()
df_comparison_output.columns = ['Organisasjonsnummer', 'Navn_Enhetsreg', 'Navn_Geodata',
                                'Ansatte_Enhetsreg', 'Ansatte_Geodata', 'Differanse', 'Differanse_pst']
df_comparison_output = df_comparison_output.sort_values('Differanse', ascending=False, key=abs)
output_file_3 = os.path.join(output_folder, "comparison_shared_enterprises.csv")
df_comparison_output.to_csv(output_file_3, index=False, encoding='utf-8')
print(f"✓ Saved: comparison_shared_enterprises.csv ({len(df_comparison_output):,} rows)")

# 4. Summary statistics
summary_data = {
    'Metric': [
        'Total enterprises - Enhetsregisteret',
        'Total enterprises - Geodata',
        'Difference',
        '',
        'Enterprises only in Enhetsregisteret',
        'Enterprises only in Geodata',
        'Enterprises in both datasets',
        '',
        'Total employees - Enhetsregisteret',
        'Total employees - Geodata',
        'Employee difference',
        '',
        'Employees in shared enterprises - Enhetsregisteret',
        'Employees in shared enterprises - Geodata',
        'Employee difference (shared)',
        '',
        'Employees in unique enterprises - Enhetsregisteret',
        'Employees in unique enterprises - Geodata',
        'Employee difference (unique)'
    ],
    'Value': [
        len(df_enhetsreg),
        len(df_geodata),
        len(df_geodata) - len(df_enhetsreg),
        '',
        len(only_in_enh),
        len(only_in_geo),
        len(in_both),
        '',
        total_enh_employees,
        total_geo_employees,
        total_geo_employees - total_enh_employees,
        '',
        total_enh_shared,
        total_geo_shared,
        total_geo_shared - total_enh_shared,
        '',
        total_employees_only_enh,
        total_employees_only_geo,
        total_employees_only_geo - total_employees_only_enh
    ]
}

df_summary = pd.DataFrame(summary_data)
output_file_4 = os.path.join(output_folder, "comparison_summary.csv")
df_summary.to_csv(output_file_4, index=False, encoding='utf-8')
print(f"✓ Saved: comparison_summary.csv")

print("\n")
print("="*80)
print("ANALYSIS COMPLETE")
print("="*80)
print("\nOutput files saved to:")
print(f"  {output_folder}")
print("\nFiles created:")
print("  1. only_in_enhetsregisteret.csv      - Enterprises missing from Geodata")
print("  2. only_in_geodata.csv                - Enterprises missing from Enhetsregisteret")
print("  3. comparison_shared_enterprises.csv  - Detailed comparison of shared enterprises")
print("  4. comparison_summary.csv             - Summary statistics")
