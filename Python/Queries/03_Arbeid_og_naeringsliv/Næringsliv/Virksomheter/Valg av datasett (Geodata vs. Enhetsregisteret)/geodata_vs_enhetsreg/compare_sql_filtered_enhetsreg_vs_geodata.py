# %% [markdown]
# # Compare SQL Filtered Enhetsregisteret vs Geodata
# 
# This script applies SQL filtering to enhetsregisteret_kombinert_clean.csv
# and compares the result with geodata_sql_filtrerte_virksomheter.csv

# %%
import pandas as pd
import numpy as np
import os

# %%
# File paths
base_path = r'c:\Users\eve1509\OneDrive - Telemark fylkeskommune\Github\Telemark\Data\03_Arbeid og næringsliv\02_Næringsliv\Virksomheter'

# Load datasets
print("Loading datasets...")
df_enhetsreg_combined = pd.read_csv(f'{base_path}\\enhetsregisteret_kombinert_clean.csv', dtype={'Org. nr.': str, 'Overordnet enhet': str})
df_geodata_filtered = pd.read_csv(f'{base_path}\\geodata_sql_filtrerte_virksomheter.csv', dtype={'Org. nr.': str, 'Overordnet enhet': str})

print(f"Enhetsregisteret combined: {len(df_enhetsreg_combined):,} rows")
print(f"Geodata SQL filtered: {len(df_geodata_filtered):,} rows")

# %%
# Apply SQL filtering to Enhetsregisteret combined dataset
print("\n" + "="*80)
print("APPLYING SQL FILTERING TO ENHETSREGISTERET COMBINED")
print("="*80)

# Standardize organization numbers
df_enhetsreg_combined['Org. nr._str'] = pd.to_numeric(df_enhetsreg_combined['Org. nr.'], errors='coerce').apply(
    lambda x: str(int(x)) if pd.notna(x) else ''
)
df_enhetsreg_combined['Overordnet enhet_str'] = pd.to_numeric(df_enhetsreg_combined['Overordnet enhet'], errors='coerce').apply(
    lambda x: str(int(x)) if pd.notna(x) else ''
)

# Get all organization numbers that are parents (have children)
parent_orgs = set(df_enhetsreg_combined[df_enhetsreg_combined['Overordnet enhet_str'] != '']['Overordnet enhet_str'].unique())

print(f"Organizations that are parents: {len(parent_orgs):,}")

# Filter out parent organizations
df_enhetsreg_filtered = df_enhetsreg_combined[~df_enhetsreg_combined['Org. nr._str'].isin(parent_orgs)].copy()

print(f"After removing parent organizations: {len(df_enhetsreg_filtered):,} rows")
print(f"Removed: {len(df_enhetsreg_combined) - len(df_enhetsreg_filtered):,} parent organizations")

# %%
# Employee statistics for filtered Enhetsregisteret
df_enhetsreg_filtered['Employees'] = pd.to_numeric(df_enhetsreg_filtered['Antall ansatte'], errors='coerce').fillna(0)
df_enhetsreg_with_emp = df_enhetsreg_filtered[df_enhetsreg_filtered['Employees'] > 0].copy()

print("\n" + "="*80)
print("ENHETSREGISTERET FILTERED STATISTICS")
print("="*80)
print(f"Total enterprises: {len(df_enhetsreg_filtered):,}")
print(f"Enterprises with employees: {len(df_enhetsreg_with_emp):,}")
print(f"Total employees: {df_enhetsreg_with_emp['Employees'].sum():,.0f}")

# %%
# Prepare Geodata dataset
df_geodata_filtered['Org. nr._str'] = pd.to_numeric(df_geodata_filtered['Org. nr.'], errors='coerce').apply(
    lambda x: str(int(x)) if pd.notna(x) else ''
)
df_geodata_filtered['Employees'] = pd.to_numeric(df_geodata_filtered['Antall ansatte'], errors='coerce').fillna(0)
df_geodata_with_emp = df_geodata_filtered[df_geodata_filtered['Employees'] > 0].copy()

print("\n" + "="*80)
print("GEODATA FILTERED STATISTICS")
print("="*80)
print(f"Total enterprises: {len(df_geodata_filtered):,}")
print(f"Enterprises with employees: {len(df_geodata_with_emp):,}")
print(f"Total employees: {df_geodata_with_emp['Employees'].sum():,.0f}")

# %%
# Compare the two filtered datasets
print("\n" + "="*80)
print("COMPARISON: ENHETSREGISTERET VS GEODATA (BOTH SQL FILTERED)")
print("="*80)

enhetsreg_orgs = set(df_enhetsreg_with_emp['Org. nr._str'].values)
geodata_orgs = set(df_geodata_with_emp['Org. nr._str'].values)

only_in_enhetsreg = enhetsreg_orgs - geodata_orgs
only_in_geodata = geodata_orgs - enhetsreg_orgs
in_both = enhetsreg_orgs & geodata_orgs

print(f"\nEnterprises in both datasets: {len(in_both):,}")
print(f"Only in Enhetsregisteret: {len(only_in_enhetsreg):,}")
print(f"Only in Geodata: {len(only_in_geodata):,}")

# Employee counts comparison
enhetsreg_total = df_enhetsreg_with_emp['Employees'].sum()
geodata_total = df_geodata_with_emp['Employees'].sum()
difference = enhetsreg_total - geodata_total

print(f"\nEmployee counts:")
print(f"  Enhetsregisteret: {enhetsreg_total:,.0f}")
print(f"  Geodata: {geodata_total:,.0f}")
print(f"  Difference: {difference:+,.0f} ({difference/geodata_total*100:+.1f}%)")

# %%
# Analyze enterprises only in Enhetsregisteret
print("\n" + "="*80)
print("ENTERPRISES ONLY IN ENHETSREGISTERET (NOT IN GEODATA)")
print("="*80)

df_only_enhetsreg = df_enhetsreg_with_emp[df_enhetsreg_with_emp['Org. nr._str'].isin(only_in_enhetsreg)].copy()
print(f"Count: {len(df_only_enhetsreg):,}")
print(f"Total employees: {df_only_enhetsreg['Employees'].sum():,.0f}")

# Top 20 by employee count
top_enhetsreg = df_only_enhetsreg.nlargest(20, 'Employees')[['Navn', 'Org. nr.', 'Employees', 'Organisasjonsform']].copy()
print("\nTop 20 by employee count:")
print(top_enhetsreg.to_string(index=False))

# Save to file
output_path = f'{base_path}\\geodata_vs_enhetsreg'
os.makedirs(output_path, exist_ok=True)
df_only_enhetsreg.to_csv(f'{output_path}\\only_in_enhetsreg_sql_filtered.csv', index=False)
print(f"\nSaved to: only_in_enhetsreg_sql_filtered.csv")

# %%
# Analyze enterprises only in Geodata
print("\n" + "="*80)
print("ENTERPRISES ONLY IN GEODATA (NOT IN ENHETSREGISTERET)")
print("="*80)

df_only_geodata = df_geodata_with_emp[df_geodata_with_emp['Org. nr._str'].isin(only_in_geodata)].copy()
print(f"Count: {len(df_only_geodata):,}")
print(f"Total employees: {df_only_geodata['Employees'].sum():,.0f}")

# Top 20 by employee count
top_geodata = df_only_geodata.nlargest(20, 'Employees')[['Navn', 'Org. nr.', 'Employees', 'Bedriftsstatus']].copy()
print("\nTop 20 by employee count:")
print(top_geodata.to_string(index=False))

# Save to file
df_only_geodata.to_csv(f'{output_path}\\only_in_geodata_sql_filtered.csv', index=False)
print(f"\nSaved to: only_in_geodata_sql_filtered.csv")

# %%
# Check for employee count differences in overlapping enterprises
print("\n" + "="*80)
print("EMPLOYEE COUNT DIFFERENCES IN OVERLAPPING ENTERPRISES")
print("="*80)

# Merge on organization number
df_comparison = df_enhetsreg_with_emp[df_enhetsreg_with_emp['Org. nr._str'].isin(in_both)].merge(
    df_geodata_with_emp[df_geodata_with_emp['Org. nr._str'].isin(in_both)],
    on='Org. nr._str',
    how='inner',
    suffixes=(' ER', ' GD')
)

# Calculate differences
df_comparison['Employee_diff'] = df_comparison['Employees ER'] - df_comparison['Employees GD']
df_comparison['Employee_diff_pct'] = (df_comparison['Employee_diff'] / df_comparison['Employees GD'] * 100).abs()

# Count enterprises with differences
has_difference = df_comparison[df_comparison['Employee_diff'] != 0]
print(f"\nEnterprises with different employee counts: {len(has_difference):,} out of {len(df_comparison):,}")

if len(has_difference) > 0:
    print(f"Total employee difference: {has_difference['Employee_diff'].sum():+,.0f}")
    
    # Top differences
    top_diff = has_difference.nlargest(10, 'Employee_diff_pct')[['Navn ER', 'Org. nr._str', 'Employees ER', 'Employees GD', 'Employee_diff']].copy()
    print("\nTop 10 by percentage difference:")
    print(top_diff.to_string(index=False))

# %%
# Summary statistics by organization form
print("\n" + "="*80)
print("BREAKDOWN BY ORGANIZATION FORM")
print("="*80)

print("\nEnhetsregisteret:")
org_form_enhetsreg = df_enhetsreg_with_emp['Organisasjonsform'].value_counts().head(10)
print(org_form_enhetsreg)

print("\nGeodatata (Bedriftsstatus):")
if 'Bedriftsstatus' in df_geodata_with_emp.columns:
    bedrift_status = df_geodata_with_emp['Bedriftsstatus'].value_counts().head(10)
    print(bedrift_status)

# %%
# FINAL SUMMARY
print("\n" + "="*80)
print("FINAL SUMMARY")
print("="*80)

print(f"""
AFTER SQL FILTERING (removing parent organizations):

ENHETSREGISTERET:
  - Total enterprises with employees: {len(df_enhetsreg_with_emp):,}
  - Total employees: {enhetsreg_total:,.0f}

GEODATA:
  - Total enterprises with employees: {len(df_geodata_with_emp):,}
  - Total employees: {geodata_total:,.0f}

OVERLAP:
  - In both datasets: {len(in_both):,} enterprises
  - Only in Enhetsregisteret: {len(only_in_enhetsreg):,} ({df_only_enhetsreg['Employees'].sum():,.0f} employees)
  - Only in Geodata: {len(only_in_geodata):,} ({df_only_geodata['Employees'].sum():,.0f} employees)

EMPLOYEE COUNT DIFFERENCE: {difference:+,.0f} ({difference/geodata_total*100:+.1f}%)
  - Enhetsregisteret has MORE employees than Geodata

POSSIBLE REASONS FOR DIFFERENCES:
  1. Different data sources and update frequencies
  2. Geodata may filter out certain organization types (e.g., government entities)
  3. Different handling of zero-employee enterprises
  4. Different coverage of specific sectors/industries
""")

print("\nDone!")
