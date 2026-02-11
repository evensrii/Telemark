# %%
"""
Focused analysis on EMPLOYEE COUNT DIFFERENCES
Comparing Geodata API vs Enhetsregisteret Laveste Nivå

The "laveste nivå" dataset represents the most granular organizational level,
combining hovedenheter with their lowest-level underenheter.
"""

import pandas as pd
import os

# %%
# Load datasets
data_folder = r"c:\Users\eve1509\OneDrive - Telemark fylkeskommune\Github\Telemark\Data\03_Arbeid og næringsliv\02_Næringsliv\Virksomheter"

print("Loading datasets...")
geodata_df = pd.read_csv(os.path.join(data_folder, "geodata_bedrifter_api.csv"))
laveste_nivaa_df = pd.read_csv(os.path.join(data_folder, "enhetsregisteret_laveste_nivaa.csv"))

print(f"✓ Loaded: Geodata={len(geodata_df):,}, Laveste Nivå={len(laveste_nivaa_df):,}")

# %% [markdown]
# ## 1. OVERALL EMPLOYEE TOTALS

# %%
print("\n" + "="*80)
print("OVERALL EMPLOYEE COUNT COMPARISON")
print("="*80)

# Geodata totals
geodata_total_emp = geodata_df['Antall ansatte'].sum()
geodata_with_emp = geodata_df['Antall ansatte'].notna().sum()
geodata_nonzero_emp = (geodata_df['Antall ansatte'] > 0).sum()

print("\n--- GEODATA API ---")
print(f"Total employees: {geodata_total_emp:,.0f}")
print(f"Organizations with employee data: {geodata_with_emp:,} ({geodata_with_emp/len(geodata_df)*100:.1f}%)")
print(f"Organizations with >0 employees: {geodata_nonzero_emp:,} ({geodata_nonzero_emp/len(geodata_df)*100:.1f}%)")
print(f"Average employees per org: {geodata_df['Antall ansatte'].mean():.2f}")
print(f"Median employees: {geodata_df['Antall ansatte'].median():.1f}")

# Laveste Nivå totals (using Antall ansatte_x from Enhetsregisteret)
laveste_total_emp = laveste_nivaa_df['Antall ansatte_x'].sum()
laveste_with_emp = laveste_nivaa_df['Antall ansatte_x'].notna().sum()
laveste_nonzero_emp = (laveste_nivaa_df['Antall ansatte_x'] > 0).sum()

print("\n--- ENHETSREGISTERET LAVESTE NIVÅ ---")
print(f"Total employees: {laveste_total_emp:,.0f}")
print(f"Organizations with employee data: {laveste_with_emp:,} ({laveste_with_emp/len(laveste_nivaa_df)*100:.1f}%)")
print(f"Organizations with >0 employees: {laveste_nonzero_emp:,} ({laveste_nonzero_emp/len(laveste_nivaa_df)*100:.1f}%)")
print(f"Average employees per org: {laveste_nivaa_df['Antall ansatte_x'].mean():.2f}")
print(f"Median employees: {laveste_nivaa_df['Antall ansatte_x'].median():.1f}")

print("\n--- DIFFERENCE ---")
print(f"Geodata vs Laveste Nivå: {geodata_total_emp - laveste_total_emp:,.0f} employees")
print(f"Percentage difference: {((geodata_total_emp - laveste_total_emp) / laveste_total_emp * 100):.1f}%")

# Distribution by level in Laveste Nivå
print("\n--- Distribution by Organizational Level (Laveste Nivå) ---")
level_dist = laveste_nivaa_df['Nivå enhetsregisteret'].value_counts().sort_index()
for level, count in level_dist.items():
    level_emp = laveste_nivaa_df[laveste_nivaa_df['Nivå enhetsregisteret'] == level]['Antall ansatte_x'].sum()
    print(f"  Nivå {level}: {count:,} orgs, {level_emp:,.0f} employees")

# %% [markdown]
# ## 2. EMPLOYEE DISTRIBUTION ANALYSIS

# %%
print("\n" + "="*80)
print("EMPLOYEE SIZE DISTRIBUTION")
print("="*80)

def categorize_size(emp):
    if pd.isna(emp) or emp == 0:
        return '0 (No employees)'
    elif emp <= 4:
        return '1-4'
    elif emp <= 9:
        return '5-9'
    elif emp <= 19:
        return '10-19'
    elif emp <= 49:
        return '20-49'
    elif emp <= 99:
        return '50-99'
    elif emp <= 249:
        return '100-249'
    elif emp <= 499:
        return '250-499'
    else:
        return '500+'

# Apply categorization
geodata_df['Size_Category'] = geodata_df['Antall ansatte'].apply(categorize_size)
laveste_nivaa_df['Size_Category'] = laveste_nivaa_df['Antall ansatte_x'].apply(categorize_size)

# Count by category
print("\n--- GEODATA API ---")
geo_dist = geodata_df['Size_Category'].value_counts().sort_index()
for cat, count in geo_dist.items():
    pct = count / len(geodata_df) * 100
    print(f"{cat:20s}: {count:6,} ({pct:5.1f}%)")

print("\n--- ENHETSREGISTERET LAVESTE NIVÅ ---")
laveste_dist = laveste_nivaa_df['Size_Category'].value_counts().sort_index()
for cat, count in laveste_dist.items():
    pct = count / len(laveste_nivaa_df) * 100
    print(f"{cat:20s}: {count:6,} ({pct:5.1f}%)")

# %% [markdown]
# ## 3. MATCHED ORGANIZATIONS - INDIVIDUAL COMPARISON

# %%
print("\n" + "="*80)
print("MATCHED ORGANIZATIONS - EMPLOYEE COUNT COMPARISON")
print("="*80)

# Find matches
geodata_orgnr = set(geodata_df['Organisasjonsnummer'].dropna().astype(str))
laveste_orgnr = set(laveste_nivaa_df['Organisasjonsnummer'].dropna().astype(str))
matched_orgnr = geodata_orgnr & laveste_orgnr

print(f"\nMatched organizations: {len(matched_orgnr):,}")

# Create merged dataset for comparison
geodata_matched = geodata_df[geodata_df['Organisasjonsnummer'].astype(str).isin(matched_orgnr)].copy()
laveste_matched = laveste_nivaa_df[laveste_nivaa_df['Organisasjonsnummer'].astype(str).isin(matched_orgnr)].copy()

# Rename columns before merge for clarity
geodata_for_merge = geodata_matched[['Organisasjonsnummer', 'Bedriftsnavn', 'Organisasjonsform', 'Antall ansatte']].copy()
geodata_for_merge.rename(columns={'Antall ansatte': 'Antall_ansatte_geo'}, inplace=True)

laveste_for_merge = laveste_matched[['Organisasjonsnummer', 'Navn', 'Antall ansatte_x', 'Nivå enhetsregisteret']].copy()
laveste_for_merge.rename(columns={'Antall ansatte_x': 'Antall_ansatte_laveste'}, inplace=True)

# Merge on org number
comparison_df = pd.merge(
    geodata_for_merge,
    laveste_for_merge,
    on='Organisasjonsnummer'
)

print(f"Merged records for comparison: {len(comparison_df):,}")

# Calculate differences
comparison_df['Emp_Diff'] = comparison_df['Antall_ansatte_geo'] - comparison_df['Antall_ansatte_laveste']
comparison_df['Emp_Diff_Abs'] = comparison_df['Emp_Diff'].abs()
comparison_df['Emp_Diff_Pct'] = (comparison_df['Emp_Diff'] / comparison_df['Antall_ansatte_laveste'] * 100).replace([float('inf'), -float('inf')], 0)

# Matching analysis
exact_match = (comparison_df['Emp_Diff_Abs'] == 0).sum()
small_diff = (comparison_df['Emp_Diff_Abs'] <= 5).sum()
medium_diff = ((comparison_df['Emp_Diff_Abs'] > 5) & (comparison_df['Emp_Diff_Abs'] <= 50)).sum()
large_diff = (comparison_df['Emp_Diff_Abs'] > 50).sum()

print(f"\nEmployee Count Match Analysis:")
print(f"  Exact match (diff = 0): {exact_match:,} ({exact_match/len(comparison_df)*100:.1f}%)")
print(f"  Small difference (≤5): {small_diff:,} ({small_diff/len(comparison_df)*100:.1f}%)")
print(f"  Medium difference (6-50): {medium_diff:,} ({medium_diff/len(comparison_df)*100:.1f}%)")
print(f"  Large difference (>50): {large_diff:,} ({large_diff/len(comparison_df)*100:.1f}%)")

# Summary stats on differences
print(f"\nDifference Statistics (Geodata - Laveste Nivå):")
print(f"  Mean difference: {comparison_df['Emp_Diff'].mean():.2f}")
print(f"  Median difference: {comparison_df['Emp_Diff'].median():.1f}")
print(f"  Std deviation: {comparison_df['Emp_Diff'].std():.2f}")
print(f"  Min difference: {comparison_df['Emp_Diff'].min():.0f}")
print(f"  Max difference: {comparison_df['Emp_Diff'].max():.0f}")

# %% [markdown]
# ## 4. LARGEST DISCREPANCIES (TOP 100)

# %%
print("\n" + "="*80)
print("TOP 100 ORGANIZATIONS WITH LARGEST EMPLOYEE COUNT DIFFERENCES")
print("="*80)

# Where Geodata has MORE employees
print("\n--- GEODATA REPORTS MORE EMPLOYEES (Top 100) ---")
geodata_higher = comparison_df[comparison_df['Emp_Diff'] > 0].nlargest(100, 'Emp_Diff')
if len(geodata_higher) > 0:
    print(f"\n{len(geodata_higher)} cases shown (Total with Geodata > Laveste Nivå: {(comparison_df['Emp_Diff'] > 0).sum():,})\n")
    display_cols = ['Bedriftsnavn', 'Organisasjonsnummer', 'Antall_ansatte_geo', 'Antall_ansatte_laveste', 'Emp_Diff', 'Nivå enhetsregisteret']
    print(geodata_higher[display_cols].to_string(index=False))
else:
    print("No cases where Geodata reports more employees")

# Where Laveste Nivå has MORE employees
print("\n\n--- LAVESTE NIVÅ REPORTS MORE EMPLOYEES (Top 100) ---")
laveste_higher = comparison_df[comparison_df['Emp_Diff'] < 0].nsmallest(100, 'Emp_Diff')
if len(laveste_higher) > 0:
    print(f"\n{len(laveste_higher)} cases shown (Total with Laveste Nivå > Geodata: {(comparison_df['Emp_Diff'] < 0).sum():,})\n")
    display_cols = ['Bedriftsnavn', 'Organisasjonsnummer', 'Antall_ansatte_geo', 'Antall_ansatte_laveste', 'Emp_Diff', 'Nivå enhetsregisteret']
    print(laveste_higher[display_cols].to_string(index=False))
else:
    print("No cases where Laveste Nivå reports more employees")

# Top 100 by absolute difference (regardless of direction)
print("\n\n--- TOP 100 BY ABSOLUTE DIFFERENCE (Regardless of Direction) ---")
top_100_abs = comparison_df.nlargest(100, 'Emp_Diff_Abs')
print(f"\n{len(top_100_abs)} organizations with largest absolute differences\n")
display_cols = ['Bedriftsnavn', 'Organisasjonsnummer', 'Antall_ansatte_geo', 'Antall_ansatte_laveste', 'Emp_Diff', 'Emp_Diff_Abs', 'Nivå enhetsregisteret']
print(top_100_abs[display_cols].to_string(index=False))

# %% [markdown]
# ## 5. BREAKDOWN BY ORGANIZATION LEVEL

# %%
print("\n" + "="*80)
print("EMPLOYEE DIFFERENCES BY ORGANIZATIONAL LEVEL")
print("="*80)

# Group by organizational level
print("\n--- By Organizational Level (Nivå enhetsregisteret) ---")
by_level = comparison_df.groupby('Nivå enhetsregisteret').agg({
    'Organisasjonsnummer': 'count',
    'Antall_ansatte_geo': 'sum',
    'Antall_ansatte_laveste': 'sum',
    'Emp_Diff': ['mean', 'sum'],
    'Emp_Diff_Abs': 'mean'
}).round(2)

by_level.columns = ['Count', 'Total_Emp_Geo', 'Total_Emp_Laveste', 'Avg_Diff', 'Total_Diff', 'Avg_Abs_Diff']
print(by_level)

# %% [markdown]
# ## 6. SUMMARY

# %%
print("\n" + "="*80)
print("EMPLOYEE COUNT SUMMARY")
print("="*80)

print(f"""
OVERALL TOTALS:
- Geodata total employees: {geodata_total_emp:,.0f}
- Laveste Nivå total employees: {laveste_total_emp:,.0f}
- Difference: {geodata_total_emp - laveste_total_emp:,.0f} ({((geodata_total_emp - laveste_total_emp) / laveste_total_emp * 100):.1f}%)

MATCHED ORGANIZATIONS ({len(comparison_df):,}):
- Exact matches: {exact_match:,} ({exact_match/len(comparison_df)*100:.1f}%)
- Small differences (≤5): {small_diff:,} ({small_diff/len(comparison_df)*100:.1f}%)
- Medium differences (6-50): {medium_diff:,} ({medium_diff/len(comparison_df)*100:.1f}%)
- Large differences (>50): {large_diff:,} ({large_diff/len(comparison_df)*100:.1f}%)

- Average difference: {comparison_df['Emp_Diff'].mean():.2f} employees
- Cases where Geodata > Laveste Nivå: {(comparison_df['Emp_Diff'] > 0).sum():,}
- Cases where Laveste Nivå > Geodata: {(comparison_df['Emp_Diff'] < 0).sum():,}

ORGANIZATIONAL LEVELS IN LAVESTE NIVÅ:
""")

for level, count in level_dist.items():
    level_emp = laveste_nivaa_df[laveste_nivaa_df['Nivå enhetsregisteret'] == level]['Antall ansatte_x'].sum()
    print(f"- Nivå {level}: {count:,} organizations, {level_emp:,.0f} employees")

print(f"""
INTERPRETATION:
The differences in employee counts between Geodata and Laveste Nivå likely stem from:

1. DATASET STRUCTURE:
   - Geodata includes all organizational units
   - Laveste Nivå focuses on the most granular operational level
   - Laveste Nivå combines hovedenheter with their lowest-level underenheter

2. DATA COLLECTION:
   - Different data collection dates/update frequencies
   - Different reporting periods (Geodata vs Enhetsregisteret)
   - Possible aggregation differences in Laveste Nivå

3. ORGANIZATIONAL HIERARCHY:
   - Nivå 1: Hovedenheter without underenheter (standalone organizations)
   - Nivå 2: Intermediate level underenheter
   - Nivå 3: Lowest level underenheter
   - Employee counts may be attributed differently at each level

4. DATA QUALITY:
   - Variations in reporting completeness
   - Possible double-counting or under-counting in certain cases
   - Different handling of temporary/seasonal employees
""")

# %%
# Optional: Export detailed comparison
# comparison_df.to_csv(os.path.join(data_folder, 'employee_comparison_geo_vs_laveste_detailed.csv'), index=False, sep=';')
# print("Detailed comparison exported to: employee_comparison_geo_vs_laveste_detailed.csv")
