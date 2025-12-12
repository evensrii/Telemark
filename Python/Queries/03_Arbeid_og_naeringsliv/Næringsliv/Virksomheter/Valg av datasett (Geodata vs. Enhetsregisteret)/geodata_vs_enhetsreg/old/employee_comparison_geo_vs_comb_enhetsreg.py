# %%
"""
Focused analysis on EMPLOYEE COUNT DIFFERENCES
Comparing Geodata API vs Enhetsregisteret (Hovedenheter + Underenheter)
"""

import pandas as pd
import os

# %%
# Load datasets
data_folder = r"c:\Users\eve1509\OneDrive - Telemark fylkeskommune\Github\Telemark\Data\03_Arbeid og næringsliv\02_Næringsliv\Virksomheter"

print("Loading datasets...")
geodata_df = pd.read_csv(os.path.join(data_folder, "geodata_bedrifter_api.csv"))
hovedenheter_df = pd.read_csv(os.path.join(data_folder, "enhetsregisteret_hovedenheter.csv"))
underenheter_df = pd.read_csv(os.path.join(data_folder, "enhetsregisteret_underenheter.csv"))

print(f"✓ Loaded: Geodata={len(geodata_df):,}, Hovedenheter={len(hovedenheter_df):,}, Underenheter={len(underenheter_df):,}")

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

# Hovedenheter totals
hoved_total_emp = hovedenheter_df['Antall ansatte'].sum()
hoved_with_emp = hovedenheter_df['Antall ansatte'].notna().sum()
hoved_nonzero_emp = (hovedenheter_df['Antall ansatte'] > 0).sum()

print("\n--- HOVEDENHETER ---")
print(f"Total employees: {hoved_total_emp:,.0f}")
print(f"Organizations with employee data: {hoved_with_emp:,} ({hoved_with_emp/len(hovedenheter_df)*100:.1f}%)")
print(f"Organizations with >0 employees: {hoved_nonzero_emp:,} ({hoved_nonzero_emp/len(hovedenheter_df)*100:.1f}%)")
print(f"Average employees per org: {hovedenheter_df['Antall ansatte'].mean():.2f}")
print(f"Median employees: {hovedenheter_df['Antall ansatte'].median():.1f}")

# Underenheter totals
under_total_emp = underenheter_df['Antall ansatte'].sum()
under_with_emp = underenheter_df['Antall ansatte'].notna().sum()
under_nonzero_emp = (underenheter_df['Antall ansatte'] > 0).sum()

print("\n--- UNDERENHETER ---")
print(f"Total employees: {under_total_emp:,.0f}")
print(f"Organizations with employee data: {under_with_emp:,} ({under_with_emp/len(underenheter_df)*100:.1f}%)")
print(f"Organizations with >0 employees: {under_nonzero_emp:,} ({under_nonzero_emp/len(underenheter_df)*100:.1f}%)")
print(f"Average employees per org: {underenheter_df['Antall ansatte'].mean():.2f}")
print(f"Median employees: {underenheter_df['Antall ansatte'].median():.1f}")

# Combined Enhetsregisteret
enhetsreg_total = hoved_total_emp + under_total_emp
print("\n--- COMBINED ENHETSREGISTERET ---")
print(f"Total employees (Hoved + Under): {enhetsreg_total:,.0f}")
print(f"\nDIFFERENCE:")
print(f"Geodata vs Enhetsregisteret: {geodata_total_emp - enhetsreg_total:,.0f} employees")
print(f"Percentage difference: {((geodata_total_emp - enhetsreg_total) / enhetsreg_total * 100):.1f}%")

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
hovedenheter_df['Size_Category'] = hovedenheter_df['Antall ansatte'].apply(categorize_size)
underenheter_df['Size_Category'] = underenheter_df['Antall ansatte'].apply(categorize_size)

# Count by category
print("\n--- GEODATA API ---")
geo_dist = geodata_df['Size_Category'].value_counts().sort_index()
for cat, count in geo_dist.items():
    pct = count / len(geodata_df) * 100
    print(f"{cat:20s}: {count:6,} ({pct:5.1f}%)")

print("\n--- HOVEDENHETER ---")
hoved_dist = hovedenheter_df['Size_Category'].value_counts().sort_index()
for cat, count in hoved_dist.items():
    pct = count / len(hovedenheter_df) * 100
    print(f"{cat:20s}: {count:6,} ({pct:5.1f}%)")

print("\n--- UNDERENHETER ---")
under_dist = underenheter_df['Size_Category'].value_counts().sort_index()
for cat, count in under_dist.items():
    pct = count / len(underenheter_df) * 100
    print(f"{cat:20s}: {count:6,} ({pct:5.1f}%)")

# %% [markdown]
# ## 3. MATCHED ORGANIZATIONS - INDIVIDUAL COMPARISON

# %%
print("\n" + "="*80)
print("MATCHED ORGANIZATIONS - EMPLOYEE COUNT COMPARISON")
print("="*80)

# Prepare combined enhetsreg
hovedenheter_df.rename(columns={'Org. nr.': 'Organisasjonsnummer'}, inplace=True)
underenheter_df.rename(columns={'Org. nr.': 'Organisasjonsnummer'}, inplace=True)
hovedenheter_df['Kilde'] = 'Hovedenhet'
underenheter_df['Kilde'] = 'Underenhet'

enhetsreg_combined = pd.concat([hovedenheter_df, underenheter_df], ignore_index=True)

# Find matches
geodata_orgnr = set(geodata_df['Organisasjonsnummer'].dropna().astype(str))
enhetsreg_orgnr = set(enhetsreg_combined['Organisasjonsnummer'].dropna().astype(str))
matched_orgnr = geodata_orgnr & enhetsreg_orgnr

print(f"\nMatched organizations: {len(matched_orgnr):,}")

# Create merged dataset for comparison
geodata_matched = geodata_df[geodata_df['Organisasjonsnummer'].astype(str).isin(matched_orgnr)].copy()
enhetsreg_matched = enhetsreg_combined[enhetsreg_combined['Organisasjonsnummer'].astype(str).isin(matched_orgnr)].copy()

# Merge on org number
comparison_df = pd.merge(
    geodata_matched[['Organisasjonsnummer', 'Bedriftsnavn', 'Organisasjonsform', 'Antall ansatte']],
    enhetsreg_matched[['Organisasjonsnummer', 'Navn', 'Antall ansatte', 'Kilde']],
    on='Organisasjonsnummer',
    suffixes=('_geo', '_enhetsreg')
)

print(f"Merged records for comparison: {len(comparison_df):,}")

# Calculate differences
comparison_df['Emp_Diff'] = comparison_df['Antall ansatte_geo'] - comparison_df['Antall ansatte_enhetsreg']
comparison_df['Emp_Diff_Abs'] = comparison_df['Emp_Diff'].abs()
comparison_df['Emp_Diff_Pct'] = (comparison_df['Emp_Diff'] / comparison_df['Antall ansatte_enhetsreg'] * 100).replace([float('inf'), -float('inf')], 0)

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
print(f"\nDifference Statistics (Geodata - Enhetsreg):")
print(f"  Mean difference: {comparison_df['Emp_Diff'].mean():.2f}")
print(f"  Median difference: {comparison_df['Emp_Diff'].median():.1f}")
print(f"  Std deviation: {comparison_df['Emp_Diff'].std():.2f}")
print(f"  Min difference: {comparison_df['Emp_Diff'].min():.0f}")
print(f"  Max difference: {comparison_df['Emp_Diff'].max():.0f}")

# %% [markdown]
# ## 4. LARGEST DISCREPANCIES

# %%
print("\n" + "="*80)
print("TOP 100 ORGANIZATIONS WITH LARGEST EMPLOYEE COUNT DIFFERENCES")
print("="*80)

# Where Geodata has MORE employees
print("\n--- GEODATA REPORTS MORE EMPLOYEES (Top 100) ---")
geodata_higher = comparison_df[comparison_df['Emp_Diff'] > 0].nlargest(100, 'Emp_Diff')
if len(geodata_higher) > 0:
    print(f"\n{len(geodata_higher)} cases shown (Total with Geodata > Enhetsreg: {(comparison_df['Emp_Diff'] > 0).sum():,})\n")
    display_cols = ['Bedriftsnavn', 'Organisasjonsnummer', 'Antall ansatte_geo', 'Antall ansatte_enhetsreg', 'Emp_Diff', 'Kilde']
    print(geodata_higher[display_cols].to_string(index=False))
else:
    print("No cases where Geodata reports more employees")

# Where Enhetsregisteret has MORE employees
print("\n\n--- ENHETSREGISTERET REPORTS MORE EMPLOYEES (Top 100) ---")
enhetsreg_higher = comparison_df[comparison_df['Emp_Diff'] < 0].nsmallest(100, 'Emp_Diff')
if len(enhetsreg_higher) > 0:
    print(f"\n{len(enhetsreg_higher)} cases shown (Total with Enhetsreg > Geodata: {(comparison_df['Emp_Diff'] < 0).sum():,})\n")
    display_cols = ['Bedriftsnavn', 'Organisasjonsnummer', 'Antall ansatte_geo', 'Antall ansatte_enhetsreg', 'Emp_Diff', 'Kilde']
    print(enhetsreg_higher[display_cols].to_string(index=False))
else:
    print("No cases where Enhetsregisteret reports more employees")

# Top 100 by absolute difference (regardless of direction)
print("\n\n--- TOP 100 BY ABSOLUTE DIFFERENCE (Regardless of Direction) ---")
top_100_abs = comparison_df.nlargest(100, 'Emp_Diff_Abs')
print(f"\n{len(top_100_abs)} organizations with largest absolute differences\n")
display_cols = ['Bedriftsnavn', 'Organisasjonsnummer', 'Antall ansatte_geo', 'Antall ansatte_enhetsreg', 'Emp_Diff', 'Emp_Diff_Abs', 'Kilde']
print(top_100_abs[display_cols].to_string(index=False))

# %% [markdown]
# ## 5. BREAKDOWN BY ORGANIZATION TYPE

# %%
print("\n" + "="*80)
print("EMPLOYEE DIFFERENCES BY ORGANIZATION TYPE")
print("="*80)

# Group by Kilde (Hovedenhet vs Underenhet)
print("\n--- By Source Type (Hoved vs Under) ---")
by_kilde = comparison_df.groupby('Kilde').agg({
    'Organisasjonsnummer': 'count',
    'Antall ansatte_geo': 'sum',
    'Antall ansatte_enhetsreg': 'sum',
    'Emp_Diff': ['mean', 'sum'],
    'Emp_Diff_Abs': 'mean'
}).round(2)

by_kilde.columns = ['Count', 'Total_Emp_Geo', 'Total_Emp_Enhetsreg', 'Avg_Diff', 'Total_Diff', 'Avg_Abs_Diff']
print(by_kilde)

# %% [markdown]
# ## 6. SUMMARY

# %%
print("\n" + "="*80)
print("EMPLOYEE COUNT SUMMARY")
print("="*80)

print(f"""
OVERALL TOTALS:
- Geodata total employees: {geodata_total_emp:,.0f}
- Enhetsregisteret total (Hoved + Under): {enhetsreg_total:,.0f}
- Difference: {geodata_total_emp - enhetsreg_total:,.0f} ({((geodata_total_emp - enhetsreg_total) / enhetsreg_total * 100):.1f}%)

MATCHED ORGANIZATIONS ({len(comparison_df):,}):
- Exact matches: {exact_match:,} ({exact_match/len(comparison_df)*100:.1f}%)
- Small differences (≤5): {small_diff:,} ({small_diff/len(comparison_df)*100:.1f}%)
- Medium differences (6-50): {medium_diff:,} ({medium_diff/len(comparison_df)*100:.1f}%)
- Large differences (>50): {large_diff:,} ({large_diff/len(comparison_df)*100:.1f}%)

- Average difference: {comparison_df['Emp_Diff'].mean():.2f} employees
- Cases where Geodata > Enhetsreg: {(comparison_df['Emp_Diff'] > 0).sum():,}
- Cases where Enhetsreg > Geodata: {(comparison_df['Emp_Diff'] < 0).sum():,}

INTERPRETATION:
The differences in employee counts likely stem from:
1. Different data collection dates/update frequencies
2. Geodata may include estimates or different reporting periods
3. Hovedenheter vs Underenheter attribution differences
4. Data quality and reporting completeness variations
""")

# %%
# Optional: Export detailed comparison
# comparison_df.to_csv(os.path.join(data_folder, 'employee_comparison_detailed.csv'), index=False, sep=';')
# print("Detailed comparison exported to: employee_comparison_detailed.csv")
