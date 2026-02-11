# %%
"""
Comprehensive comparison of Geodata API vs Enhetsregisteret (Hovedenheter + Underenheter)
This script analyzes the differences between business data from two sources:
1. Geodata API (geodata_bedrifter_api.csv)
2. Combined Enhetsregisteret data (hovedenheter + underenheter)
"""

import pandas as pd
import os

# %% [markdown]
# ## 1. Load Data

# %%
# Define file paths
data_folder = r"c:\Users\eve1509\OneDrive - Telemark fylkeskommune\Github\Telemark\Data\03_Arbeid og næringsliv\02_Næringsliv\Virksomheter"
geodata_file = os.path.join(data_folder, "geodata_bedrifter_api.csv")
hovedenheter_file = os.path.join(data_folder, "enhetsregisteret_hovedenheter.csv")
underenheter_file = os.path.join(data_folder, "enhetsregisteret_underenheter.csv")

# Load datasets
print("Loading datasets...")
geodata_df = pd.read_csv(geodata_file)
hovedenheter_df = pd.read_csv(hovedenheter_file)
underenheter_df = pd.read_csv(underenheter_file)

print(f"✓ Geodata: {len(geodata_df):,} records")
print(f"✓ Hovedenheter: {len(hovedenheter_df):,} records")
print(f"✓ Underenheter: {len(underenheter_df):,} records")

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

print("\n--- HOVEDENHETER ---")
print(f"Total records: {len(hovedenheter_df):,}")
print(f"Columns: {len(hovedenheter_df.columns)}")
print(f"Column names:\n{', '.join(hovedenheter_df.columns.tolist())}")

print("\n--- UNDERENHETER ---")
print(f"Total records: {len(underenheter_df):,}")
print(f"Columns: {len(underenheter_df.columns)}")
print(f"Column names:\n{', '.join(underenheter_df.columns.tolist())}")

# %% [markdown]
# ## 3. Combine Enhetsregisteret Data

# %%
# Prepare hovedenheter for combining
hovedenheter_combined = hovedenheter_df.copy()
hovedenheter_combined['Kilde'] = 'Hovedenhet'
hovedenheter_combined.rename(columns={'Org. nr.': 'Organisasjonsnummer'}, inplace=True)

# Prepare underenheter for combining
underenheter_combined = underenheter_df.copy()
underenheter_combined['Kilde'] = 'Underenhet'
underenheter_combined.rename(columns={'Org. nr.': 'Organisasjonsnummer'}, inplace=True)

# Combine them
enhetsreg_combined = pd.concat([hovedenheter_combined, underenheter_combined], ignore_index=True)

print(f"\nCombined Enhetsregisteret: {len(enhetsreg_combined):,} records")
print(f"  - Hovedenheter: {len(enhetsreg_combined[enhetsreg_combined['Kilde'] == 'Hovedenhet']):,}")
print(f"  - Underenheter: {len(enhetsreg_combined[enhetsreg_combined['Kilde'] == 'Underenhet']):,}")

# %% [markdown]
# ## 4. Organization Number Analysis

# %%
print("\n" + "="*80)
print("ORGANIZATION NUMBER COMPARISON")
print("="*80)

# Get unique org numbers from each source
geodata_orgnr = set(geodata_df['Organisasjonsnummer'].dropna().astype(str))
enhetsreg_orgnr = set(enhetsreg_combined['Organisasjonsnummer'].dropna().astype(str))

print(f"\nUnique organization numbers:")
print(f"  Geodata API: {len(geodata_orgnr):,}")
print(f"  Enhetsregisteret (combined): {len(enhetsreg_orgnr):,}")

# Find overlaps and differences
only_geodata = geodata_orgnr - enhetsreg_orgnr
only_enhetsreg = enhetsreg_orgnr - geodata_orgnr
in_both = geodata_orgnr & enhetsreg_orgnr

print(f"\nOverlap analysis:")
print(f"  In both datasets: {len(in_both):,}")
print(f"  Only in Geodata: {len(only_geodata):,}")
print(f"  Only in Enhetsregisteret: {len(only_enhetsreg):,}")

overlap_pct_geodata = (len(in_both) / len(geodata_orgnr) * 100) if geodata_orgnr else 0
overlap_pct_enhetsreg = (len(in_both) / len(enhetsreg_orgnr) * 100) if enhetsreg_orgnr else 0

print(f"\nOverlap percentages:")
print(f"  {overlap_pct_geodata:.1f}% of Geodata organizations are in Enhetsregisteret")
print(f"  {overlap_pct_enhetsreg:.1f}% of Enhetsregisteret organizations are in Geodata")

# %% [markdown]
# ## 5. Analyze Organizations Only in Geodata

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
# ## 6. Analyze Organizations Only in Enhetsregisteret

# %%
print("\n" + "="*80)
print("ORGANIZATIONS ONLY IN ENHETSREGISTERET")
print("="*80)

only_enhetsreg_df = enhetsreg_combined[
    enhetsreg_combined['Organisasjonsnummer'].astype(str).isin(only_enhetsreg)
].copy()

print(f"\nTotal: {len(only_enhetsreg_df):,} organizations")

if len(only_enhetsreg_df) > 0:
    print("\n--- By Source (Hoved/Under) ---")
    source_counts = only_enhetsreg_df['Kilde'].value_counts()
    for source, count in source_counts.items():
        print(f"  {source}: {count:,}")
    
    print("\n--- By Organizational Form (top 10) ---")
    if 'Organisasjonsform' in only_enhetsreg_df.columns:
        org_form_counts = only_enhetsreg_df['Organisasjonsform'].value_counts()
        for form, count in org_form_counts.head(10).items():
            print(f"  {form}: {count:,}")
    
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
    
    only_enhetsreg_df['Emp_Category'] = only_enhetsreg_df['Antall ansatte'].apply(categorize_employees)
    emp_cat_counts = only_enhetsreg_df['Emp_Category'].value_counts()
    for cat, count in emp_cat_counts.items():
        print(f"  {cat}: {count:,}")
    
    print("\n--- Sample records (first 10) ---")
    sample_cols = ['Navn', 'Organisasjonsnummer', 'Kilde', 'Antall ansatte']
    if 'NACE 1 - Bransje' in only_enhetsreg_df.columns:
        sample_cols.append('NACE 1 - Bransje')
    print(only_enhetsreg_df[sample_cols].head(10).to_string(index=False))

# %% [markdown]
# ## 7. Employee Count Comparison (for matched organizations)

# %%
print("\n" + "="*80)
print("EMPLOYEE COUNT COMPARISON (MATCHED ORGANIZATIONS)")
print("="*80)

# Get matching records
geodata_matched = geodata_df[geodata_df['Organisasjonsnummer'].astype(str).isin(in_both)].copy()
enhetsreg_matched = enhetsreg_combined[
    enhetsreg_combined['Organisasjonsnummer'].astype(str).isin(in_both)
].copy()

print(f"\nMatched organizations: {len(in_both):,}")
print(f"Geodata records with employee data: {geodata_matched['Antall ansatte'].notna().sum():,}")
print(f"Enhetsreg records with employee data: {enhetsreg_matched['Antall ansatte'].notna().sum():,}")

# Merge for comparison
merged_emp = pd.merge(
    geodata_matched[['Organisasjonsnummer', 'Bedriftsnavn', 'Antall ansatte']],
    enhetsreg_matched[['Organisasjonsnummer', 'Navn', 'Antall ansatte', 'Kilde']],
    on='Organisasjonsnummer',
    suffixes=('_geodata', '_enhetsreg')
)

print(f"\nMerged comparison records: {len(merged_emp):,}")

# Compare employee counts
merged_emp['Emp_Diff'] = merged_emp['Antall ansatte_geodata'] - merged_emp['Antall ansatte_enhetsreg']
merged_emp['Emp_Match'] = merged_emp['Emp_Diff'].abs() < 1

print(f"\nEmployee count matches: {merged_emp['Emp_Match'].sum():,} ({merged_emp['Emp_Match'].sum()/len(merged_emp)*100:.1f}%)")
print(f"Employee count differences: {(~merged_emp['Emp_Match']).sum():,}")

# Show examples of differences
print("\n--- Examples of employee count differences (top 10 by absolute difference) ---")
diff_examples = merged_emp[~merged_emp['Emp_Match']].nlargest(10, 'Emp_Diff', keep='first')
display_cols = ['Bedriftsnavn', 'Organisasjonsnummer', 'Antall ansatte_geodata', 
                'Antall ansatte_enhetsreg', 'Emp_Diff', 'Kilde']
if len(diff_examples) > 0:
    print(diff_examples[display_cols].to_string(index=False))
else:
    print("  No significant differences found")

# %% [markdown]
# ## 8. Coverage Analysis

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

print("\n--- Enhetsregisteret Coverage ---")
print(f"Total organizations: {len(enhetsreg_combined):,}")
print(f"With employee data: {enhetsreg_combined['Antall ansatte'].notna().sum():,}")
print(f"With NACE code: {enhetsreg_combined['NACE 1'].notna().sum():,}")

# %% [markdown]
# ## 9. Summary Statistics

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
        'Only in Enhetsreg',
        'Avg Employees (Geodata)',
        'Avg Employees (Enhetsreg)',
        'Total Employees (Geodata)',
        'Total Employees (Enhetsreg)'
    ],
    'Geodata API': [
        f"{len(geodata_df):,}",
        f"{len(geodata_orgnr):,}",
        f"{len(in_both):,}",
        f"{len(only_geodata):,}",
        "-",
        f"{geodata_df['Antall ansatte'].mean():.1f}",
        "-",
        f"{geodata_df['Antall ansatte'].sum():,.0f}",
        "-"
    ],
    'Enhetsregisteret': [
        f"{len(enhetsreg_combined):,}",
        f"{len(enhetsreg_orgnr):,}",
        f"{len(in_both):,}",
        "-",
        f"{len(only_enhetsreg):,}",
        "-",
        f"{enhetsreg_combined['Antall ansatte'].mean():.1f}",
        "-",
        f"{enhetsreg_combined['Antall ansatte'].sum():,.0f}"
    ]
}

summary_df = pd.DataFrame(summary_stats)
print("\n" + summary_df.to_string(index=False))

# %% [markdown]
# ## 10. Key Findings Summary

# %%
print("\n" + "="*80)
print("KEY FINDINGS")
print("="*80)

print(f"""
1. DATASET SIZES:
   - Geodata API contains {len(geodata_df):,} records
   - Enhetsregisteret contains {len(enhetsreg_combined):,} records (combined)
     * {len(hovedenheter_df):,} hovedenheter
     * {len(underenheter_df):,} underenheter

2. ORGANIZATION OVERLAP:
   - {len(in_both):,} organizations appear in both datasets
   - {len(only_geodata):,} organizations only in Geodata API ({len(only_geodata)/len(geodata_orgnr)*100:.1f}% of Geodata)
   - {len(only_enhetsreg):,} organizations only in Enhetsregisteret ({len(only_enhetsreg)/len(enhetsreg_orgnr)*100:.1f}% of Enhetsreg)

3. DATA COMPLETENESS:
   - Geodata has geographic coordinates (Lon/Lat) for mapping
   - Geodata includes revenue and operational result data
   - Enhetsregisteret has more comprehensive organizational structure data
   - Employee counts differ between sources for many matched organizations

4. UNIQUE FEATURES:
   Geodata API:
   - Geographic coordinates (Lon, Lat)
   - Revenue and profit data (Omsetning, Driftsresultat)
   - More detailed NACE categorization levels
   - Load date for tracking updates
   
   Enhetsregisteret:
   - Distinction between hovedenheter and underenheter
   - Sector classification (Sektorkode, Sektor)
   - More detailed organizational structure (Overordnet enhet)
   - Multiple NACE codes (NACE 1, 2, 3)

5. POTENTIAL ISSUES:
   - Significant number of organizations not overlapping between sources
   - Employee counts don't always match for same organization
   - Different update frequencies may explain discrepancies
""")

print("\n" + "="*80)
print("ANALYSIS COMPLETE")
print("="*80)
