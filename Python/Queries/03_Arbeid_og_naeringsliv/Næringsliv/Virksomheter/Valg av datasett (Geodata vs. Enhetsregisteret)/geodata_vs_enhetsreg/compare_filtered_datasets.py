# %%
"""
Compare SQL-Filtered Geodata and Enhetsregisteret Kombinert Datasets

This script compares the two filtered datasets to identify:
1. Enterprises only in Geodata
2. Enterprises only in Enhetsregisteret Kombinert
3. Enterprises in both datasets

It also analyzes patterns to explain the differences.
"""

import os
import pandas as pd

# %% [markdown]
# ## 1. Load Filtered Datasets

# %%
# Define file paths
data_folder = r"c:\Users\eve1509\OneDrive - Telemark fylkeskommune\Github\Telemark\Data\03_Arbeid og næringsliv\02_Næringsliv\Virksomheter"
geodata_file = os.path.join(data_folder, "geodata_sql_filtrerte_virksomheter.csv")
enhetsreg_file = os.path.join(data_folder, "enhetsreg_kombi_sql_filtrerte_virksomheter.csv")

# Create output folder
output_folder = os.path.join(data_folder, "geodata_vs_enhetsregisteret", "output_2")
os.makedirs(output_folder, exist_ok=True)

# Load datasets
print("="*80)
print("LOADING FILTERED DATASETS")
print("="*80)

geodata_df = pd.read_csv(geodata_file)
enhetsreg_df = pd.read_csv(enhetsreg_file)

print(f"\n✓ Geodata (filtered): {len(geodata_df):,} records")
print(f"✓ Enhetsregisteret Kombinert (filtered): {len(enhetsreg_df):,} records")

# Clean up 'Overordnet enhet' column - remove trailing .0 from both datasets
if 'Overordnet enhet' in geodata_df.columns:
    geodata_df['Overordnet enhet'] = pd.to_numeric(geodata_df['Overordnet enhet'], errors='coerce').apply(
        lambda x: str(int(x)) if pd.notna(x) else ''
    )
if 'Overordnet enhet' in enhetsreg_df.columns:
    enhetsreg_df['Overordnet enhet'] = pd.to_numeric(enhetsreg_df['Overordnet enhet'], errors='coerce').apply(
        lambda x: str(int(x)) if pd.notna(x) else ''
    )
print("✓ Cleaned 'Overordnet enhet' columns (removed trailing .0)")

# Get employee counts
geodata_employees = pd.to_numeric(geodata_df['Antall ansatte'], errors='coerce').sum()
enhetsreg_employees = pd.to_numeric(enhetsreg_df['Antall ansatte'], errors='coerce').sum()

print(f"\nTotal employees:")
print(f"  Geodata: {geodata_employees:,.0f}")
print(f"  Enhetsregisteret: {enhetsreg_employees:,.0f}")

# %% [markdown]
# ## 2. Compare Organization Numbers

# %%
print("\n" + "="*80)
print("ORGANIZATION NUMBER COMPARISON")
print("="*80)

# Get unique org numbers
# Both datasets now use 'Org. nr.' column
geodata_orgnr_series = pd.to_numeric(geodata_df['Org. nr.'], errors='coerce').dropna()
geodata_orgnr = set(geodata_orgnr_series.astype(int).astype(str))

enhetsreg_orgnr_series = pd.to_numeric(enhetsreg_df['Org. nr.'], errors='coerce').dropna()
enhetsreg_orgnr = set(enhetsreg_orgnr_series.astype(int).astype(str))

print(f"\nUnique organization numbers:")
print(f"  Geodata: {len(geodata_orgnr):,}")
print(f"  Enhetsregisteret: {len(enhetsreg_orgnr):,}")

# Find overlaps and differences
in_both = geodata_orgnr & enhetsreg_orgnr
only_geodata = geodata_orgnr - enhetsreg_orgnr
only_enhetsreg = enhetsreg_orgnr - geodata_orgnr

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
# ## 3. Create Output Datasets

# %%
print("\n" + "="*80)
print("CREATING OUTPUT DATASETS")
print("="*80)

# Create temporary string columns for comparison in both datasets
geodata_df['Org. nr._str'] = pd.to_numeric(geodata_df['Org. nr.'], errors='coerce').apply(
    lambda x: str(int(x)) if pd.notna(x) else None
)
enhetsreg_df['Org. nr._str'] = pd.to_numeric(enhetsreg_df['Org. nr.'], errors='coerce').apply(
    lambda x: str(int(x)) if pd.notna(x) else None
)

# Organizations only in Geodata
geodata_only_df = geodata_df[geodata_df['Org. nr._str'].isin(only_geodata)].copy()
geodata_only_df = geodata_only_df.drop(columns=['Org. nr._str'])
print(f"\n✓ Geodata only: {len(geodata_only_df):,} records")

# Organizations only in Enhetsregisteret
enhetsreg_only_df = enhetsreg_df[enhetsreg_df['Org. nr._str'].isin(only_enhetsreg)].copy()
enhetsreg_only_df = enhetsreg_only_df.drop(columns=['Org. nr._str'])
print(f"✓ Enhetsregisteret only: {len(enhetsreg_only_df):,} records")

# Organizations in both datasets
geodata_both_df = geodata_df[geodata_df['Org. nr._str'].isin(in_both)].copy()
geodata_both_df = geodata_both_df.drop(columns=['Org. nr._str'])

enhetsreg_both_df = enhetsreg_df[enhetsreg_df['Org. nr._str'].isin(in_both)].copy()
enhetsreg_both_df = enhetsreg_both_df.drop(columns=['Org. nr._str'])
print(f"✓ In both datasets: {len(in_both):,} organizations")

# %% [markdown]
# ## 4. Analyze Patterns - Geodata Only

# %%
print("\n" + "="*80)
print("ANALYSIS: ENTERPRISES ONLY IN GEODATA")
print("="*80)

geodata_only_employees = pd.to_numeric(geodata_only_df['Antall ansatte'], errors='coerce').sum()
print(f"\nTotal enterprises: {len(geodata_only_df):,}")
print(f"Total employees: {geodata_only_employees:,.0f}")

# By organization type
print(f"\n--- By Organization Type (orfkode) - Top 10 ---")
if 'orfkode' in geodata_only_df.columns:
    org_type_counts = geodata_only_df['orfkode'].value_counts().head(10)
    for org_type, count in org_type_counts.items():
        # Get employee count for this org type
        emp_count = pd.to_numeric(
            geodata_only_df[geodata_only_df['orfkode'] == org_type]['Antall ansatte'],
            errors='coerce'
        ).sum()
        print(f"  {org_type}: {count:,} enterprises ({emp_count:,.0f} employees)")

# By municipality
print(f"\n--- By Municipality - Top 10 ---")
if 'Kommunenavn' in geodata_only_df.columns:
    muni_counts = geodata_only_df['Kommunenavn'].value_counts().head(10)
    for muni, count in muni_counts.items():
        emp_count = pd.to_numeric(
            geodata_only_df[geodata_only_df['Kommunenavn'] == muni]['Antall ansatte'],
            errors='coerce'
        ).sum()
        print(f"  {muni}: {count:,} enterprises ({emp_count:,.0f} employees)")

# Employee distribution
print(f"\n--- Employee Size Distribution ---")
if 'Intervallkode antall ansatte' in geodata_only_df.columns:
    emp_interval_counts = geodata_only_df['Intervallkode antall ansatte'].value_counts()
    for interval, count in emp_interval_counts.head(10).items():
        print(f"  {interval}: {count:,} enterprises")

# %% [markdown]
# ## 5. Analyze Patterns - Enhetsregisteret Only

# %%
print("\n" + "="*80)
print("ANALYSIS: ENTERPRISES ONLY IN ENHETSREGISTERET")
print("="*80)

enhetsreg_only_employees = pd.to_numeric(enhetsreg_only_df['Antall ansatte'], errors='coerce').sum()
print(f"\nTotal enterprises: {len(enhetsreg_only_df):,}")
print(f"Total employees: {enhetsreg_only_employees:,.0f}")

# By organization type
print(f"\n--- By Organization Type - Top 10 ---")
if 'Organisasjonsform' in enhetsreg_only_df.columns:
    org_type_counts = enhetsreg_only_df['Organisasjonsform'].value_counts().head(10)
    for org_type, count in org_type_counts.items():
        emp_count = pd.to_numeric(
            enhetsreg_only_df[enhetsreg_only_df['Organisasjonsform'] == org_type]['Antall ansatte'],
            errors='coerce'
        ).sum()
        print(f"  {org_type}: {count:,} enterprises ({emp_count:,.0f} employees)")

# By sector
print(f"\n--- By Sector - Top 10 ---")
if 'Sektor' in enhetsreg_only_df.columns:
    sector_counts = enhetsreg_only_df['Sektor'].value_counts().head(10)
    for sector, count in sector_counts.items():
        emp_count = pd.to_numeric(
            enhetsreg_only_df[enhetsreg_only_df['Sektor'] == sector]['Antall ansatte'],
            errors='coerce'
        ).sum()
        print(f"  {sector}: {count:,} enterprises ({emp_count:,.0f} employees)")

# By municipality
print(f"\n--- By Municipality - Top 10 ---")
if 'Forretningsadresse - Kommune' in enhetsreg_only_df.columns:
    muni_counts = enhetsreg_only_df['Forretningsadresse - Kommune'].value_counts().head(10)
    for muni, count in muni_counts.items():
        emp_count = pd.to_numeric(
            enhetsreg_only_df[enhetsreg_only_df['Forretningsadresse - Kommune'] == muni]['Antall ansatte'],
            errors='coerce'
        ).sum()
        print(f"  {muni}: {count:,} enterprises ({emp_count:,.0f} employees)")

# By activity (NACE)
print(f"\n--- By Main Activity (NACE 1) - Top 10 ---")
if 'NACE 1 - Bransje' in enhetsreg_only_df.columns:
    nace_counts = enhetsreg_only_df['NACE 1 - Bransje'].value_counts().head(10)
    for nace, count in nace_counts.items():
        emp_count = pd.to_numeric(
            enhetsreg_only_df[enhetsreg_only_df['NACE 1 - Bransje'] == nace]['Antall ansatte'],
            errors='coerce'
        ).sum()
        print(f"  {nace}: {count:,} enterprises ({emp_count:,.0f} employees)")

# Top 20 largest employers
print(f"\n--- Top 20 Largest Employers Only in Enhetsregisteret ---")
enhetsreg_only_sorted = enhetsreg_only_df.sort_values('Antall ansatte', ascending=False)
top_20 = enhetsreg_only_sorted.head(20)
for idx, row in top_20.iterrows():
    print(f"  {row['Navn']}: {row['Antall ansatte']:,.0f} employees")

# %% [markdown]
# ## 6. Analyze Enterprises in Both Datasets

# %%
print("\n" + "="*80)
print("ANALYSIS: ENTERPRISES IN BOTH DATASETS")
print("="*80)

geodata_both_employees = pd.to_numeric(geodata_both_df['Antall ansatte'], errors='coerce').sum()
enhetsreg_both_employees = pd.to_numeric(enhetsreg_both_df['Antall ansatte'], errors='coerce').sum()

print(f"\nTotal enterprises: {len(in_both):,}")
print(f"Total employees (Geodata): {geodata_both_employees:,.0f}")
print(f"Total employees (Enhetsregisteret): {enhetsreg_both_employees:,.0f}")
print(f"Employee difference: {abs(geodata_both_employees - enhetsreg_both_employees):,.0f}")

# %% [markdown]
# ## 7. Save Output Files

# %%
print("\n" + "="*80)
print("SAVING OUTPUT FILES")
print("="*80)

# Save Geodata only
geodata_only_file = os.path.join(output_folder, "geodata_only.csv")
geodata_only_df.to_csv(geodata_only_file, index=False)
print(f"\n✓ Saved: geodata_only.csv ({len(geodata_only_df):,} records)")

# Save Enhetsregisteret only
enhetsreg_only_file = os.path.join(output_folder, "enhetsregisteret_kombi_only.csv")
enhetsreg_only_df.to_csv(enhetsreg_only_file, index=False)
print(f"✓ Saved: enhetsregisteret_kombi_only.csv ({len(enhetsreg_only_df):,} records)")

# Save both - merge the two datasets for complete information
# Prepare for merge
geodata_both_for_merge = geodata_both_df.copy()
geodata_both_for_merge['Source'] = 'Geodata'

enhetsreg_both_for_merge = enhetsreg_both_df.copy()
enhetsreg_both_for_merge['Source'] = 'Enhetsregisteret'

# Both datasets now have 'Org. nr.' column, so no renaming needed
# Combine both sources
both_combined = pd.concat([geodata_both_for_merge, enhetsreg_both_for_merge], ignore_index=True, sort=False)
both_file = os.path.join(output_folder, "both_geodata_and_enhetsreg_kombi.csv")
both_combined.to_csv(both_file, index=False)
print(f"✓ Saved: both_geodata_and_enhetsreg_kombi.csv ({len(both_combined):,} records from {len(in_both):,} unique organizations)")

# %% [markdown]
# ## 8. Summary Report

# %%
print("\n" + "="*80)
print("COMPREHENSIVE SUMMARY REPORT")
print("="*80)

print(f"""
DATASET OVERVIEW:
-----------------
Geodata (SQL filtered):
  - Total enterprises: {len(geodata_df):,}
  - Total employees: {geodata_employees:,.0f}
  
Enhetsregisteret Kombinert (SQL filtered):
  - Total enterprises: {len(enhetsreg_df):,}
  - Total employees: {enhetsreg_employees:,.0f}

Difference:
  - {len(geodata_df) - len(enhetsreg_df):,} more enterprises in Geodata
  - {geodata_employees - enhetsreg_employees:,.0f} more employees in Geodata

OVERLAP ANALYSIS:
-----------------
Enterprises in both datasets: {len(in_both):,} ({overlap_pct_geodata:.1f}% of Geodata, {overlap_pct_enhetsreg:.1f}% of Enhetsregisteret)
  - Employees in Geodata: {geodata_both_employees:,.0f}
  - Employees in Enhetsregisteret: {enhetsreg_both_employees:,.0f}

Enterprises only in Geodata: {len(only_geodata):,} ({len(only_geodata)/len(geodata_orgnr)*100:.1f}% of Geodata)
  - Total employees: {geodata_only_employees:,.0f}

Enterprises only in Enhetsregisteret: {len(only_enhetsreg):,} ({len(only_enhetsreg)/len(enhetsreg_orgnr)*100:.1f}% of Enhetsregisteret)
  - Total employees: {enhetsreg_only_employees:,.0f}

KEY FINDINGS - WHY THE DIFFERENCE?
-----------------------------------
""")

# Calculate key statistics for pattern identification
if 'orfkode' in geodata_only_df.columns:
    top_orfkode = geodata_only_df['orfkode'].value_counts().head(3)
    print("1. ORGANIZATION TYPES IN GEODATA ONLY:")
    for orf, count in top_orfkode.items():
        print(f"   - {orf}: {count:,} enterprises")

if 'Organisasjonsform' in enhetsreg_only_df.columns:
    top_org_form = enhetsreg_only_df['Organisasjonsform'].value_counts().head(3)
    print("\n2. ORGANIZATION TYPES IN ENHETSREGISTERET ONLY:")
    for org_form, count in top_org_form.items():
        print(f"   - {org_form}: {count:,} enterprises")

if 'Sektor' in enhetsreg_only_df.columns:
    sector_breakdown = enhetsreg_only_df['Sektor'].value_counts()
    print("\n3. SECTOR BREAKDOWN (Enhetsregisteret only):")
    for sector, count in sector_breakdown.items():
        pct = count / len(enhetsreg_only_df) * 100
        print(f"   - {sector}: {count:,} enterprises ({pct:.1f}%)")

# Employee size comparison
print("\n4. EMPLOYEE SIZE DISTRIBUTION:")
print(f"   Geodata only - Average employees: {geodata_only_employees/len(geodata_only_df):.1f}")
print(f"   Enhetsregisteret only - Average employees: {enhetsreg_only_employees/len(enhetsreg_only_df):.1f}")

print("\n5. DATA SOURCE CHARACTERISTICS:")
print("   - Geodata: Geographic database, may include location-based registrations")
print("   - Enhetsregisteret: Legal entity register from Brønnøysund")
print("   - Potential reasons for differences:")
print("     a) Different update frequencies")
print("     b) Different registration requirements")
print("     c) Geodata may include spatial/location-specific entities")
print("     d) Enhetsregisteret may include more administrative/holding companies")
print("     e) Different handling of inactive/closed enterprises")

print("\nOUTPUT FILES:")
print("-------------")
print(f"All files saved to: {output_folder}")
print(f"  1. geodata_only.csv - {len(geodata_only_df):,} records")
print(f"  2. enhetsregisteret_kombi_only.csv - {len(enhetsreg_only_df):,} records")
print(f"  3. both_geodata_and_enhetsreg_kombi.csv - {len(both_combined):,} records")

print("\n" + "="*80)
print("ANALYSIS COMPLETE")
print("="*80)

# %%
