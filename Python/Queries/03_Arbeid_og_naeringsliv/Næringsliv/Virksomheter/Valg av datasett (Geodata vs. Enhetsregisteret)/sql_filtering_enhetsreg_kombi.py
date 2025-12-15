# %%
"""
SQL Filtering of Enhetsregisteret Kombinert Data

This script loads the clean combined Enhetsregisteret file and applies filtering.

Prerequisites:
- Run combine_enhetsregisteret.py first to create enhetsregisteret_kombinert_clean.csv

Data scope:
- Enhetsregisteret contains only enterprises with ≥5 employees (except missing data)
- This matches the filtered Geodata for comparable analysis

Filters applied:
1. SQL filter: Org. nr. NOT IN (Overordnet enhet)
   - Removes organizations where Org. nr. appears in Overordnet enhet column
   - Keeps only enterprises that are NOT parents of other organizations
   - Retains all employee counts (including 0/missing) for data completeness

Output:
- enhetsreg_kombi_sql_filtrerte_virksomheter.csv
"""

import os
import pandas as pd

# %% [markdown]
# ## 1. Load Enhetsregisteret Kombinert Data

# %%
# Define file paths
data_folder = r"c:\Users\eve1509\OneDrive - Telemark fylkeskommune\Github\Telemark\Data\03_Arbeid og næringsliv\02_Næringsliv\Virksomheter"
input_file = os.path.join(data_folder, "enhetsregisteret_kombinert_clean.csv")

# Load clean combined dataset
print("="*80)
print("LOADING ENHETSREGISTERET DATA")
print("="*80)

df = pd.read_csv(input_file)
print(f"\n✓ Loaded: {len(df):,} records from enhetsregisteret_kombinert_clean.csv")
print(f"  Columns: {len(df.columns)}")

# Check employee data statistics
print("\n" + "="*80)
print("EMPLOYEE DATA STATISTICS (BEFORE FILTER)")
print("="*80)

employees = pd.to_numeric(df['Antall ansatte'], errors='coerce')
total_employees = employees.sum()
records_with_employee_data = employees.notna().sum()
records_missing_employee_data = employees.isna().sum()
records_with_zero_employees = (employees == 0).sum()
records_with_positive_employees = (employees > 0).sum()

print(f"\nTotal records: {len(df):,}")
print(f"\nEmployee data availability:")
print(f"  Records WITH employee data: {records_with_employee_data:,} ({records_with_employee_data/len(df)*100:.1f}%)")
print(f"  Records MISSING employee data: {records_missing_employee_data:,} ({records_missing_employee_data/len(df)*100:.1f}%)")
print(f"\nAmong records with employee data:")
print(f"  Records with 0 employees: {records_with_zero_employees:,}")
print(f"  Records with >0 employees: {records_with_positive_employees:,}")
print(f"  Total employees: {total_employees:,.0f}")

# %% [markdown]
# ## 2. Apply SQL Filter

# %%
print("\n" + "="*80)
print("APPLYING SQL FILTER: Org. nr. NOT IN (Overordnet enhet)")
print("="*80)
print("Filter removes parent organizations, keeping only lowest level entities.")

# Create set of parent organization numbers
# Convert to numeric first to handle any strings, then to int and string
parent_orgnr_series = pd.to_numeric(df['Overordnet enhet'], errors='coerce').dropna()
parent_orgnr = set(parent_orgnr_series.astype(int).astype(str))
print(f"\nUnique parent organization numbers: {len(parent_orgnr):,}")

# Apply filter: Keep only organizations whose Org. nr. is NOT in the parent set
orgnr_series = pd.to_numeric(df['Org. nr.'], errors='coerce').dropna()
orgnr_set = set(orgnr_series.astype(int).astype(str))
overlap = orgnr_set & parent_orgnr
print(f"Organizations to be removed (parents): {len(overlap):,}")

# Convert Org. nr. to string for comparison
df['Org. nr._str'] = pd.to_numeric(df['Org. nr.'], errors='coerce').apply(lambda x: str(int(x)) if pd.notna(x) else None)
df_filtered = df[~df['Org. nr._str'].isin(parent_orgnr)].copy()
df_filtered = df_filtered.drop(columns=['Org. nr._str'])

print(f"\nSQL filter results:")
print(f"  Records before filter: {len(df):,}")
print(f"  Records after filter: {len(df_filtered):,}")
print(f"  Records removed: {len(df) - len(df_filtered):,}")

# %% [markdown]
# ## 3. Summary Statistics (Keeping All Employee Counts)

# %%
print("\n" + "="*80)
print("FILTERED DATA SUMMARY")
print("="*80)

# Employee statistics for final filtered data
employees_final = pd.to_numeric(df_filtered['Antall ansatte'], errors='coerce')
total_employees_final = employees_final.sum()
records_with_employee_data_final = employees_final.notna().sum()
average_employees = employees_final.mean()
median_employees = employees_final.median()

zero_employees_count = (employees_final == 0).sum()
positive_employees_count = (employees_final > 0).sum()

print(f"\nFinal employee data:")
print(f"  Total enterprises: {len(df_filtered):,}")
print(f"  Records with employee data: {records_with_employee_data_final:,}")
print(f"  Records with 0 employees: {zero_employees_count:,} (KEPT)")
print(f"  Records with >0 employees: {positive_employees_count:,}")
print(f"  Total employees: {total_employees_final:,.0f}")
print(f"  Average employees per enterprise: {average_employees:.1f}")
print(f"  Median employees per enterprise: {median_employees:.0f}")

# Employee distribution
print(f"\nEmployee distribution:")
print(f"  Min: {employees_final.min():.0f}")
print(f"  25th percentile: {employees_final.quantile(0.25):.0f}")
print(f"  75th percentile: {employees_final.quantile(0.75):.0f}")
print(f"  Max: {employees_final.max():.0f}")

# Impact of filters
employees_removed = total_employees - total_employees_final
print(f"\nImpact of all filters:")
print(f"  Employees removed: {employees_removed:,.0f}")
print(f"  Percentage removed: {(employees_removed/total_employees*100):.1f}%")

# Breakdown by organization type
print(f"\nBreakdown by organization type (top 10):")
if 'Organisasjonsform' in df_filtered.columns:
    org_type_counts = df_filtered['Organisasjonsform'].value_counts().head(10)
    for org_type, count in org_type_counts.items():
        print(f"  {org_type}: {count:,}")

# Breakdown by sector
print(f"\nBreakdown by sector (top 10):")
if 'Sektor' in df_filtered.columns:
    sector_counts = df_filtered['Sektor'].value_counts().head(10)
    for sector, count in sector_counts.items():
        print(f"  {sector}: {count:,}")

# Show sample of filtered data
print(f"\nSample of filtered data (first 10 records):")
sample_cols = ['Org. nr.', 'Navn', 'Antall ansatte']
if 'Forretningsadresse - Kommune' in df_filtered.columns:
    sample_cols.append('Forretningsadresse - Kommune')
print(df_filtered[sample_cols].head(10).to_string(index=False))

# %% [markdown]
# ## 5. Clean and Save to CSV

# %%
print("\n" + "="*80)
print("CLEANING AND SAVING TO CSV")
print("="*80)

# Clean up 'Overordnet enhet' column - remove trailing .0
df_filtered['Overordnet enhet'] = pd.to_numeric(df_filtered['Overordnet enhet'], errors='coerce').apply(
    lambda x: str(int(x)) if pd.notna(x) else ''
)
print(f"✓ Cleaned 'Overordnet enhet' column (removed trailing .0)")

file_name = "enhetsreg_kombi_sql_filtrerte_virksomheter.csv"
output_file = os.path.join(data_folder, file_name)

# Save to CSV
df_filtered.to_csv(output_file, index=False)

print(f"✓ Filtered dataset saved to: {output_file}")

# %% [markdown]
# ## 6. Final Summary

# %%
print("\n" + "="*80)
print("PROCESSING COMPLETE")
print("="*80)
print(f"\nInput file: enhetsregisteret_kombinert_clean.csv")
print(f"Output file: {file_name}")
print(f"\nFilters applied:")
print(f"  1. SQL filter: Org. nr. NOT IN (Overordnet enhet)")
print(f"  2. Remove entries with 0 employees")
print(f"\nOriginal dataset:")
print(f"  Total records: {len(df):,}")
print(f"  Total employees: {total_employees:,.0f}")
print(f"\nFiltered dataset:")
print(f"  Total enterprises: {len(df_filtered):,}")
print(f"  Total employees: {total_employees_final:,.0f}")
print(f"  Average employees per enterprise: {average_employees:.1f}")
print(f"  Median employees per enterprise: {median_employees:.0f}")
print(f"\nRecords removed:")
print(f"  By SQL filter: {len(df) - len(df_filtered) - zero_employees_filtered:,}")
print(f"  By 0 employees filter: {zero_employees_filtered:,}")
print(f"  Total removed: {len(df) - len(df_filtered):,}")
print(f"  Percentage of original: {((len(df) - len(df_filtered))/len(df)*100):.1f}%")

print("\n" + "="*80)

# %%
