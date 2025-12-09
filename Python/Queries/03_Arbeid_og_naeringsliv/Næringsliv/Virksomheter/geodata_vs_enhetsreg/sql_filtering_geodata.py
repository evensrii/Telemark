# %%
"""
SQL Filtering of Geodata Enterprises
This script applies multiple filters to match Enhetsregisteret data scope:

1. Employee filter: Exclude enterprises with exactly 1 employee (matching Enhetsregisteret scope)
2. SQL filter: firorgnr NOT IN (firorgnrknytning) to exclude parent organizations

Filter logic:
- First removes enterprises with exactly 1 employee (keeps null, 0, 2+)
- Then removes organizations where firorgnr appears in firorgnrknytning column
- Keeps only enterprises that are NOT parents of other organizations
- Focuses on comparable data between the two datasets
"""

import os
import pandas as pd

# %% [markdown]
# ## 1. Load Geodata

# %%
# Define file paths
data_folder = r"c:\Users\eve1509\OneDrive - Telemark fylkeskommune\Github\Telemark\Data\03_Arbeid og næringsliv\02_Næringsliv\Virksomheter"
input_file = os.path.join(data_folder, "geodata_bedrifter_api.csv")

# Load dataset
print("="*80)
print("LOADING GEODATA")
print("="*80)
df = pd.read_csv(input_file)
print(f"✓ Loaded: {len(df):,} records from geodata_bedrifter_api.csv")
print(f"  Columns: {len(df.columns)}")

# Check for employee data
# Convert to numeric, handling any non-numeric values
employees = pd.to_numeric(df['Antall ansatte'], errors='coerce')
total_employees = employees.sum()
records_with_employee_data = employees.notna().sum()

print(f"\nEmployee data (before filter):")
print(f"  Records with employee data: {records_with_employee_data:,}")
print(f"  Total employees: {total_employees:,.0f}")

# Count records by employee range
zero_employees = (employees == 0).sum()
one_employee = (employees == 1).sum()
two_to_four = ((employees >= 2) & (employees <= 4)).sum()
five_plus = (employees >= 5).sum()
null_employees = employees.isna().sum()

print(f"  Records with null/missing employees: {null_employees:,}")
print(f"  Records with 0 employees: {zero_employees:,}")
print(f"  Records with exactly 1 employee: {one_employee:,}")
print(f"  Records with 2-4 employees: {two_to_four:,}")
print(f"  Records with 5+ employees: {five_plus:,}")

# %% [markdown]
# ## 2. Filter Out Enterprises with Exactly 1 Employee

# %%
print("\n" + "="*80)
print("FILTERING OUT ENTERPRISES WITH EXACTLY 1 EMPLOYEE")
print("="*80)
print("Matching Enhetsregisteret scope: removing only enterprises with exactly 1 employee")
print("Keeping: null, 0, 2, 3, 4, 5+ employees")

# Convert employee data to numeric
employees_numeric = pd.to_numeric(df['Antall ansatte'], errors='coerce')

# Count before filtering
before_count = len(df)
exactly_one = (employees_numeric == 1).sum()

# Apply filter: remove only records with exactly 1 employee
df = df[employees_numeric != 1].copy()

print(f"\nEmployee filter results:")
print(f"  Records before filter: {before_count:,}")
print(f"  Records after filter: {len(df):,}")
print(f"  Records removed (exactly 1 employee): {exactly_one:,}")
print(f"  Percentage kept: {(len(df)/before_count*100):.1f}%")

# Recalculate employee statistics after filtering
employees_after_filter = pd.to_numeric(df['Antall ansatte'], errors='coerce')
total_employees_after_filter = employees_after_filter.sum()
print(f"\nEmployee data after filter:")
print(f"  Total employees: {total_employees_after_filter:,.0f}")
print(f"  Average per enterprise: {employees_after_filter.mean():.1f}")
print(f"  Records with null employees: {employees_after_filter.isna().sum():,} (kept)")

# %% [markdown]
# ## 3. Apply SQL Filter (Parent Organization Removal)

# %%
print("\n" + "="*80)
print("APPLYING SQL FILTER: firorgnr NOT IN (firorgnrknytning)")
print("="*80)
print("Filter removes parent organizations, keeping only lowest level entities.")

# Create set of parent organization numbers
# Convert floats to int first to remove .0 suffix, then to string
parent_orgnr = set(df['firorgnrknytning'].dropna().astype(int).astype(str))
print(f"\nUnique parent organization numbers: {len(parent_orgnr):,}")

# Apply filter: Keep only organizations whose firorgnr is NOT in the parent set
orgnr_set = set(df['firorgnr'].dropna().astype(str))
overlap = orgnr_set & parent_orgnr
print(f"Organizations to be removed (parents): {len(overlap):,}")

df_filtered = df[~df['firorgnr'].astype(str).isin(parent_orgnr)].copy()

print(f"\nSQL filter results:")
print(f"  Records before filter: {len(df):,}")
print(f"  Records after filter: {len(df_filtered):,}")
print(f"  Records removed: {len(df) - len(df_filtered):,}")

# %% [markdown]
# ## 4. Summary Statistics

# %%
print("\n" + "="*80)
print("FILTERED DATA SUMMARY")
print("="*80)

# Employee statistics for final filtered data (recalculate from final dataframe)
employees_final = pd.to_numeric(df_filtered['Antall ansatte'], errors='coerce')
total_employees_filtered = employees_final.sum()
records_with_employee_data_filtered = employees_final.notna().sum()

print(f"\nEmployee data (after all filters):")
print(f"  Total enterprises: {len(df_filtered):,}")
print(f"  Records with employee data: {records_with_employee_data_filtered:,}")
print(f"  Total employees: {total_employees_filtered:,.0f}")
print(f"  Average per enterprise: {employees_final.mean():.1f}")
print(f"  Note: Enterprises with exactly 1 employee were excluded")

employees_removed = total_employees - total_employees_filtered
print(f"\nImpact of filter:")
print(f"  Employees removed: {employees_removed:,.0f}")
print(f"  Percentage removed: {(employees_removed/total_employees*100):.1f}%")

# Show sample of filtered data
print(f"\nSample of filtered data (first 5 records):")
print(df_filtered[['firorgnr', 'Bedriftsnavn', 'Kommunenavn', 'Antall ansatte']].head())
print("\nNote: Columns will be renamed and reordered before saving.")

# %% [markdown]
# ## 5. Rename and Reorder Columns

# %%
print("\n" + "="*80)
print("RENAMING AND REORDERING COLUMNS")
print("="*80)

# Rename key columns to match Enhetsregisteret format
df_filtered = df_filtered.rename(columns={
    'Bedriftsnavn': 'Navn',
    'firorgnr': 'Org. nr.',
    'firorgnrknytning': 'Overordnet enhet'
})

print("✓ Renamed columns:")
print("  'Bedriftsnavn' → 'Navn'")
print("  'firorgnr' → 'Org. nr.'")
print("  'firorgnrknytning' → 'Overordnet enhet'")

# Reorder columns: key columns first, then the rest
key_columns = ['Navn', 'Org. nr.', 'Overordnet enhet', 'Antall ansatte']
other_columns = [col for col in df_filtered.columns if col not in key_columns]
new_column_order = key_columns + other_columns

df_filtered = df_filtered[new_column_order]

print(f"\n✓ Reordered columns: {', '.join(key_columns)} first, then {len(other_columns)} others")

# Clean up 'Overordnet enhet' column - remove trailing .0
df_filtered['Overordnet enhet'] = pd.to_numeric(df_filtered['Overordnet enhet'], errors='coerce').apply(
    lambda x: str(int(x)) if pd.notna(x) else ''
)
print(f"\n✓ Cleaned 'Overordnet enhet' column (removed trailing .0)")

# %% [markdown]
# ## 6. Save to CSV

# %%
print("\n" + "="*80)
print("SAVING TO CSV")
print("="*80)

file_name = "geodata_sql_filtrerte_virksomheter.csv"
output_file = os.path.join(data_folder, file_name)

# Save to CSV
df_filtered.to_csv(output_file, index=False)

print(f"✓ Filtered dataset saved to: {output_file}")

# %% [markdown]
# ## 7. Final Summary

# %%
print("\n" + "="*80)
print("PROCESSING COMPLETE")
print("="*80)
print(f"\nInput file: geodata_bedrifter_api.csv")
print(f"Output file: {file_name}")
print(f"\nFilters applied (in order):")
print(f"  1. Employee filter: Exclude enterprises with exactly 1 employee")
print(f"     (Matching Enhetsregisteret's data scope)")
print(f"     Kept: null, 0, 2, 3, 4, 5+ employees")
print(f"  2. SQL filter: firorgnr NOT IN (firorgnrknytning)")
print(f"     (Exclude parent organizations)")
print(f"\nOriginal dataset:")
print(f"  Total records: {before_count:,}")
print(f"  Total employees: {total_employees:,.0f}")
print(f"\nFiltered dataset:")
print(f"  Total records: {len(df_filtered):,}")
print(f"  Records with employee data: {records_with_employee_data_filtered:,}")
print(f"  Total employees: {total_employees_filtered:,.0f}")
print(f"\nRecords removed:")
print(f"  Total removed: {before_count - len(df_filtered):,}")
print(f"  Percentage of original: {((before_count - len(df_filtered))/before_count*100):.1f}%")

print("\n" + "="*80)
