# %%
"""
SQL Filtering of Geodata Enterprises
This script applies the SQL filter: firorgnr NOT IN (firorgnrknytning)
to exclude parent organizations and keep only the lowest level enterprises.

Filter logic:
- Removes organizations where firorgnr appears in firorgnrknytning column
- Keeps only enterprises that are NOT parents of other organizations
- Focuses on organizations with employee data for comparison purposes
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

# %% [markdown]
# ## 2. Apply SQL Filter

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

print(f"\nFilter results:")
print(f"  Records before filter: {len(df):,}")
print(f"  Records after filter: {len(df_filtered):,}")
print(f"  Records removed: {len(df) - len(df_filtered):,}")

# %% [markdown]
# ## 3. Summary Statistics

# %%
print("\n" + "="*80)
print("FILTERED DATA SUMMARY")
print("="*80)

# Employee statistics for filtered data
employees_filtered = pd.to_numeric(df_filtered['Antall ansatte'], errors='coerce')
total_employees_filtered = employees_filtered.sum()
records_with_employee_data_filtered = employees_filtered.notna().sum()

print(f"\nEmployee data (after filter):")
print(f"  Records with employee data: {records_with_employee_data_filtered:,}")
print(f"  Total employees: {total_employees_filtered:,.0f}")

employees_removed = total_employees - total_employees_filtered
print(f"\nImpact of filter:")
print(f"  Employees removed: {employees_removed:,.0f}")
print(f"  Percentage removed: {(employees_removed/total_employees*100):.1f}%")

# Show sample of filtered data
print(f"\nSample of filtered data (first 5 records):")
print(df_filtered[['firorgnr', 'Bedriftsnavn', 'Kommunenavn', 'Antall ansatte']].head())

# %% [markdown]
# ## 4. Save to CSV

# %%
print("\n" + "="*80)
print("SAVING TO CSV")
print("="*80)

file_name = "geodata_sql_filtrerte_virksomheter.csv"
output_file = os.path.join(data_folder, file_name)

# Save to CSV
df_filtered.to_csv(output_file, index=False)

print(f"✓ Filtered dataset saved to: {output_file}")

print("\n" + "="*80)
print("PROCESSING COMPLETE")
print("="*80)
print(f"Output file: {file_name}")
print(f"Total records: {len(df_filtered):,}")
print(f"Records with employee data: {records_with_employee_data_filtered:,}")
print(f"Total employees: {total_employees_filtered:,.0f}")
