# %% [markdown]
# # Verify Existing SQL Filtered File
# 
# Compare the newly generated SQL-filtered Enhetsregisteret with the existing
# enhetsreg_kombi_sql_filtrerte_virksomheter.csv file

# %%
import pandas as pd

# %%
# File paths
base_path = r'c:\Users\eve1509\OneDrive - Telemark fylkeskommune\Github\Telemark\Data\03_Arbeid og næringsliv\02_Næringsliv\Virksomheter'

# Load datasets
print("Loading datasets...")
df_combined = pd.read_csv(f'{base_path}\\enhetsregisteret_kombinert_clean.csv', dtype={'Org. nr.': str, 'Overordnet enhet': str})
df_existing_filtered = pd.read_csv(f'{base_path}\\enhetsreg_kombi_sql_filtrerte_virksomheter.csv', dtype={'Org. nr.': str, 'Overordnet enhet': str})

print(f"Combined: {len(df_combined):,} rows")
print(f"Existing SQL filtered file: {len(df_existing_filtered):,} rows")

# %%
# Apply SQL filtering to combined dataset (same logic as before)
df_combined['Org. nr._str'] = pd.to_numeric(df_combined['Org. nr.'], errors='coerce').apply(
    lambda x: str(int(x)) if pd.notna(x) else ''
)
df_combined['Overordnet enhet_str'] = pd.to_numeric(df_combined['Overordnet enhet'], errors='coerce').apply(
    lambda x: str(int(x)) if pd.notna(x) else ''
)

# Get all organization numbers that are parents
parent_orgs = set(df_combined[df_combined['Overordnet enhet_str'] != '']['Overordnet enhet_str'].unique())
print(f"\nParent organizations: {len(parent_orgs):,}")

# Filter out parent organizations
df_newly_filtered = df_combined[~df_combined['Org. nr._str'].isin(parent_orgs)].copy()
print(f"Newly filtered (after removing parents): {len(df_newly_filtered):,} rows")

# %%
# Compare organization numbers
df_existing_filtered['Org. nr._str'] = pd.to_numeric(df_existing_filtered['Org. nr.'], errors='coerce').apply(
    lambda x: str(int(x)) if pd.notna(x) else ''
)

newly_filtered_orgs = set(df_newly_filtered['Org. nr._str'].values)
existing_filtered_orgs = set(df_existing_filtered['Org. nr._str'].values)

only_in_newly = newly_filtered_orgs - existing_filtered_orgs
only_in_existing = existing_filtered_orgs - newly_filtered_orgs
in_both = newly_filtered_orgs & existing_filtered_orgs

print("\n" + "="*80)
print("COMPARISON: NEWLY FILTERED VS EXISTING FILE")
print("="*80)
print(f"In both: {len(in_both):,}")
print(f"Only in newly filtered: {len(only_in_newly):,}")
print(f"Only in existing file: {len(only_in_existing):,}")

# %%
# Check if they're identical
if len(only_in_newly) == 0 and len(only_in_existing) == 0:
    print("\n✅ IDENTICAL: The newly filtered results match the existing file perfectly!")
else:
    print("\n⚠️ DIFFERENCES FOUND:")
    
    if len(only_in_newly) > 0:
        df_only_newly = df_newly_filtered[df_newly_filtered['Org. nr._str'].isin(only_in_newly)]
        print(f"\nOrganizations only in newly filtered ({len(only_in_newly)}):")
        print(df_only_newly[['Navn', 'Org. nr.', 'Antall ansatte']].head(10).to_string(index=False))
    
    if len(only_in_existing) > 0:
        df_only_existing = df_existing_filtered[df_existing_filtered['Org. nr._str'].isin(only_in_existing)]
        print(f"\nOrganizations only in existing file ({len(only_in_existing)}):")
        print(df_only_existing[['Navn', 'Org. nr.', 'Antall ansatte']].head(10).to_string(index=False))

# %%
# Employee statistics comparison
df_newly_filtered['Employees'] = pd.to_numeric(df_newly_filtered['Antall ansatte'], errors='coerce').fillna(0)
df_existing_filtered['Employees'] = pd.to_numeric(df_existing_filtered['Antall ansatte'], errors='coerce').fillna(0)

newly_with_emp = df_newly_filtered[df_newly_filtered['Employees'] > 0]
existing_with_emp = df_existing_filtered[df_existing_filtered['Employees'] > 0]

print("\n" + "="*80)
print("EMPLOYEE STATISTICS")
print("="*80)
print(f"Newly filtered:")
print(f"  - Enterprises with employees: {len(newly_with_emp):,}")
print(f"  - Total employees: {newly_with_emp['Employees'].sum():,.0f}")

print(f"\nExisting file:")
print(f"  - Enterprises with employees: {len(existing_with_emp):,}")
print(f"  - Total employees: {existing_with_emp['Employees'].sum():,.0f}")

# %%
print("\n" + "="*80)
print("CONCLUSION")
print("="*80)

if len(only_in_newly) == 0 and len(only_in_existing) == 0:
    print("""
✅ The SQL filtering logic I applied in the comparison script produces 
   IDENTICAL results to the existing 'enhetsreg_kombi_sql_filtrerte_virksomheter.csv' file.

This confirms that:
1. The existing file was created using the same SQL filtering approach
2. The comparison results are valid
3. Both datasets can be used interchangeably for analysis
""")
else:
    print(f"""
⚠️ There are differences between the newly filtered results and the existing file:
   - {len(only_in_newly):,} organizations only in newly filtered
   - {len(only_in_existing):,} organizations only in existing file

This suggests the existing file may have been created with different filtering 
logic or from a different source version.
""")

print("Done!")
