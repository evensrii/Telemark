# %% [markdown]
# # Analyze Orphaned Underenheter
# 
# This script identifies all underenheter in Telemark whose Level 1 parent organization
# is located outside Telemark region. These "orphaned" underenheter are excluded from
# the lowest level dataset but included in the SQL-filtered combined dataset.

# %%
import pandas as pd
import numpy as np

# %%
# File paths
base_path = r'c:\Users\eve1509\OneDrive - Telemark fylkeskommune\Github\Telemark\Data\03_Arbeid og næringsliv\02_Næringsliv\Virksomheter'

# Load datasets
print("Loading datasets...")
df_combined = pd.read_csv(f'{base_path}\\enhetsregisteret_kombinert_clean.csv', dtype={'Org. nr.': str, 'Overordnet enhet': str})
df_hovedenheter = pd.read_csv(f'{base_path}\\enhetsregisteret_hovedenheter.csv', dtype={'Org. nr.': str})
df_underenheter = pd.read_csv(f'{base_path}\\enhetsregisteret_underenheter.csv', dtype={'Org. nr.': str, 'Overordnet enhet': str})
df_sql_filtered = pd.read_csv(f'{base_path}\\enhetsreg_kombi_sql_filtrerte_virksomheter.csv', dtype={'Org. nr.': str, 'Overordnet enhet': str})
df_lowest = pd.read_csv(f'{base_path}\\enhetsregisteret_laveste_nivaa.csv', dtype={'Organisasjonsnummer': str})

print(f"Combined: {len(df_combined):,} rows")
print(f"Hovedenheter: {len(df_hovedenheter):,} rows")
print(f"Underenheter: {len(df_underenheter):,} rows")
print(f"SQL filtered: {len(df_sql_filtered):,} rows")
print(f"Lowest level: {len(df_lowest):,} rows")

# %%
# Identify all underenheter (those with a parent organization)
underenheter_in_combined = df_combined[df_combined['Overordnet enhet'].notna() & (df_combined['Overordnet enhet'] != '')].copy()
print(f"\nTotal underenheter in combined dataset: {len(underenheter_in_combined):,}")

# %%
# Identify which parent organizations are IN Telemark (hovedenheter)
telemark_parents = set(df_hovedenheter['Org. nr.'].dropna())
print(f"Parent organizations in Telemark: {len(telemark_parents):,}")

# %%
# Classify underenheter: those with parents IN Telemark vs OUTSIDE Telemark
underenheter_in_combined['Parent in Telemark'] = underenheter_in_combined['Overordnet enhet'].isin(telemark_parents)

# Orphaned underenheter = those whose parent is NOT in Telemark
orphaned = underenheter_in_combined[~underenheter_in_combined['Parent in Telemark']].copy()
non_orphaned = underenheter_in_combined[underenheter_in_combined['Parent in Telemark']].copy()

print("\n" + "="*80)
print("CLASSIFICATION OF UNDERENHETER")
print("="*80)
print(f"Underenheter with parent IN Telemark: {len(non_orphaned):,}")
print(f"Underenheter with parent OUTSIDE Telemark (orphaned): {len(orphaned):,}")

# %%
# Calculate employee counts
orphaned['Employees'] = pd.to_numeric(orphaned['Antall ansatte'], errors='coerce').fillna(0)
non_orphaned['Employees'] = pd.to_numeric(non_orphaned['Antall ansatte'], errors='coerce').fillna(0)

orphaned_total_employees = orphaned['Employees'].sum()
non_orphaned_total_employees = non_orphaned['Employees'].sum()

print(f"\nEmployees in underenheter with parent IN Telemark: {non_orphaned_total_employees:,.0f}")
print(f"Employees in underenheter with parent OUTSIDE Telemark: {orphaned_total_employees:,.0f}")

# %%
# Check how many orphaned underenheter have employees
orphaned_with_employees = orphaned[orphaned['Employees'] > 0].copy()
print(f"\nOrphaned underenheter with employees: {len(orphaned_with_employees):,}")
print(f"Total employees in orphaned underenheter: {orphaned_with_employees['Employees'].sum():,.0f}")

# %%
# Which orphaned underenheter are in SQL-filtered dataset?
df_sql_filtered['Org. nr._str'] = df_sql_filtered['Org. nr.'].astype(str)
orphaned['Org. nr._str'] = orphaned['Org. nr.'].astype(str)

orphaned_in_sql_filtered = orphaned[orphaned['Org. nr._str'].isin(df_sql_filtered['Org. nr._str'])].copy()

print("\n" + "="*80)
print("ORPHANED UNDERENHETER IN SQL-FILTERED DATASET")
print("="*80)
print(f"Orphaned underenheter in SQL-filtered dataset: {len(orphaned_in_sql_filtered):,}")
print(f"Employees: {orphaned_in_sql_filtered['Employees'].sum():,.0f}")

# %%
# Which orphaned underenheter are in lowest level dataset?
df_lowest['Organisasjonsnummer_str'] = df_lowest['Organisasjonsnummer'].astype(str)
orphaned_in_lowest = orphaned[orphaned['Org. nr._str'].isin(df_lowest['Organisasjonsnummer_str'])].copy()

print(f"\nOrphaned underenheter in lowest level dataset: {len(orphaned_in_lowest):,}")
print(f"Employees: {orphaned_in_lowest['Employees'].sum():,.0f}")

# %%
# The GAP: Orphaned underenheter in SQL-filtered but NOT in lowest level
orphaned_missing_from_lowest = orphaned_in_sql_filtered[~orphaned_in_sql_filtered['Org. nr._str'].isin(df_lowest['Organisasjonsnummer_str'])].copy()

print("\n" + "="*80)
print("THE GAP: ORPHANED UNDERENHETER MISSING FROM LOWEST LEVEL")
print("="*80)
print(f"Count: {len(orphaned_missing_from_lowest):,}")
print(f"Employees: {orphaned_missing_from_lowest['Employees'].sum():,.0f}")

# %%
# Top orphaned underenheter by employee count
print("\n" + "="*80)
print("TOP 30 ORPHANED UNDERENHETER BY EMPLOYEE COUNT")
print("="*80)

top_orphaned = orphaned_missing_from_lowest.nlargest(30, 'Employees')[['Navn', 'Org. nr.', 'Overordnet enhet', 'Employees', 'Organisasjonsform']].copy()
print(top_orphaned.to_string(index=False))

# %%
# Unique parent organizations for orphaned underenheter
unique_parents = orphaned_missing_from_lowest['Overordnet enhet'].unique()
print(f"\n{len(unique_parents):,} unique parent organizations (outside Telemark)")

# Count how many children each parent has
parent_counts = orphaned_missing_from_lowest.groupby('Overordnet enhet').agg(
    Children_count=('Org. nr.', 'count'),
    Total_employees=('Employees', 'sum')
).sort_values('Total_employees', ascending=False)

print("\nTop 20 parent organizations by total employee count:")
print(parent_counts.head(20).to_string())

# %%
# Save detailed results
output_path = f'{base_path}\\enhetsreg_lavnivaa vs enhetsreg_kombi'

# All orphaned underenheter missing from lowest level
orphaned_missing_from_lowest.to_csv(f'{output_path}\\orphaned_underenheter_missing.csv', index=False)
print(f"\nSaved detailed list to: orphaned_underenheter_missing.csv")

# Summary by parent
parent_counts.to_csv(f'{output_path}\\orphaned_parents_summary.csv')
print(f"Saved parent summary to: orphaned_parents_summary.csv")

# %%
# FINAL SUMMARY
print("\n" + "="*80)
print("FINAL SUMMARY")
print("="*80)

print(f"""
ORPHANED UNDERENHETER ANALYSIS:
--------------------------------
Total underenheter in Telemark: {len(underenheter_in_combined):,}
  - With parent IN Telemark: {len(non_orphaned):,} ({non_orphaned_total_employees:,.0f} employees)
  - With parent OUTSIDE Telemark: {len(orphaned):,} ({orphaned_total_employees:,.0f} employees)

IMPACT ON SQL-FILTERED VS LOWEST LEVEL DISCREPANCY:
---------------------------------------------------
Orphaned underenheter in SQL-filtered dataset: {len(orphaned_in_sql_filtered):,}
  - Employees: {orphaned_in_sql_filtered['Employees'].sum():,.0f}

Orphaned underenheter in lowest level dataset: {len(orphaned_in_lowest):,}
  - Employees: {orphaned_in_lowest['Employees'].sum():,.0f}

THE GAP (orphaned underenheter missing from lowest level):
  - Count: {len(orphaned_missing_from_lowest):,} enterprises
  - Employees: {orphaned_missing_from_lowest['Employees'].sum():,.0f}
  - Unique parent organizations: {len(unique_parents):,}

This accounts for a significant portion of the ~14,796 employee discrepancy.

RECOMMENDATION:
--------------
Modify create_lowest_level_dataset.py to include orphaned underenheter
(those whose Level 1 parent is outside Telemark region).
""")

print("\nDone!")
