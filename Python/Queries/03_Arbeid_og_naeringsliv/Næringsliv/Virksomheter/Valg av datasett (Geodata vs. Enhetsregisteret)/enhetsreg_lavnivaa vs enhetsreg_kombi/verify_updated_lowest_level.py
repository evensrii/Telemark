# %% [markdown]
# # Verify Updated Lowest Level Dataset
# 
# This script compares the updated lowest level dataset (with orphaned underenheter)
# against the SQL-filtered combined dataset to verify the employee discrepancy is resolved.

# %%
import pandas as pd
import numpy as np

# %%
# File paths
base_path = r'c:\Users\eve1509\OneDrive - Telemark fylkeskommune\Github\Telemark\Data\03_Arbeid og næringsliv\02_Næringsliv\Virksomheter'

# Load datasets
print("Loading datasets...")
df_sql_filtered = pd.read_csv(f'{base_path}\\enhetsreg_kombi_sql_filtrerte_virksomheter.csv', dtype={'Org. nr.': str, 'Overordnet enhet': str})
df_lowest = pd.read_csv(f'{base_path}\\enhetsregisteret_laveste_nivaa.csv', dtype={'Organisasjonsnummer': str})

print(f"SQL filtered combined: {len(df_sql_filtered):,} rows")
print(f"Lowest level: {len(df_lowest):,} rows")

# %%
# Standardize organization numbers and employee columns
df_sql_filtered['Org. nr._str'] = df_sql_filtered['Org. nr.'].astype(str)
df_lowest['Org. nr._str'] = df_lowest['Organisasjonsnummer'].astype(str)

df_sql_filtered['Employees'] = pd.to_numeric(df_sql_filtered['Antall ansatte'], errors='coerce').fillna(0)
df_lowest['Employees'] = pd.to_numeric(df_lowest['Antall ansatte Enhetsreg'], errors='coerce').fillna(0)

# %%
# Filter for enterprises with employees
df_sql_with_emp = df_sql_filtered[df_sql_filtered['Employees'] > 0].copy()
df_lowest_with_emp = df_lowest[df_lowest['Employees'] > 0].copy()

print("\n" + "="*80)
print("ENTERPRISES WITH EMPLOYEES")
print("="*80)
print(f"SQL filtered: {len(df_sql_with_emp):,} enterprises, {df_sql_with_emp['Employees'].sum():,.0f} employees")
print(f"Lowest level: {len(df_lowest_with_emp):,} enterprises, {df_lowest_with_emp['Employees'].sum():,.0f} employees")

# Calculate differences
enterprise_diff = len(df_sql_with_emp) - len(df_lowest_with_emp)
employee_diff = df_sql_with_emp['Employees'].sum() - df_lowest_with_emp['Employees'].sum()

print(f"\nDifference:")
print(f"  Enterprises: {enterprise_diff:+,}")
print(f"  Employees: {employee_diff:+,.0f}")

# %%
# Compare organization numbers
sql_orgs = set(df_sql_with_emp['Org. nr._str'].values)
lowest_orgs = set(df_lowest_with_emp['Org. nr._str'].values)

only_in_sql = sql_orgs - lowest_orgs
only_in_lowest = lowest_orgs - sql_orgs
in_both = sql_orgs & lowest_orgs

print("\n" + "="*80)
print("ORGANIZATION OVERLAP")
print("="*80)
print(f"In both datasets: {len(in_both):,}")
print(f"Only in SQL filtered: {len(only_in_sql):,}")
print(f"Only in lowest level: {len(only_in_lowest):,}")

# %%
# Analyze enterprises only in SQL filtered
if len(only_in_sql) > 0:
    df_only_sql = df_sql_with_emp[df_sql_with_emp['Org. nr._str'].isin(only_in_sql)].copy()
    
    print("\n" + "="*80)
    print("ENTERPRISES ONLY IN SQL FILTERED (STILL MISSING FROM LOWEST LEVEL)")
    print("="*80)
    print(f"Count: {len(df_only_sql):,}")
    print(f"Total employees: {df_only_sql['Employees'].sum():,.0f}")
    
    # Top 20 by employee count
    top_missing = df_only_sql.nlargest(20, 'Employees')[['Navn', 'Org. nr.', 'Overordnet enhet', 'Employees']].copy()
    print("\nTop 20 by employee count:")
    print(top_missing.to_string(index=False))
    
    # Save to file
    output_path = f'{base_path}\\enhetsreg_lavnivaa vs enhetsreg_kombi'
    df_only_sql.to_csv(f'{output_path}\\still_missing_after_update.csv', index=False)
    print(f"\nSaved to: still_missing_after_update.csv")

# %%
# Analyze enterprises only in lowest level
if len(only_in_lowest) > 0:
    df_only_lowest = df_lowest_with_emp[df_lowest_with_emp['Org. nr._str'].isin(only_in_lowest)].copy()
    
    print("\n" + "="*80)
    print("ENTERPRISES ONLY IN LOWEST LEVEL (NOT IN SQL FILTERED)")
    print("="*80)
    print(f"Count: {len(df_only_lowest):,}")
    print(f"Total employees: {df_only_lowest['Employees'].sum():,.0f}")
    
    # These should be filtered out by SQL (parents)
    top_extra = df_only_lowest.nlargest(10, 'Employees')[['Navn', 'Organisasjonsnummer', 'Nivå enhetsregisteret', 'Employees']].copy()
    print("\nTop 10 by employee count:")
    print(top_extra.to_string(index=False))

# %%
# Check level distribution in lowest level dataset
print("\n" + "="*80)
print("LEVEL DISTRIBUTION IN LOWEST LEVEL DATASET")
print("="*80)

if 'Nivå enhetsregisteret' in df_lowest_with_emp.columns:
    level_stats = df_lowest_with_emp.groupby('Nivå enhetsregisteret').agg(
        Count=('Org. nr._str', 'count'),
        Total_Employees=('Employees', 'sum')
    )
    print(level_stats)

# %%
# Success criteria
print("\n" + "="*80)
print("VERIFICATION SUMMARY")
print("="*80)

# Calculate improvement from original discrepancy
original_sql_count = 3769
original_sql_employees = 93052
original_lowest_count = 2974
original_lowest_employees = 78256

original_enterprise_gap = original_sql_count - original_lowest_count
original_employee_gap = original_sql_employees - original_lowest_employees

current_enterprise_gap = len(df_sql_with_emp) - len(df_lowest_with_emp)
current_employee_gap = df_sql_with_emp['Employees'].sum() - df_lowest_with_emp['Employees'].sum()

print(f"""
ORIGINAL DISCREPANCY:
  Enterprises: {original_enterprise_gap:,} ({original_lowest_count:,} vs {original_sql_count:,})
  Employees: {original_employee_gap:,.0f} ({original_lowest_employees:,.0f} vs {original_sql_employees:,.0f})

CURRENT DISCREPANCY:
  Enterprises: {current_enterprise_gap:+,} ({len(df_lowest_with_emp):,} vs {len(df_sql_with_emp):,})
  Employees: {current_employee_gap:+,.0f} ({df_lowest_with_emp['Employees'].sum():,.0f} vs {df_sql_with_emp['Employees'].sum():,.0f})

IMPROVEMENT:
  Enterprises resolved: {original_enterprise_gap - current_enterprise_gap:,} ({(original_enterprise_gap - current_enterprise_gap)/original_enterprise_gap*100:.1f}%)
  Employees resolved: {original_employee_gap - current_employee_gap:,.0f} ({(original_employee_gap - current_employee_gap)/original_employee_gap*100:.1f}%)
""")

if abs(current_enterprise_gap) < 50 and abs(current_employee_gap) < 500:
    print("✅ SUCCESS: Discrepancy largely resolved! Remaining differences are minimal.")
elif abs(current_enterprise_gap) < original_enterprise_gap * 0.1 and abs(current_employee_gap) < original_employee_gap * 0.1:
    print("✅ SUBSTANTIAL IMPROVEMENT: >90% of discrepancy resolved.")
elif current_enterprise_gap < original_enterprise_gap * 0.5:
    print("⚠️ PARTIAL IMPROVEMENT: ~50% of discrepancy resolved. Further investigation needed.")
else:
    print("❌ MINIMAL IMPROVEMENT: Discrepancy remains. Review orphaned underenhet logic.")

print("\nDone!")
