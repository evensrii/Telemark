# %%
"""
Compare SQL-Filtered Combined vs Lowest Level Datasets

This script compares:
- enhetsreg_kombi_sql_filtrerte_virksomheter.csv (3,769 enterprises, 93,052 employees)
- enhetsregisteret_laveste_nivaa.csv (2,974 enterprises, 78,256 employees)

Goal: Identify which enterprises WITH employees differ and WHY
"""

import pandas as pd
import os

# Define paths
data_folder = r"c:\Users\eve1509\OneDrive - Telemark fylkeskommune\Github\Telemark\Data\03_Arbeid og næringsliv\02_Næringsliv\Virksomheter"
output_folder = os.path.join(data_folder, "enhetsreg_lavnivaa vs enhetsreg_kombi")

# Input files
sql_filtered_file = os.path.join(data_folder, "enhetsreg_kombi_sql_filtrerte_virksomheter.csv")
lowest_level_file = os.path.join(data_folder, "enhetsregisteret_laveste_nivaa.csv")
combined_clean_file = os.path.join(data_folder, "enhetsregisteret_kombinert_clean.csv")

print("="*80)
print("LOADING DATASETS")
print("="*80)

# Load datasets
df_sql = pd.read_csv(sql_filtered_file)
df_lowest = pd.read_csv(lowest_level_file)
df_combined_clean = pd.read_csv(combined_clean_file)

print(f"\n✓ SQL filtered combined: {len(df_sql):,} records")
print(f"✓ Lowest level: {len(df_lowest):,} records")
print(f"✓ Combined clean (for reference): {len(df_combined_clean):,} records")

# %% [markdown]
# ## 1. Standardize Organization Numbers

# %%
print("\n" + "="*80)
print("STANDARDIZING DATA")
print("="*80)

# Convert org numbers to string for consistent matching
df_sql['Org. nr._str'] = pd.to_numeric(df_sql['Org. nr.'], errors='coerce').apply(
    lambda x: str(int(x)) if pd.notna(x) else ''
)
df_lowest['Org. nr._str'] = pd.to_numeric(df_lowest['Organisasjonsnummer'], errors='coerce').apply(
    lambda x: str(int(x)) if pd.notna(x) else ''
)
df_combined_clean['Org. nr._str'] = pd.to_numeric(df_combined_clean['Org. nr.'], errors='coerce').apply(
    lambda x: str(int(x)) if pd.notna(x) else ''
)

# Get employee columns
df_sql['Employees'] = pd.to_numeric(df_sql['Antall ansatte'], errors='coerce')
df_lowest['Employees'] = pd.to_numeric(
    df_lowest.get('Antall ansatte Enhetsreg', df_lowest.get('Antall ansatte')), 
    errors='coerce'
)
df_combined_clean['Employees'] = pd.to_numeric(df_combined_clean['Antall ansatte'], errors='coerce')

# Filter to only include enterprises WITH employees (>0)
df_sql_with_emp = df_sql[df_sql['Employees'] > 0].copy()
df_lowest_with_emp = df_lowest[df_lowest['Employees'] > 0].copy()

print(f"\nEnterprises with employees (>0):")
print(f"  SQL filtered: {len(df_sql_with_emp):,} enterprises, {df_sql_with_emp['Employees'].sum():,.0f} employees")
print(f"  Lowest level: {len(df_lowest_with_emp):,} enterprises, {df_lowest_with_emp['Employees'].sum():,.0f} employees")

# %% [markdown]
# ## 2. Identify Which Enterprises Differ

# %%
print("\n" + "="*80)
print("ENTERPRISE COMPARISON")
print("="*80)

sql_orgs = set(df_sql_with_emp['Org. nr._str'].values)
lowest_orgs = set(df_lowest_with_emp['Org. nr._str'].values)

only_in_sql = sql_orgs - lowest_orgs
only_in_lowest = lowest_orgs - sql_orgs
in_both = sql_orgs & lowest_orgs

print(f"\nOrganization overlap (with employees):")
print(f"  In both datasets: {len(in_both):,}")
print(f"  Only in SQL filtered: {len(only_in_sql):,}")
print(f"  Only in lowest level: {len(only_in_lowest):,}")

# %% [markdown]
# ## 3. Analyze Enterprises ONLY in SQL Filtered

# %%
print("\n" + "="*80)
print("ENTERPRISES ONLY IN SQL FILTERED (NOT IN LOWEST LEVEL)")
print("="*80)

if len(only_in_sql) > 0:
    df_only_sql = df_sql_with_emp[df_sql_with_emp['Org. nr._str'].isin(only_in_sql)].copy()
    total_emp_only_sql = df_only_sql['Employees'].sum()
    
    print(f"\nCount: {len(df_only_sql):,} enterprises")
    print(f"Total employees: {total_emp_only_sql:,.0f}")
    print(f"Average employees: {total_emp_only_sql / len(df_only_sql):.1f}")
    
    # Check if these exist in the combined_clean file
    df_only_sql['In Combined Clean'] = df_only_sql['Org. nr._str'].isin(df_combined_clean['Org. nr._str'])
    in_combined_clean = df_only_sql['In Combined Clean'].sum()
    
    print(f"\nPresence in combined_clean.csv:")
    print(f"  Found: {in_combined_clean:,}")
    print(f"  Not found: {len(df_only_sql) - in_combined_clean:,}")
    
    # For those in combined_clean, check if they are parents (have "Overordnet enhet" pointing to them)
    df_combined_clean['Overordnet enhet_str'] = pd.to_numeric(df_combined_clean['Overordnet enhet'], errors='coerce').apply(
        lambda x: str(int(x)) if pd.notna(x) else ''
    )
    parent_orgs = set(df_combined_clean['Overordnet enhet_str'].values) - {''}
    
    df_only_sql['Is Parent'] = df_only_sql['Org. nr._str'].isin(parent_orgs)
    is_parent_count = df_only_sql['Is Parent'].sum()
    
    # Also check if they HAVE a parent
    df_only_sql = df_only_sql.merge(
        df_combined_clean[['Org. nr._str', 'Overordnet enhet_str']].rename(columns={'Overordnet enhet_str': 'Parent'}),
        on='Org. nr._str',
        how='left'
    )
    has_parent_count = (df_only_sql['Parent'] != '').sum()
    
    print(f"\nParent-child analysis:")
    print(f"  Are parents (other orgs point to them): {is_parent_count:,}")
    print(f"  Have parent (they point to another org): {has_parent_count:,}")
    print(f"  Neither parent nor child: {len(df_only_sql) - is_parent_count - has_parent_count + df_only_sql[(df_only_sql['Is Parent']) & (df_only_sql['Parent'] != '')].shape[0]:,}")
    
    # Get parent employee totals
    parent_employees = df_only_sql[df_only_sql['Is Parent']]['Employees'].sum()
    print(f"\nEmployees in parent organizations: {parent_employees:,.0f} ({parent_employees/total_emp_only_sql*100:.1f}% of total)")
    
    # Show top 20 by employee count
    print(f"\nTop 20 enterprises by employee count:")
    print(f"{'Org.nr.':>12} {'Name':50} {'Employees':>10} {'Parent?':>8} {'Has Parent':>12}")
    print("-" * 95)
    
    df_top = df_only_sql.nlargest(20, 'Employees')
    for idx, row in df_top.iterrows():
        org_nr = row['Org. nr._str']
        name = row.get('Navn', 'N/A')[:48]
        employees = row['Employees']
        is_parent = "YES" if row['Is Parent'] else ""
        parent = row['Parent'] if row['Parent'] != '' else ""
        print(f"{org_nr:>12} {name:50} {employees:>10,.0f} {is_parent:>8} {parent:>12}")
    
    # Save to CSV
    output_file = os.path.join(output_folder, "only_in_sql_filtered.csv")
    df_only_sql.to_csv(output_file, index=False)
    print(f"\n✓ Full list saved to: only_in_sql_filtered.csv")

# %% [markdown]
# ## 4. Analyze Enterprises ONLY in Lowest Level

# %%
print("\n" + "="*80)
print("ENTERPRISES ONLY IN LOWEST LEVEL (NOT IN SQL FILTERED)")
print("="*80)

if len(only_in_lowest) > 0:
    df_only_lowest = df_lowest_with_emp[df_lowest_with_emp['Org. nr._str'].isin(only_in_lowest)].copy()
    total_emp_only_lowest = df_only_lowest['Employees'].sum()
    
    print(f"\nCount: {len(df_only_lowest):,} enterprises")
    print(f"Total employees: {total_emp_only_lowest:,.0f}")
    print(f"Average employees: {total_emp_only_lowest / len(df_only_lowest):.1f}")
    
    # Check level distribution if available
    if 'Nivå enhetsregisteret' in df_only_lowest.columns:
        level_dist = df_only_lowest['Nivå enhetsregisteret'].value_counts().sort_index()
        print(f"\nLevel distribution:")
        for level, count in level_dist.items():
            level_emp = df_only_lowest[df_only_lowest['Nivå enhetsregisteret'] == level]['Employees'].sum()
            print(f"  Level {level}: {count:,} enterprises, {level_emp:,.0f} employees")
    
    # Check if these exist in combined_clean (they shouldn't if they passed SQL filter in lowest level logic)
    df_only_lowest['In Combined Clean'] = df_only_lowest['Org. nr._str'].isin(df_combined_clean['Org. nr._str'])
    in_combined_clean_count = df_only_lowest['In Combined Clean'].sum()
    
    print(f"\nPresence in combined_clean.csv:")
    print(f"  Found: {in_combined_clean_count:,}")
    print(f"  Not found: {len(df_only_lowest) - in_combined_clean_count:,}")
    
    # For those in combined_clean, why were they filtered out by SQL query?
    if in_combined_clean_count > 0:
        df_in_combined = df_only_lowest[df_only_lowest['In Combined Clean']].copy()
        df_in_combined = df_in_combined.merge(
            df_combined_clean[['Org. nr._str', 'Overordnet enhet_str']],
            on='Org. nr._str',
            how='left'
        )
        
        # Check if they would be removed by SQL filter (org nr appears in Overordnet enhet column)
        df_in_combined['Would Be Filtered'] = df_in_combined['Org. nr._str'].isin(parent_orgs)
        would_be_filtered = df_in_combined['Would Be Filtered'].sum()
        
        print(f"\nWhy filtered out from SQL query:")
        print(f"  Would be removed (org nr in Overordnet enhet): {would_be_filtered:,}")
        print(f"  Other reason: {in_combined_clean_count - would_be_filtered:,}")
    
    # Show top 15 by employee count
    print(f"\nTop 15 enterprises by employee count:")
    print(f"{'Org.nr.':>12} {'Name':50} {'Employees':>10} {'Level':>6}")
    print("-" * 85)
    
    df_top = df_only_lowest.nlargest(15, 'Employees')
    for idx, row in df_top.iterrows():
        org_nr = row['Org. nr._str']
        name = row.get('Navn', 'N/A')[:48]
        employees = row['Employees']
        level = row.get('Nivå enhetsregisteret', 'N/A')
        print(f"{org_nr:>12} {name:50} {employees:>10,.0f} {str(level):>6}")
    
    # Save to CSV
    output_file = os.path.join(output_folder, "only_in_lowest_level.csv")
    df_only_lowest.to_csv(output_file, index=False)
    print(f"\n✓ Full list saved to: only_in_lowest_level.csv")

# %% [markdown]
# ## 5. Summary and Root Cause Analysis

# %%
print("\n" + "="*80)
print("SUMMARY - ACCOUNTING FOR THE 14,796 EMPLOYEE DIFFERENCE")
print("="*80)

print(f"\nStarting point (SQL filtered combined):")
print(f"  Enterprises with employees: {len(df_sql_with_emp):,}")
print(f"  Total employees: {df_sql_with_emp['Employees'].sum():,.0f}")

print(f"\nEnding point (lowest level):")
print(f"  Enterprises with employees: {len(df_lowest_with_emp):,}")
print(f"  Total employees: {df_lowest_with_emp['Employees'].sum():,.0f}")

print(f"\nTotal difference:")
print(f"  Enterprises: {len(df_sql_with_emp) - len(df_lowest_with_emp):,}")
print(f"  Employees: {df_sql_with_emp['Employees'].sum() - df_lowest_with_emp['Employees'].sum():,.0f}")

if len(only_in_sql) > 0:
    print(f"\nEmployees in enterprises ONLY in SQL filtered:")
    print(f"  {total_emp_only_sql:,.0f} employees in {len(only_in_sql):,} enterprises")
    if is_parent_count > 0:
        print(f"  → {is_parent_count:,} of these are PARENT organizations ({parent_employees:,.0f} employees)")
        print(f"     (These were likely replaced by their children in the lowest level dataset)")

if len(only_in_lowest) > 0:
    print(f"\nEmployees in enterprises ONLY in lowest level:")
    print(f"  {total_emp_only_lowest:,.0f} employees in {len(only_in_lowest):,} enterprises")
    print(f"  → These are likely CHILDREN that replaced parents")

net_employee_diff = total_emp_only_sql - total_emp_only_lowest if len(only_in_sql) > 0 and len(only_in_lowest) > 0 else 0
if abs(net_employee_diff) > 0:
    print(f"\nNet employee difference from substitution:")
    print(f"  Parents removed: -{total_emp_only_sql:,.0f}")
    print(f"  Children added: +{total_emp_only_lowest:,.0f}")
    print(f"  Net loss: {net_employee_diff:,.0f}")
    print(f"\n  → This accounts for {abs(net_employee_diff)/(df_sql_with_emp['Employees'].sum() - df_lowest_with_emp['Employees'].sum())*100:.1f}% of the total difference")

print(f"\n{'='*80}")
print(f"LIKELY ROOT CAUSE:")
print(f"{'='*80}")
print(f"""
The 'lowest level' transformation replaces parent organizations with their children
based on the 20% threshold logic. However, when a parent has MORE employees than
the sum of its children (>20% difference), the logic keeps the parent at Level 1.

This creates a mismatch because:
1. Some parents in SQL-filtered combined are replaced by children in lowest level
2. The children may have FEWER total employees than their parent
3. This causes an employee loss during the transformation

The {net_employee_diff:,.0f} employee loss suggests that parent employee counts
are NOT being fully captured when drilling down to children.
""")

print(f"All output files saved to:")
print(f"  {output_folder}")
print(f"{'='*80}")
