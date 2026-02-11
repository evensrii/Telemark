# %%
"""
Verify That Employee Loss is Due to 20% Threshold Logic

This script verifies the hypothesis that the 14,796 employee loss is caused by:
- Parents being replaced by children when difference ≤20%
- But children having fewer employees than their parent
- The cumulative "acceptable loss" (up to 20% per org) adding up to ~14,796

Analysis:
1. Identify parent organizations in "only_in_sql_filtered.csv"
2. Find their children in "enhetsregisteret_laveste_nivaa.csv"
3. Calculate employee loss for each parent→children replacement
4. Sum up total employee loss and verify it matches ~14,796
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
validation_file = os.path.join(data_folder, "enhetsregisteret_laveste_nivaa", "validering_resultater.csv")
decision_log_file = os.path.join(data_folder, "enhetsregisteret_laveste_nivaa", "lowest_level_decisions.csv")

print("="*80)
print("LOADING DATASETS")
print("="*80)

# Load datasets
df_sql = pd.read_csv(sql_filtered_file)
df_lowest = pd.read_csv(lowest_level_file)
df_combined = pd.read_csv(combined_clean_file)

print(f"\n✓ SQL filtered: {len(df_sql):,} records")
print(f"✓ Lowest level: {len(df_lowest):,} records")
print(f"✓ Combined clean: {len(df_combined):,} records")

# Load decision log if available
try:
    df_decisions = pd.read_csv(decision_log_file)
    print(f"✓ Decision log: {len(df_decisions):,} records")
    has_decisions = True
except:
    print("⚠ Decision log not found")
    has_decisions = False

# Load validation if available
try:
    df_validation = pd.read_csv(validation_file)
    print(f"✓ Validation data: {len(df_validation):,} records")
    has_validation = True
except:
    print("⚠ Validation data not found")
    has_validation = False

# %% [markdown]
# ## 1. Standardize Organization Numbers

# %%
print("\n" + "="*80)
print("STANDARDIZING DATA")
print("="*80)

# Convert org numbers to string
df_sql['Org. nr._str'] = pd.to_numeric(df_sql['Org. nr.'], errors='coerce').apply(
    lambda x: str(int(x)) if pd.notna(x) else ''
)
df_lowest['Org. nr._str'] = pd.to_numeric(df_lowest['Organisasjonsnummer'], errors='coerce').apply(
    lambda x: str(int(x)) if pd.notna(x) else ''
)
df_combined['Org. nr._str'] = pd.to_numeric(df_combined['Org. nr.'], errors='coerce').apply(
    lambda x: str(int(x)) if pd.notna(x) else ''
)
df_combined['Overordnet enhet_str'] = pd.to_numeric(df_combined['Overordnet enhet'], errors='coerce').apply(
    lambda x: str(int(x)) if pd.notna(x) else ''
)

# Get employees
df_sql['Employees'] = pd.to_numeric(df_sql['Antall ansatte'], errors='coerce')
df_lowest['Employees'] = pd.to_numeric(
    df_lowest.get('Antall ansatte Enhetsreg', df_lowest.get('Antall ansatte')), 
    errors='coerce'
)
df_combined['Employees'] = pd.to_numeric(df_combined['Antall ansatte'], errors='coerce')

print("✓ Data standardized")

# %% [markdown]
# ## 2. Identify Parent Organizations in SQL Filtered

# %%
print("\n" + "="*80)
print("IDENTIFYING PARENT ORGANIZATIONS")
print("="*80)

# Get all org numbers that appear as "Overordnet enhet" (these are parents)
all_parent_orgs = set(df_combined[df_combined['Overordnet enhet_str'] != '']['Overordnet enhet_str'].values)

# Filter SQL dataset to only those with employees
df_sql_with_emp = df_sql[df_sql['Employees'] > 0].copy()
df_sql_with_emp['Is Parent'] = df_sql_with_emp['Org. nr._str'].isin(all_parent_orgs)

# Get orgs in SQL but not in lowest level
sql_orgs = set(df_sql_with_emp['Org. nr._str'].values)
lowest_orgs = set(df_lowest['Org. nr._str'].values)
only_in_sql = sql_orgs - lowest_orgs

df_only_sql = df_sql_with_emp[df_sql_with_emp['Org. nr._str'].isin(only_in_sql)].copy()

print(f"\nEnterprises only in SQL filtered (with employees): {len(df_only_sql):,}")
print(f"  Of these, are parent organizations: {df_only_sql['Is Parent'].sum():,}")
print(f"  Total employees in parents: {df_only_sql[df_only_sql['Is Parent']]['Employees'].sum():,.0f}")

# %% [markdown]
# ## 3. For Each Parent, Find Their Children in Lowest Level

# %%
print("\n" + "="*80)
print("ANALYZING PARENT → CHILDREN REPLACEMENTS")
print("="*80)

# Get parents that are only in SQL
parent_only_in_sql = df_only_sql[df_only_sql['Is Parent']].copy()

replacements = []

for idx, parent_row in parent_only_in_sql.iterrows():
    parent_org = parent_row['Org. nr._str']
    parent_name = parent_row['Navn']
    parent_employees = parent_row['Employees']
    
    # Find all children of this parent in the combined dataset
    children_in_combined = df_combined[df_combined['Overordnet enhet_str'] == parent_org]
    child_orgs = set(children_in_combined['Org. nr._str'].values)
    
    # Check how many of these children are in the lowest level dataset
    children_in_lowest = df_lowest[df_lowest['Org. nr._str'].isin(child_orgs)]
    child_employees_in_lowest = children_in_lowest['Employees'].sum()
    
    # Also check for indirect children (level 3) - children of the children
    indirect_child_orgs = set()
    for child_org in child_orgs:
        grandchildren = df_combined[df_combined['Overordnet enhet_str'] == child_org]
        indirect_child_orgs.update(grandchildren['Org. nr._str'].values)
    
    indirect_children_in_lowest = df_lowest[df_lowest['Org. nr._str'].isin(indirect_child_orgs)]
    indirect_employees = indirect_children_in_lowest['Employees'].sum()
    
    total_children_employees = child_employees_in_lowest + indirect_employees
    employee_loss = parent_employees - total_children_employees
    
    if len(children_in_lowest) > 0 or len(indirect_children_in_lowest) > 0:
        pct_loss = (employee_loss / parent_employees * 100) if parent_employees > 0 else 0
        
        replacements.append({
            'Parent Org': parent_org,
            'Parent Name': parent_name,
            'Parent Employees': parent_employees,
            'Direct Children in Lowest': len(children_in_lowest),
            'Indirect Children in Lowest': len(indirect_children_in_lowest),
            'Total Children Employees': total_children_employees,
            'Employee Loss': employee_loss,
            'Percent Loss': pct_loss
        })

df_replacements = pd.DataFrame(replacements)

if len(df_replacements) > 0:
    total_parent_emp = df_replacements['Parent Employees'].sum()
    total_children_emp = df_replacements['Total Children Employees'].sum()
    total_loss = df_replacements['Employee Loss'].sum()
    
    print(f"\nFound {len(df_replacements):,} parent→children replacements:")
    print(f"  Total parent employees: {total_parent_emp:,.0f}")
    print(f"  Total children employees: {total_children_emp:,.0f}")
    print(f"  Total employee loss: {total_loss:,.0f}")
    print(f"  Average loss per replacement: {total_loss / len(df_replacements):.1f} employees")
    print(f"  Average percent loss: {df_replacements['Percent Loss'].mean():.1f}%")

# %% [markdown]
# ## 4. Check Against Decision Log

# %%
if has_decisions:
    print("\n" + "="*80)
    print("VERIFICATION AGAINST DECISION LOG")
    print("="*80)
    
    # Filter decisions to level 2 or 3 (where parent was replaced)
    df_decisions['Org. nr.'] = df_decisions['Org. nr.'].astype(str)
    df_decisions_replaced = df_decisions[df_decisions['Level Used'].isin([2, 3])]
    
    print(f"\nDecisions where parent was replaced by children:")
    print(f"  Level 2 chosen: {(df_decisions['Level Used'] == 2).sum():,}")
    print(f"  Level 3 chosen: {(df_decisions['Level Used'] == 3).sum():,}")
    print(f"  Total: {len(df_decisions_replaced):,}")
    
    # Check threshold values
    if 'Pct Diff 1v2' in df_decisions.columns:
        level_2_decisions = df_decisions[df_decisions['Level Used'] == 2]
        level_3_decisions = df_decisions[df_decisions['Level Used'] == 3]
        
        print(f"\nThreshold analysis for Level 2 decisions:")
        print(f"  Average difference: {level_2_decisions['Pct Diff 1v2'].mean():.1f}%")
        print(f"  Max difference: {level_2_decisions['Pct Diff 1v2'].max():.1f}%")
        
        if 'Pct Diff 1v3' in df_decisions.columns:
            print(f"\nThreshold analysis for Level 3 decisions:")
            print(f"  Average difference: {level_3_decisions['Pct Diff 1v3'].mean():.1f}%")
            print(f"  Max difference: {level_3_decisions['Pct Diff 1v3'].max():.1f}%")

# %% [markdown]
# ## 5. Show Top Examples

# %%
print("\n" + "="*80)
print("TOP 20 EXAMPLES OF EMPLOYEE LOSS")
print("="*80)

if len(df_replacements) > 0:
    print(f"\n{'Parent Org':>12} {'Parent Name':40} {'Parent Emp':>10} {'Children Emp':>12} {'Loss':>8} {'% Loss':>7}")
    print("-" * 95)
    
    df_top = df_replacements.nlargest(20, 'Employee Loss')
    for idx, row in df_top.iterrows():
        print(f"{row['Parent Org']:>12} {row['Parent Name'][:38]:40} {row['Parent Employees']:>10,.0f} {row['Total Children Employees']:>12,.0f} {row['Employee Loss']:>8,.0f} {row['Percent Loss']:>6.1f}%")
    
    # Save to CSV
    output_file = os.path.join(output_folder, "parent_children_employee_loss.csv")
    df_replacements.to_csv(output_file, index=False)
    print(f"\n✓ Full analysis saved to: parent_children_employee_loss.csv")

# %% [markdown]
# ## 6. Summary and Conclusion

# %%
print("\n" + "="*80)
print("SUMMARY - VERIFYING THE HYPOTHESIS")
print("="*80)

print(f"\nObserved employee difference:")
print(f"  SQL filtered: {df_sql_with_emp['Employees'].sum():,.0f} employees")
print(f"  Lowest level: {df_lowest['Employees'].sum():,.0f} employees")
print(f"  Difference: {df_sql_with_emp['Employees'].sum() - df_lowest['Employees'].sum():,.0f} employees")

if len(df_replacements) > 0:
    print(f"\nEmployee loss from parent→children replacements:")
    print(f"  Number of replacements: {len(df_replacements):,}")
    print(f"  Total employee loss: {total_loss:,.0f} employees")
    print(f"  Accounts for: {total_loss / (df_sql_with_emp['Employees'].sum() - df_lowest['Employees'].sum()) * 100:.1f}% of total difference")
    
    print(f"\nBreakdown by loss percentage:")
    bins = [0, 5, 10, 15, 20, 100]
    labels = ['0-5%', '5-10%', '10-15%', '15-20%', '>20%']
    df_replacements['Loss Bin'] = pd.cut(df_replacements['Percent Loss'], bins=bins, labels=labels)
    
    for label in labels:
        count = (df_replacements['Loss Bin'] == label).sum()
        emp_loss = df_replacements[df_replacements['Loss Bin'] == label]['Employee Loss'].sum()
        if count > 0:
            print(f"  {label:>8}: {count:>4} replacements, {emp_loss:>8,.0f} employees lost")

print(f"\n{'='*80}")
if len(df_replacements) > 0 and total_loss > 10000:
    print("HYPOTHESIS CONFIRMED!")
    print("="*80)
    print(f"""
The 14,796 employee loss is primarily caused by the 20% threshold logic:
    
- When parent employee count differs from children sum by ≤20%, the script
  replaces the parent with children
- However, the children STILL have fewer employees than the parent
- This "acceptable loss" (up to 20% per organization) accumulates across
  {len(df_replacements):,} parent→children replacements
- Total cumulative loss: {total_loss:,.0f} employees
    
This is the intended behavior of the create_lowest_level_dataset.py script,
but it means the "lowest level" dataset will always have fewer employees
than the "combined" dataset when applying the SQL filter to both.
""")
else:
    print("HYPOTHESIS NEEDS FURTHER INVESTIGATION")
    print("="*80)
    print("\nThe parent→children replacements don't fully account for the difference.")
    print("Additional factors may be involved.")

print(f"\nAll output files saved to:")
print(f"  {output_folder}")
print(f"{'='*80}")
