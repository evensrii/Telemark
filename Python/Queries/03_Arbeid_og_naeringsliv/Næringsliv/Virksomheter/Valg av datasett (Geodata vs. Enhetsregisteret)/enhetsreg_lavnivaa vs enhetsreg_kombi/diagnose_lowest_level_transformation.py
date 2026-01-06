# %%
"""
Diagnose Employee Loss in "Lowest Level" Transformation

This script investigates why the enhetsregisteret_laveste_nivaa.csv dataset
(2,974 enterprises, 78,256 employees) has significantly fewer employees than
the SQL-filtered combined dataset (3,769 enterprises, 93,052 employees).

Key question: What happens during the "lowest level" transformation that causes
a loss of 14,796 employees (15.9%)?

Analysis approach:
1. Compare BEFORE SQL filtering to isolate transformation impact
2. Identify which organizations are excluded or changed
3. Check if the 20% threshold logic is causing data loss
4. Verify if parent organizations are being properly replaced by children
"""

import pandas as pd
import os

# Define paths
data_folder = r"c:\Users\eve1509\OneDrive - Telemark fylkeskommune\Github\Telemark\Data\03_Arbeid og næringsliv\02_Næringsliv\Virksomheter"
output_folder = os.path.join(data_folder, "enhetsreg_lavnivaa vs enhetsreg_kombi")

# Input files
combined_file = os.path.join(data_folder, "enhetsregisteret_kombinert_clean.csv")
lowest_level_file = os.path.join(data_folder, "enhetsregisteret_laveste_nivaa.csv")
hovedenheter_file = os.path.join(data_folder, "enhetsregisteret_hovedenheter.csv")
underenheter_file = os.path.join(data_folder, "enhetsregisteret_underenheter.csv")

print("="*80)
print("LOADING DATASETS")
print("="*80)

# Load datasets
df_combined = pd.read_csv(combined_file)
df_lowest = pd.read_csv(lowest_level_file)
df_hoved = pd.read_csv(hovedenheter_file, low_memory=False)
df_under = pd.read_csv(underenheter_file)

print(f"\n✓ Combined clean: {len(df_combined):,} records")
print(f"✓ Lowest level: {len(df_lowest):,} records")
print(f"✓ Hovedenheter: {len(df_hoved):,} records")
print(f"✓ Underenheter: {len(df_under):,} records")

# %% [markdown]
# ## 1. Employee Totals BEFORE Any Filtering

# %%
print("\n" + "="*80)
print("EMPLOYEE TOTALS - BEFORE ANY SQL FILTERING")
print("="*80)

# Get employee counts
emp_combined = pd.to_numeric(df_combined['Antall ansatte'], errors='coerce')
emp_lowest = pd.to_numeric(df_lowest.get('Antall ansatte Enhetsreg', df_lowest.get('Antall ansatte')), errors='coerce')
emp_hoved = pd.to_numeric(df_hoved['Antall ansatte'], errors='coerce')
emp_under = pd.to_numeric(df_under['Antall ansatte'], errors='coerce')

print(f"\nRaw source files:")
print(f"  Hovedenheter: {len(df_hoved):,} enterprises, {emp_hoved.sum():,.0f} total employees")
print(f"  Underenheter: {len(df_under):,} enterprises, {emp_under.sum():,.0f} total employees")
print(f"  Combined (hovedenheter + underenheter): {len(df_hoved) + len(df_under):,} enterprises")

print(f"\nTransformed datasets:")
print(f"  Combined clean: {len(df_combined):,} enterprises, {emp_combined.sum():,.0f} total employees")
print(f"  Lowest level: {len(df_lowest):,} enterprises, {emp_lowest.sum():,.0f} total employees")

print(f"\nEmployee difference:")
print(f"  Combined - Lowest = {emp_combined.sum() - emp_lowest.sum():,.0f} employees lost")
print(f"  That's {(1 - emp_lowest.sum()/emp_combined.sum())*100:.1f}% reduction")

# %% [markdown]
# ## 2. Identify Level Distribution in Lowest Level Dataset

# %%
print("\n" + "="*80)
print("LEVEL DISTRIBUTION IN LOWEST LEVEL DATASET")
print("="*80)

if 'Nivå enhetsregisteret' in df_lowest.columns:
    level_dist = df_lowest['Nivå enhetsregisteret'].value_counts().sort_index()
    print(f"\nEnterprises by level:")
    for level, count in level_dist.items():
        level_df = df_lowest[df_lowest['Nivå enhetsregisteret'] == level]
        level_employees = pd.to_numeric(level_df.get('Antall ansatte Enhetsreg', level_df.get('Antall ansatte')), errors='coerce').sum()
        print(f"  Level {level}: {count:,} enterprises, {level_employees:,.0f} employees")
else:
    print("\n⚠ 'Nivå enhetsregisteret' column not found in lowest level dataset")

# %% [markdown]
# ## 3. Check for Duplicate Counting in Combined vs Hierarchical Logic

# %%
print("\n" + "="*80)
print("DUPLICATE/HIERARCHY ANALYSIS")
print("="*80)

# In the combined file, both parents and children are included
# This could lead to "double counting" if we're not careful

# Get parent-child relationships
df_combined['Org. nr._str'] = pd.to_numeric(df_combined.get('Org. nr.', df_combined.get('Organisasjonsnummer')), errors='coerce').apply(
    lambda x: str(int(x)) if pd.notna(x) else ''
)
df_combined['Overordnet enhet_str'] = pd.to_numeric(df_combined['Overordnet enhet'], errors='coerce').apply(
    lambda x: str(int(x)) if pd.notna(x) else ''
)

# Count how many enterprises in combined have a parent that's also in combined
combined_orgs = set(df_combined['Org. nr._str'].values)
has_parent_in_dataset = df_combined['Overordnet enhet_str'].isin(combined_orgs)
enterprises_with_parent = has_parent_in_dataset.sum()

print(f"\nIn combined dataset:")
print(f"  Total enterprises: {len(df_combined):,}")
print(f"  Enterprises with parent also in dataset: {enterprises_with_parent:,}")
print(f"  Enterprises without parent in dataset: {len(df_combined) - enterprises_with_parent:,}")

# Get employees for each group
df_with_parent = df_combined[has_parent_in_dataset]
df_without_parent = df_combined[~has_parent_in_dataset]

emp_with_parent = pd.to_numeric(df_with_parent['Antall ansatte'], errors='coerce').sum()
emp_without_parent = pd.to_numeric(df_without_parent['Antall ansatte'], errors='coerce').sum()

print(f"\nEmployees by parent status:")
print(f"  With parent in dataset: {emp_with_parent:,.0f} employees ({enterprises_with_parent:,} enterprises)")
print(f"  Without parent in dataset: {emp_without_parent:,.0f} employees ({len(df_without_parent):,} enterprises)")

# %% [markdown]
# ## 4. Identify Missing Organizations

# %%
print("\n" + "="*80)
print("MISSING ORGANIZATIONS ANALYSIS")
print("="*80)

# Convert lowest level org numbers to string
df_lowest['Org. nr._str'] = pd.to_numeric(df_lowest.get('Organisasjonsnummer', df_lowest.get('Org. nr.')), errors='coerce').apply(
    lambda x: str(int(x)) if pd.notna(x) else ''
)

combined_orgs = set(df_combined['Org. nr._str'].values)
lowest_orgs = set(df_lowest['Org. nr._str'].values)

only_in_combined = combined_orgs - lowest_orgs
only_in_lowest = lowest_orgs - combined_orgs

print(f"\nOrganization comparison:")
print(f"  In both datasets: {len(combined_orgs & lowest_orgs):,}")
print(f"  Only in combined: {len(only_in_combined):,}")
print(f"  Only in lowest: {len(only_in_lowest):,}")

# Analyze enterprises only in combined
if len(only_in_combined) > 0:
    df_only_combined = df_combined[df_combined['Org. nr._str'].isin(only_in_combined)]
    emp_only_combined = pd.to_numeric(df_only_combined['Antall ansatte'], errors='coerce').sum()
    
    print(f"\nEnterprises ONLY in combined dataset:")
    print(f"  Count: {len(df_only_combined):,}")
    print(f"  Total employees: {emp_only_combined:,.0f}")
    print(f"  Average employees: {emp_only_combined / len(df_only_combined):.1f}")
    
    # Check if these are mostly parents
    has_parent = df_only_combined['Overordnet enhet_str'].isin(combined_orgs)
    is_parent = df_only_combined['Org. nr._str'].isin(df_combined['Overordnet enhet_str'].values)
    
    print(f"\n  Breakdown:")
    print(f"    - Are children (have parent in dataset): {has_parent.sum():,}")
    print(f"    - Are parents (someone points to them): {is_parent.sum():,}")
    print(f"    - Neither parent nor child: {(~has_parent & ~is_parent).sum():,}")
    
    # Show top examples by employee count
    print(f"\n  Top 10 examples by employee count:")
    df_top_examples = df_only_combined.nlargest(10, 'Antall ansatte')
    for idx, row in df_top_examples.iterrows():
        org_nr = row['Org. nr._str']
        navn = row.get('Navn', 'N/A')
        employees = row['Antall ansatte']
        parent = row['Overordnet enhet_str']
        is_parent_flag = "IS PARENT" if org_nr in df_combined['Overordnet enhet_str'].values else ""
        parent_info = f" (parent: {parent})" if parent and parent != '' else " (no parent)"
        print(f"    {org_nr}: {navn[:50]:50} - {employees:>6.0f} emp {parent_info} {is_parent_flag}")
    
    # Save full list to CSV
    output_file = os.path.join(output_folder, "enterprises_only_in_combined.csv")
    df_only_combined.to_csv(output_file, index=False)
    print(f"\n  ✓ Full list saved to: enterprises_only_in_combined.csv")

# %%[markdown]
# ## 5. Check for Employee Count Mismatches

# %%
print("\n" + "="*80)
print("EMPLOYEE COUNT MISMATCHES")
print("="*80)

# For organizations in both datasets, check if employee counts match
common_orgs = combined_orgs & lowest_orgs
df_combined_common = df_combined[df_combined['Org. nr._str'].isin(common_orgs)].copy()
df_lowest_common = df_lowest[df_lowest['Org. nr._str'].isin(common_orgs)].copy()

# Create lookup for comparison
combined_lookup = df_combined_common.set_index('Org. nr._str')['Antall ansatte'].apply(lambda x: pd.to_numeric(x, errors='coerce'))
lowest_lookup = df_lowest_common.set_index('Org. nr._str')[df_lowest_common.columns[df_lowest_common.columns.str.contains('Antall ansatte')][0]].apply(lambda x: pd.to_numeric(x, errors='coerce'))

# Find mismatches
mismatches = []
for org in common_orgs:
    if org in combined_lookup.index and org in lowest_lookup.index:
        emp_combined = combined_lookup[org]
        emp_lowest = lowest_lookup[org]
        if pd.notna(emp_combined) and pd.notna(emp_lowest) and emp_combined != emp_lowest:
            mismatches.append({
                'Org. nr.': org,
                'Employees Combined': emp_combined,
                'Employees Lowest': emp_lowest,
                'Difference': emp_combined - emp_lowest
            })

if len(mismatches) > 0:
    df_mismatches = pd.DataFrame(mismatches)
    total_diff = df_mismatches['Difference'].sum()
    
    print(f"\nFound {len(mismatches):,} organizations with different employee counts:")
    print(f"  Total employee difference: {total_diff:,.0f}")
    print(f"\n  Top 10 by absolute difference:")
    df_top_mismatches = df_mismatches.reindex(df_mismatches['Difference'].abs().sort_values(ascending=False).index).head(10)
    for idx, row in df_top_mismatches.iterrows():
        print(f"    {row['Org. nr.']}: Combined={row['Employees Combined']:.0f}, Lowest={row['Employees Lowest']:.0f}, Diff={row['Difference']:.0f}")
    
    # Save to CSV
    output_file = os.path.join(output_folder, "employee_count_mismatches.csv")
    df_mismatches.to_csv(output_file, index=False)
    print(f"\n  ✓ Full list saved to: employee_count_mismatches.csv")
else:
    print(f"\n✓ No employee count mismatches found for common organizations")

# %% [markdown]
# ## 6. Summary and Conclusions

# %%
print("\n" + "="*80)
print("SUMMARY - WHERE DID THE 14,796 EMPLOYEES GO?")
print("="*80)

print(f"\nStarting point (combined clean):")
print(f"  Enterprises: {len(df_combined):,}")
print(f"  Employees: {emp_combined.sum():,.0f}")

print(f"\nEnding point (lowest level):")
print(f"  Enterprises: {len(df_lowest):,}")
print(f"  Employees: {emp_lowest.sum():,.0f}")

print(f"\nTotal loss:")
print(f"  Enterprises: {len(df_combined) - len(df_lowest):,} ({(1 - len(df_lowest)/len(df_combined))*100:.1f}% reduction)")
print(f"  Employees: {emp_combined.sum() - emp_lowest.sum():,.0f} ({(1 - emp_lowest.sum()/emp_combined.sum())*100:.1f}% reduction)")

print(f"\nAccounting for the loss:")
if len(only_in_combined) > 0:
    print(f"  1. Enterprises removed entirely: {len(only_in_combined):,} enterprises, {emp_only_combined:,.0f} employees")
if len(mismatches) > 0:
    print(f"  2. Employee count differences: {len(mismatches):,} enterprises, {total_diff:,.0f} employee difference")

remaining_unexplained = (emp_combined.sum() - emp_lowest.sum()) - emp_only_combined - (total_diff if len(mismatches) > 0 else 0)
if abs(remaining_unexplained) > 1:
    print(f"  3. Unexplained: {remaining_unexplained:,.0f} employees")
else:
    print(f"\n✓ Full discrepancy accounted for!")

print(f"\n{'='*80}")
print(f"All output files saved to:")
print(f"  {output_folder}")
print(f"{'='*80}")
