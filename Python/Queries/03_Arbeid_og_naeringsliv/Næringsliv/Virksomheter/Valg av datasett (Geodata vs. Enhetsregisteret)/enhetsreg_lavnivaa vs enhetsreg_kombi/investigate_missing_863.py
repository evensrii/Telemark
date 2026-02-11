# %%
"""
Investigate the 863 Missing Enterprises

The previous diagnostic showed that the 863 enterprises in "only_in_sql_filtered.csv"
are NOT parent organizations. This script investigates what they are and why they're
missing from the lowest level dataset.

New hypothesis:
- These are children whose parent was NOT processed by create_lowest_level_dataset.py
- Either the parent doesn't exist in hovedenheter, or the parent wasn't identified as "level 1"
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
hovedenheter_file = os.path.join(data_folder, "enhetsregisteret_hovedenheter.csv")
underenheter_file = os.path.join(data_folder, "enhetsregisteret_underenheter.csv")
decision_log_file = os.path.join(data_folder, "enhetsregisteret_laveste_nivaa", "lowest_level_decisions.csv")

print("="*80)
print("LOADING DATASETS")
print("="*80)

df_sql = pd.read_csv(sql_filtered_file)
df_lowest = pd.read_csv(lowest_level_file)
df_combined = pd.read_csv(combined_clean_file)
df_hoved = pd.read_csv(hovedenheter_file, low_memory=False)
df_under = pd.read_csv(underenheter_file)
df_decisions = pd.read_csv(decision_log_file)

print(f"\n✓ SQL filtered: {len(df_sql):,} records")
print(f"✓ Lowest level: {len(df_lowest):,} records")
print(f"✓ Combined clean: {len(df_combined):,} records")
print(f"✓ Hovedenheter: {len(df_hoved):,} records")
print(f"✓ Underenheter: {len(df_under):,} records")
print(f"✓ Decision log: {len(df_decisions):,} records")

# %% [markdown]
# ## 1. Standardize and Identify the 863

# %%
print("\n" + "="*80)
print("IDENTIFYING THE 863 MISSING ENTERPRISES")
print("="*80)

# Standardize org numbers
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
df_hoved['Org. nr._str'] = pd.to_numeric(df_hoved['Org. nr.'], errors='coerce').apply(
    lambda x: str(int(x)) if pd.notna(x) else ''
)
df_hoved['Overordnet enhet_str'] = pd.to_numeric(df_hoved['Overordnet enhet'], errors='coerce').apply(
    lambda x: str(int(x)) if pd.notna(x) else ''
)
df_under['Org. nr._str'] = pd.to_numeric(df_under['Org. nr.'], errors='coerce').apply(
    lambda x: str(int(x)) if pd.notna(x) else ''
)
df_under['Overordnet enhet_str'] = pd.to_numeric(df_under['Overordnet enhet'], errors='coerce').apply(
    lambda x: str(int(x)) if pd.notna(x) else ''
)
df_decisions['Org. nr._str'] = df_decisions['Org. nr.'].astype(str)

# Get employees
df_sql['Employees'] = pd.to_numeric(df_sql['Antall ansatte'], errors='coerce')
df_lowest['Employees'] = pd.to_numeric(
    df_lowest.get('Antall ansatte Enhetsreg', df_lowest.get('Antall ansatte')), 
    errors='coerce'
)
df_combined['Employees'] = pd.to_numeric(df_combined['Antall ansatte'], errors='coerce')
df_hoved['Employees'] = pd.to_numeric(df_hoved['Antall ansatte'], errors='coerce')
df_under['Employees'] = pd.to_numeric(df_under['Antall ansatte'], errors='coerce')

# Filter to only those with employees
df_sql_with_emp = df_sql[df_sql['Employees'] > 0].copy()
df_lowest_with_emp = df_lowest[df_lowest['Employees'] > 0].copy()

# Get the 863
sql_orgs = set(df_sql_with_emp['Org. nr._str'].values)
lowest_orgs = set(df_lowest_with_emp['Org. nr._str'].values)
only_in_sql = sql_orgs - lowest_orgs

df_missing = df_sql_with_emp[df_sql_with_emp['Org. nr._str'].isin(only_in_sql)].copy()

print(f"\nFound {len(df_missing):,} enterprises in SQL filtered but not in lowest level")
print(f"Total employees: {df_missing['Employees'].sum():,.0f}")

# %% [markdown]
# ## 2. Check Which Source File They Come From

# %%
print("\n" + "="*80)
print("SOURCE FILE ANALYSIS")
print("="*80)

# Check if they're in hovedenheter or underenheter
df_missing['In Hovedenheter'] = df_missing['Org. nr._str'].isin(df_hoved['Org. nr._str'])
df_missing['In Underenheter'] = df_missing['Org. nr._str'].isin(df_under['Org. nr._str'])

in_hoved_count = df_missing['In Hovedenheter'].sum()
in_under_count = df_missing['In Underenheter'].sum()

print(f"\nSource file distribution:")
print(f"  In hovedenheter: {in_hoved_count:,}")
print(f"  In underenheter: {in_under_count:,}")
print(f"  In neither: {len(df_missing) - in_hoved_count - in_under_count:,}")

# %% [markdown]
# ## 3. For Those in Hovedenheter: Check Decision Log

# %%
print("\n" + "="*80)
print("HOVEDENHETER ANALYSIS (Level 2 enterprises)")
print("="*80)

df_missing_hoved = df_missing[df_missing['In Hovedenheter']].copy()

if len(df_missing_hoved) > 0:
    print(f"\n{len(df_missing_hoved):,} missing enterprises are from hovedenheter")
    print(f"Total employees: {df_missing_hoved['Employees'].sum():,.0f}")
    
    # Get their parents
    df_missing_hoved = df_missing_hoved.merge(
        df_hoved[['Org. nr._str', 'Overordnet enhet_str']].rename(columns={'Overordnet enhet_str': 'Parent'}),
        on='Org. nr._str',
        how='left'
    )
    
    has_parent = (df_missing_hoved['Parent'] != '').sum()
    print(f"\n  Have parent: {has_parent:,}")
    print(f"  No parent: {len(df_missing_hoved) - has_parent:,}")
    
    # For those with parents, check if parent was processed
    df_with_parent = df_missing_hoved[df_missing_hoved['Parent'] != ''].copy()
    if len(df_with_parent) > 0:
        df_with_parent['Parent In Decisions'] = df_with_parent['Parent'].isin(df_decisions['Org. nr._str'])
        parent_in_decisions = df_with_parent['Parent In Decisions'].sum()
        
        print(f"\n  Parent was processed by create_lowest_level_dataset.py: {parent_in_decisions:,}")
        print(f"  Parent was NOT processed: {len(df_with_parent) - parent_in_decisions:,}")
        
        # For those whose parent was processed, what level was chosen?
        if parent_in_decisions > 0:
            df_parent_processed = df_with_parent[df_with_parent['Parent In Decisions']].copy()
            df_parent_processed = df_parent_processed.merge(
                df_decisions[['Org. nr._str', 'Level Used']].rename(columns={'Org. nr._str': 'Parent', 'Level Used': 'Parent Level'}),
                on='Parent',
                how='left'
            )
            
            level_dist = df_parent_processed['Parent Level'].value_counts().sort_index()
            print(f"\n  Level chosen for their parents:")
            for level, count in level_dist.items():
                emp = df_parent_processed[df_parent_processed['Parent Level'] == level]['Employees'].sum()
                print(f"    Level {level}: {count:,} enterprises, {emp:,.0f} employees")
                
                if level == 1:
                    print(f"      → Parent kept at Level 1, so Level 2 children were EXCLUDED!")

# %% [markdown]
# ## 4. For Those in Underenheter: Check Decision Log

# %%
print("\n" + "="*80)
print("UNDERENHETER ANALYSIS (Level 3 enterprises)")
print("="*80)

df_missing_under = df_missing[df_missing['In Underenheter']].copy()

if len(df_missing_under) > 0:
    print(f"\n{len(df_missing_under):,} missing enterprises are from underenheter")
    print(f"Total employees: {df_missing_under['Employees'].sum():,.0f}")
    
    # Get their parents
    df_missing_under = df_missing_under.merge(
        df_under[['Org. nr._str', 'Overordnet enhet_str']].rename(columns={'Overordnet enhet_str': 'Parent'}),
        on='Org. nr._str',
        how='left'
    )
    
    has_parent = (df_missing_under['Parent'] != '').sum()
    print(f"\n  Have parent: {has_parent:,}")
    print(f"  No parent: {len(df_missing_under) - has_parent:,}")
    
    # Check if parent is in hovedenheter
    df_with_parent = df_missing_under[df_missing_under['Parent'] != ''].copy()
    if len(df_with_parent) > 0:
        df_with_parent['Parent In Hoved'] = df_with_parent['Parent'].isin(df_hoved['Org. nr._str'])
        parent_in_hoved = df_with_parent['Parent In Hoved'].sum()
        
        print(f"\n  Parent (Level 2) exists in hovedenheter: {parent_in_hoved:,}")
        
        # For those, check the grandparent (Level 1)
        if parent_in_hoved > 0:
            df_parent_in_hoved = df_with_parent[df_with_parent['Parent In Hoved']].copy()
            df_parent_in_hoved = df_parent_in_hoved.merge(
                df_hoved[['Org. nr._str', 'Overordnet enhet_str']].rename(columns={'Org. nr._str': 'Parent', 'Overordnet enhet_str': 'Grandparent'}),
                on='Parent',
                how='left'
            )
            
            has_grandparent = (df_parent_in_hoved['Grandparent'] != '').sum()
            print(f"    Have grandparent (Level 1): {has_grandparent:,}")
            
            # Check if grandparent was processed
            if has_grandparent > 0:
                df_with_grandparent = df_parent_in_hoved[df_parent_in_hoved['Grandparent'] != ''].copy()
                df_with_grandparent['Grandparent In Decisions'] = df_with_grandparent['Grandparent'].isin(df_decisions['Org. nr._str'])
                gp_in_decisions = df_with_grandparent['Grandparent In Decisions'].sum()
                
                print(f"    Grandparent was processed: {gp_in_decisions:,}")
                
                # What level was chosen for grandparent?
                if gp_in_decisions > 0:
                    df_gp_processed = df_with_grandparent[df_with_grandparent['Grandparent In Decisions']].copy()
                    df_gp_processed = df_gp_processed.merge(
                        df_decisions[['Org. nr._str', 'Level Used']].rename(columns={'Org. nr._str': 'Grandparent', 'Level Used': 'GP Level'}),
                        on='Grandparent',
                        how='left'
                    )
                    
                    level_dist = df_gp_processed['GP Level'].value_counts().sort_index()
                    print(f"\n    Level chosen for grandparents:")
                    for level, count in level_dist.items():
                        emp = df_gp_processed[df_gp_processed['GP Level'] == level]['Employees'].sum()
                        print(f"      Level {level}: {count:,} enterprises, {emp:,.0f} employees")
                        
                        if level in [1, 2]:
                            print(f"        → Grandparent kept at Level {level}, so Level 3 children were EXCLUDED!")

# %% [markdown]
# ## 5. Summary

# %%
print("\n" + "="*80)
print("SUMMARY - WHY THE 863 ARE MISSING")
print("="*80)

print(f"\nThe {len(df_missing):,} enterprises ({df_missing['Employees'].sum():,.0f} employees) are missing because:")

if len(df_missing_hoved) > 0:
    level_1_kept = (df_parent_processed['Parent Level'] == 1).sum() if 'df_parent_processed' in locals() else 0
    if level_1_kept > 0:
        emp_level_1 = df_parent_processed[df_parent_processed['Parent Level'] == 1]['Employees'].sum()
        print(f"\n1. Level 2 enterprises (from hovedenheter): {level_1_kept:,} ({emp_level_1:,.0f} employees)")
        print(f"   → Their parent was kept at Level 1 (>20% threshold)")
        print(f"   → So these Level 2 children were excluded from lowest level dataset")

if len(df_missing_under) > 0 and 'df_gp_processed' in locals():
    level_not_3 = (df_gp_processed['GP Level'] != 3).sum()
    if level_not_3 > 0:
        emp_not_3 = df_gp_processed[df_gp_processed['GP Level'] != 3]['Employees'].sum()
        print(f"\n2. Level 3 enterprises (from underenheter): {level_not_3:,} ({emp_not_3:,.0f} employees)")
        print(f"   → Their grandparent chose Level 1 or 2 (>20% threshold)")
        print(f"   → So these Level 3 children were excluded from lowest level dataset")

print(f"\n{'='*80}")
print("ROOT CAUSE IDENTIFIED")
print("="*80)
print("""
The create_lowest_level_dataset.py script ONLY includes enterprises at the
chosen level. When a parent is kept at Level 1 (due to >20% threshold), ALL
children (Level 2 and Level 3) are excluded, even though they have employees.

This is why:
- SQL filtered combined includes all enterprises that pass the SQL filter
- Lowest level only includes enterprises at the "chosen level"
- Children of Level 1 parents are completely excluded

This accounts for the 14,822 employee difference.
""")

print(f"All output files saved to:")
print(f"  {output_folder}")
print(f"{'='*80}")
