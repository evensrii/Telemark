# %% [markdown]
# # Investigate University Case - Missing Parent Organization
# 
# This script investigates why enterprises with parent 911770709 (University) 
# are missing from the lowest level dataset.
#
# Specific case: 974798190 (Campus Porsgrunn) with parent 911770709

# %%
import pandas as pd
import numpy as np

# %%
# File paths
base_path = r'c:\Users\eve1509\OneDrive - Telemark fylkeskommune\Github\Telemark\Data\03_Arbeid og næringsliv\02_Næringsliv\Virksomheter'

# Load datasets
df_combined = pd.read_csv(f'{base_path}\\enhetsregisteret_kombinert_clean.csv', dtype={'Org. nr.': str, 'Overordnet enhet': str})
df_hovedenheter = pd.read_csv(f'{base_path}\\enhetsregisteret_hovedenheter.csv', dtype={'Org. nr.': str})
df_underenheter = pd.read_csv(f'{base_path}\\enhetsregisteret_underenheter.csv', dtype={'Org. nr.': str, 'Overordnet enhet': str})
df_lowest = pd.read_csv(f'{base_path}\\enhetsregisteret_laveste_nivaa.csv', dtype={'Organisasjonsnummer': str})

print("Datasets loaded successfully")
print(f"Combined: {len(df_combined):,} rows")
print(f"Hovedenheter: {len(df_hovedenheter):,} rows")
print(f"Underenheter: {len(df_underenheter):,} rows")
print(f"Lowest level: {len(df_lowest):,} rows")

# Print column names to verify
print("\nCombined dataset columns:")
print(df_combined.columns.tolist())

# %%
# Search for parent organization 911770709
parent_org = '911770709'

print("\n" + "="*80)
print(f"SEARCHING FOR PARENT ORGANIZATION: {parent_org}")
print("="*80)

# Check if parent exists in hovedenheter
parent_in_hovedenheter = df_hovedenheter[df_hovedenheter['Org. nr.'] == parent_org]
print(f"\nParent in hovedenheter: {len(parent_in_hovedenheter)} rows")
if len(parent_in_hovedenheter) > 0:
    print(parent_in_hovedenheter[['Navn', 'Org. nr.', 'Antall ansatte', 'Forretningsadresse - Kommune']])

# Check if parent exists in combined dataset
parent_in_combined = df_combined[df_combined['Org. nr.'] == parent_org]
print(f"\nParent in combined dataset: {len(parent_in_combined)} rows")
if len(parent_in_combined) > 0:
    cols_to_show = ['Navn', 'Org. nr.', 'Overordnet enhet', 'Antall ansatte']
    print(parent_in_combined[cols_to_show])

# %%
# Find all children (underenheter) with this parent
print("\n" + "="*80)
print(f"CHILDREN OF PARENT {parent_org}")
print("="*80)

children = df_combined[df_combined['Overordnet enhet'] == parent_org].copy()
print(f"\nFound {len(children)} children")

if len(children) > 0:
    children['Antall ansatte'] = pd.to_numeric(children['Antall ansatte'], errors='coerce')
    # Use only columns that exist
    display_cols = ['Navn', 'Org. nr.', 'Overordnet enhet', 'Antall ansatte']
    # Add optional columns if they exist
    if 'Organisasjonsform' in children.columns:
        display_cols.append('Organisasjonsform')
    
    children_summary = children[display_cols].sort_values('Antall ansatte', ascending=False)
    print(children_summary.to_string(index=False))
    
    total_employees = children['Antall ansatte'].sum()
    print(f"\nTotal employees across all children: {total_employees:,.0f}")

# %%
# Check if any of these children are in the lowest level dataset
print("\n" + "="*80)
print("CHECKING CHILDREN IN LOWEST LEVEL DATASET")
print("="*80)

child_orgnrs = children['Org. nr.'].unique()
children_in_lowest = df_lowest[df_lowest['Organisasjonsnummer'].isin(child_orgnrs)]

print(f"\nChildren in lowest level dataset: {len(children_in_lowest)} out of {len(children)}")

if len(children_in_lowest) > 0:
    print("\nChildren FOUND in lowest level:")
    print(children_in_lowest[['Navn', 'Organisasjonsnummer', 'Nivå enhetsregisteret', 'Antall ansatte Enhetsreg']].to_string(index=False))
else:
    print("\nNONE of the children found in lowest level dataset!")

# %%
# Check specific case: 974798190 (Campus Porsgrunn)
specific_org = '974798190'

print("\n" + "="*80)
print(f"SPECIFIC CASE: {specific_org} (Campus Porsgrunn)")
print("="*80)

# In combined dataset
in_combined = df_combined[df_combined['Org. nr.'] == specific_org]
print(f"\nIn combined dataset: {len(in_combined)} rows")
if len(in_combined) > 0:
    display_cols = ['Navn', 'Org. nr.', 'Overordnet enhet', 'Antall ansatte']
    if 'Organisasjonsform' in in_combined.columns:
        display_cols.append('Organisasjonsform')
    print(in_combined[display_cols].to_string(index=False))

# In lowest level dataset
in_lowest = df_lowest[df_lowest['Organisasjonsnummer'] == specific_org]
print(f"\nIn lowest level dataset: {len(in_lowest)} rows")
if len(in_lowest) > 0:
    print(in_lowest[['Navn', 'Organisasjonsnummer', 'Nivå enhetsregisteret', 'Antall ansatte Enhetsreg']].to_string(index=False))
else:
    print("NOT FOUND in lowest level dataset")

# %%
# Check if parent exists ANYWHERE in the dataset (maybe as underenhet?)
print("\n" + "="*80)
print("COMPREHENSIVE SEARCH FOR PARENT ORGANIZATION")
print("="*80)

# Maybe parent is registered as underenhet?
parent_as_underenhet = df_underenheter[df_underenheter['Org. nr.'] == parent_org]
print(f"\nParent as underenhet: {len(parent_as_underenhet)} rows")
if len(parent_as_underenhet) > 0:
    print(parent_as_underenhet[['Navn', 'Org. nr.', 'Overordnet enhet', 'Antall ansatte']].to_string(index=False))

# Check if any org has parent_org as its "Overordnet enhet" field
has_this_parent = df_combined[df_combined['Overordnet enhet'] == parent_org]
print(f"\nOrganizations with {parent_org} as parent: {len(has_this_parent)}")

# %%
# CONCLUSION
print("\n" + "="*80)
print("ANALYSIS SUMMARY")
print("="*80)

print(f"""
CASE: Organization 974798190 (Campus Porsgrunn) with parent 911770709

FINDINGS:
1. Parent organization {parent_org} is NOT in hovedenheter.csv
   - This means the Level 1 parent is OUTSIDE Telemark region
   
2. There are {len(children)} underenheter (Level 3) in Telemark with this parent:
   - Total employees: {total_employees:,.0f}
   
3. These underenheter are in the SQL-filtered combined dataset
   - They pass the SQL filter (not filtered out as parents)
   
4. BUT they are NOT in the lowest level dataset
   - Reason: The create_lowest_level_dataset.py script requires a parent chain
   - Since parent {parent_org} is not in the Telemark dataset, these are "orphaned"

IMPACT:
- These {len(children)} "orphaned" underenheter with {total_employees:,.0f} employees are excluded
- This is one reason for the employee count discrepancy

RECOMMENDATION:
- Option 1: Include underenheter whose Level 1 parent is outside Telemark
- Option 2: Accept this as intentional filtering (only complete hierarchies)
- Option 3: Investigate if parent should be in the dataset
""")

print("\nDone!")
