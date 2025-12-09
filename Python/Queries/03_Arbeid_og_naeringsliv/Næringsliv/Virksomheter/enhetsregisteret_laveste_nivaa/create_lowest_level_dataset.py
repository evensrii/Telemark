"""
Create dataset of enterprises at the lowest meaningful level.

For each level 1 enterprise, this script determines whether to use:
- Level 1: If discrepancy with lower levels is >20%
- Level 2: If no level 3 exists and discrepancy ≤20%
- Level 3: If exists and discrepancy ≤20%
"""

import pandas as pd
import os
import sys

# Define paths
data_base_path = r"c:\Users\eve1509\OneDrive - Telemark fylkeskommune\Github\Telemark\Data\03_Arbeid og næringsliv\02_Næringsliv\Virksomheter"
output_path = os.path.join(data_base_path, "enhetsregisteret_laveste_nivaa")

# Input files
hovedenheter_path = os.path.join(data_base_path, "enhetsregisteret_hovedenheter.csv")
underenheter_path = os.path.join(data_base_path, "enhetsregisteret_underenheter.csv")
validation_path = os.path.join(output_path, "validering_resultater.csv")
geodata_path = os.path.join(data_base_path, "geodata_bedrifter_api.csv")

# Output files
temp_output_file = os.path.join(output_path, "enhetsregisteret_laveste_nivaa_temp.csv")
final_output_file = os.path.join(data_base_path, "enhetsregisteret_laveste_nivaa.csv")

print("Reading data files...")
df_hoved = pd.read_csv(hovedenheter_path, low_memory=False)
df_under = pd.read_csv(underenheter_path)
df_validation = pd.read_csv(validation_path)

# Convert org numbers to strings for consistent matching
df_hoved['Org. nr.'] = df_hoved['Org. nr.'].astype('Int64').astype(str)
df_under['Org. nr.'] = df_under['Org. nr.'].astype('Int64').astype(str)
df_hoved['Overordnet enhet'] = df_hoved['Overordnet enhet'].apply(
    lambda x: '' if pd.isna(x) else str(int(x))
)
df_under['Overordnet enhet'] = df_under['Overordnet enhet'].astype('Int64').astype(str)

# Convert validation org numbers to string
df_validation['Org. nr.'] = df_validation['Org. nr.'].astype(str)

# Identify level 1 entities: those with no parent OR those whose parent is not in the dataset
all_org_numbers_in_hoved = set(df_hoved['Org. nr.'].values)
level_1_entities = df_hoved[
    (df_hoved['Overordnet enhet'] == '') | 
    (~df_hoved['Overordnet enhet'].isin(all_org_numbers_in_hoved))
]

# Identify how many have no parent vs external parent
no_parent_count = len(df_hoved[df_hoved['Overordnet enhet'] == ''])
external_parent_count = len(level_1_entities) - no_parent_count

print(f"\nTotal level 1 entities: {len(level_1_entities)}")
print(f"  - Entities with no parent: {no_parent_count}")
print(f"  - Entities with external parent: {external_parent_count}")
print("Determining lowest meaningful level for each enterprise...\n")

# Pre-filter validation results for faster lookup
df_validation_1v2 = df_validation[df_validation['Level'] == '1 vs 2'].set_index('Org. nr.')
df_validation_1v3 = df_validation[df_validation['Level'] == '1 vs 3'].set_index('Org. nr.')

# Create parent lookups for faster filtering
df_hoved_indexed = df_hoved.set_index('Overordnet enhet')
df_under_indexed = df_under.set_index('Overordnet enhet')

# Store results
lowest_level_data = []
decision_log = []

# Process each level 1 entity with progress indicator
total = len(level_1_entities)
for counter, (idx, entity) in enumerate(level_1_entities.iterrows(), 1):
    org_nr = entity['Org. nr.']
    navn = entity['Navn']
    
    # Progress indicator
    if counter % 1000 == 0 or counter == total:
        print(f"Processing: {counter:,}/{total:,} ({counter/total*100:.1f}%)")
    
    # Get validation results for this entity using indexed lookup (much faster)
    pct_diff_1v2 = None
    pct_diff_1v3 = None
    
    if org_nr in df_validation_1v2.index:
        row = df_validation_1v2.loc[org_nr]
        if row['Parent Employees'] > 0:
            pct_diff_1v2 = abs(row['Difference'] / row['Parent Employees'] * 100)
    
    if org_nr in df_validation_1v3.index:
        row = df_validation_1v3.loc[org_nr]
        if row['Parent Employees'] > 0:
            pct_diff_1v3 = abs(row['Difference'] / row['Parent Employees'] * 100)
    
    # Decision logic
    decision = None
    level_used = None
    
    # Check if level 3 entities exist using indexed lookup (much faster)
    level_2_children = df_hoved_indexed.loc[org_nr] if org_nr in df_hoved_indexed.index else pd.DataFrame()
    if isinstance(level_2_children, pd.Series):
        level_2_children = level_2_children.to_frame().T
    
    level_2_org_nrs = level_2_children['Org. nr.'].tolist() if len(level_2_children) > 0 else []
    
    direct_level_3 = df_under_indexed.loc[org_nr] if org_nr in df_under_indexed.index else pd.DataFrame()
    if isinstance(direct_level_3, pd.Series):
        direct_level_3 = direct_level_3.to_frame().T
    
    # Get indirect level 3 children
    indirect_level_3_list = []
    for l2_org in level_2_org_nrs:
        if l2_org in df_under_indexed.index:
            l3_children = df_under_indexed.loc[l2_org]
            if isinstance(l3_children, pd.Series):
                l3_children = l3_children.to_frame().T
            indirect_level_3_list.append(l3_children)
    
    indirect_level_3 = pd.concat(indirect_level_3_list) if indirect_level_3_list else pd.DataFrame()
    all_level_3 = pd.concat([direct_level_3, indirect_level_3]) if len(direct_level_3) > 0 or len(indirect_level_3) > 0 else pd.DataFrame()
    
    has_level_3 = len(all_level_3) > 0
    has_level_2 = len(level_2_children) > 0
    
    if has_level_3:
        # Level 3 exists - check if it sums up properly
        if pct_diff_1v3 is not None and pct_diff_1v3 <= 20:
            # Level 3 sums up well, use it
            decision = f"1 vs 3 diff = {pct_diff_1v3:.1f}% ≤ 20% -> Use level 3"
            level_used = 3
            for _, child in all_level_3.iterrows():
                child_dict = child.to_dict()
                child_dict['Nivå enhetsregisteret'] = 3
                lowest_level_data.append(child_dict)
        else:
            # Level 3 doesn't sum up well (>20% or no validation data)
            # Fall back to check level 2
            if has_level_2:
                if pct_diff_1v2 is not None and pct_diff_1v2 <= 20:
                    # Level 2 sums up well, use it
                    diff_str_1v3 = f"{pct_diff_1v3:.1f}%" if pct_diff_1v3 is not None else "N/A"
                    diff_str_1v2 = f"{pct_diff_1v2:.1f}%"
                    decision = f"1 vs 3 diff = {diff_str_1v3} > 20%, but 1 vs 2 diff = {diff_str_1v2} ≤ 20% -> Use level 2"
                    level_used = 2
                    for _, child in level_2_children.iterrows():
                        child_dict = child.to_dict()
                        child_dict['Nivå enhetsregisteret'] = 2
                        lowest_level_data.append(child_dict)
                else:
                    # Neither level 3 nor level 2 sums up well, use level 1
                    diff_str_1v3 = f"{pct_diff_1v3:.1f}%" if pct_diff_1v3 is not None else "N/A"
                    diff_str_1v2 = f"{pct_diff_1v2:.1f}%" if pct_diff_1v2 is not None else "N/A"
                    decision = f"1 vs 3 diff = {diff_str_1v3} > 20%, 1 vs 2 diff = {diff_str_1v2} > 20% -> Use level 1"
                    level_used = 1
                    entity_dict = entity.to_dict()
                    entity_dict['Nivå enhetsregisteret'] = 1
                    lowest_level_data.append(entity_dict)
            else:
                # Level 3 doesn't sum up and no level 2 exists, use level 1
                diff_str = f"{pct_diff_1v3:.1f}%" if pct_diff_1v3 is not None else "N/A"
                decision = f"1 vs 3 diff = {diff_str} > 20%, no level 2 -> Use level 1"
                level_used = 1
                entity_dict = entity.to_dict()
                entity_dict['Nivå enhetsregisteret'] = 1
                lowest_level_data.append(entity_dict)
    elif has_level_2:
        # No level 3, but level 2 exists
        if pct_diff_1v2 is not None and pct_diff_1v2 > 20:
            # Use level 1
            decision = f"No level 3, 1 vs 2 diff = {pct_diff_1v2:.1f}% > 20% -> Use level 1"
            level_used = 1
            entity_dict = entity.to_dict()
            entity_dict['Nivå enhetsregisteret'] = 1
            lowest_level_data.append(entity_dict)
        else:
            # Use level 2
            diff_str = f"{pct_diff_1v2:.1f}%" if pct_diff_1v2 is not None else "N/A"
            decision = f"No level 3, 1 vs 2 diff = {diff_str} ≤ 20% -> Use level 2"
            level_used = 2
            for _, child in level_2_children.iterrows():
                child_dict = child.to_dict()
                child_dict['Nivå enhetsregisteret'] = 2
                lowest_level_data.append(child_dict)
    else:
        # No children at all, use level 1
        decision = "No children -> Use level 1"
        level_used = 1
        entity_dict = entity.to_dict()
        entity_dict['Nivå enhetsregisteret'] = 1
        lowest_level_data.append(entity_dict)
    
    decision_log.append({
        'Org. nr.': org_nr,
        'Navn': navn,
        'Level Used': level_used,
        'Has Level 2': has_level_2,
        'Has Level 3': has_level_3,
        'Pct Diff 1v2': pct_diff_1v2,
        'Pct Diff 1v3': pct_diff_1v3,
        'Decision': decision
    })

# Create DataFrame from collected data
df_lowest_level = pd.DataFrame(lowest_level_data)

# Remove duplicates (in case an entity appears in both direct and indirect level 3)
df_lowest_level = df_lowest_level.drop_duplicates(subset=['Org. nr.'])

# Rename 'Org. nr.' to 'Organisasjonsnummer'
df_lowest_level = df_lowest_level.rename(columns={'Org. nr.': 'Organisasjonsnummer'})

# Move 'Nivå enhetsregisteret' column to position 3 (after Organisasjonsnummer and Navn)
if 'Nivå enhetsregisteret' in df_lowest_level.columns:
    cols = list(df_lowest_level.columns)
    cols.remove('Nivå enhetsregisteret')
    # Insert as third column (index 2)
    cols.insert(2, 'Nivå enhetsregisteret')
    df_lowest_level = df_lowest_level[cols]

# Sort by number of employees (descending - highest first)
df_lowest_level = df_lowest_level.sort_values('Antall ansatte', ascending=False)

# Keep only the required columns for temp file
columns_to_keep = ['Organisasjonsnummer', 'Navn', 'Nivå enhetsregisteret', 'Antall ansatte']
df_temp = df_lowest_level[columns_to_keep].copy()

# Save temp file
df_temp.to_csv(temp_output_file, index=False)
print(f"\n✓ Temp file saved to: {temp_output_file}")

# ===== MERGE WITH GEODATA =====
print("\n" + "="*80)
print("MERGING WITH GEODATA")
print("="*80)

print(f"\nReading geodata file: {geodata_path}")
df_geodata = pd.read_csv(geodata_path)

# Check for the correct column name in geodata (now 'firorgnr' instead of 'Organisasjonsnummer')
if 'firorgnr' in df_geodata.columns:
    # Rename to 'Organisasjonsnummer' for consistent merging
    df_geodata = df_geodata.rename(columns={'firorgnr': 'Organisasjonsnummer'})
    df_geodata['Organisasjonsnummer'] = df_geodata['Organisasjonsnummer'].astype(str)
elif 'Organisasjonsnummer' in df_geodata.columns:
    # Legacy support if file still uses old column name
    df_geodata['Organisasjonsnummer'] = df_geodata['Organisasjonsnummer'].astype(str)
else:
    print("ERROR: Neither 'firorgnr' nor 'Organisasjonsnummer' column found in geodata file!")
    print(f"Available columns: {list(df_geodata.columns)}")
    sys.exit(1)

print(f"Geodata file contains {len(df_geodata)} rows")
print(f"Temp file contains {len(df_temp)} rows")

# Merge temp data with geodata (left join on temp to keep all temp rows)
df_final = df_temp.merge(
    df_geodata,
    on='Organisasjonsnummer',
    how='left',
    suffixes=(' Enhetsreg', ' Geodata')
)

# Analyze merge results - check which temp entries are NOT in geodata
geodata_orgnr = set(df_geodata['Organisasjonsnummer'].values)
temp_orgnr = set(df_temp['Organisasjonsnummer'].values)

matched_count = len(temp_orgnr & geodata_orgnr)  # Intersection
not_in_geodata_count = len(temp_orgnr - geodata_orgnr)  # In temp but not in geodata

print(f"\nMerge results:")
print(f"  ✓ Entries from temp file found in geodata: {matched_count}")
print(f"  ✗ Entries from temp file NOT found in geodata: {not_in_geodata_count}")

if not_in_geodata_count > 0:
    # Get the entries not in geodata
    not_in_geodata_orgnr = temp_orgnr - geodata_orgnr
    not_in_geodata_df = df_temp[df_temp['Organisasjonsnummer'].isin(not_in_geodata_orgnr)]
    
    # Calculate total employees
    total_employees_not_in_geodata = not_in_geodata_df['Antall ansatte'].sum()
    
    print(f"  → Total employees in unmatched entries: {total_employees_not_in_geodata:,.0f}")
    
    print(f"\nExamples of entries in temp file NOT found in geodata (up to 5):")
    for idx, row in not_in_geodata_df.head(5).iterrows():
        print(f"  - {row['Organisasjonsnummer']}: {row['Navn']} ({row['Antall ansatte']:.0f} ansatte)")

# Remove 'Antall ansatte Geodata' column - we only keep the Enhetsreg version
if 'Antall ansatte Geodata' in df_final.columns:
    df_final = df_final.drop(columns=['Antall ansatte Geodata'])
    print(f"\n  → Removed 'Antall ansatte Geodata' column from final output")

# Save final file
df_final.to_csv(final_output_file, index=False)
print(f"\n✓ Final file saved to: {final_output_file}")

# Save decision log
decision_log_file = os.path.join(output_path, "lowest_level_decisions.csv")
df_decision_log = pd.DataFrame(decision_log)
df_decision_log.to_csv(decision_log_file, index=False)

# Print summary
print("="*80)
print("SUMMARY")
print("="*80)
print(f"\nTotal level 1 entities processed: {len(level_1_entities)}")
print(f"Total entities in lowest level dataset: {len(df_lowest_level)}")
print(f"\nLevel distribution (by decision):")
level_counts = df_decision_log['Level Used'].value_counts().sort_index()
for level, count in level_counts.items():
    print(f"  Level {level} chosen: {count} times")

print(f"\nLevel distribution (in final dataset):")
if 'Nivå enhetsregisteret' in df_lowest_level.columns:
    dataset_level_counts = df_lowest_level['Nivå enhetsregisteret'].value_counts().sort_index()
    for level, count in dataset_level_counts.items():
        print(f"  Level {level}: {count} entities")

print(f"\n✓ Temp file saved to: {temp_output_file}")
print(f"  (includes 'Nivå enhetsregisteret' column as 3rd column)")
print(f"✓ Final file saved to: {final_output_file}")
print(f"✓ Decision log saved to: {decision_log_file}")

# Additional summary: Detailed breakdown by level with/without employees
print("\n" + "="*80)
print("DETAILED SUMMARY - BREAKDOWN BY LEVEL AND EMPLOYEE STATUS")
print("="*80)

if 'Nivå enhetsregisteret' in df_lowest_level.columns:
    print("\nI datasettet på lavest mulig nivå:")
    print(f"{'':40} {'Virksomheter':>15} {'Ansatte':>15}")
    print("-" * 70)
    
    # Process each level (1, 2, 3)
    for level in sorted(df_lowest_level['Nivå enhetsregisteret'].unique()):
        df_level = df_lowest_level[df_lowest_level['Nivå enhetsregisteret'] == level]
        
        # With employees
        df_with_emp = df_level[df_level['Antall ansatte'] > 0]
        count_with_emp = len(df_with_emp)
        total_emp = df_with_emp['Antall ansatte'].sum()
        
        # Without employees
        df_without_emp = df_level[df_level['Antall ansatte'] == 0]
        count_without_emp = len(df_without_emp)
        
        # Total
        count_total = len(df_level)
        
        print(f"Nivå {level}, med ansatte{' ':23} {count_with_emp:>15,} {total_emp:>15,.0f}")
        print(f"Nivå {level}, uten ansatte{' ':22} {count_without_emp:>15,}")
        print(f"Nivå {level}{' ':35} {count_total:>15,} {total_emp:>15,.0f}")
        print()
    
    # Grand totals
    total_entities = len(df_lowest_level)
    total_entities_with_emp = len(df_lowest_level[df_lowest_level['Antall ansatte'] > 0])
    grand_total_emp = df_lowest_level['Antall ansatte'].sum()
    
    print("-" * 70)
    print(f"{'TOTALT I DATASETT':40} {total_entities:>15,} {grand_total_emp:>15,.0f}")
    print(f"{'TOTALT MED ANSATTE':40} {total_entities_with_emp:>15,} {grand_total_emp:>15,.0f}")

# Show some examples where level 1 was used due to >20% difference
print("\n" + "="*80)
print("EXAMPLES: Level 1 used due to >20% difference")
print("="*80)
df_level_1_used = df_decision_log[df_decision_log['Level Used'] == 1]
df_level_1_with_children = df_level_1_used[(df_level_1_used['Has Level 2'] == True) | (df_level_1_used['Has Level 3'] == True)]

if len(df_level_1_with_children) > 0:
    print(f"\nFound {len(df_level_1_with_children)} level 1 entities with children but >20% difference:\n")
    for idx, row in df_level_1_with_children.head(10).iterrows():
        print(f"{row['Navn']}")
        print(f"  Org. nr.: {row['Org. nr.']}")
        print(f"  {row['Decision']}\n")
else:
    print("\nNo level 1 entities used despite having children (all differences ≤20%)")

print("="*80)
print("PROCESS COMPLETE")
print("="*80)
