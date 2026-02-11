"""
Validate hierarchical employee counts in enhetsregisteret data.

This script:
1. Checks if employee counts at parent levels match the sum of their children
2. Validates ALL entities (no employee count filtering)
3. Analyzes patterns in entities with large discrepancies (>20%)
4. Saves results to CSV and analysis to TXT file
"""

import pandas as pd
import os
import sys
from io import StringIO

# Define paths
data_base_path = r"c:\Users\eve1509\OneDrive - Telemark fylkeskommune\Github\Telemark\Data\03_Arbeid og næringsliv\02_Næringsliv\Virksomheter"
output_path = os.path.join(data_base_path, "enhetsregisteret_laveste_nivaa")
hovedenheter_path = os.path.join(data_base_path, "enhetsregisteret_hovedenheter.csv")
underenheter_path = os.path.join(data_base_path, "enhetsregisteret_underenheter.csv")

# Create output directory if it doesn't exist
os.makedirs(output_path, exist_ok=True)

# Read data
print("Reading data files...")
df_hoved = pd.read_csv(hovedenheter_path, low_memory=False)
df_under = pd.read_csv(underenheter_path)

print(f"Hovedenheter: {len(df_hoved)} rows")
print(f"Underenheter: {len(df_under)} rows\n")

# Convert org numbers to strings for consistent matching
# Convert to int first (where not NaN) to avoid .0 suffix when converting to string
df_hoved['Org. nr.'] = df_hoved['Org. nr.'].astype('Int64').astype(str)
df_under['Org. nr.'] = df_under['Org. nr.'].astype('Int64').astype(str)
df_hoved['Overordnet enhet'] = df_hoved['Overordnet enhet'].apply(
    lambda x: '' if pd.isna(x) else str(int(x))
)
df_under['Overordnet enhet'] = df_under['Overordnet enhet'].astype('Int64').astype(str)

# Identify hierarchy levels
# Level 1: entities with no parent OR entities whose parent is not in the dataset
all_org_numbers_in_hoved = set(df_hoved['Org. nr.'].values)
level_1_entities = df_hoved[
    (df_hoved['Overordnet enhet'] == '') | 
    (~df_hoved['Overordnet enhet'].isin(all_org_numbers_in_hoved))
]

# Level 2: entities in hovedenheter with parent IN the dataset
level_2_entities = df_hoved[
    (df_hoved['Overordnet enhet'] != '') & 
    (df_hoved['Overordnet enhet'].isin(all_org_numbers_in_hoved))
]

# Count breakdown
no_parent_count = len(df_hoved[df_hoved['Overordnet enhet'] == ''])
external_parent_count = len(level_1_entities) - no_parent_count

print(f"Level 1 entities: {len(level_1_entities)}")
print(f"  - No parent: {no_parent_count}")
print(f"  - External parent (not in dataset): {external_parent_count}")
print(f"Level 2 entities (in hovedenheter with parent in dataset): {len(level_2_entities)}\n")

# Validation results
validation_results = []

# ===== VALIDATION 1: Level 1 → Level 2 =====
print("="*80)
print("VALIDATION 1: Level 1 entities - checking if parent employee count equals sum of children")
print("="*80)

for idx, entity in level_1_entities.iterrows():
    org_nr = entity['Org. nr.']
    parent_name = entity['Navn']
    parent_employees = entity['Antall ansatte']
    
    # Find all children (from both hovedenheter and underenheter)
    children_hoved = df_hoved[df_hoved['Overordnet enhet'] == org_nr]
    children_under = df_under[df_under['Overordnet enhet'] == org_nr]
    
    # Calculate sum of children employees
    sum_hoved = children_hoved['Antall ansatte'].sum()
    sum_under = children_under['Antall ansatte'].sum()
    total_children_employees = sum_hoved + sum_under
    
    # Count children
    num_children = len(children_hoved) + len(children_under)
    
    if num_children == 0:
        # No children - this is OK, entity might have no sub-units
        continue
    
    # Check if parent equals sum of children
    difference = parent_employees - total_children_employees
    matches = abs(difference) < 0.01  # Allow for tiny floating point errors
    
    result = {
        'Level': '1 vs 2',
        'Org. nr.': org_nr,
        'Navn': parent_name,
        'Parent Employees': parent_employees,
        'Children Count': num_children,
        'Children Sum': total_children_employees,
        'Difference': difference,
        'Matches': matches
    }
    validation_results.append(result)
    
    if not matches:
        print(f"\n⚠️  MISMATCH FOUND:")
        print(f"   Entity: {parent_name} ({org_nr})")
        print(f"   Parent employees: {parent_employees:,.0f}")
        print(f"   Children sum: {total_children_employees:,.0f}")
        print(f"   Difference: {difference:,.0f}")
        print(f"   Children: {num_children} units ({len(children_hoved)} in hovedenheter, {len(children_under)} in underenheter)")

# ===== VALIDATION 2: Level 2 → Level 3 =====
print("\n" + "="*80)
print("VALIDATION 2: Level 2 entities - checking if parent employee count equals sum of children")
print("="*80)

for idx, entity in level_2_entities.iterrows():
    org_nr = entity['Org. nr.']
    parent_name = entity['Navn']
    parent_employees = entity['Antall ansatte']
    
    # Find all children (only from underenheter, as level 3 entities are only there)
    children = df_under[df_under['Overordnet enhet'] == org_nr]
    
    # Calculate sum of children employees
    total_children_employees = children['Antall ansatte'].sum()
    num_children = len(children)
    
    if num_children == 0:
        # No children - this is OK, entity might have no sub-units
        continue
    
    # Check if parent equals sum of children
    difference = parent_employees - total_children_employees
    matches = abs(difference) < 0.01  # Allow for tiny floating point errors
    
    result = {
        'Level': '2 vs 3',
        'Org. nr.': org_nr,
        'Navn': parent_name,
        'Parent Employees': parent_employees,
        'Children Count': num_children,
        'Children Sum': total_children_employees,
        'Difference': difference,
        'Matches': matches
    }
    validation_results.append(result)
    
    if not matches:
        print(f"\n⚠️  MISMATCH FOUND:")
        print(f"   Entity: {parent_name} ({org_nr})")
        print(f"   Parent employees: {parent_employees:,.0f}")
        print(f"   Children sum: {total_children_employees:,.0f}")
        print(f"   Difference: {difference:,.0f}")
        print(f"   Children: {num_children} units in underenheter")

# ===== VALIDATION 3: Level 1 → Level 3 (all descendants) =====
print("\n" + "="*80)
print("VALIDATION 3: Level 1 entities - checking if parent employee count equals sum of all level 3 descendants")
print("="*80)

for idx, entity in level_1_entities.iterrows():
    org_nr = entity['Org. nr.']
    parent_name = entity['Navn']
    parent_employees = entity['Antall ansatte']
    
    # Find all level 2 children (from hovedenheter)
    level_2_children = df_hoved[df_hoved['Overordnet enhet'] == org_nr]
    level_2_org_nrs = level_2_children['Org. nr.'].tolist()
    
    # Find all level 3 descendants
    # Direct children: underenheter pointing to this level 1 entity
    direct_children = df_under[df_under['Overordnet enhet'] == org_nr]
    
    # Indirect children: underenheter pointing to any level 2 child
    indirect_children = df_under[df_under['Overordnet enhet'].isin(level_2_org_nrs)]
    
    # Find level 2 entities that have NO level 3 children (leaf nodes)
    level_2_with_children = indirect_children['Overordnet enhet'].unique()
    level_2_leaf_nodes = level_2_children[~level_2_children['Org. nr.'].isin(level_2_with_children)]
    
    # Combine: level 3 entities + level 2 leaf nodes
    all_descendants = pd.concat([direct_children, indirect_children])
    total_children_employees = all_descendants['Antall ansatte'].sum() + level_2_leaf_nodes['Antall ansatte'].sum()
    num_level_3 = len(all_descendants)
    num_level_2_leaves = len(level_2_leaf_nodes)
    
    num_children = num_level_3 + num_level_2_leaves
    
    if num_children == 0:
        # No descendants at all - this is OK
        continue
    
    # Check if parent equals sum of level 3 descendants
    difference = parent_employees - total_children_employees
    matches = abs(difference) < 0.01  # Allow for tiny floating point errors
    
    result = {
        'Level': '1 vs 3',
        'Org. nr.': org_nr,
        'Navn': parent_name,
        'Parent Employees': parent_employees,
        'Children Count': num_children,
        'Children Sum': total_children_employees,
        'Difference': difference,
        'Matches': matches
    }
    validation_results.append(result)
    
    if not matches:
        print(f"\n⚠️  MISMATCH FOUND:")
        print(f"   Entity: {parent_name} ({org_nr})")
        print(f"   Parent employees: {parent_employees:,.0f}")
        print(f"   Level 3 descendants sum: {total_children_employees:,.0f}")
        print(f"   Difference: {difference:,.0f}")
        print(f"   Descendants: {num_children} units ({num_level_3} level 3, {num_level_2_leaves} level 2 leaf nodes)")

# ===== SUMMARY =====
print("\n" + "="*80)
print("SUMMARY")
print("="*80)

df_results = pd.DataFrame(validation_results)

if len(df_results) > 0:
    total_checked = len(df_results)
    matches = df_results['Matches'].sum()
    mismatches = total_checked - matches
    
    print(f"\nTotal entities with children: {total_checked}")
    print(f"Matches: {matches} ({100*matches/total_checked:.1f}%)")
    print(f"Mismatches: {mismatches} ({100*mismatches/total_checked:.1f}%)")
    
    # Show mismatch details
    if mismatches > 0:
        print("\n" + "-"*80)
        print("MISMATCH DETAILS:")
        print("-"*80)
        mismatches_df = df_results[~df_results['Matches']].copy()
        mismatches_df = mismatches_df.sort_values('Difference', key=abs, ascending=False)
        
        for idx, row in mismatches_df.iterrows():
            print(f"\nLevel {row['Level']}: {row['Navn']}")
            print(f"  Org. nr.: {row['Org. nr.']}")
            print(f"  Parent: {row['Parent Employees']:,.0f} employees")
            print(f"  Children sum: {row['Children Sum']:,.0f} employees ({row['Children Count']} units)")
            print(f"  Difference: {row['Difference']:+,.0f}")
        
        # Save detailed results to CSV
        csv_output_path = os.path.join(output_path, "validering_resultater.csv")
        df_results.to_csv(csv_output_path, index=False)
        print(f"\n✓ Detailed results saved to: {csv_output_path}")
    else:
        print("\n✓ All hierarchical employee counts match! No discrepancies found.")
else:
    print("No entities with children found to validate.")

print("\n" + "="*80)
print("SUMMARY STATISTICS (Manual Verification)")
print("="*80)

# Calculate total employees at each level
# Level 1: All entities with no parent AND with employees
level_1_with_employees = level_1_entities[level_1_entities['Antall ansatte'] > 0]
total_ansatte_niva_1 = level_1_with_employees['Antall ansatte'].sum()
antall_virksomheter_niva_1 = len(level_1_with_employees)

# Level 2: All entities in hovedenheter with a parent AND with employees
level_2_with_employees = level_2_entities[level_2_entities['Antall ansatte'] > 0]
total_ansatte_niva_2 = level_2_with_employees['Antall ansatte'].sum()
antall_virksomheter_niva_2 = len(level_2_with_employees)

# Level 3: All entities in underenheter with employees
level_3_with_employees = df_under[df_under['Antall ansatte'] > 0]
total_ansatte_niva_3 = level_3_with_employees['Antall ansatte'].sum()
antall_virksomheter_niva_3 = len(level_3_with_employees)

print("\n--- NIVÅ 1 vs NIVÅ 2 ---")
print(f"Totalt ansatte nivå 1: {total_ansatte_niva_1:,.0f}")
print(f"Totalt ansatte nivå 2: {total_ansatte_niva_2:,.0f}")
differanse_1v2 = total_ansatte_niva_1 - total_ansatte_niva_2
pct_1v2 = (differanse_1v2 / total_ansatte_niva_1 * 100) if total_ansatte_niva_1 > 0 else 0
print(f"Differanse: {differanse_1v2:,.0f}")
print(f"Prosentvis forskjell: {pct_1v2:.1f}%")
print(f"\nAntall virksomheter med ansatte, nivå 1: {antall_virksomheter_niva_1}")
print(f"Antall virksomheter med ansatte, nivå 2: {antall_virksomheter_niva_2}")

print("\n--- NIVÅ 1 vs NIVÅ 3 (eller 2) ---")
print(f"Totalt ansatte nivå 1: {total_ansatte_niva_1:,.0f}")
print(f"Totalt ansatte nivå 3 (eller 2): {total_ansatte_niva_3:,.0f}")
differanse_1v3 = total_ansatte_niva_1 - total_ansatte_niva_3
pct_1v3 = (differanse_1v3 / total_ansatte_niva_1 * 100) if total_ansatte_niva_1 > 0 else 0
print(f"Differanse: {differanse_1v3:,.0f}")
print(f"Prosentvis forskjell: {pct_1v3:.1f}%")
print(f"\nAntall virksomheter med ansatte, nivå 1: {antall_virksomheter_niva_1}")
print(f"Antall virksomheter med ansatte, nivå 2: {antall_virksomheter_niva_2}")
print(f"Antall virksomheter med ansatte, nivå 3 (eller 2 hvis 3 mangler): {antall_virksomheter_niva_3}")
print("\n* Hvis ikke virksomheten finnes på nivå 3, brukes nivå 2.")

print("\n" + "="*80)
print("VALIDATION COMPLETE")
print("="*80)

# ============================================================================
# PART 2: ANALYZE DISCREPANCIES (1 vs 3 comparison)
# ============================================================================

print("\n" + "="*80)
print("STARTING DISCREPANCY ANALYSIS (>20% difference)")
print("="*80 + "\n")

# Create a StringIO buffer to capture all print output
output_buffer = StringIO()

def tee_print(*args, **kwargs):
    """Print to both console and buffer"""
    message = ' '.join(map(str, args))
    print(message, **kwargs)
    output_buffer.write(message + '\n')

# Filter for 1 vs 3 comparisons only
df_1vs3 = df_results[df_results['Level'] == '1 vs 3'].copy()

tee_print(f"Total 1 vs 3 comparisons: {len(df_1vs3)}")
tee_print(f"Matches: {df_1vs3['Matches'].sum()}")
tee_print(f"Mismatches: {(~df_1vs3['Matches']).sum()}\n")

# Calculate percentage difference
df_1vs3['Pct_Difference'] = (df_1vs3['Difference'] / df_1vs3['Parent Employees'] * 100).abs()

# Filter for large discrepancies (>20%)
df_large_disc = df_1vs3[df_1vs3['Pct_Difference'] > 20].copy()

tee_print(f"Entities with >20% discrepancy: {len(df_large_disc)}")
tee_print(f"Percentage of total: {len(df_large_disc)/len(df_1vs3)*100:.1f}%\n")

# Merge with hovedenheter to get additional fields
df_large_disc['Org. nr.'] = df_large_disc['Org. nr.'].astype(str)
df_large_disc = df_large_disc.merge(
    df_hoved[['Org. nr.', 'Sektorkode', 'Sektor', 'Organisasjonsform', 'NACE 1', 'NACE 1 - Bransje', 'NACE 2', 'NACE 2 - Bransje', 'NACE 3', 'NACE 3 - Bransje']],
    on='Org. nr.',
    how='left'
)

# Sort by percentage difference
df_large_disc = df_large_disc.sort_values('Pct_Difference', ascending=False)

tee_print("="*80)
tee_print("TOP 20 ENTITIES WITH LARGEST PERCENTAGE DISCREPANCIES")
tee_print("="*80)
for idx, row in df_large_disc.head(20).iterrows():
    tee_print(f"\n{row['Navn']}")
    tee_print(f"  Org. nr.: {row['Org. nr.']}")
    tee_print(f"  Parent: {row['Parent Employees']:,.0f} | Children sum: {row['Children Sum']:,.0f}")
    tee_print(f"  Difference: {row['Difference']:+,.0f} ({row['Pct_Difference']:.1f}%)")
    tee_print(f"  Sektor: {row['Sektor']}")
    tee_print(f"  Organisasjonsform: {row['Organisasjonsform']}")
    tee_print(f"  NACE 1: {row['NACE 1 - Bransje']}")

# Pattern analysis
tee_print("\n" + "="*80)
tee_print("PATTERN ANALYSIS - Entities with >20% Discrepancy")
tee_print("="*80)

# By Sektor
tee_print("\n--- BY SEKTOR ---")
sektor_counts = df_large_disc['Sektor'].value_counts()
sektor_pct = df_large_disc['Sektor'].value_counts(normalize=True) * 100
sektor_df = pd.DataFrame({
    'Count': sektor_counts,
    'Percentage': sektor_pct.round(1)
})
tee_print(sektor_df.to_string())

# By Organisasjonsform
tee_print("\n--- BY ORGANISASJONSFORM ---")
org_counts = df_large_disc['Organisasjonsform'].value_counts()
org_pct = df_large_disc['Organisasjonsform'].value_counts(normalize=True) * 100
org_df = pd.DataFrame({
    'Count': org_counts,
    'Percentage': org_pct.round(1)
})
tee_print(org_df.to_string())

# By Sektorkode
tee_print("\n--- BY SEKTORKODE ---")
sektorkode_counts = df_large_disc['Sektorkode'].value_counts()
sektorkode_pct = df_large_disc['Sektorkode'].value_counts(normalize=True) * 100
sektorkode_df = pd.DataFrame({
    'Count': sektorkode_counts,
    'Percentage': sektorkode_pct.round(1)
})
tee_print(sektorkode_df.to_string())

# By NACE 1
tee_print("\n--- BY NACE 1 (Top 10) ---")
nace1_counts = df_large_disc['NACE 1 - Bransje'].value_counts().head(10)
nace1_pct = df_large_disc['NACE 1 - Bransje'].value_counts(normalize=True).head(10) * 100
nace1_df = pd.DataFrame({
    'Count': nace1_counts,
    'Percentage': nace1_pct.round(1)
})
tee_print(nace1_df.to_string())

# Compare with overall population (1 vs 3)
tee_print("\n" + "="*80)
tee_print("COMPARISON WITH OVERALL POPULATION (all 1 vs 3 entities)")
tee_print("="*80)

# Merge overall population with hovedenheter
df_1vs3_all = df_1vs3.copy()
df_1vs3_all['Org. nr.'] = df_1vs3_all['Org. nr.'].astype(str)
df_1vs3_all = df_1vs3_all.merge(
    df_hoved[['Org. nr.', 'Sektorkode', 'Sektor', 'Organisasjonsform']],
    on='Org. nr.',
    how='left'
)

tee_print("\n--- SEKTOR COMPARISON ---")
tee_print(f"{'Sektor':<40} {'Large Disc %':>15} {'Overall %':>15}")
tee_print("-"*80)
for sektor in df_large_disc['Sektor'].value_counts().index:
    large_pct = (df_large_disc['Sektor'] == sektor).sum() / len(df_large_disc) * 100
    overall_pct = (df_1vs3_all['Sektor'] == sektor).sum() / len(df_1vs3_all) * 100
    tee_print(f"{str(sektor)[:40]:<40} {large_pct:>14.1f}% {overall_pct:>14.1f}%")

tee_print("\n--- ORGANISASJONSFORM COMPARISON ---")
tee_print(f"{'Organisasjonsform':<40} {'Large Disc %':>15} {'Overall %':>15}")
tee_print("-"*80)
for org in df_large_disc['Organisasjonsform'].value_counts().index[:10]:
    large_pct = (df_large_disc['Organisasjonsform'] == org).sum() / len(df_large_disc) * 100
    overall_pct = (df_1vs3_all['Organisasjonsform'] == org).sum() / len(df_1vs3_all) * 100
    tee_print(f"{str(org)[:40]:<40} {large_pct:>14.1f}% {overall_pct:>14.1f}%")

# Save detailed analysis CSV
csv_analysis_path = os.path.join(output_path, "avvik_analyse_20pct.csv")
df_large_disc.to_csv(csv_analysis_path, index=False)
tee_print(f"\n✓ Detailed analysis CSV saved to: {csv_analysis_path}")

# Save analysis text file
txt_output_path = os.path.join(output_path, "avvik_analyse.txt")
with open(txt_output_path, 'w', encoding='utf-8') as f:
    f.write(output_buffer.getvalue())
tee_print(f"✓ Analysis text file saved to: {txt_output_path}")

tee_print("\n" + "="*80)
tee_print("ANALYSIS COMPLETE")
tee_print("="*80)
