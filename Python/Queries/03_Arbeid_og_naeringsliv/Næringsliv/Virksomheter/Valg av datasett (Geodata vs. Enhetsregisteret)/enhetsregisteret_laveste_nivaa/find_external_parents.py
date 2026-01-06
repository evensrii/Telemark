"""
Find enterprises in hovedenheter with parents not in the same file.

These are entities that reference a parent organization outside the dataset.
"""

import pandas as pd
import os

# Define paths
data_base_path = r"c:\Users\eve1509\OneDrive - Telemark fylkeskommune\Github\Telemark\Data\03_Arbeid og næringsliv\02_Næringsliv\Virksomheter"
hovedenheter_path = os.path.join(data_base_path, "enhetsregisteret_hovedenheter.csv")
output_path = os.path.join(data_base_path, "enhetsregisteret_laveste_nivaa")

print("Reading hovedenheter file...")
df_hoved = pd.read_csv(hovedenheter_path, low_memory=False)

print(f"Total rows in hovedenheter: {len(df_hoved)}\n")

# Convert org numbers to strings
df_hoved['Org. nr.'] = df_hoved['Org. nr.'].astype('Int64').astype(str)
df_hoved['Overordnet enhet'] = df_hoved['Overordnet enhet'].apply(
    lambda x: '' if pd.isna(x) else str(int(x))
)

# Get all organization numbers in the file
all_org_numbers = set(df_hoved['Org. nr.'].values)

# Find entities with a parent
entities_with_parent = df_hoved[df_hoved['Overordnet enhet'] != ''].copy()
print(f"Entities with a parent reference: {len(entities_with_parent)}")

# Find entities where parent is NOT in the file
entities_with_external_parent = entities_with_parent[
    ~entities_with_parent['Overordnet enhet'].isin(all_org_numbers)
].copy()

print(f"Entities with EXTERNAL parent (not in file): {len(entities_with_external_parent)}\n")

if len(entities_with_external_parent) > 0:
    print("="*80)
    print("ENTITIES WITH EXTERNAL PARENTS")
    print("="*80)
    
    # Sort by number of employees (descending)
    entities_with_external_parent = entities_with_external_parent.sort_values(
        'Antall ansatte', ascending=False
    )
    
    print(f"\n{'Navn':<50} {'Org. nr.':<15} {'Parent Org.':<15} {'Employees':>10}")
    print("-"*95)
    
    for idx, row in entities_with_external_parent.iterrows():
        navn = row['Navn'][:47] + "..." if len(row['Navn']) > 50 else row['Navn']
        org_nr = row['Org. nr.']
        parent = row['Overordnet enhet']
        employees = row['Antall ansatte']
        
        print(f"{navn:<50} {org_nr:<15} {parent:<15} {employees:>10.0f}")
    
    # Save to CSV
    output_file = os.path.join(output_path, "entities_with_external_parents.csv")
    entities_with_external_parent.to_csv(output_file, index=False)
    print(f"\n✓ Full list saved to: {output_file}")
    
    # Summary statistics
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    print(f"Total entities with external parents: {len(entities_with_external_parent)}")
    print(f"Total employees in these entities: {entities_with_external_parent['Antall ansatte'].sum():,.0f}")
    
    # Count unique external parents
    unique_external_parents = entities_with_external_parent['Overordnet enhet'].nunique()
    print(f"Number of unique external parent organizations: {unique_external_parents}")
    
    # Show most common external parents
    print("\nMost common external parent organizations:")
    parent_counts = entities_with_external_parent['Overordnet enhet'].value_counts().head(10)
    for parent_org, count in parent_counts.items():
        print(f"  {parent_org}: {count} entities")
    
else:
    print("✓ All entities with parents have their parent in the file.")

print("\n" + "="*80)
print("ANALYSIS COMPLETE")
print("="*80)
