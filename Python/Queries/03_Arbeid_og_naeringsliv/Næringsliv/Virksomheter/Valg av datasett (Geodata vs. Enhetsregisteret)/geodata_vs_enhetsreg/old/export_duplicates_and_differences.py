# %%
"""
Export duplicate records from Even's dataset and unique records from Eirik's dataset
"""

import pandas as pd
import os

# %%
# Define file paths
data_folder = r"c:\Users\eve1509\OneDrive - Telemark fylkeskommune\Github\Telemark\Data\03_Arbeid og næringsliv\02_Næringsliv\Virksomheter\geodata_vs_enhetsregisteret"
even_file = os.path.join(data_folder, "geodata_bedrifter_api_raw.csv")
eirik_file = os.path.join(data_folder, "Bedrifter_Telemark_Eirik.csv")

# Output files
even_duplicates_output = os.path.join(data_folder, "even_duplicates.csv")
eirik_unique_output = os.path.join(data_folder, "eirik_unique_orgs.csv")

# %%
# Load datasets
print("Loading datasets...")
even_df = pd.read_csv(even_file)
eirik_df = pd.read_csv(eirik_file, sep=';')

print(f"✓ Even's Geodata (API): {len(even_df):,} records")
print(f"✓ Eirik's Geodata (Export): {len(eirik_df):,} records")

# %% [markdown]
# ## 1. Extract duplicate records from Even's dataset

# %%
print("\n" + "="*80)
print("EXTRACTING DUPLICATES FROM EVEN'S DATASET")
print("="*80)

# Identify all records where the organization number appears more than once
even_duplicates = even_df[even_df.duplicated(subset=['firorgnr'], keep=False)].copy()

# Sort by organization number for easier review
even_duplicates = even_duplicates.sort_values('firorgnr')

print(f"\nTotal duplicate records found: {len(even_duplicates):,}")
print(f"Number of unique organizations with duplicates: {even_duplicates['firorgnr'].nunique():,}")

# Save to CSV
even_duplicates.to_csv(even_duplicates_output, index=False)
print(f"\n✓ Saved to: {even_duplicates_output}")

# Show summary
print(f"\nSample of duplicate records:")
print(even_duplicates[['objectid', 'orfkode', 'firorgnr', 'firfirmanavn1', 'stfstatusfirmaid']].head(10).to_string(index=False))

# %% [markdown]
# ## 2. Extract unique organizations from Eirik's dataset

# %%
print("\n" + "="*80)
print("EXTRACTING UNIQUE ORGANIZATIONS FROM EIRIK'S DATASET")
print("="*80)

# Get unique organization numbers from both datasets
even_unique_orgs = set(even_df['firorgnr'].unique())
eirik_unique_orgs = set(eirik_df['firorgnr'].unique())

# Find organizations only in Eirik's dataset
only_in_eirik = eirik_unique_orgs - even_unique_orgs

# Filter Eirik's data to get these records
eirik_unique_records = eirik_df[eirik_df['firorgnr'].isin(only_in_eirik)].copy()

# Sort by organization number
eirik_unique_records = eirik_unique_records.sort_values('firorgnr')

print(f"\nOrganizations only in Eirik's dataset: {len(only_in_eirik):,}")
print(f"Total records for these organizations: {len(eirik_unique_records):,}")

# Save to CSV
eirik_unique_records.to_csv(eirik_unique_output, index=False, sep=';')
print(f"\n✓ Saved to: {eirik_unique_output}")

# Show summary
print(f"\nSample of unique organizations in Eirik's dataset:")
print(eirik_unique_records[['firorgnr', 'firfirmanavn1', 'kommunenavn']].head(10).to_string(index=False))

# %% [markdown]
# ## 3. Summary Statistics

# %%
print("\n" + "="*80)
print("SUMMARY")
print("="*80)

print(f"""
Dataset Statistics:
-------------------
Even's dataset:
  - Total records: {len(even_df):,}
  - Unique organizations: {even_df['firorgnr'].nunique():,}
  - Duplicate records: {len(even_duplicates):,}
  - Organizations with duplicates: {even_duplicates['firorgnr'].nunique():,}

Eirik's dataset:
  - Total records: {len(eirik_df):,}
  - Unique organizations: {len(eirik_unique_orgs):,}
  - Organizations only in Eirik's data: {len(only_in_eirik):,}

Common organizations: {len(even_unique_orgs & eirik_unique_orgs):,}

Output Files Created:
---------------------
1. {even_duplicates_output}
   - Contains {len(even_duplicates):,} duplicate records from Even's dataset
   
2. {eirik_unique_output}
   - Contains {len(eirik_unique_records):,} records for {len(only_in_eirik):,} organizations
     that are only in Eirik's dataset
""")

print("\n" + "="*80)
print("EXPORT COMPLETE")
print("="*80)
