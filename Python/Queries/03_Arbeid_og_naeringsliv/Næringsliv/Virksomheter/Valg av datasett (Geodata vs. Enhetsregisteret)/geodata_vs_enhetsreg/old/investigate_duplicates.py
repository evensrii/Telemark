# %%
"""
Investigation of duplicate pattern in Even's Geodata
This script investigates why Even's dataset appears to have ~1700 duplicates
"""

import pandas as pd
import os

# %%
# Define file paths
data_folder = r"c:\Users\eve1509\OneDrive - Telemark fylkeskommune\Github\Telemark\Data\03_Arbeid og næringsliv\02_Næringsliv\Virksomheter\geodata_vs_enhetsregisteret"
even_file = os.path.join(data_folder, "geodata_raw_new_even.csv")
eirik_file = os.path.join(data_folder, "Bedrifter_Telemark_Eirik.csv")

# Load datasets
print("Loading datasets...")
even_df = pd.read_csv(even_file)
eirik_df = pd.read_csv(eirik_file, sep=';')

print(f"✓ Even's Geodata (API): {len(even_df):,} records")
print(f"✓ Eirik's Geodata (Export): {len(eirik_df):,} records")

# %% [markdown]
# ## 1. Check for duplicate organization numbers

# %%
print("\n" + "="*80)
print("DUPLICATE ANALYSIS")
print("="*80)

# Count duplicates
even_duplicates = even_df['firorgnr'].duplicated().sum()
eirik_duplicates = eirik_df['firorgnr'].duplicated().sum()

even_unique = even_df['firorgnr'].nunique()
eirik_unique = eirik_df['firorgnr'].nunique()

print(f"\nEven's data:")
print(f"  Total records: {len(even_df):,}")
print(f"  Unique org numbers: {even_unique:,}")
print(f"  Duplicate records: {even_duplicates:,}")
print(f"  Organizations appearing multiple times: {len(even_df[even_df.duplicated(subset=['firorgnr'], keep=False)]['firorgnr'].unique()):,}")

print(f"\nEirik's data:")
print(f"  Total records: {len(eirik_df):,}")
print(f"  Unique org numbers: {eirik_unique:,}")
print(f"  Duplicate records: {eirik_duplicates:,}")
print(f"  Organizations appearing multiple times: {len(eirik_df[eirik_df.duplicated(subset=['firorgnr'], keep=False)]['firorgnr'].unique()):,}")

print(f"\nDifference in unique orgs: {abs(even_unique - eirik_unique):,}")
print(f"Difference in total records: {abs(len(even_df) - len(eirik_df)):,}")

# %% [markdown]
# ## 1b. Employee Count Analysis

# %%
print("\n" + "="*80)
print("EMPLOYEE COUNT ANALYSIS (firantansatt)")
print("="*80)

# Convert firantansatt to numeric (it may be stored as string or have text values)
even_df['firantansatt_numeric'] = pd.to_numeric(even_df['firantansatt'], errors='coerce')
eirik_df['firantansatt_numeric'] = pd.to_numeric(eirik_df['firantansatt'], errors='coerce')

even_total_employees = even_df['firantansatt_numeric'].sum()
eirik_total_employees = eirik_df['firantansatt_numeric'].sum()

print(f"\nTotal employees (firantansatt):")
print(f"  Even's data: {even_total_employees:,.0f}")
print(f"  Eirik's data: {eirik_total_employees:,.0f}")
print(f"  Difference: {abs(even_total_employees - eirik_total_employees):,.0f}")

# Check how many records have employee data
even_with_employees = even_df['firantansatt_numeric'].notna().sum()
eirik_with_employees = eirik_df['firantansatt_numeric'].notna().sum()

print(f"\nRecords with employee data:")
print(f"  Even's data: {even_with_employees:,} ({even_with_employees/len(even_df)*100:.1f}%)")
print(f"  Eirik's data: {eirik_with_employees:,} ({eirik_with_employees/len(eirik_df)*100:.1f}%)")

# %% [markdown]
# ## 2. Analyze the duplicates in Even's data

# %%
print("\n" + "="*80)
print("PATTERN ANALYSIS - EVEN'S DUPLICATES")
print("="*80)

# Get organizations that appear multiple times in Even's data
even_dup_orgs = even_df[even_df.duplicated(subset=['firorgnr'], keep=False)].copy()
print(f"\nTotal duplicate records in Even's data: {len(even_dup_orgs):,}")

# Analyze by orfkode (organization type)
print("\n--- Duplicate records by orfkode (organization type) ---")
orfkode_dist = even_dup_orgs['orfkode'].value_counts()
for orfkode, count in orfkode_dist.items():
    print(f"  {orfkode}: {count:,} records")

# %% [markdown]
# ## 3. Look at specific examples

# %%
print("\n" + "="*80)
print("EXAMPLE DUPLICATE ORGANIZATIONS (First 10)")
print("="*80)

# Get unique org numbers that appear multiple times
dup_org_numbers = even_df[even_df.duplicated(subset=['firorgnr'], keep=False)]['firorgnr'].unique()[:10]

for org_nr in dup_org_numbers:
    org_records = even_df[even_df['firorgnr'] == org_nr][['objectid', 'orfkode', 'firorgnr', 'firfirmanavn1', 'stfstatusfirmaid']]
    print(f"\n--- Org nr: {org_nr} ---")
    print(org_records.to_string(index=False))

# %% [markdown]
# ## 4. Check firorgnrknytning pattern

# %%
print("\n" + "="*80)
print("PARENT-CHILD RELATIONSHIP ANALYSIS")
print("="*80)

# Check if duplicates have parent-child relationships
print("\nLooking for parent-child patterns (same name, different org numbers)...")

# Group by company name and check for multiple org numbers
name_groups = even_df.groupby('firfirmanavn1')['firorgnr'].apply(lambda x: x.nunique() if x.notna().any() else 0)
multi_orgnr_names = name_groups[name_groups > 1].sort_values(ascending=False)

print(f"\nCompanies with multiple organization numbers: {len(multi_orgnr_names):,}")
print(f"Top 10 companies with most org numbers:")
for name, count in multi_orgnr_names.head(10).items():
    print(f"  {name}: {count} different org numbers")

# Show examples
print("\n--- Example: Organizations with same name but different org numbers (first 5) ---")
for name in multi_orgnr_names.head(5).index:
    org_records = even_df[even_df['firfirmanavn1'] == name][
        ['objectid', 'orfkode', 'firorgnr', 'firorgnrknytning', 'firfirmanavn1', 'stfstatusfirmaid']
    ]
    print(f"\n{name}:")
    print(org_records.to_string(index=False))

# %% [markdown]
# ## 5. Check stfstatusfirmaid differences

# %%
print("\n" + "="*80)
print("STATUS ANALYSIS (stfstatusfirmaid)")
print("="*80)

# Check if duplicates have different status codes
print("\nAnalyzing stfstatusfirmaid for duplicate organizations...")

# Get first 20 organizations with duplicates
dup_org_numbers = even_df[even_df.duplicated(subset=['firorgnr'], keep=False)]['firorgnr'].unique()[:20]

status_patterns = {}
for org_nr in dup_org_numbers:
    statuses = even_df[even_df['firorgnr'] == org_nr]['stfstatusfirmaid'].unique()
    status_key = tuple(sorted(statuses))
    if status_key not in status_patterns:
        status_patterns[status_key] = 0
    status_patterns[status_key] += 1

print(f"\nStatus patterns in duplicate records:")
for pattern, count in sorted(status_patterns.items(), key=lambda x: x[1], reverse=True):
    print(f"  Status codes {pattern}: {count} organizations")

# %% [markdown]
# ## 6. Critical Question: 1700 Duplicates vs 1700 Extra Records - Coincidence?

# %%
print("\n" + "="*80)
print("INVESTIGATING THE 1700 PATTERN")
print("="*80)

print(f"""
The Numbers:
- Even has ~1,700 duplicate records (same org numbers appearing multiple times)
- Eirik has ~1,700 MORE total records than Even
- Both datasets claim to be similar extracts

Question: Is this a coincidence or a systematic difference?
""")

# Get the org numbers that are unique to each dataset
even_org_set = set(even_df['firorgnr'].dropna().unique())
eirik_org_set = set(eirik_df['firorgnr'].dropna().unique())

only_in_even = even_org_set - eirik_org_set
only_in_eirik = eirik_org_set - even_org_set

print(f"\nOrganization numbers unique to each dataset:")
print(f"  Only in Even's data: {len(only_in_even):,}")
print(f"  Only in Eirik's data: {len(only_in_eirik):,}")
print(f"  In both datasets: {len(even_org_set & eirik_org_set):,}")

# Get duplicate org numbers in Even's data
even_dup_orgs = even_df[even_df.duplicated(subset=['firorgnr'], keep=False)]['firorgnr'].unique()

print(f"\n\nCritical comparison:")
print(f"  Organizations with duplicates in Even: {len(even_dup_orgs):,}")
print(f"  Organizations only in Eirik: {len(only_in_eirik):,}")

# Check if the organizations that appear only in Eirik are the PARENT organizations
# of the duplicates in Even (via firorgnrknytning)
even_dup_records = even_df[even_df.duplicated(subset=['firorgnr'], keep=False)].copy()
parent_orgs_in_duplicates = even_dup_records['firorgnrknytning'].dropna().unique()

overlap_with_eirik_only = set(parent_orgs_in_duplicates) & only_in_eirik

print(f"\n\nParent organization analysis:")
print(f"  Unique parent orgs (firorgnrknytning) in Even's duplicates: {len(parent_orgs_in_duplicates):,}")
print(f"  Of these, how many are ONLY in Eirik's data: {len(overlap_with_eirik_only):,}")

if len(overlap_with_eirik_only) > 0:
    print(f"\n  ⚠️ PATTERN DETECTED: {len(overlap_with_eirik_only):,} parent organizations referenced")
    print(f"     in Even's duplicates exist ONLY in Eirik's dataset!")

# Check the reverse: do organizations only in Even have children?
even_only_records = even_df[even_df['firorgnr'].isin(only_in_even)]
even_only_with_children = even_only_records[even_only_records['firorgnr'].isin(
    even_df['firorgnrknytning'].dropna().unique()
)]

print(f"\n\nReverse check - organizations only in Even:")
print(f"  Organizations only in Even that are PARENTS (have children): {len(even_only_with_children['firorgnr'].unique()):,}")

# %% [markdown]
# ## 7. Key Finding Summary

# %%
print("\n" + "="*80)
print("KEY FINDINGS")
print("="*80)

diff_total_records = abs(len(even_df) - len(eirik_df))
diff_unique_orgs = abs(even_unique - eirik_unique)

print(f"""
THE 1700 PATTERN EXPLAINED:

Difference in total records: {diff_total_records:,}
Number of duplicate records in Even: {even_duplicates:,}
Organizations with duplicates in Even: {len(even_dup_orgs):,}

HYPOTHESIS: Even's data contains both main units and sub-units, while Eirik's data
may only contain one level of organizational hierarchy.

Evidence:
1. Even's data has {len(even_df):,} total records but only {even_unique:,} unique organization numbers
   - This means {even_duplicates:,} records share org numbers with other records

2. Eirik's data has {len(eirik_df):,} total records and {eirik_unique:,} unique organization numbers
   - Difference in unique orgs: {diff_unique_orgs:,}

3. The 'orfkode' field shows different organization types:
   - Type 1: Main organizational unit (Enkeltpersonforetak, Aksjeselskap, etc.)
   - Type 9: Sub-unit (Underenhet til næringsdrivende og offentlig forvaltning)

4. The 'firorgnrknytning' field likely contains the parent organization number for sub-units

5. Organizations only in Eirik: {len(only_in_eirik):,}
   Organizations only in Even: {len(only_in_even):,}

CONCLUSION:
{"✅ The ~1,700 difference appears to be RELATED:" if diff_total_records > 1500 else "⚠️ The patterns need further investigation:"}
- Even has {even_duplicates:,} duplicate records (same org number appearing multiple times)
- Eirik has {diff_total_records:,} MORE records total
- The numbers are {"very similar" if abs(even_duplicates - diff_total_records) < 100 else "somewhat different"}

This suggests that:
- Even's API extract includes BOTH parent units AND sub-units as separate records,
  but uses the SAME organization number for both (causing "duplicates")
- Eirik's export includes BOTH levels as SEPARATE records with DIFFERENT org numbers
- The datasets have the same number of lines but represent organizational hierarchy differently

RECOMMENDATION:
- Verify this by examining the 'stfstatusfirmaid' and 'orfkode' values for duplicates
- Decide on the correct approach: should we deduplicate Even's data or is this intentional?
- For fair comparison, consider comparing on total records OR on unique org numbers, but be consistent
""")

print("\n" + "="*80)
print("INVESTIGATION COMPLETE")
print("="*80)
