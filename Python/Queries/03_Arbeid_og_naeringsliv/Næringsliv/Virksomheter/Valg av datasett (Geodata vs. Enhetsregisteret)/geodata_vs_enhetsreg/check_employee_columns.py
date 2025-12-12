"""
Quick script to compare Antall ansatte Enhetsreg and Antall ansatte Geodata columns
in the enhetsregisteret_laveste_nivaa.csv file.
"""

import pandas as pd

# File path
file_path = r"c:\Users\eve1509\OneDrive - Telemark fylkeskommune\Github\Telemark\Data\03_Arbeid og næringsliv\02_Næringsliv\Virksomheter\enhetsregisteret_laveste_nivaa.csv"

print("="*80)
print("COMPARISON: Antall ansatte Enhetsreg vs Antall ansatte Geodata")
print("="*80)
print("\nLoading file...")

# Load data
df = pd.read_csv(file_path, low_memory=False)

print(f"Total rows: {len(df):,}\n")

# Check which columns exist
print("Columns with 'ansatte' in name:")
ansatte_cols = [col for col in df.columns if 'ansatte' in col.lower()]
for col in ansatte_cols:
    print(f"  - {col}")
print()

# Convert to numeric
df['Antall ansatte Enhetsreg'] = pd.to_numeric(df['Antall ansatte Enhetsreg'], errors='coerce').fillna(0)
df['Antall ansatte Geodata'] = pd.to_numeric(df['Antall ansatte Geodata'], errors='coerce').fillna(0)

# Calculate totals
total_enhetsreg = df['Antall ansatte Enhetsreg'].sum()
total_geodata = df['Antall ansatte Geodata'].sum()
difference = total_geodata - total_enhetsreg

print("="*80)
print("TOTALS")
print("="*80)
print(f"Antall ansatte Enhetsreg (from enhetsregisteret): {total_enhetsreg:,.0f}")
print(f"Antall ansatte Geodata (from geodata):            {total_geodata:,.0f}")
print(f"Difference (Geodata - Enhetsreg):                  {difference:,.0f}")
print(f"Percentage difference:                             {(difference/total_enhetsreg*100):.2f}%")
print()

# Check how many rows have different values
df['diff'] = df['Antall ansatte Geodata'] - df['Antall ansatte Enhetsreg']
rows_with_diff = (df['diff'] != 0).sum()
rows_same = (df['diff'] == 0).sum()

print("="*80)
print("ROW-BY-ROW COMPARISON")
print("="*80)
print(f"Rows with same values:      {rows_same:,} ({rows_same/len(df)*100:.1f}%)")
print(f"Rows with different values: {rows_with_diff:,} ({rows_with_diff/len(df)*100:.1f}%)")
print()

# Show examples of differences
if rows_with_diff > 0:
    print("="*80)
    print("TOP 10 LARGEST DIFFERENCES")
    print("="*80)
    df_sorted = df.sort_values('diff', ascending=False, key=abs)
    print(f"{'Org.nr.':<12} {'Name':<40} {'Enhetsreg':<12} {'Geodata':<12} {'Diff':<10}")
    print("-" * 90)
    
    for idx, row in df_sorted.head(10).iterrows():
        name = str(row['Navn'])[:38]
        print(f"{row['Organisasjonsnummer']:<12} {name:<40} {row['Antall ansatte Enhetsreg']:<12.0f} "
              f"{row['Antall ansatte Geodata']:<12.0f} {row['diff']:<10.0f}")
    print()

# Check rows where one is 0 and the other is not
only_in_enhetsreg = ((df['Antall ansatte Enhetsreg'] > 0) & (df['Antall ansatte Geodata'] == 0)).sum()
only_in_geodata = ((df['Antall ansatte Enhetsreg'] == 0) & (df['Antall ansatte Geodata'] > 0)).sum()
both_zero = ((df['Antall ansatte Enhetsreg'] == 0) & (df['Antall ansatte Geodata'] == 0)).sum()

print("="*80)
print("ZERO VALUES ANALYSIS")
print("="*80)
print(f"Both columns have 0:                   {both_zero:,}")
print(f"Only Enhetsreg has value (Geodata=0):  {only_in_enhetsreg:,}")
print(f"Only Geodata has value (Enhetsreg=0):  {only_in_geodata:,}")
print()

print("="*80)
print("CONCLUSION")
print("="*80)
print("\n'Antall ansatte Enhetsreg' comes from the enhetsregisteret data (temp file)")
print("'Antall ansatte Geodata' comes from the geodata file")
print("These are merged in create_lowest_level_dataset.py line 250-256")
print("\nWhich column should be used for employee counts?")
print("  → Typically use 'Antall ansatte Geodata' as it's more complete/accurate")
print("  → Or use 'Antall ansatte Geodata' with fallback to 'Antall ansatte Enhetsreg' if missing")
