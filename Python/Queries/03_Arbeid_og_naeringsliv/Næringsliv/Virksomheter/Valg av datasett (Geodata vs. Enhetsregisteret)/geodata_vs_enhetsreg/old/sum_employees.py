# Sum employees from two CSV files
import pandas as pd

# File paths
file1 = r"c:\Users\eve1509\OneDrive - Telemark fylkeskommune\Github\Telemark\Data\03_Arbeid og næringsliv\02_Næringsliv\Virksomheter\geodata_vs_enhetsregisteret\geodata_bedrifter_api.csv"
file2 = r"c:\Users\eve1509\OneDrive - Telemark fylkeskommune\Github\Telemark\Data\03_Arbeid og næringsliv\02_Næringsliv\Virksomheter\geodata_vs_enhetsregisteret\Bedrifter_Telemark_Eirik.csv"

# Read the files
print("Reading geodata_bedrifter_api.csv...")
df_geodata = pd.read_csv(file1)
print(f"Rows: {len(df_geodata)}")
print(f"Columns: {list(df_geodata.columns[:10])}...")

print("\nReading Bedrifter_Telemark_Eirik.csv...")
df_eirik = pd.read_csv(file2, sep=';')
print(f"Rows: {len(df_eirik)}")
print(f"Columns: {list(df_eirik.columns[:10])}...")

# %%
# Check the employee columns
print("=" * 60)
print("GEODATA FILE - 'Antall ansatte' column")
print("=" * 60)
print(f"Column dtype: {df_geodata['Antall ansatte'].dtype}")
print(f"Unique values: {df_geodata['Antall ansatte'].unique()}")
print(f"Sample values:\n{df_geodata['Antall ansatte'].head(20)}")

print("\n" + "=" * 60)
print("EIRIK FILE - 'firantansatt' column")
print("=" * 60)
print(f"Column dtype: {df_eirik['firantansatt'].dtype}")
print(f"Unique values (first 50): {sorted(df_eirik['firantansatt'].unique())[:50]}")
print(f"Sample values:\n{df_eirik['firantansatt'].head(20)}")

# %%
# Sum employees in each file

# For geodata file - handle empty strings and missing values
print("=" * 60)
print("GEODATA FILE (geodata_bedrifter_api.csv)")
print("=" * 60)

# Replace empty strings with NaN
df_geodata['Antall ansatte'] = df_geodata['Antall ansatte'].replace('', pd.NA)

# Count missing values
missing_geodata = df_geodata['Antall ansatte'].isna().sum()
print(f"Missing/empty values: {missing_geodata}")

# Convert to numeric
df_geodata['Antall ansatte_numeric'] = pd.to_numeric(df_geodata['Antall ansatte'], errors='coerce')
sum_geodata = df_geodata['Antall ansatte_numeric'].sum()
print(f"Total employees (sum of numeric values): {sum_geodata:,.0f}")

print("\n" + "=" * 60)
print("EIRIK FILE (Bedrifter_Telemark_Eirik.csv)")
print("=" * 60)

# Count missing values
missing_eirik = df_eirik['firantansatt'].isna().sum()
print(f"Missing values: {missing_eirik}")

# Sum employees
sum_eirik = df_eirik['firantansatt'].sum()
print(f"Total employees: {sum_eirik:,.0f}")

print("\n" + "=" * 60)
print("SUMMARY")
print("=" * 60)
print(f"geodata_bedrifter_api.csv:    {sum_geodata:>12,.0f} employees")
print(f"Bedrifter_Telemark_Eirik.csv: {sum_eirik:>12,.0f} employees")
print(f"Difference:                    {abs(sum_geodata - sum_eirik):>12,.0f}")
