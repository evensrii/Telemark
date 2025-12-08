# %%
import pandas as pd

# %%
# Load the file
file_path = r"c:\Users\eve1509\OneDrive - Telemark fylkeskommune\Github\Telemark\Data\03_Arbeid og næringsliv\02_Næringsliv\Virksomheter\geodata_vs_enhetsregisteret\eirik_unique_orgs.csv"
df = pd.read_csv(file_path, sep=';')

# %%
# Calculate sum of firantansatt
total_firantansatt = df['firantansatt'].sum()

print(f"Sum of firantansatt: {total_firantansatt:,.0f}")
print(f"Number of records: {len(df):,}")
print(f"Average per record: {total_firantansatt/len(df):.2f}")
