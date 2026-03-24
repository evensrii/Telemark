import os
import pandas as pd

# Import the utility functions from the Helper_scripts folder
from Helper_scripts.github_functions import handle_output_data

################# Data #################

# Read the input file
input_path = os.path.join(
    os.environ["PYTHONPATH"],
    "..",
    "Data",
    "Folkehelseundersøkelsen",
    "2025",
    "PBI inndata",
    "frequency_tables_all_kommuner_2025_999_only.csv",
)

df = pd.read_csv(input_path, sep=";", encoding="utf-8")

print(df.head())

# Mapping from Variabel codes to readable Kategori labels
variabel_to_kategori = {
    "gang": "Gang- og sykkelvei",
    "transport": "Offentlig transport",
    "butikk": "Servicetilbud",
    "tilhoerighet": "Tilhørighet",
    "trivsel": "Trivsel i nærmiljøet",
    "trygg": "Trygghet",
}

# Filter to the relevant variables, exclude Telemark county row
df_filtered = df[
    (df["Variabel"].isin(variabel_to_kategori.keys())) & (df["Kommune"] != "Telemark")
].copy()

# Map variable codes to readable category labels
df_filtered["Kategori"] = df_filtered["Variabel"].map(variabel_to_kategori)

# Keep only the relevant columns and rename
df_filtered = df_filtered[["Kommunenummer", "Kommune", "Kategori", "Andel_vektet"]].copy()
df_filtered = df_filtered.rename(columns={"Andel_vektet": "Andel"})

# Reset index
df_filtered = df_filtered.reset_index(drop=True)

print(df_filtered)

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "bolyst_og_attraktivitet.csv"
github_folder = "Data/Boligbehovsanalyse_2026/Bolyst_og_attraktivitet/FHUS"
temp_folder = os.environ.get("TEMP_FOLDER")

# Call the function and get the "New Data" status
is_new_data = handle_output_data(df_filtered, file_name, github_folder, temp_folder, keepcsv=True)

# Output results for debugging/testing
if is_new_data:
    print("New data detected and pushed to GitHub.")
else:
    print("No new data detected.")
