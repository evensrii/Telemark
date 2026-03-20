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

df.head()

# Filter to only "trygg" rows, exclude Telemark county row
df_filtered = df[(df["Variabel"] == "trygg") & (df["Kommune"] != "Telemark")].copy()

df_filtered.head()

# Keep only the relevant columns and rename
df_filtered = df_filtered[["Kommunenummer", "Kommune", "Andel_vektet"]].copy()
df_filtered = df_filtered.rename(columns={"Andel_vektet": "Andel"})

# Reset index
df_filtered = df_filtered.reset_index(drop=True)

print(df_filtered)

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "trygghet.csv"
github_folder = "Data/Boligbehovsanalyse_2026/Bolyst_og_attraktivitet"
temp_folder = os.environ.get("TEMP_FOLDER")

# Call the function and get the "New Data" status
is_new_data = handle_output_data(df_filtered, file_name, github_folder, temp_folder, keepcsv=True)

# Output results for debugging/testing
if is_new_data:
    print("New data detected and pushed to GitHub.")
else:
    print("No new data detected.")
