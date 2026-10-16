import os
import pandas as pd

# Import the utility functions from the Helper_scripts folder
from Helper_scripts.utility_functions import fetch_data
from Helper_scripts.github_functions import handle_output_data

# Capture the name of the current script
script_name = os.path.basename(__file__)

# List to collect errors during execution
error_messages = []

# ============================================================
# Step 1: Query sysselsatte etter bostedskommune (Telemark) og
#         arbeidsstedskommune (hele landet), SSB tabell 03321.
#         Fetched as CSV with codes and texts, since the JSON-stat
#         labels don't include kommunenummer.
# ============================================================

GET_URL = (
    "https://data.ssb.no/api/pxwebapi/v2/tables/03321/data?lang=no"
    "&valueCodes[ContentsCode]=Sysselsatte"
    "&valueCodes[Tid]=from(2024)"
    "&valueCodes[ArbstedKomm]=*"
    "&valueCodes[Bokommuen]=4001,4003,4005,4010,4012,4014,4016,4018,4020,4022,4024,4026,4028,4030,4032,4034,4036"
    "&outputFormat=csv"
    "&outputFormatParams=UseCodesAndTexts,SeparatorSemicolon"
)

try:
    df_raw = fetch_data(
        url=GET_URL,
        payload=None,
        error_messages=error_messages,
        query_name="Pendling per kommune",
        response_type="csv",
    )
except Exception as e:
    print(f"Error occurred: {e}")
    raise RuntimeError(
        "A critical error occurred during data fetching, stopping execution."
    )

print(f"Raw data: {len(df_raw)} rows")
print(df_raw.head(10))

# ============================================================
# Step 2: Clean and reshape to long format, one row per
#         bostedskommune, arbeidsstedskommune and år
# ============================================================

# Region cells look like "3101 - 3101 Halden"; year columns end with " - 2024"
df = df_raw.copy()
df.columns = ["Arbeidssted", "Bosted"] + [col.split(" - ")[-1] for col in df.columns[2:]]

df = df.melt(id_vars=["Arbeidssted", "Bosted"], var_name="År", value_name="Sysselsatte")

for col, prefix in [("Bosted", "Bostedskommune"), ("Arbeidssted", "Arbeidsstedskommune")]:
    parts = df[col].str.extract(r"^(?P<kode>[^ ]+) - [^ ]+ (?P<navn>.+)$")
    df[f"{prefix}nummer"] = parts["kode"]
    df[prefix] = parts["navn"]

df["År"] = df["År"].astype(int)
df["Sysselsatte"] = pd.to_numeric(df["Sysselsatte"], errors="coerce")

if df[["Bostedskommunenummer", "Arbeidsstedskommunenummer"]].isna().any().any():
    raise ValueError("Could not parse kommunenummer/kommune from SSB labels")

# "*" includes historical municipalities, e.g. "Halden (-2019)", which are always 0
historical = df["Arbeidsstedskommune"].str.contains(r"\(.*\d{4}\)$", regex=True)
if df.loc[historical, "Sysselsatte"].sum() > 0:
    print("Warning: historical municipalities have non-zero values and are dropped")
df = df[~historical]

df = df[
    ["Bostedskommunenummer", "Bostedskommune", "Arbeidsstedskommunenummer", "Arbeidsstedskommune", "År", "Sysselsatte"]
].sort_values(["Bostedskommunenummer", "Arbeidsstedskommunenummer", "År"]).reset_index(drop=True)

print(f"Cleaned data: {len(df)} rows")
print(df.head(20))

# ============================================================
# Step 3: Save to CSV, compare and upload to GitHub
# ============================================================

file_name = "pendling_per_kommune.csv"
task_name = "Mobilitet og infrastruktur - Pendling per kommune"
github_folder = "Data/05_Mobilitet og infrastruktur/Pendling"
temp_folder = os.environ.get("TEMP_FOLDER")

value_columns = ["Sysselsatte"]

# Call the function and get the "New Data" status
is_new_data = handle_output_data(
    df, file_name, github_folder, temp_folder, keepcsv=True, value_columns=value_columns
)

# Write the "New Data" status to a unique log file
log_dir = os.environ.get("LOG_FOLDER", os.getcwd())
task_name_safe = task_name.replace(".", "_").replace(" ", "_")
new_data_status_file = os.path.join(log_dir, f"new_data_status_{task_name_safe}.log")

# Write the result in a detailed format
with open(new_data_status_file, "w", encoding="utf-8") as log_file:
    log_file.write(f"{task_name_safe},{file_name},{'Yes' if is_new_data else 'No'}\n")

# Output results for debugging/testing
if is_new_data:
    print("New data detected and pushed to GitHub.")
else:
    print("No new data detected.")

print(f"New data status log written to {new_data_status_file}")
