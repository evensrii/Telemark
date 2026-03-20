import os
import pandas as pd

# Import the utility functions from the Helper_scripts folder
from Helper_scripts.utility_functions import fetch_data
from Helper_scripts.email_functions import notify_errors
from Helper_scripts.github_functions import handle_output_data

# Capture the name of the current script
script_name = os.path.basename(__file__)

# Example list of error messages to collect errors during execution
error_messages = []

################# Spørring #################

# SSB API v2 GET URL (tabell 13563 - Utenforskap per fylke og alder)
GET_URL = (
    "https://data.ssb.no/api/pxwebapi/v2/tables/13563/data?lang=no"
    "&outputFormat=json-stat2"
    "&valuecodes[ContentsCode]=*"
    "&valuecodes[Tid]=top(1)"
    "&valuecodes[Region]=F-31,F-32,F-03,F-34,F-33,F-39,F-40,F-42,F-11,F-46,F-15,F-50,F-18,F-55,F-56"
    "&codelist[Region]=agg_KommFylker"
    "&valuecodes[HovArbStyrkStatus]=TOT3,NEET2"
    "&codelist[HovArbStyrkStatus]=vs_ArbStatus2018niva4a"
    "&valuecodes[Alder]=15-29,30-61"
    "&heading=ContentsCode,Tid,Alder"
    "&stub=Region,HovArbStyrkStatus"
)

## Kjøre spørringer i try-except for å fange opp feil. Quitter hvis feil.

try:
    df = fetch_data(
        url=GET_URL,
        payload=None,  # None = GET request (new SSB API v2)
        error_messages=error_messages,
        query_name="Andel utenfor per fylke og alder (13563)",
        response_type="json",
    )
except Exception as e:
    print(f"Error occurred: {e}")
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError(
        "A critical error occurred during data fetching, stopping execution."
    )

### DATA CLEANING

df.head(20)

# Pivot wider: one column per arbeidsstyrkestatus, for each region+alder combo
df_wide = df.pivot_table(
    index=["region", "alder"],
    columns="prioritert arbeidsstyrkestatus",
    values="value",
    aggfunc="first"
).reset_index()

df_wide.columns.name = None
df_wide.head()

# Calculate NEET percentage: NEET / I alt * 100
neet_col = [c for c in df_wide.columns if "Utenfor" in str(c)][0]
total_col = "I alt"
df_wide["prosent"] = (df_wide[neet_col] / df_wide[total_col] * 100).round(1)

df_wide.head()

# Pivot age groups into columns
df_pivot = df_wide.pivot_table(
    index="region",
    columns="alder",
    values="prosent",
    aggfunc="first"
).reset_index()

df_pivot.columns.name = None
df_pivot.head()

# Simplify fylke names using metadata file
fylkesnavn = pd.read_csv(
    os.path.join(os.environ["PYTHONPATH"], "Metadata", "forenklede_fylkesnavn.csv"),
    sep=";",
    header=None,
    names=["original", "forenklet"]
)
navn_map = dict(zip(fylkesnavn["original"], fylkesnavn["forenklet"]))
df_pivot["region"] = df_pivot["region"].replace(navn_map)

# Extract year from original data
year = df.iloc[0]["år"]

# Rename columns to include year
col_map = {"region": "Fylke"}
for col in df_pivot.columns:
    if "15-29" in str(col):
        col_map[col] = f"15-29 år ({year})"
    elif "30-61" in str(col):
        col_map[col] = f"30-61 år ({year})"
df_pivot = df_pivot.rename(columns=col_map)

# Sort descending by 15-29 column
df_pivot = df_pivot.sort_values(f"15-29 år ({year})", ascending=False).reset_index(drop=True)

df_pivot

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "andel_utenfor_fylke_alder.csv"
task_name = "Opplaering og kompetanse - Andel utenfor per fylke og alder"
github_folder = "Data/02_Opplæring og kompetanse/Utenforskap"
temp_folder = os.environ.get("TEMP_FOLDER")

# Call the function and get the "New Data" status
is_new_data = handle_output_data(df_pivot, file_name, github_folder, temp_folder, keepcsv=True)

# Write the "New Data" status to a unique log file
log_dir = os.environ.get("LOG_FOLDER", os.getcwd())  # Default to current working directory
task_name_safe = task_name.replace(".", "_").replace(" ", "_")  # Ensure the task name is file-system safe
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