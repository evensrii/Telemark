import os
import pandas as pd
from pyjstat import pyjstat

# Import the utility functions from the Helper_scripts folder
from Helper_scripts.utility_functions import fetch_data
from Helper_scripts.email_functions import notify_errors
from Helper_scripts.github_functions import handle_output_data

# Capture the name of the current script
script_name = os.path.basename(__file__)

# Example list of error messages to collect errors during execution
error_messages = []

################# Spørring 1: Kommuner #################

GET_URL_KOMMUNER = (
    "https://data.ssb.no/api/pxwebapi/v2/tables/09475/data?lang=no"
    "&outputFormat=json-stat2"
    "&valueCodes[ContentsCode]=DeltaProsent"
    "&valueCodes[Tid]=top(1)"
    "&valueCodes[Region]=4001,4003,4005,4010,4012,4014,4016,4018,4020,4022,4024,4026,4028,4030,4032,4034,4036"
    "&codelist[Region]=agg_KommunerV1"
    "&outputValues[Region]=aggregated"
)

################# Spørring 2: Fylke og land #################

GET_URL_FYLKE_LAND = (
    "https://data.ssb.no/api/pxwebapi/v2/tables/09475/data?lang=no"
    "&outputFormat=json-stat2"
    "&valueCodes[ContentsCode]=DeltaProsent"
    "&valueCodes[Tid]=top(1)"
    "&valueCodes[Region]=0,40"
)

## Kjøre spørringer i try-except for å fange opp feil. Quitter hvis feil.

try:
    df_kommuner = fetch_data(
        url=GET_URL_KOMMUNER,
        payload=None,
        error_messages=error_messages,
        query_name="Valgdeltakelse kommunevalg - kommuner (09475)",
        response_type="json",
    )
    df_fylke_land = fetch_data(
        url=GET_URL_FYLKE_LAND,
        payload=None,
        error_messages=error_messages,
        query_name="Valgdeltakelse kommunevalg - fylke og land (09475)",
        response_type="json",
    )
except Exception as e:
    print(f"Error occurred: {e}")
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError(
        "A critical error occurred during data fetching, stopping execution."
    )

################# Kombinere datasett #################

df = pd.concat([df_kommuner, df_fylke_land], ignore_index=True)

print(df.head(20))
print(df.columns.tolist())

################# Data cleaning #################

print("Kolonner:", df.columns.tolist())

# Drop the statistikkvariabel column (only one variable in this query)
if "statistikkvariabel" in df.columns:
    df = df.drop(columns=["statistikkvariabel"])

# Rename columns (handle both "år" and "tid" depending on API response)
rename_map = {"value": "Andel"}
if "region" in df.columns:
    rename_map["region"] = "Kommune"
if "år" in df.columns:
    rename_map["år"] = "År"
elif "tid" in df.columns:
    rename_map["tid"] = "År"
elif "fireårlig" in df.columns:
    rename_map["fireårlig"] = "År"

df = df.rename(columns=rename_map)

# Rename "Hele landet" if present
df.loc[df["Kommune"] == "Hele landet", "Kommune"] = "Landet"

# Andel as integer (rounded)
df["Andel"] = df["Andel"].astype(float).round(0).astype(int)

# Get the year dynamically from the data
year = df["År"].astype(str).str[:4].iloc[0]

# Sort: Kommuner alphabetically, then Telemark, then Landet
sort_map = {}
unique_kommuner = df["Kommune"].unique().tolist()

regular = sorted([k for k in unique_kommuner if k not in ["Telemark", "Landet"]])
for i, k in enumerate(regular, start=1):
    sort_map[k] = i
sort_map["Telemark"] = len(regular) + 1
sort_map["Landet"] = len(regular) + 2

df["SortOrder"] = df["Kommune"].map(sort_map)
df = df.sort_values("SortOrder").reset_index(drop=True)
df = df.drop(columns=["SortOrder", "År"])

# Add Label column (same as Kommune)
df["Label"] = df["Kommune"]

# Rename Andel column to include year dynamically
df = df.rename(columns={"Andel": f"Andel ({year})"})

# Final column order
df = df[["Kommune", f"Andel ({year})", "Label"]]

print("\n--- Ferdig datasett ---")
print(df)
print(f"\nAntall rader: {len(df)}")

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "valgdeltakelse_kommunevalg.csv"
task_name = "Folkehelse - Valgdeltakelse kommunevalg"
github_folder = "Data/08_Folkehelse og levekår/Miljø"
temp_folder = os.environ.get("TEMP_FOLDER")

# Call the function and get the "New Data" status
is_new_data = handle_output_data(
    df,
    file_name,
    github_folder,
    temp_folder,
    keepcsv=True,
    value_columns=[f"Andel ({year})"],
)

# Write the "New Data" status to a unique log file
log_dir = os.environ.get("LOG_FOLDER", os.getcwd())
task_name_safe = task_name.replace(".", "_").replace(" ", "_")
new_data_status_file = os.path.join(log_dir, f"new_data_status_{task_name_safe}.log")

with open(new_data_status_file, "w", encoding="utf-8") as log_file:
    log_file.write(f"{task_name_safe},{file_name},{'Yes' if is_new_data else 'No'}\n")

if is_new_data:
    print("New data detected and pushed to GitHub.")
else:
    print("No new data detected.")

print(f"New data status log written to {new_data_status_file}")