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

################# Spørring #################

# SSB API v2 GET URL (tabell 12272 - Minoritetsspråklige barn i barnehager 1-5 år)
# Henter andel minoritetsspråklige barn i barnehage for alle kommuner (historiske og nåværende),
# samt landet og Telemark.
GET_URL = (
    "https://data.ssb.no/api/pxwebapi/v2/tables/12272/data?lang=no"
    "&outputFormat=json-stat2"
    "&valueCodes[ContentsCode]=KOSandelminsptot0000"
    "&valueCodes[Tid]=from(2015)"
    "&valueCodes[KOKkommuneregion0000]=EAK,EKA08,EKA38,EKA40,"
    "4001,4003,4005,4010,4012,4014,4016,4018,4020,4022,4024,4026,4028,4030,4032,4034,4036,"
    "3806,3807,3808,3812,3813,3814,3815,3816,3817,3818,3819,3820,3821,3822,3823,3824,3825,"
    "0805,0806,0807,0811,0814,0815,0817,0819,0821,0822,0826,0827,0828,0829,0830,0831,0833,0834"
)

## Kjøre spørring i try-except for å fange opp feil. Quitter hvis feil.

try:
    df = fetch_data(
        url=GET_URL,
        payload=None,  # None = GET request (new SSB API v2)
        error_messages=error_messages,
        query_name="Minoritetsspråklige barn i barnehage (12272)",
        response_type="json",
    )
except Exception as e:
    print(f"Error occurred: {e}")
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError(
        "A critical error occurred during data fetching, stopping execution."
    )

print(df.head(20))
print(df.columns.tolist())
print(df["region"].unique())

################# Data cleaning #################

# Drop the statistikkvariabel column (only one variable in this query)
df = df.drop(columns=["statistikkvariabel"])

# Drop rows where value is NaN (periods where kommune didn't exist)
df = df.dropna(subset=["value"])

# Build a mapping from region label to kommunenummer
# The API returns labels like "Porsgrunn", "Porsgrunn (2020-2023)", "Porsgrunn (-2019)"
# We keep all kommuner as-is (no aggregation of Sauherad and Bø pre 2020)

kommunenummer_map = {
    "Telemark (-2019)": "07",
    "Vestfold og Telemark (2020-2023)": "38",
    "Telemark": "40",
    "Landet": "00",
    # Nåværende kommuner (2024-)
    "Porsgrunn": "4001",
    "Skien": "4003",
    "Notodden": "4005",
    "Siljan": "4010",
    "Bamble": "4012",
    "Kragerø": "4014",
    "Drangedal": "4016",
    "Nome": "4018",
    "Midt-Telemark": "4020",
    "Seljord": "4022",
    "Hjartdal": "4024",
    "Tinn": "4026",
    "Kviteseid": "4028",
    "Nissedal": "4030",
    "Fyresdal": "4032",
    "Tokke": "4034",
    "Vinje": "4036",
    # 2020-2023
    "Porsgrunn (2020-2023)": "3806",
    "Skien (2020-2023)": "3807",
    "Notodden (2020-2023)": "3808",
    "Siljan (2020-2023)": "3812",
    "Bamble (2020-2023)": "3813",
    "Kragerø (2020-2023)": "3814",
    "Drangedal (2020-2023)": "3815",
    "Nome (2020-2023)": "3816",
    "Midt-Telemark (2020-2023)": "3817",
    "Tinn (2020-2023)": "3818",
    "Hjartdal (2020-2023)": "3819",
    "Seljord (2020-2023)": "3820",
    "Kviteseid (2020-2023)": "3821",
    "Nissedal (2020-2023)": "3822",
    "Fyresdal (2020-2023)": "3823",
    "Tokke (2020-2023)": "3824",
    "Vinje (2020-2023)": "3825",
    # Pre-2020
    "Porsgrunn (-2019)": "0805",
    "Skien (-2019)": "0806",
    "Notodden (-2019)": "0807",
    "Siljan (-2019)": "0811",
    "Bamble (-2019)": "0814",
    "Kragerø (-2019)": "0815",
    "Drangedal (-2019)": "0817",
    "Nome (-2019)": "0819",
    "Bø (Telemark) (-2019)": "0821",
    "Sauherad (-2019)": "0822",
    "Tinn (-2019)": "0826",
    "Hjartdal (-2019)": "0827",
    "Seljord (-2019)": "0828",
    "Kviteseid (-2019)": "0829",
    "Nissedal (-2019)": "0830",
    "Fyresdal (-2019)": "0831",
    "Tokke (-2019)": "0833",
    "Vinje (-2019)": "0834",
}

df["Kommunenummer"] = df["region"].map(kommunenummer_map)

# Debug: check for any unmapped regions
unmapped = df[df["Kommunenummer"].isna()]["region"].unique()
if len(unmapped) > 0:
    print(f"WARNING: Unmapped regions: {unmapped}")

# Clean kommune names: remove parenthetical suffixes like " (2020-2023)" and " (-2019)"
# For Telemark entries, also unify the name to "Telemark"
df["region"] = df["region"].str.replace(r"\s*\(.*?\)", "", regex=True).str.strip()

# Rename columns
df = df.rename(columns={
    "region": "Kommune",
    "år": "År",
    "value": "Andel",
})

# Divide andel by 100 (convert from percent to decimal)
df["Andel"] = df["Andel"] / 100

# Reorder columns
df = df[["Kommunenummer", "Kommune", "År", "Andel"]]

# Sort by Kommune and År
df = df.sort_values(["Kommune", "År"]).reset_index(drop=True)

print("\n--- Ferdig datasett ---")
print(df.head(30))
print(f"\nAntall rader: {len(df)}")
print(f"Kommuner: {df['Kommune'].unique()}")
print(f"År: {sorted(df['År'].unique())}")

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "minoriteter_barnehage_pbi.csv"
task_name = "Folkehelse - Minoriteter barnehage"
github_folder = "Data/08_Folkehelse og levekår/Oppvekst og levekår"
temp_folder = os.environ.get("TEMP_FOLDER")

# Call the function and get the "New Data" status
is_new_data = handle_output_data(
    df,
    file_name,
    github_folder,
    temp_folder,
    keepcsv=True,
    value_columns=["Andel"],
)

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
