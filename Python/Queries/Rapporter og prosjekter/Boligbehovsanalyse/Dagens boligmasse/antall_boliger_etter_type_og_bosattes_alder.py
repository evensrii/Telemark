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

# SSB API v2 GET URL (tabell 11031 - Antall boliger etter type og bosattes alder)
GET_URL = (
    "https://data.ssb.no/api/pxwebapi/v2/tables/11031/data?lang=no"
    "&outputFormat=json-stat2"
    "&valuecodes[ContentsCode]=*"
    "&valuecodes[Tid]=from(2024)"
    "&valuecodes[Region]=4001,4003,4005,4012,4014,4020"
    "&codelist[Region]=vs_KommunStore"
    "&valuecodes[Byggeareal]=*"
    "&valuecodes[Alder]=*"
    "&heading=ContentsCode,Tid,Byggeareal"
    "&stub=Region,Alder"
)

## Kjøre spørringer i try-except for å fange opp feil. Quitter hvis feil.

try:
    df = fetch_data(
        url=GET_URL,
        payload=None,  # None = GET request (new SSB API v2)
        error_messages=error_messages,
        query_name="Antall boliger etter type og bosattes alder (11031)",
        response_type="json",
    )
except Exception as e:
    print(f"Error occurred: {e}")
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError(
        "A critical error occurred during data fetching, stopping execution."
    )

print(df.head())
print(df.columns.tolist())

################# Data cleaning #################

# Add Kommunenummer based on kommune name
kommunenummer_map = {
    "Porsgrunn": "4001",
    "Skien": "4003",
    "Notodden": "4005",
    "Bamble": "4012",
    "Kragerø": "4014",
    "Midt-Telemark": "4020",
}
df["Kommunenummer"] = df["region"].map(kommunenummer_map)

# Transform year to DD-MM-YYYY (1st of January)
df["år"] = "01.01." + df["år"].astype(str)

# Rename columns
df = df.rename(columns={
    "region": "Kommune",
    "alder": "Alder",
    "statistikkvariabel": "Statistikkvariabel",
    "år": "År",
    "bygningstype": "Bygningstype",
    "value": "Verdi",
})

# Reorder columns with Kommunenummer first
df = df[["Kommunenummer", "Kommune", "Alder", "Statistikkvariabel", "År", "Bygningstype", "Verdi"]]

print(df.head())

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "antall_boliger_etter_type_og_bosattes_alder.csv"
github_folder = "Data/Boligbehovsanalyse_2026/Dagens boligmasse"
temp_folder = os.environ.get("TEMP_FOLDER")

# Call the function and get the "New Data" status
is_new_data = handle_output_data(df, file_name, github_folder, temp_folder, keepcsv=True)

# Output results for debugging/testing
if is_new_data:
    print("New data detected and pushed to GitHub.")
else:
    print("No new data detected.")
