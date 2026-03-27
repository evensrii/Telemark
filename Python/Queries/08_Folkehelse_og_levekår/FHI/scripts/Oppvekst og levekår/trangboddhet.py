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

# FHI API endpoint (tabell 795 - Trangboddhet)
POST_URL = "https://statistikk-data.fhi.no/api/open/v1/nokkel/Table/795/data"

payload = {
    "response": {"format": "json-stat2"},
    "dimensions": [
        {
            "filter": "item",
            "values": [
                "0", "40", "4001", "4003", "4005", "4010", "4012", "4014",
                "4016", "4018", "4020", "4022", "4024", "4026", "4028",
                "4030", "4032", "4034", "4036",
            ],
            "code": "GEO",
        },
        {
            "filter": "bottom",
            "values": [
                "10",
            ],
            "code": "AAR",
        },
        {"filter": "item", "values": ["0_120"], "code": "ALDER"},
        {"filter": "item", "values": ["0", "2"], "code": "INNVKAT"},
        {"filter": "item", "values": ["trangt"], "code": "BODD"},
        {"filter": "item", "values": ["RATE"], "code": "MEASURE_TYPE"},
    ],
}

## Kjøre spørringer i try-except for å fange opp feil. Quitter hvis feil.

try:
    df = fetch_data(
        url=POST_URL,
        payload=payload,
        error_messages=error_messages,
        query_name="Trangboddhet (FHI tabell 795)",
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

# Drop columns "status" and "Måltall"
df = df.drop(columns=["status", "Måltall"])

# Add Kommunenummer column
kommunenummer_map = {
    "Hele landet": "00",
    "Telemark": "40",
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
}
df["Kommunenummer"] = df["Geografi"].map(kommunenummer_map)

# Divide value by 100 and rename to "Andel"
df["value"] = pd.to_numeric(df["value"], errors="coerce") / 100
df = df.rename(columns={"value": "Andel"})

# Format År column as YYYY-MM-DD (1st of January)
df["År"] = df["År"].astype(str).str[:4] + "-01-01"

# Capitalize values in Innvandringsbakgrunn column
df["Innvandringsbakgrunn"] = df["Innvandringsbakgrunn"].str.capitalize()

# Reorder columns with Kommunenummer first
cols = ["Kommunenummer"] + [c for c in df.columns if c != "Kommunenummer"]
df = df[cols]

print(df.head())

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "trangboddhet.csv"
github_folder = "Data/08_Folkehelse og levekår/Oppvekst og levekår"
temp_folder = os.environ.get("TEMP_FOLDER")

# Call the function and get the "New Data" status
is_new_data = handle_output_data(df, file_name, github_folder, temp_folder, keepcsv=True)

# Output results for debugging/testing
if is_new_data:
    print("New data detected and pushed to GitHub.")
else:
    print("No new data detected.")
