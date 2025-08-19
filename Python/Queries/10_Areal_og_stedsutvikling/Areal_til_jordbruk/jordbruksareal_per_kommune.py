import os
import pandas as pd
from pyjstat import pyjstat

# Import the utility functions from the Helper_scripts folder
from Helper_scripts.utility_functions import fetch_data
from Helper_scripts.email_functions import notify_errors
from Helper_scripts.github_functions import handle_output_data

# Capture the name of the current script
script_name = os.path.basename(__file__)

# Example list of error messages to collect errors during execution <--- Eksempel på liste for å samle feilmeldinger under kjøring
error_messages = []

# Endepunkt for SSB API
POST_URL = "https://data.ssb.no/api/v0/no/table/09594/"

# Spørring for å hente ut data fra SSB
payload = {
    "query": [
        {
            "code": "Region",
            "selection": {
                "filter": "agg:KommSummer",
                "values": [
                    "K-4001",
                    "K-4003",
                    "K-4005",
                    "K-4010",
                    "K-4012",
                    "K-4014",
                    "K-4016",
                    "K-4018",
                    "K-4020",
                    "K-4022",
                    "K-4024",
                    "K-4026",
                    "K-4028",
                    "K-4030",
                    "K-4032",
                    "K-4034",
                    "K-4036",
                ],
            },
        },
        {
            "code": "ArealKlasse",
            "selection": {"filter": "vs:ArealHovedklasser2", "values": ["15-16"]},
        },
        {"code": "Tid", "selection": {"filter": "top", "values": ["1"]}},
    ],
    "response": {"format": "json-stat2"},
}


## Kjøre spørringer i try-except for å fange opp feil. Quitter hvis feil.

try:
    df = fetch_data(
        url=POST_URL,
        payload=payload,  # The JSON payload for POST requests. If None, a GET request is used.
        error_messages=error_messages,
        query_name="Jordbruksareal per kommune",
        response_type="json",  # The expected response type, either 'json' or 'csv'.
        # delimiter=";", # The delimiter for CSV data (default: ';').
        # encoding="ISO-8859-1", # The encoding for CSV data (default: 'ISO-8859-1').
    )
except Exception as e:
    print(f"Error occurred: {e}")
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError(
        "A critical error occurred during data fetching, stopping execution."
    )


# Dropp kolonner "arealklasse" og "statistikkvariabel"
df = df.drop(columns=["arealklasse", "statistikkvariabel"])

# Endre kolonnenavn til Kommunenavn, År og Jordbruksareal
df.columns = ["Kommunenavn", "År", "Jordbruksareal"]

# Lag en variabel "siste_aar" som inneholder siste år i datasettet
siste_aar = df["År"].max()

# Legg til siste år i parentes i kolonnenavnet "Jordbruksareal", eks. "Jordbruksareal (2019)"
df.columns = ["Kommunenavn", "År", f"Jordbruksareal ({siste_aar})"]

# Fjern kolonnen "År"
df = df.drop(columns=["År"])

# Multiply by 1000 to get hectares
df[f"Jordbruksareal ({siste_aar})"] = df[f"Jordbruksareal ({siste_aar})"] * 1000

# Show as integer/no decimals
df[f"Jordbruksareal ({siste_aar})"] = df[f"Jordbruksareal ({siste_aar})"].astype(float)

# Sort by "Jordbruksareal (2019)" descending
df = df.sort_values(by=f"Jordbruksareal ({siste_aar})", ascending=False)

print(df)
print("\nData types:")
print(df.dtypes)

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "jordbruksareal_per_kommune.csv"
task_name = "Areal - Jordbruksareal per kommune"
github_folder = "Data/10_Areal- og stedsutvikling/Areal til jordbruk"
temp_folder = os.environ.get("TEMP_FOLDER")

# Call the function and get the "New Data" status
is_new_data = handle_output_data(df, file_name, github_folder, temp_folder, keepcsv=True)

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