import requests
import sys
import os
import glob
from io import BytesIO
from io import StringIO
import pandas as pd
from pyjstat import pyjstat

# Import the utility functions from the Helper_scripts folder
from Helper_scripts.utility_functions import fetch_data
from Helper_scripts.utility_functions import delete_files_in_temp_folder
from Helper_scripts.email_functions import notify_errors
from Helper_scripts.github_functions import upload_github_file
from Helper_scripts.github_functions import download_github_file
from Helper_scripts.github_functions import compare_to_github

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
            "selection": {"filter": "vs:ArealUnderklasser02", "values": ["15", "16"]},
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
        query_name="Fulldyrka vs ikke-fulldyrka jordbruksareal",
        response_type="json",  # The expected response type, either 'json' or 'csv'.
        # delimiter=";", # The delimiter for CSV data (default: ';'). Comment if json.
        # encoding="ISO-8859-1", # The encoding for CSV data (default: 'ISO-8859-1'). Comment if json.
    )
except Exception as e:
    print(f"Error occurred: {e}")
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError(
        "A critical error occurred during data fetching, stopping execution."
    )

# Dropp kolonner "statistikkvariabel"
df = df.drop(columns=["statistikkvariabel"])

# Lag en variabel "siste_aar" som inneholder siste år i datasettet
siste_aar = df["år"].max()

# Pivoter på kolonne "arealklasse"
df_pivot = df.pivot(index="region", columns="arealklasse", values="value").reset_index()

# Set index to None
df_pivot.columns.name = None  # Remove the multi-index name for columns

# Multiply all valus in columns "Fullt dyrka jordbruksareal" and "Ikke-fulldyrka jordbruksareal" by 1000 to get hectares
df_pivot["Fulldyrka jord"] = df_pivot["Fulldyrka jord"] * 1000
df_pivot["Ikke fulldyrka jord"] = df_pivot["Ikke fulldyrka jord"] * 1000

# Show all values as integers/no decimals
df_pivot["Fulldyrka jord"] = df_pivot["Fulldyrka jord"].astype(int)
df_pivot["Ikke fulldyrka jord"] = df_pivot["Ikke fulldyrka jord"].astype(int)

# Remove column "Region"
df_pivot = df_pivot.drop(columns=["region"])

# Sum each of the columns "Fulldyrka jord" and "Ikke fulldyrka jord" to get total area per column
total_fulldyrka = df_pivot["Fulldyrka jord"].sum()
total_ikke_fulldyrka = df_pivot["Ikke fulldyrka jord"].sum()

# Create a new dataframe with the total area per column
df_total = pd.DataFrame(
    {
        "Jordbruksareal": ["Fulldyrka jord", "Ikke fulldyrka jord"],
        f"Dekar ({siste_aar})": [total_fulldyrka, total_ikke_fulldyrka],
    }
)

df_total.head()

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "fulldyrka_vs_ikke-fulldyrka.csv"
github_folder = "Data/10_Areal- og stedsutvikling/Areal til jordbruk"
temp_folder = os.environ.get("TEMP_FOLDER")

# Call the function and get the "New Data" status
is_new_data = compare_to_github(df_total, file_name, github_folder, temp_folder)

# Write the "New Data" status to a log file
with open("new_data_status.log", "w", encoding="utf-8") as log_file:
    if is_new_data:
        log_file.write(f"{file_name},New Data,Yes\n")
    else:
        log_file.write(f"{file_name},New Data,No\n")


##################### Remove temporary local files #####################

delete_files_in_temp_folder()
