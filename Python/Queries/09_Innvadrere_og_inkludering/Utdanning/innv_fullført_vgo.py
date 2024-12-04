import requests
import sys
import os
import glob
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

################# Fullført - Innvandrere #################

# Endepunkt for SSB API
POST_URL_innv = "https://data.ssb.no/api/v0/no/table/13628/"

# Spørring for å hente ut data fra SSB
payload_innv = {
    "query": [
        {"code": "Region", "selection": {"filter": "item", "values": ["08"]}},
        {
            "code": "FullforingVGO",
            "selection": {"filter": "item", "values": ["1a", "2a", "4b"]},
        },
        {"code": "UtdProgram", "selection": {"filter": "item", "values": ["00"]}},
        {"code": "Kjonn", "selection": {"filter": "item", "values": ["0"]}},
        {
            "code": "ContentsCode",
            "selection": {"filter": "item", "values": ["Prosent"]},
        },
    ],
    "response": {"format": "json-stat2"},
}

## Kjøre spørringer i try-except for å fange opp feil. Quitter hvis feil.

try:
    df_innvandrere = fetch_data(
        url=POST_URL_innv,
        payload=payload_innv,  # The JSON payload for POST requests. If None, a GET request is used.
        error_messages=error_messages,
        query_name="Innvandrere",
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

# df_innvandrere.info()
f_innvandrere.head()

# Remove columns
df_innvandrere = df_innvandrere.drop(
    columns=["region", "kjønn", "todelt utdanningsprogram", "statistikkvariabel"]
)

# Group by 'fullføringsgrad' and 'intervall (år)', and sum the 'value' column
df_innvandrere["fullføringsgrad"] = "Fullført"
df_innvandrere = (
    df_innvandrere.groupby(["fullføringsgrad", "intervall (år)"])["value"]
    .sum()
    .reset_index()
)

# Legge til kategori for innvandrere
df_innvandrere["gruppe"] = "Innvandrere"


################# Fullført - Hele befolkningen #################

# Endepunkt for SSB API
POST_URL_hele_bef = "https://data.ssb.no/api/v0/no/table/12971/"

# Spørring for å hente ut data fra SSB
payload_hele_bef = {
    "query": [
        {"code": "Region", "selection": {"filter": "item", "values": ["08"]}},
        {
            "code": "FullforingVGO",
            "selection": {"filter": "item", "values": ["1a", "2a", "4b"]},
        },
        {"code": "UtdProgram", "selection": {"filter": "item", "values": ["00"]}},
        {"code": "Kjonn", "selection": {"filter": "item", "values": ["0"]}},
        {
            "code": "ContentsCode",
            "selection": {"filter": "item", "values": ["Prosent"]},
        },
    ],
    "response": {"format": "json-stat2"},
}


try:
    df_befolkningen = fetch_data(
        url=POST_URL_hele_bef,
        payload=payload_hele_bef,  # The JSON payload for POST requests. If None, a GET request is used.
        error_messages=error_messages,
        query_name="Hele befolkningen",
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

# df_befolkningen.info()
df_befolkningen.head()
# dtale.show(df_befolkningen, open_browser=True)

# Remove columns
df_befolkningen = df_befolkningen.drop(
    columns=["region", "kjønn", "todelt utdanningsprogram", "statistikkvariabel"]
)

# Group by 'fullføringsgrad' and 'intervall (år)', and sum the 'value' column
df_befolkningen["fullføringsgrad"] = "Fullført"
df_befolkningen = (
    df_befolkningen.groupby(["fullføringsgrad", "intervall (år)"])["value"]
    .sum()
    .reset_index()
)

# Legge til kategori for innvandrere
df_befolkningen["gruppe"] = "Hele befolkningen"


########## Merge datasett ##########

# Merge df_aggregert_fylke to df_fylker
df_telemark = pd.concat([df_befolkningen, df_innvandrere], ignore_index=True)

# Create new column "Oppstartsår" containing the number before "-" in the "intervall (år)" column
df_telemark["År"] = df_telemark["intervall (år)"].str.split("-").str[0]

# Remove column "fullføringsgrad"
df_telemark = df_telemark.drop(columns=["fullføringsgrad", "intervall (år)"])

# Rename columns to "Andel", "Gruppe" and "År"
df_telemark = df_telemark.rename(columns={"value": "Andel", "gruppe": "Gruppe"})

# Reorder columns to "År", "Gruppe" and "Andel"
df_telemark = df_telemark[["År", "Gruppe", "Andel"]]

# Set the number of decimals in the "Andel" column to 1
df_telemark["Andel"] = df_telemark["Andel"].round(1)

# Pivotere til rett format
df_telemark_pivot = df_telemark.pivot_table(
    index=["År"], columns="Gruppe", values="Andel"
).reset_index()

# Flatten the MultiIndex columns
df_telemark_pivot.columns = [col for col in df_telemark_pivot.columns]

df_telemark_pivot.head()

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "fullført_vgo_innv_befolk.csv"
github_folder = "Data/09_Innvandrere og inkludering/Utdanningsnivå Telemark"
temp_folder = os.environ.get("TEMP_FOLDER")

compare_to_github(
    df_telemark_pivot, file_name, github_folder, temp_folder
)  # <--- Endre navn på dataframe her!

##################### Remove temporary local files #####################

delete_files_in_temp_folder()
