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
from Helper_scripts.github_functions import handle_output_data

# Capture the name of the current script
script_name = os.path.basename(__file__)

# Example list of error messages to collect errors during execution <--- Eksempel på liste for å samle feilmeldinger under kjøring
error_messages = []

# Alle kommuner, siste år (dvs. "top")

# NB: Prøve å sette opp også med 2024-kommunenummer. Hvis ikke må scriptet oppdateres senere.

# Endepunkt for SSB API
POST_URL = "https://data.ssb.no/api/v0/no/table/11607/"

################# VTFK (-2023) #################

# Spørring for å hente ut data fra SSB
payload_vtfk = {
    "query": [
        {
            "code": "Region",
            "selection": {
                "filter": "agg_single:Komm2020",
                "values": [
                    "3806",
                    "3807",
                    "3808",
                    "3812",
                    "3813",
                    "3814",
                    "3815",
                    "3816",
                    "3817",
                    "3818",
                    "3819",
                    "3820",
                    "3821",
                    "3822",
                    "3823",
                    "3824",
                    "3825",
                ],
            },
        },
        {"code": "Alder", "selection": {"filter": "item", "values": ["15-74"]}},
        {"code": "Kjonn", "selection": {"filter": "item", "values": ["0"]}},
        {"code": "Landbakgrunn", "selection": {"filter": "item", "values": ["zzz"]}},
        {
            "code": "ContentsCode",
            "selection": {"filter": "item", "values": ["Sysselsatte2"]},
        },
        {"code": "Tid", "selection": {"filter": "top", "values": ["1"]}},
    ],
    "response": {"format": "json-stat2"},
}

################# TFK (2024-) ################# (spørring ok for 2023, men gir bare "None"-verdier)

payload_tfk = {
    "query": [
        {
            "code": "Region",
            "selection": {
                "filter": "agg_single:KommGjeldende",
                "values": [
                    "4001",
                    "4003",
                    "4005",
                    "4010",
                    "4012",
                    "4014",
                    "4016",
                    "4018",
                    "4020",
                    "4022",
                    "4024",
                    "4026",
                    "4028",
                    "4030",
                    "4032",
                    "4034",
                    "4036",
                ],
            },
        },
        {"code": "Alder", "selection": {"filter": "item", "values": ["15-74"]}},
        {"code": "Kjonn", "selection": {"filter": "item", "values": ["0"]}},
        {"code": "Landbakgrunn", "selection": {"filter": "item", "values": ["zzz"]}},
        {
            "code": "ContentsCode",
            "selection": {"filter": "item", "values": ["Sysselsatte2"]},
        },
        {"code": "Tid", "selection": {"filter": "top", "values": ["1"]}},
    ],
    "response": {"format": "json-stat2"},
}

## Kjøre spørringer i try-except for å fange opp feil. Quitter hvis feil.

try:
    df_vtfk = fetch_data(
        url=POST_URL,
        payload=payload_vtfk,
        error_messages=error_messages,
        query_name="VTFK Query",
        response_type="json",
    )
    df_tfk = fetch_data(
        url=POST_URL,
        payload=payload_tfk,
        error_messages=error_messages,
        query_name="TFK Query",
        response_type="json",
    )

except Exception as e:
    print(f"Error occurred: {e}")
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError(
        "A critical error occurred during data fetching, stopping execution."
    )

# Proceed with data analysis only if all queries succeeded

####### Slå sammen datasettene #######

print(f"Her hentes tallene for Vestfold og Telemark i {df_vtfk['år'].unique()}")
print(f"Her hentes tallene for Telemark i {df_tfk['år'].unique()}")

# Merge the two dataframes
df = pd.concat([df_vtfk, df_tfk])

# Convert "år" to datetime
df["år"] = pd.to_datetime(df["år"])

df.head()


##### Forsikre meg om at jeg får riktig resultat også når TFK-tall kommer (2024-)

# Extract the most recent year
most_recent_year = df["år"].max()

# Filter rows where 'år' is the most recent year and 'value' is not NaN
df_filtered = df[(df["år"] == most_recent_year) & (df["value"].notna())]

# Reset index if needed
df_filtered = df_filtered.reset_index(drop=True)

# Remove any symbols after the space in the "region" column (including the space)
df_filtered["region"] = df_filtered["region"].str.split(" ").str[0]

# For tittel på kolonnen
most_recent_year_year = most_recent_year.year

##### Videre bearbeiding av datasettet #####

# Remove columns alder, kjønn, landbakgrunn, statistikkvariabel and år
df_filtered = df_filtered.drop(
    columns=["alder", "kjønn", "landbakgrunn", "statistikkvariabel", "år"]
)

# Copy the column "region" to a new column "label
df_filtered["label"] = df_filtered["region"]

# Rename the column names to "Kommune", "Andel" and "Label"
df_filtered.columns = ["Kommune", f"Andel {most_recent_year_year}", "Label"]

df_filtered.head()

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "andel_sysselsatte_innvandrere.csv"
task_name = "Innvandrere - Sysselsatte"
github_folder = "Data/09_Innvandrere og inkludering/Arbeid og inntekt"
temp_folder = os.environ.get("TEMP_FOLDER")

# Call the function and get the "New Data" status
is_new_data = handle_output_data(df_filtered, file_name, github_folder, temp_folder, keepcsv=True)

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