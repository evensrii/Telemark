import requests
import sys
import os
import glob
import pandas as pd
from pyjstat import pyjstat

# Import the utility functions from the Helper_scripts folder
from Helper_scripts.utility_functions import fetch_data
from Helper_scripts.utility_functions import delete_files_in_temp_folder
from Helper_scripts.email_functions import notify_errors
from Helper_scripts.github_functions import upload_github_file
from Helper_scripts.github_functions import download_github_file

temp_folder = os.environ.get("TEMP_FOLDER")

# Capture the name of the current script
script_name = os.path.basename(__file__)

# Example list of error messages to collect errors during execution <--- Eksempel på liste for å samle feilmeldinger under kjøring
error_messages = []

# Alle kommuner, siste år (dvs. "top")

# NB: Prøve å sette opp også med 2024-kommunenummer. Hvis ikke må scriptet oppdateres senere.

# Endepunkt for SSB API
POST_URL = "https://data.ssb.no/api/v0/no/table/11607/"
# POST_URL = "https://data.ssb0/no/table/11607/"

################# Spørring VTFK (-2023) #################

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

################# Spørring TFK (2024-) ################# (spørring ok for 2023, men gir bare "None"-verdier)

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
    # Fetch data using the fetch_data function, with separate calls for each request
    df_vtfk = fetch_data(POST_URL, payload_vtfk, error_messages, query_name="VTFK")
    df_tfk = fetch_data(POST_URL, payload_tfk, error_messages, query_name="TFK")

except Exception as e:
    # If any query fails, send the error notification and stop execution
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

##### Forsikre meg om at jeg får riktig resultat også når TFK-tall kommer (2024-)

# Extract the most recent year
most_recent_year = df["år"].max()

# Filter rows where 'år' is the most recent year and 'value' is not NaN
df_filtered = df[(df["år"] == most_recent_year) & (df["value"].notna())]

# Reset index if needed
df_filtered = df_filtered.reset_index(drop=True)

# Remove any symbols after the space in the "region" column (including the space)
df_filtered["region"] = df_filtered["region"].str.split(" ").str[0]


##### Videre bearbeiding av datasettet #####

# Remove columns alder, kjønn, landbakgrunn, statistikkvariabel and år
df_filtered = df_filtered.drop(
    columns=["alder", "kjønn", "landbakgrunn", "statistikkvariabel", "år"]
)

# Copy the column "region" to a new column "label
df_filtered["label"] = df_filtered["region"]

# Rename the column names to "Kommune", "Andel" and "Label"
df_filtered.columns = ["Kommune", "Andel", "Label"]


##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "andel_sysselsatte_innvandrere.csv"
github_folder = "Data/09_Innvandrere og inkludering/Arbeid og inntekt"

# Lagre som .csv i Temp folder
csv_file_name = file_name
df_filtered.to_csv(os.path.join(temp_folder, csv_file_name), index=False)

# GitHub configuration (Repo etc. is defined in the function)
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")  # Ensure this is set in your environment

destination_folder = github_folder

github_file_path = f"{destination_folder}/{csv_file_name}"
local_file_path = os.path.join(temp_folder, csv_file_name)

# Download the existing file from GitHub
existing_data = download_github_file(github_file_path)

# Check if new data compared to Github
if existing_data is not None:
    # Compare the existing data with the new data
    existing_df = existing_data.astype(str).sort_values(by=list(existing_data.columns))
    new_df = (
        pd.read_csv(local_file_path)
        .astype(str)
        .sort_values(by=list(existing_data.columns))
    )

    if existing_df.equals(new_df):
        print("No new data to upload. Skipping GitHub update.")
    else:
        print("New data detected. Uploading to GitHub.")
        upload_github_file(
            local_file_path, github_file_path, message=f"(Updating {csv_file_name})"
        )
else:
    # If the file does not exist on GitHub, upload the new file
    print("File not found on GitHub. Uploading new file.")
    upload_github_file(
        local_file_path, github_file_path, message=f"(Adding {csv_file_name})"
    )

##################### Remove temporary local files #####################

delete_files_in_temp_folder()
