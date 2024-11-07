import requests
import sys
import os
import glob
import pandas as pd
from pyjstat import pyjstat

# Legge til directory hvor man finner github_functions.py i sys.path for å kunne importere denne
current_directory = os.path.dirname(os.path.abspath(__file__))
two_levels_up_directory = os.path.abspath(
    os.path.join(current_directory, os.pardir, os.pardir)
)
sys.path.append(two_levels_up_directory)

from email_functions import notify_errors
from github_functions import upload_file_to_github

# Capture the name of the current script
script_name = os.path.basename(__file__)

# Example list of error messages to collect errors during execution <--- Eksempel på liste for å samle feilmeldinger under kjøring
error_messages = []

# Alle kommuner, siste år (dvs. "top")

# NB: Prøve å sette opp også med 2024-kommunenummer. Hvis ikke må scriptet oppdateres senere.

# Endepunkt for SSB API
POST_URL = "https://data.ssbssb.no/api/v0/no/table/11607/"

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

## Kjøre spørringer i try-except for å fange opp feil. Quitter dermed ikke før feilsjekk til slutt.

try:
    # Attempt the request
    resultat_vtfk = requests.post(POST_URL, json=payload_vtfk)
    resultat_vtfk.raise_for_status()  # Raises exception for non-200 responses

    # If successful, proceed with data processing
    dataset = pyjstat.Dataset.read(resultat_vtfk.text)
    df_vtfk = dataset.write("dataframe")
    print("VTFK data loaded successfully")

except requests.exceptions.RequestException as e:
    # On failure, log the error, send notification, and terminate the script
    error_message = f"Error in VTFK request: {str(e)}"
    print(error_message)
    error_messages.append(error_message)
    notify_errors(error_messages, script_name=script_name)  # Send notification
    raise RuntimeError("Data request failed, stopping further execution.")
    # exit(1)  # <-- Bedre når jeg ikke kjører i en Jupyter notebook.

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

## Kjøre spørringer i try-except for å fange opp feil. Quitter dermed ikke før feilsjekk til slutt.

try:
    resultat_tfk = requests.post(POST_URL, json=payload_tfk)
    resultat_tfk.raise_for_status()  # Raises HTTPError for bad responses (non-200 status codes)

    dataset = pyjstat.Dataset.read(resultat_tfk.text)
    df_tfk = dataset.write("dataframe")
    print("TFK data loaded successfully")

except requests.exceptions.RequestException as e:
    error_message = f"Error in TFK request: {str(e)}"
    print(error_message)
    error_messages.append(error_message)
    notify_errors(error_messages, script_name=script_name)  # Send notification
    raise RuntimeError("Data request failed, stopping further execution.")

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


#### Save df as a csv file

# Ønsket filnavn <----------- MÅ ENDRES MANUELT!
csv_file_name = f"andel_sysselsatte_innvandrere.csv"
df_filtered.to_csv(
    (f"../../Temp/{csv_file_name}"), index=False
)  # Relativt til dette scriptet.

##################### Opplasting til Github #####################

# Hvis eksisterer, oppdater filen. Hvis ikke, opprett filen.

csv_file = f"../../Temp/{csv_file_name}"
destination_folder = "Data/09_Innvandrere og inkludering/Arbeid og inntekt"  # Mapper som ikke eksisterer vil opprettes automatisk.
github_repo = "evensrii/Telemark"
git_branch = "main"

upload_file_to_github(csv_file, destination_folder, github_repo, git_branch)

##################### Remove temporary files #####################

# Delete files in folder using glob


def delete_files_in_folder(folder_path):
    # Construct the path pattern to match all files in the folder
    files = glob.glob(os.path.join(folder_path, "*"))

    # Iterate over the list of files and delete each one
    for file_path in files:
        try:
            os.remove(file_path)
            print(f"Deleted file: {file_path}")
        except Exception as e:
            print(f"Error deleting file {file_path}: {e}")


# Specify the folder path
folder_path = "..\..\Temp"

# Call the function to delete files
delete_files_in_folder(folder_path)
