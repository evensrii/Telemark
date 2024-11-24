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

# Telemark, siste år.

## Har kun andeler, og kun for Vestfold og Telemark samlet. Kan dermed ikke regne ut for Telemark samlet basert på absolutte tall.
## Utarbeider scriptet slik at det i framtiden kan hente ut tall for kun Telemark.


###### Andel innvandrere i lavinntekt ######

# Finner URL vha. "Inspiser side" og fane "Network" (F12)
url = "https://app-simapi-prod.azurewebsites.net/download_csv/f/vedvarende_lavinntekt"

## Kjøre spørringer i try-except for å fange opp feil. Quitter hvis feil.

try:
    df = fetch_data(
        url=url,
        error_messages=error_messages,
        query_name="Lavinntekt",
        payload=None,
        response_type="csv",
        delimiter=";",
        encoding="ISO-8859-1",
    )

except Exception as e:
    print(f"Error occurred: {e}")
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError(
        "A critical error occurred during data fetching, stopping execution."
    )


# Make a GET request to the URL to download the file
response = requests.get(url)

# Hente ut innhold (data)
url = response.content

if response.status_code == 200:

    df = pd.read_csv(BytesIO(url), delimiter=";", encoding="ISO-8859-1")

else:
    print(f"Failed to download the file. Status code: {response.status_code}")

df.info()
df.head()

# Set data types
df["Fylkesnummer"] = df["Fylkesnummer"].astype(str).str.zfill(2)
df["År"] = pd.to_datetime(df["År"], format="%Y")

# Fange opp VTFK, og etterhvert også TFK-tallene.
df = df[df["Fylkesnummer"].isin(["38", "40"])]
# dtale.show(df, open_browser=True)

# Get the most recent year in the dataset
most_recent_year = df["År"].max()

# Add the year ({most_recent_year.year}) to the "Andel" column name
andel_column_name = f"Andel ({most_recent_year.year})"
df = df.rename(columns={"Antall": andel_column_name})

# Filter rows based on the most recent year
df = df[df["År"] == most_recent_year]

# Filter based on other criteria
df = df[df["Bakgrunn"] != "Hele befolkningen"]
df = df[~((df["Bakgrunn"] == "Innvandrere") & (df["Verdensregion"] == "Alle"))]

# Replace values
df["Verdensregion"] = df["Verdensregion"].replace(
    {
        "Øst-Europa utenom EU, Asia (inkl.Tyrkia), Afrika, Sør- og Mellom-Amerika og Oseania utenom Australia og New Zealand": "Gruppe 2-land (Asia, Afrika, Latin-Amerika osv.)",
        "EU/EFTA-land, Nord-Amerika, Australia og New Zealand": "Gruppe 1-land (EU, Storbritannia, USA osv.)",
        "Alle": "Befolkningen unntatt innvandrere",
    }
)

# Convert "Andel" to numeric and round to nearest integer
df[andel_column_name] = pd.to_numeric(
    df[andel_column_name].str.replace(",", "."), errors="coerce"
)
df[andel_column_name] = df[andel_column_name].round(0).astype(int)

# Rename columns
df = df.rename(columns={"Verdensregion": "Gruppe"})

# Remove columns
df = df.drop(columns=["Bakgrunn", "Fylkesnummer", "Fylke", "År", "Enhet"])

# Reset index
df = df.reset_index(drop=True)


##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "andel_innvandrere_i_lavinntekt.csv"
github_folder = "Data/09_Innvandrere og inkludering/Arbeid og inntekt"
temp_folder = os.environ.get("TEMP_FOLDER")

compare_to_github(
    df, file_name, github_folder, temp_folder
)  # <--- Endre navn på dataframe her!

##################### Remove temporary local files #####################

delete_files_in_temp_folder()
