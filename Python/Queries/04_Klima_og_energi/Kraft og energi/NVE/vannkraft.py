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

temp_folder = os.environ.get("TEMP_FOLDER")


def get_hydro_power_plants_in_operation():
    url = "https://api.nve.no/web/Powerplant/GetHydroPowerPlantsInOperation"

    # Make the request, return data
    response = requests.get(url)
    data = response.json()

    # Convert to pandas DataFrame
    df = pd.DataFrame(data)
    selected_columns = [
        "Navn",
        "Fylke",
        "Kommune",
        "KommuneNr",
        "ForsteUtnyttelseAvFalletDato",
        "MidProd_91_20",
    ]

    df_selected = df[selected_columns]
    df_selected = df_selected[df_selected["Fylke"] == "Telemark "]
    df_selected = df_selected.drop(columns=["Fylke", "KommuneNr"])
    df_selected.columns = [
        "Kraftverk",
        "Kommune",
        "Produksjon oppstart",
        "Årlig produksjon (1991-2020)",
    ]

    return df_selected


df = get_hydro_power_plants_in_operation()

df.info()
df.head()

df.to_excel(
    f"{temp_folder}/vannkraft_telemark.xlsx", index=False
)  # Relativt til dette scriptet

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "vannkraft_telemark.xlsx"
github_folder = (
    "Data/04_Klima og ressursforvaltning/Kraft og energi/Kraftproduksjon/NVE"
)
temp_folder = os.environ.get("TEMP_FOLDER")

compare_to_github(
    df, file_name, github_folder, temp_folder
)  # <--- Endre navn på dataframe her!

##################### Remove temporary local files #####################

delete_files_in_temp_folder()
