import requests
import pandas as pd
import sys
import os

# import github_functions as github_functions

# Get the directory of the current script
current_directory = os.path.dirname(os.path.abspath(__file__))

# Get the parent directory
parent_directory = os.path.abspath(os.path.join(current_directory, os.pardir))

# Add the parent directory to sys.path
sys.path.append(parent_directory)

# Assuming you have a module named `github_functions.py` in the parent directory
import github_functions


def get_hydro_power_plants_in_operation():
    url = "https://api.nve.no/web/Powerplant/GetHydroPowerPlantsInOperation"

    # Make the request, return data
    response = requests.get(url)
    data = response.json()

    # convert to pandas dataframe, write to Excel
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
    df_selected.to_excel("../Temp/vannkraft_telemark.xlsx", index=False)
    print(df_selected)


if __name__ == "__main__":
    get_hydro_power_plants_in_operation()

##################### Opplasting til Github #####################


# Hvis eksisterer, oppdater filen. Hvis ikke, opprett filen.

source_file = "../Temp/vannkraft_telemark.xlsx"
destination_folder = "Data/04_Klima og ressursforvaltning"
github_repo = "evensrii/Telemark"
git_branch = "main"

github_functions.upload_to_github(
    source_file, destination_folder, github_repo, git_branch
)
