import requests
import pandas as pd
import sys
import os
import glob

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
    df_selected.to_excel(
        "../Temp/vannkraft_telemark.xlsx", index=False
    )  # Relativt til dette scriptet
    print(df_selected)


if __name__ == "__main__":
    get_hydro_power_plants_in_operation()


##################### Opplasting til Github #####################

# Hvis eksisterer, oppdater filen. Hvis ikke, opprett filen.

source_file = "../Temp/vannkraft_telemark.xlsx"  # Relativt til dette scriptet.
destination_folder = "Data/04_Klima og ressursforvaltning/Kraft og energi"  # Mapper som ikke eksisterer vil opprettes automatisk.
github_repo = "evensrii/Telemark"
git_branch = "main"

github_functions.upload_to_github(
    source_file, destination_folder, github_repo, git_branch
)

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
folder_path = "../Temp"

# Call the function to delete files
delete_files_in_folder(folder_path)
