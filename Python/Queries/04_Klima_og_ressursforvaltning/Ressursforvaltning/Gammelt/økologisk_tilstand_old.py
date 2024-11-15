import requests
from io import BytesIO
from io import StringIO
import numpy as np
import pandas as pd
import datetime as dt
import sys
import os
import glob

# Størstedelen av Telemark faller inn under Vest-Viken vannregion.
# (Mindre deler av Telemark drenerer også til Agder vannregion..)
# Vest-Viken reetablert, var tidligere Vestfold og Telemark vannregion.

url = "https://vann-nett.no/innsyn-api/chart/diagram2?riverbasindistrict=5111&naturalcode=nwb&onsize=false"

# Make the GET request
response = requests.get(url)

# Check if the request was successful
if response.status_code == 200:
    # Parse the JSON response
    data = response.json()

    # Convert the data into a pandas DataFrame
    df_original = pd.DataFrame(
        {
            "Table Label": data["tableLabels"],
            "Color": data["colors"],
            "RW Count": [row[0] for row in data["counts"]],
            "LW Count": [row[1] for row in data["counts"]],
            "CW Count": [row[2] for row in data["counts"]],
            "ALL Count": [row[3] for row in data["counts"]],
            "RW Percentage": [row[0] for row in data["percentages"]],
            "LW Percentage": [row[1] for row in data["percentages"]],
            "CW Percentage": [row[2] for row in data["percentages"]],
            "ALL Percentage": [row[3] for row in data["percentages"]],
        }
    )

    # Display the DataFrame
    print(df_original)
else:
    print(f"Failed to retrieve data: {response.status_code}")

df = df_original.copy()

# Remove columns "Color", "RW Count", "LW Count", "CW Count", "ALL Count"
df = df.drop(
    columns=["Color", "RW Count", "LW Count", "CW Count", "ALL Count", "ALL Percentage"]
)

# Set column names to "Vannkvalitet", "Elv", "Innsjø", "Kystvann" and "Samlet"
df.columns = ["Vannkvalitet", "Elv", "Innsjø", "Kystvann"]

df_restructured = df.set_index("Vannkvalitet").T.reset_index()
df_restructured.columns = [
    "Kategori",
    "Svært god",
    "God",
    "Moderat",
    "Dårlig",
    "Svært dårlig",
    "Udefinert",
]

df_restructured = df_restructured.drop(columns=["Udefinert"])

############# Save dfs as a csv files

csv = "økologisk_tilstand_vann.csv"
df_restructured.to_csv((f"../../Temp/{csv}"), index=False)


##################### Opplasting til Github #####################

# Legge til directory hvor man finner github_functions.py i sys.path for å kunne importere denne
current_directory = os.path.dirname(os.path.abspath(__file__))
two_levels_up_directory = os.path.abspath(
    os.path.join(current_directory, os.pardir, os.pardir)
)
sys.path.append(two_levels_up_directory)

from github_functions import upload_file_to_github

# Hvis eksisterer, oppdater filen. Hvis ikke, opprett filen.

csv_file = f"../../Temp/{csv}"
destination_folder = "Data/04_Klima og ressursforvaltning/Ressursforvaltning"  # Mapper som ikke eksisterer vil opprettes automatisk.
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
folder_path = "../../Temp"

# Call the function to delete files
delete_files_in_folder(folder_path)
