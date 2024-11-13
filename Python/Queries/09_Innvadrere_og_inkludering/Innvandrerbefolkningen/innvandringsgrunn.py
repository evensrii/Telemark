## Skal ende opp med et datasett lik det som importeres i den allerede eksisterende .pbix-filen.
# Dvs. kun hente inn dataene automatisk, ikke manuelt, som nå.
# Ikke behov for noe særlig databehandling.

import requests
from io import BytesIO
from io import StringIO
import pandas as pd
import sys
import os
import glob

## Imdi gir ingen direkte url til .xlsx-fil, må trykke JS-knapp som trigger "OnClick"-event.
# Jeg bruker requests for å simulere nedlasting av filen.

# Finner URL vha. "Inspiser side" og fane "Network"
url = "https://app-simapi-prod.azurewebsites.net/download_csv/k/befolkning_innvandringsgrunn"

# Make a GET request to the URL to download the file
response = requests.get(url)

# Hente ut innhold (data)
url_content = response.content

if response.status_code == 200:

    df = pd.read_csv(
        BytesIO(url_content),
        delimiter=";",
        encoding="ISO-8859-1",
        dtype={"Kommunenummer": str},
    )

else:
    print(f"Failed to download the file. Status code: {response.status_code}")

# Print the unique values in the column "Kommunenummer"
print(df["Kommunenummer"].unique())
df.dtypes

#### Save df as a csv file

# Ønsket filnavn <----------- MÅ ENDRES MANUELT!
csv_file_name = "innvandringsgrunn_telemark.csv"

df.to_csv((f"../../Temp/{csv_file_name}"), index=False)  # Relativt til dette scriptet.

##################### Opplasting til Github #####################

# Legge til directory hvor man finner github_functions.py i sys.path for å kunne importere denne
current_directory = os.path.dirname(os.path.abspath(__file__))
two_levels_up_directory = os.path.abspath(
    os.path.join(current_directory, os.pardir, os.pardir)
)
sys.path.append(two_levels_up_directory)

from github_functions import upload_file_to_github

# Hvis eksisterer, oppdater filen. Hvis ikke, opprett filen.

csv_file = f"../../Temp/{csv_file_name}"
destination_folder = "Data/09_Innvandrere og inkludering/Innvandrerbefolkningen"  # Mapper som ikke eksisterer vil opprettes automatisk.
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
