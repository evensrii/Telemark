import requests
from io import BytesIO
from io import StringIO
import numpy as np
import pandas as pd
import datetime as dt
import sys
import os
import glob
import dtale

###### Fylker tom. 2019 + 2024

# Finner URL vha. "Inspiser side" og fane "Network" (F12)
url_fylker = "https://app-simapi-prod.azurewebsites.net/download_csv/f/barnehagedeltakelse_spraak"

# Make a GET request to the URL to download the file
response = requests.get(url_fylker)

# Hente ut innhold (data)
url_fylker_content = response.content

if response.status_code == 200:

    df_fylker = pd.read_csv(
        BytesIO(url_fylker_content), delimiter=";", encoding="ISO-8859-1"
    )

else:
    print(f"Failed to download the file. Status code: {response.status_code}")

df_fylker.info()
df_fylker.head()

# Convert fylkesnummer til string with leading zeros and convert to string
df_fylker["Fylkesnummer"] = df_fylker["Fylkesnummer"].astype(str).str.zfill(2)
print(df_fylker["Fylkesnummer"].unique())

# Filter rows
df_fylker = df_fylker[
    df_fylker["Fylkesnummer"].isin(["08", "40"])
]  # Telemark before and after VTFK.
df_fylker = df_fylker[df_fylker["Enhet"] == "Prosent"]
df_fylker = df_fylker[df_fylker["Språk"] != "Alle"]
df_fylker = df_fylker[df_fylker["Alder"] == "5 år"]
df_fylker = df_fylker[df_fylker["År"] != 2013]

# Remove columns
df_fylker = df_fylker.drop(columns=["Enhet", "Fylkesnummer", "Fylke", "Alder"])

# Rename columns
df_fylker = df_fylker.rename(columns={"Antall": "Andel"})

# Format "År" as datetime
df_fylker["År"] = pd.to_datetime(df_fylker["År"], format="%Y")

# Rename values containing "Minoritetsspråklige (utenom norsk" to "Minoritetsspråklige"
df_fylker["Språk"] = df_fylker["Språk"].replace(
    {
        "Minoritetsspråklige (utenom norsk, samisk, svensk, dansk og engelsk)": "Minoritetsspråklige",
        "Ikke-minoritetsspråklige (inkl. norsk, samisk, svensk, dansk og engelsk)": "Ikke-minoritetsspråklige",
    }
)

# Sort by "Språk" (ascending) and "År" (ascending)
df_fylker = df_fylker.sort_values(by=["Språk", "År"], ascending=True)

# If values in "Andel" is >100, set to 100
# df_fylker["Andel"] = np.where(df_fylker["Andel"] > 100, 100, df_fylker["Andel"])

# Reset index
df_fylker = df_fylker.reset_index(drop=True)

# dtale.show(open_browser=True)


###### Telemark 2020-2023 (Aggregeres fra kommunenivå)

## Har ikke andel for Telemark, men får hentet ut antall barn i barnehage, og andelen disse utgjør, per kommune.
## Kan da beregne og summere tall for (barn i barnehage) og (totalt antall barn), og fra dette kalkulere den totale
## andelen barn i barnehage i Telemark.

url_kommuner = "https://app-simapi-prod.azurewebsites.net/download_csv/k/barnehagedeltakelse_spraak"

# Make a GET request to the URL to download the file
response = requests.get(url_kommuner)

# Hente ut innhold (data)
url_kommuner_content = response.content

if response.status_code == 200:

    df_kommuner = pd.read_csv(
        BytesIO(url_kommuner_content), delimiter=";", encoding="ISO-8859-1"
    )

else:
    print(f"Failed to download the file. Status code: {response.status_code}")

df_kommuner.info()
df_kommuner.head()

# dtale.show(df_kommuner, open_browser=True)

# Convert the column "Kommunenummer" to a string with 4 digits
df_kommuner["Kommunenummer"] = (
    df_kommuner["Kommunenummer"].astype(str).str.pad(width=4, side="left", fillchar="0")
)

# Dictionary for innfylling av manglende kommunenavn, samt filtrering av datasettet

kommuner_telemark = {
    "3806": "Porsgrunn",
    "3807": "Skien",
    "3808": "Notodden",
    "3812": "Siljan",
    "3813": "Bamble",
    "3814": "Kragerø",
    "3815": "Drangedal",
    "3816": "Nome",
    "3817": "Midt-Telemark",
    "3818": "Tinn",
    "3819": "Hjartdal",
    "3820": "Seljord",
    "3821": "Kviteseid",
    "3822": "Nissedal",
    "3823": "Fyresdal",
    "3824": "Tokke",
    "3825": "Vinje",
}

## Filtrering av kommuner i Telemark
df_kommuner = df_kommuner[df_kommuner["Kommunenummer"].isin(kommuner_telemark.keys())]

## Innfylling av manglende kommunenavn
df_kommuner["Kommune"] = df_kommuner["Kommune"].fillna(
    df_kommuner["Kommunenummer"].map(kommuner_telemark)
)

# Filter rows
df_kommuner = df_kommuner[df_kommuner["År"].isin([2020, 2021, 2022, 2023])]
df_kommuner = df_kommuner[df_kommuner["Alder"] == "5 år"]
df_kommuner = df_kommuner[df_kommuner["Språk"] != "Alle"]

# Conversions
df_kommuner["År"] = pd.to_datetime(df_kommuner["År"], format="%Y")
df_kommuner["Antall"] = pd.to_numeric(df_kommuner["Antall"], errors="coerce")

# Rename values containing "Minoritetsspråklige (utenom norsk" to "Minoritetsspråklige"
df_kommuner["Språk"] = df_kommuner["Språk"].replace(
    {
        "Minoritetsspråklige (utenom norsk, samisk, svensk, dansk og engelsk)": "Minoritetsspråklige",
        "Ikke-minoritetsspråklige (inkl. norsk, samisk, svensk, dansk og engelsk)": "Ikke-minoritetsspråklige",
    }
)

# Pivot table so that "Personer" and "Andel" are in separate columns
df_kommuner = df_kommuner.pivot_table(
    index=["Kommunenummer", "Kommune", "År", "Språk", "Alder"],
    columns="Enhet",
    values="Antall",
).reset_index()

# Flatten the MultiIndex columns
df_kommuner.columns = [col for col in df_kommuner.columns]

# If values in "Prosent" is >100, set to 100
df_kommuner["Prosent"] = np.where(
    df_kommuner["Prosent"] > 100, 100, df_kommuner["Prosent"]
)

# Calculate the total number of children in each group based on personer and prosent
df_kommuner["antall_barn_tot"] = 100 / df_kommuner["Prosent"] * df_kommuner["Personer"]

# Rename the column "Personer" to "antall_barn_i_bhg"
df_kommuner = df_kommuner.rename(columns={"Personer": "antall_barn_i_bhg"})

# Group by "År" and "Språk" and sum the columns "antall_barn_i_bhg" and "antall_barn_tot"
df_aggregert_fylke = (
    df_kommuner.groupby(["År", "Språk"])[["antall_barn_i_bhg", "antall_barn_tot"]]
    .sum()
    .reset_index()
)

# Calculate the percentage of children in kindergarten in Telemark
df_aggregert_fylke["andel_i_bhg"] = (
    df_kommuner["antall_barn_i_bhg"] / df_kommuner["antall_barn_tot"] * 100
)

# Remove columns "antall_barn_i_bhg" and "antall_barn_tot"
df_aggregert_fylke = df_aggregert_fylke.drop(
    columns=["antall_barn_i_bhg", "antall_barn_tot"]
)
df_aggregert_fylke = df_aggregert_fylke.rename(columns={"andel_i_bhg": "Andel"})


########## Merge df_fylker (fylkesdata) and df_aggregert_fylke (kommunedata) ##########

# Merge df_aggregert_fylke to df_fylker
df_telemark = pd.concat([df_fylker, df_aggregert_fylke], ignore_index=True)

# Sort by "Språk" and "År"
df_telemark = df_telemark.sort_values(by=["Språk", "År"], ascending=True)

# Pivotere til rett format
df_telemark_pivot = df_telemark.pivot_table(
    index=["År"], columns="Språk", values="Andel"
).reset_index()
df_telemark_pivot = df_telemark_pivot[
    ["År", "Minoritetsspråklige", "Ikke-minoritetsspråklige"]
]

#### Save df as a csv file

# Ønsket filnavn <----------- MÅ ENDRES MANUELT!
csv_file_name = f"minoritetsspråklige_barnehage.csv"
df_telemark_pivot.to_csv(
    (f"../../Temp/{csv_file_name}"), index=False
)  # Relativt til dette scriptet.

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
destination_folder = "Data/09_Innvandrere og inkludering/Utdanningsnivå Telemark"  # Mapper som ikke eksisterer vil opprettes automatisk.
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
