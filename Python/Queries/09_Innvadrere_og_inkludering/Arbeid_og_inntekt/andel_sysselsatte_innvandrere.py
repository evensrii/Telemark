import requests
import sys
import os
import glob
import pandas as pd
from pyjstat import pyjstat

# Alle kommuner, siste år (dvs. "top")

# NB: Prøve å sette opp også med 2024-kommunenummer. Hvis ikke må scriptet oppdateres senere.

# Endepunkt for SSB API
POST_URL = "https://data.ssb.no/api/v0/no/table/11607/"

################# Spørring VTFK (-2023) #################

# Spørring for å hente ut data fra SSB
payload = {
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


resultat_vtfk = requests.post(POST_URL, json=payload)

if resultat_vtfk.status_code == 200:
    print("Spørring ok")
else:
    print(f"Spørring feilet. Statuskode: {resultat_vtfk.status_code}")

dataset = pyjstat.Dataset.read(resultat_vtfk.text)
df_vtfk = dataset.write("dataframe")
df_vtfk.head()
df_vtfk.info()

# Print the first value in the column "år"
print(f"Her hentes tallene for{df_vtfk['år'].unique()}")

################# Spørring TFK (2024-) ################# (spørring ok for 2023, men gir bare "None"-verdier)

payload = {
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

resultat_tfk = requests.post(POST_URL, json=payload)

if resultat_tfk.status_code == 200:
    print("Spørring ok")
else:
    print(f"Spørring feilet. Statuskode: {resultat_tfk.status_code}")

dataset = pyjstat.Dataset.read(resultat_tfk.text)
df_tfk = dataset.write("dataframe")
df_tfk.head()
df_tfk.info()

# Print the first value in the column "år"
print(f"Her hentes tallene for{df_tfk['år'].unique()}")

####### Slå sammen datasettene #######

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

# Legge til directory hvor man finner github_functions.py i sys.path for å kunne importere denne
current_directory = os.path.dirname(os.path.abspath(__file__))
two_levels_up_directory = os.path.abspath(
    os.path.join(current_directory, os.pardir, os.pardir)
)
sys.path.append(two_levels_up_directory)

from github_functions import upload_file_to_github

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
folder_path = "../../Temp"

# Call the function to delete files
delete_files_in_folder(folder_path)
