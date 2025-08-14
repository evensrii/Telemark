import requests
import sys
import os
import glob
from io import BytesIO
from io import StringIO
import pandas as pd
from pyjstat import pyjstat
import numpy as np

# Import the utility functions from the Helper_scripts folder
from Helper_scripts.utility_functions import fetch_data
from Helper_scripts.email_functions import notify_errors
from Helper_scripts.github_functions import handle_output_data

# Capture the name of the current script
script_name = os.path.basename(__file__)

# Example list of error messages to collect errors during execution <--- Eksempel på liste for å samle feilmeldinger under kjøring
error_messages = []

###### Telemark tom. 2019 + 2024

# Finner URL vha. "Inspiser side" og fane "Network" (F12)
url_fylker = "https://app-simapi-prod.azurewebsites.net/download_csv/f/barnehagedeltakelse_spraak"

## Kjøre spørringer i try-except for å fange opp feil. Quitter hvis feil.

try:
    df_fylker = fetch_data(
        url=url_fylker,
        payload=None,  # The JSON payload for POST requests. If None, a GET request is used.
        error_messages=error_messages,
        query_name="Fylker",
        response_type="csv",  # The expected response type, either 'json' or 'csv'.
        delimiter=";",  # The delimiter for CSV data (default: ';').
        encoding="ISO-8859-1",  # The encoding for CSV data (default: 'ISO-8859-1').
    )
except Exception as e:
    print(f"Error occurred: {e}")
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError(
        "A critical error occurred during data fetching, stopping execution."
    )

df_telemark = df_fylker.copy()

# df_telemark.info()
df_telemark.head()

#Identify duplicates
#print(df_telemark.duplicated().sum()) #9640 duplikater

# Remove duplicates
df_telemark = df_telemark.drop_duplicates()

# Convert fylkesnummer til string with leading zeros and convert to string
df_telemark["Fylkesnummer"] = df_telemark["Fylkesnummer"].astype(str).str.zfill(2)
#print(df_telemark["Fylkesnummer"].unique())

# Filter rows
df_telemark = df_telemark[
    df_telemark["Fylkesnummer"].isin(["08", "40"])
]  # Telemark before and after VTFK.

#Sort by colum "År"
df_telemark = df_telemark.sort_values(by="År", ascending=True)
df_telemark = df_telemark[df_telemark["Enhet"] == "Prosent"]
df_telemark = df_telemark[df_telemark["Alder"] == "5 år"]
df_telemark = df_telemark[df_telemark["Språk"] != "Alle"]
df_telemark = df_telemark[df_telemark["År"] != 2013]

# Remove columns
df_telemark = df_telemark.drop(columns=["Enhet", "Fylkesnummer", "Fylke", "Alder"])

# Rename columns
df_telemark = df_telemark.rename(columns={"Antall": "Andel"})

# Format "År" as datetime
df_telemark["År"] = pd.to_datetime(df_telemark["År"], format="%Y")

# Rename values containing "Minoritetsspråklige (utenom norsk" to "Minoritetsspråklige"
df_telemark["Språk"] = df_telemark["Språk"].replace(
    {
        "Minoritetsspråklige (utenom norsk, samisk, svensk, dansk og engelsk)": "Minoritetsspråklige",
        "Ikke-minoritetsspråklige (inkl. norsk, samisk, svensk, dansk og engelsk)": "Ikke-minoritetsspråklige",
    }
)

# Sort by "Språk" (ascending) and "År" (ascending)
df_telemark = df_telemark.sort_values(by=["Språk", "År"], ascending=True)

# If values in "Andel" is >100, set to 100
# df_telemark["Andel"] = np.where(df_telemark["Andel"] > 100, 100, df_telemark["Andel"])

# Reset index
df_telemark = df_telemark.reset_index(drop=True)

# If values in "Prosent" is >100, set to 100
df_telemark["Andel"] = np.where(df_telemark["Andel"] > 100, 100, df_telemark["Andel"])

# dtale.show(open_browser=True)



###### VESTFOLD OG TELEMARK 2020 - 2023

df_vestfold_telemark = df_fylker.copy()

# df_vestfold_telemark.info()
df_vestfold_telemark.head()

# Convert fylkesnummer til string with leading zeros and convert to string
df_vestfold_telemark["Fylkesnummer"] = df_vestfold_telemark["Fylkesnummer"].astype(str).str.zfill(2)
print(df_vestfold_telemark["Fylkesnummer"].unique())

# Filter rows
df_vestfold_telemark = df_vestfold_telemark[
    df_vestfold_telemark["Fylkesnummer"].isin(["38"])
]  # Telemark before and after VTFK.

#Identify duplicates
#print(df_vestfold_telemark.duplicated().sum()) #9640 duplikater

# Remove duplicates
df_vestfold_telemark = df_vestfold_telemark.drop_duplicates()

#Sort by colum "År"
df_vestfold_telemark = df_vestfold_telemark.sort_values(by="År", ascending=True)
df_vestfold_telemark = df_vestfold_telemark[df_vestfold_telemark["Enhet"] == "Prosent"]
df_vestfold_telemark = df_vestfold_telemark[df_vestfold_telemark["Alder"] == "5 år"]
df_vestfold_telemark = df_vestfold_telemark[df_vestfold_telemark["Språk"] != "Alle"]
df_vestfold_telemark = df_vestfold_telemark[df_vestfold_telemark["År"] != 2013]

# Remove columns
df_vestfold_telemark = df_vestfold_telemark.drop(columns=["Enhet", "Fylkesnummer", "Fylke", "Alder"])

# Rename columns
df_vestfold_telemark = df_vestfold_telemark.rename(columns={"Antall": "Andel"})

# Format "År" as datetime
df_vestfold_telemark["År"] = pd.to_datetime(df_vestfold_telemark["År"], format="%Y")

# Rename values containing "Minoritetsspråklige (utenom norsk" to "Minoritetsspråklige"
df_vestfold_telemark["Språk"] = df_vestfold_telemark["Språk"].replace(
    {
        "Minoritetsspråklige (utenom norsk, samisk, svensk, dansk og engelsk)": "Minoritetsspråklige",
        "Ikke-minoritetsspråklige (inkl. norsk, samisk, svensk, dansk og engelsk)": "Ikke-minoritetsspråklige",
    }
)

# Sort by "Språk" (ascending) and "År" (ascending)
df_vestfold_telemark = df_vestfold_telemark.sort_values(by=["Språk", "År"], ascending=True)

# If values in "Andel" is >100, set to 100
# df_vestfold_telemark["Andel"] = np.where(df_vestfold_telemark["Andel"] > 100, 100, df_vestfold_telemark["Andel"])

# Reset index
df_vestfold_telemark = df_vestfold_telemark.reset_index(drop=True)

# If values in "Prosent" is >100, set to 100
df_vestfold_telemark["Andel"] = np.where(df_vestfold_telemark["Andel"] > 100, 100, df_vestfold_telemark["Andel"])


###### Telemark 2020-2023 (Aggregeres fra kommunenivå)

### NEI, FOR ALT FOR MANGE PRIKKEDE TALL FOR MINORITETSSÅRÅKELIGE I DISTRIKTENE!!

## Har ikke andel for Telemark, men får hentet ut antall barn i barnehage, og andelen disse utgjør, per kommune.
## Kan da beregne og summere tall for (barn i barnehage) og (totalt antall barn), og fra dette kalkulere den totale
## andelen barn i barnehage i Telemark.
""" 
url_kommuner = "https://app-simapi-prod.azurewebsites.net/download_csv/k/barnehagedeltakelse_spraak"


try:
    df_kommuner = fetch_data(
        url=url_kommuner,
        payload=None,  # The JSON payload for POST requests. If None, a GET request is used.
        error_messages=error_messages,
        query_name="Kommuner",
        response_type="csv",  # The expected response type, either 'json' or 'csv'.
        delimiter=";",  # The delimiter for CSV data (default: ';').
        encoding="ISO-8859-1",  # The encoding for CSV data (default: 'ISO-8859-1').
    )
except Exception as e:
    print(f"Error occurred: {e}")
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError(
        "A critical error occurred during data fetching, stopping execution."
    )

# df_kommuner.info()
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

#Identify duplicates
print(df_kommuner.duplicated().sum())

# Remove duplicates
df_kommuner = df_kommuner.drop_duplicates()

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

# List all NaN values
df_kommuner[df_kommuner.isnull().any(axis=1)]

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
df_aggregert_fylke = df_aggregert_fylke.rename(columns={"andel_i_bhg": "Andel"}) """


########## Merge df_telemark (<-- 2019, 2024 -->) og df_vestfold_telemark (2020 - 2023) ##########

# Merge df_aggregert_fylke to df_telemark
df_barnehage = pd.concat([df_telemark, df_vestfold_telemark], ignore_index=True)

# Sort by "Språk" and "År"
df_barnehage = df_barnehage.sort_values(by=["Språk", "År"], ascending=True)

# Pivotere til rett format
df_barnehage_pivot = df_barnehage.pivot_table(
    index=["År"], columns="Språk", values="Andel"
).reset_index()
df_barnehage_pivot = df_barnehage_pivot[
    ["År", "Minoritetsspråklige", "Ikke-minoritetsspråklige"]
]

df_barnehage_pivot.head()

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "minoritetsspråklige_barnehage.csv"
task_name = "Innvandrere - Minoriteter i barnehage"
github_folder = "Data/09_Innvandrere og inkludering/Utdanningsnivå Telemark"
temp_folder = os.environ.get("TEMP_FOLDER")

# Call the function and get the "New Data" status
is_new_data = handle_output_data(df_barnehage_pivot, file_name, github_folder, temp_folder, keepcsv=True)

# Write the "New Data" status to a unique log file
log_dir = os.environ.get("LOG_FOLDER", os.getcwd())  # Default to current working directory
task_name_safe = file_name.replace(".", "_").replace(" ", "_")  # Ensure the task name is file-system safe
new_data_status_file = os.path.join(log_dir, f"new_data_status_{task_name_safe}.log")

# Write the result in a detailed format
with open(new_data_status_file, "w", encoding="utf-8") as log_file:
    log_file.write(f"{task_name_safe},{file_name},{'Yes' if is_new_data else 'No'}\n")

# Output results for debugging/testing
if is_new_data:
    print("New data detected and pushed to GitHub.")
else:
    print("No new data detected.")

print(f"New data status log written to {new_data_status_file}")
