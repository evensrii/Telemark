import os
import pandas as pd

# Import the utility functions from the Helper_scripts folder
from Helper_scripts.utility_functions import fetch_data
from Helper_scripts.email_functions import notify_errors
from Helper_scripts.github_functions import handle_output_data

# Capture the name of the current script
script_name = os.path.basename(__file__)

# Example list of error messages to collect errors during execution
error_messages = []

################# Spørring #################

# SSB API v2 GET URL (tabell 05940 - Fullførte boliger etter boligtype)
GET_URL = (
    "https://data.ssb.no/api/pxwebapi/v2/tables/05940/data?lang=no"
    "&outputFormat=json-stat2"  
    "&valuecodes[Tid]=*"
    "&valuecodes[Region]=K-4001,K-4003,K-4005,K-4010,K-4012,K-4014,K-4016,K-4018,K-4020,K-4022,K-4024,K-4026,K-4028,K-4030,K-4032,K-4034,K-4036"
    "&codelist[Region]=agg_KommSummer"
    "&valuecodes[Byggeareal]=*"
    "&valuecodes[ContentsCode]=Fullforte"
    "&heading=Region,ContentsCode,Tid"
    "&stub=Byggeareal"
)

## Kjøre spørringer i try-except for å fange opp feil. Quitter hvis feil.

try:
    df = fetch_data(
        url=GET_URL,
        payload=None,  # None = GET request (new SSB API v2)
        error_messages=error_messages,
        query_name="Fullførte boliger etter boligtype (05940)",
        response_type="json",
    )
except Exception as e:
    print(f"Error occurred: {e}")
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError(
        "A critical error occurred during data fetching, stopping execution."
    )

print(df.head())
print(df.columns.tolist())

################# Data cleaning #################

# Drop statistikkvariabel column
df = df.drop(columns=["statistikkvariabel"])

# Add Kommunenummer based on kommune name
kommunenummer_map = {
    "Porsgrunn": "4001",
    "Skien": "4003",
    "Notodden": "4005",
    "Siljan": "4010",
    "Bamble": "4012",
    "Kragerø": "4014",
    "Drangedal": "4016",
    "Nome": "4018",
    "Midt-Telemark": "4020",
    "Seljord": "4022",
    "Hjartdal": "4024",
    "Tinn": "4026",
    "Kviteseid": "4028",
    "Nissedal": "4030",
    "Fyresdal": "4032",
    "Tokke": "4034",
    "Vinje": "4036",
}
df["Kommunenummer"] = df["region"].map(kommunenummer_map)

# Transform year to DD-MM-YYYY (1st of January)
df["år"] = "01.01." + df["år"].astype(str)

# Rename columns
df = df.rename(columns={
    "region": "Kommune",
    "år": "År",
    "bygningstype": "Bygningstype",
    "value": "Antall",
})

# Create "Største bygningstyper" column (top 9 by total county-wide Antall, rest = "Andre")
top9 = (
    df.groupby("Bygningstype")["Antall"]
    .sum()
    .nlargest(9)
)
top9_rank = {boligtype: rank for rank, boligtype in enumerate(top9.index, start=1)}
df["Største bygningstyper"] = df["Bygningstype"].where(df["Bygningstype"].isin(top9.index), "Andre")
df["SortStorsteBygningstype"] = df["Bygningstype"].map(top9_rank).fillna(10).astype(int)

# Create "SortBygningstype" column (rank all bygningstyper by county-wide total)
all_rank = (
    df.groupby("Bygningstype")["Antall"]
    .sum()
    .rank(ascending=False, method="min")
    .astype(int)
)
df["SortBygningstype"] = df["Bygningstype"].map(all_rank)

# Reorder columns with Kommunenummer first
df = df[["Kommunenummer", "Kommune", "År", "Bygningstype", "SortBygningstype", "Største bygningstyper", "SortStorsteBygningstype", "Antall"]]

print(df.head())

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "fullførte_boliger_etter_boligtype.csv"
task_name = "Bolig - Fullforte boliger etter boligtype"
github_folder = "Data/10_Areal- og stedsutvikling/Bolig/Dagens boligmasse"
temp_folder = os.environ.get("TEMP_FOLDER")

# Call the function and get the "New Data" status
is_new_data = handle_output_data(df, file_name, github_folder, temp_folder, keepcsv=True)

# Write the "New Data" status to a unique log file
log_dir = os.environ.get("LOG_FOLDER", os.getcwd())
task_name_safe = task_name.replace(".", "_").replace(" ", "_")
new_data_status_file = os.path.join(log_dir, f"new_data_status_{task_name_safe}.log")

with open(new_data_status_file, "w", encoding="utf-8") as log_file:
    log_file.write(f"{task_name_safe},{file_name},{'Yes' if is_new_data else 'No'}\n")

if is_new_data:
    print("New data detected and pushed to GitHub.")
else:
    print("No new data detected.")

print(f"New data status log written to {new_data_status_file}")
