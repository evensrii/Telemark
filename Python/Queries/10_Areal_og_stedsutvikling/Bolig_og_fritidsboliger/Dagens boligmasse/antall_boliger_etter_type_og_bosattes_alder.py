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

# SSB API v2 GET URL (tabell 11031 - Antall boliger etter type og bosattes alder)
GET_URL = (
    "https://data.ssb.no/api/pxwebapi/v2/tables/11031/data?lang=no"
    "&outputFormat=json-stat2"
    "&valuecodes[ContentsCode]=*"
    "&valuecodes[Tid]=from(2024)"
    "&valuecodes[Region]=4001,4003,4005,4012,4014,4020"
    "&codelist[Region]=vs_KommunStore"
    "&valuecodes[Byggeareal]=*"
    "&valuecodes[Alder]=*"
    "&heading=ContentsCode,Tid,Byggeareal"
    "&stub=Region,Alder"
)

## Kjøre spørringer i try-except for å fange opp feil. Quitter hvis feil.

try:
    df = fetch_data(
        url=GET_URL,
        payload=None,  # None = GET request (new SSB API v2)
        error_messages=error_messages,
        query_name="Antall boliger etter type og bosattes alder (11031)",
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

# Add Kommunenummer based on kommune name
kommunenummer_map = {
    "Porsgrunn": "4001",
    "Skien": "4003",
    "Notodden": "4005",
    "Bamble": "4012",
    "Kragerø": "4014",
    "Midt-Telemark": "4020",
}
df["Kommunenummer"] = df["region"].map(kommunenummer_map)

# Transform year to DD-MM-YYYY (1st of January)
df["år"] = "01.01." + df["år"].astype(str)

# Rename columns
df = df.rename(columns={
    "region": "Kommune",
    "alder": "Alder",
    "statistikkvariabel": "Statistikkvariabel",
    "år": "År",
    "bygningstype": "Bygningstype",
    "value": "Verdi",
})

# Pivot Statistikkvariabel into separate columns
index_cols = ["Kommunenummer", "Kommune", "Alder", "År", "Bygningstype"]
df = df.pivot_table(index=index_cols, columns="Statistikkvariabel", values="Verdi", aggfunc="first").reset_index()
df.columns.name = None

# Rename and transform
df = df.rename(columns={"Antall personer": "Antall", "Personer (prosent)": "Andel"})
df["Andel"] = pd.to_numeric(df["Andel"], errors="coerce") / 100

# Rename bygningstyper
df["Bygningstype"] = df["Bygningstype"].replace({
    "Boligblokk": "Leilighet",
    "Annen boligbygging": "Andre boligbygg",
    "Rekkehus, kjedehus, andre småhus": "Rekkehus, tomannsbolig og småhus",
    "Tomannsbolig": "Rekkehus, tomannsbolig og småhus",
})

# Rename "Alder i alt" to "Alle aldre"
df["Alder"] = df["Alder"].replace({"Alder i alt": "Alle aldre"})

# Aggregate age groups to 5 broader groups
alder_aggregering = {
    "0-19 år": "0-19 år",
    "20-29 år": "20-39 år",
    "30-39 år": "20-39 år",
    "40-49 år": "40-66 år",
    "50-66 år": "40-66 år",
    "67-79 år": "67-79 år",
    "80 år eller eldre": "80+",
    "Alle aldre": "Alle aldre",
}
df["Alder"] = df["Alder"].map(alder_aggregering)

# Aggregate numeric values after grouping
# Sum "Antall", recalculate "Andel" as share within each Kommune/År/Bygningstype
group_cols = ["Kommunenummer", "Kommune", "Alder", "År", "Bygningstype"]
df = df.groupby(group_cols, as_index=False).agg({"Antall": "sum"})

# Recalculate Andel: share of each age group within Kommune/År/Bygningstype
total_cols = ["Kommunenummer", "Kommune", "År", "Bygningstype"]
df["Andel"] = df["Antall"] / df.groupby(total_cols)["Antall"].transform("sum")

# Sort column: rank age groups by ascending age, with "Alle aldre" last
alder_rank = {
    "0-19 år": 1,
    "20-39 år": 2,
    "40-66 år": 3,
    "67-79 år": 4,
    "80+": 5,
    "Alle aldre": 6,
}
df["SortAlder"] = df["Alder"].map(alder_rank).fillna(0).astype(int)

# Check for unmapped age groups
unmapped_alder = df[df["SortAlder"] == 0]["Alder"].unique()
if len(unmapped_alder) > 0:
    print(f"WARNING: Unmapped Alder values: {unmapped_alder}")

# Sort column: rank bygningstyper by summed Antall for "Alle aldre" only
rank_bygningstype = (
    df[df["Alder"] == "Alle aldre"]
    .groupby("Bygningstype")["Antall"]
    .sum()
    .rank(ascending=False, method="min")
    .astype(int)
)
df["SortBygningstype"] = df["Bygningstype"].map(rank_bygningstype).fillna(0).astype(int)

# Reorder columns
df = df[["Kommunenummer", "Kommune", "Alder", "SortAlder", "År", "Bygningstype", "SortBygningstype", "Antall", "Andel"]]

print(df.head())

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "antall_boliger_etter_type_og_bosattes_alder.csv"
task_name = "Bolig - Antall boliger etter type og bosattes alder"
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