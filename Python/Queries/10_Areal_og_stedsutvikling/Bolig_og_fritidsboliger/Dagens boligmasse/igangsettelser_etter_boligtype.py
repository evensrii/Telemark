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

# SSB API v2 GET URL (tabell 05940 - Igangsettelser etter boligtype)
GET_URL = (
    "https://data.ssb.no/api/pxwebapi/v2/tables/05940/data?lang=no"
    "&outputFormat=json-stat2"
    "&valuecodes[Tid]=*"
    "&valuecodes[Region]=K-4001,K-4003,K-4005,K-4010,K-4012,K-4014,K-4016,K-4018,K-4020,K-4022,K-4024,K-4026,K-4028,K-4030,K-4032,K-4034,K-4036"
    "&codelist[Region]=agg_KommSummer"
    "&valuecodes[Byggeareal]=*"
    "&valuecodes[ContentsCode]=Igangsatte"
    "&heading=Region,ContentsCode,Tid"
    "&stub=Byggeareal"
)

## Kjøre spørringer i try-except for å fange opp feil. Quitter hvis feil.

try:
    df = fetch_data(
        url=GET_URL,
        payload=None,  # None = GET request (new SSB API v2)
        error_messages=error_messages,
        query_name="Igangsettelser etter boligtype (05940)",
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

# Fix subtle mismatches between the SSB API data and the SSB Klass classification system
replacements = {
    "Kjedehus inkl.atriumhus": "Kjedehus inkl. atriumhus",
    "Våningshus tomannsbolig, horisontaltdelt": "Våningshus, tomannsbolig, horisontaldelt",
    "Del av våninghus  tomannsbustad, vertikaltdelt": "Våningshus, tomannsbolig, vertikaldelt",
    "Del av tomannsbolig, vertikaldelt": "Tomannsbolig, vertikaldelt",
}
df["Bygningstype"] = df["Bygningstype"].replace(replacements)

# Use regex to catch all "Stort sammenbygd boligbygg ..." variants (with and without suffix)
df["Bygningstype"] = df["Bygningstype"].str.replace(
    r"^Stort sammenbygd boligbygg", "Store sammenbygde boligbygg", regex=True
)

# Fix "eller over" → "og over" (must run after the regex rename above)
df["Bygningstype"] = df["Bygningstype"].replace(
    {"Store sammenbygde boligbygg på 5 etasjer eller over": "Store sammenbygde boligbygg på 5 etasjer og over"}
)

# --- Bygningstype two-digit level classification (SSB Klass) ---
# Map each detailed bygningstype to its two-digit parent group
bygningstype_to_9gr = {
    # 11 - Enebolig
    "Enebolig": "Enebolig",
    "Enebolig med hybelleilighet, sokkelleilighet o.l.": "Enebolig",
    "Våningshus": "Enebolig",
    # 12 - Tomannsbolig
    "Tomannsbolig, vertikaldelt": "Tomannsbolig",
    "Tomannsbolig, horisontaldelt": "Tomannsbolig",
    "Våningshus, tomannsbolig, vertikaldelt": "Tomannsbolig",
    "Våningshus, tomannsbolig, horisontaldelt": "Tomannsbolig",
    # 13 - Rekkehus, kjedehus, andre småhus
    "Rekkehus": "Rekkehus, kjedehus, andre småhus",
    "Kjedehus inkl. atriumhus": "Rekkehus, kjedehus, andre småhus",
    "Terrassehus": "Rekkehus, kjedehus, andre småhus",
    "Andre småhus med 3 boliger eller flere": "Rekkehus, kjedehus, andre småhus",
    # 14 - Store boligbygg
    "Store frittliggende boligbygg på 2 etasjer": "Store boligbygg",
    "Store frittliggende boligbygg på 3 og 4 etasjer": "Store boligbygg",
    "Store frittliggende boligbygg på 5 etasjer eller over": "Store boligbygg",
    "Store sammenbygde boligbygg på 2 etasjer": "Store boligbygg",
    "Store sammenbygde boligbygg på 3 og 4 etasjer": "Store boligbygg",
    "Store sammenbygde boligbygg på 5 etasjer og over": "Store boligbygg",
    "Store sammenbygde boligbygg": "Store boligbygg",
    # 15 - Bygning for bofellesskap
    "Bo- og servicesenter": "Bygning for bofellesskap",
    "Studenthjem/studentboliger": "Bygning for bofellesskap",
    "Annen bygning for bofellesskap": "Bygning for bofellesskap",
    # 16 - Fritidsbolig
    "Fritidsbygning (hytter, sommerhus o.l.)": "Fritidsbolig",
    "Helårsbolig benyttet som fritidsbolig": "Fritidsbolig",
    "Våningshus benyttet som fritidsbolig": "Fritidsbolig",
    # 17 - Koie, seterhus og lignende
    "Seterhus, sel, rorbu o.l.": "Koie, seterhus og lignende",
    "Skogs- og utmarkskoie, gamme": "Koie, seterhus og lignende",
    # 18 - Garasje og uthus til bolig
    "Garasje, uthus, anneks knyttet til bolig": "Garasje og uthus til bolig",
    "Garasje, uthus, anneks knyttet til fritidsbolig": "Garasje og uthus til bolig",
    "Naust, båthus, sjøbu": "Garasje og uthus til bolig",
    # 19 - Annen boligbygning
    "Boligbrakker": "Annen boligbygning",
    "Annen boligbygning (f.eks. sekundærbolig reindrift)": "Annen boligbygning",
    # Not in SSB Klass bolig classification — kept as own category
    "Andre bygg enn boligbygg": "Andre bygg enn boligbygg",
}
df["Bygningstype_9_gr"] = df["Bygningstype"].map(bygningstype_to_9gr)

# Check for unmapped values and print diagnostics
unmapped_9gr = df[df["Bygningstype_9_gr"].isna()]["Bygningstype"].unique()
if len(unmapped_9gr) > 0:
    print(f"WARNING: Unmapped Bygningstype values for 9-group: {unmapped_9gr}")
    print("These will need to be added to the bygningstype_to_9gr mapping dict.")
    print("All unique Bygningstype values in data:")
    for val in sorted(df["Bygningstype"].unique()):
        mapped = "OK" if val in bygningstype_to_9gr else "MISSING"
        print(f"  [{mapped}] '{val}'")

# --- Bygningstype four-group classification (from Excel screenshot) ---
# Enebolig = 11, Leilighet = 14, Rekkehus tomannsbolig og småhus = 12+13, Annen boligbygging = 15+16+17+18+19
ninegr_to_4gr = {
    "Enebolig": "Enebolig",
    "Tomannsbolig": "Rekkehus, tomannsbolig og småhus",
    "Rekkehus, kjedehus, andre småhus": "Rekkehus, tomannsbolig og småhus",
    "Store boligbygg": "Leilighet",
    "Bygning for bofellesskap": "Annen boligbygging",
    "Fritidsbolig": "Annen boligbygging",
    "Koie, seterhus og lignende": "Annen boligbygging",
    "Garasje og uthus til bolig": "Annen boligbygging",
    "Annen boligbygning": "Annen boligbygging",
    "Andre bygg enn boligbygg": "Andre bygg enn boligbygg",
}
df["Bygningstype_4_gr"] = df["Bygningstype_9_gr"].map(ninegr_to_4gr)

# Determine actual number of unique groups for dynamic column naming
n_9gr = df["Bygningstype_9_gr"].nunique()
n_4gr = df["Bygningstype_4_gr"].nunique()
col_9gr = f"Bygningstype_{n_9gr}_gr"
col_4gr = f"Bygningstype_{n_4gr}_gr"
df = df.rename(columns={
    "Bygningstype_9_gr": col_9gr,
    "Bygningstype_4_gr": col_4gr,
})

# --- Sort columns: rank categories by summed Antall across all rows ---

# Sort_Bygningstype: rank all detailed bygningstyper
all_rank = (
    df.groupby("Bygningstype")["Antall"]
    .sum()
    .rank(ascending=False, method="min")
    .astype(int)
)
df["Sort_Bygningstype"] = df["Bygningstype"].map(all_rank).fillna(0).astype(int)

# Sort for the 9/10-group column
sort_col_9gr = f"Sort_{col_9gr}"
rank_9gr = (
    df.groupby(col_9gr)["Antall"]
    .sum()
    .rank(ascending=False, method="min")
    .astype(int)
)
df[sort_col_9gr] = df[col_9gr].map(rank_9gr).fillna(0).astype(int)

# Sort for the 4/5-group column
sort_col_4gr = f"Sort_{col_4gr}"
rank_4gr = (
    df.groupby(col_4gr)["Antall"]
    .sum()
    .rank(ascending=False, method="min")
    .astype(int)
)
df[sort_col_4gr] = df[col_4gr].map(rank_4gr).fillna(0).astype(int)

# Reorder columns with Kommunenummer first
df = df[[
    "Kommunenummer", "Kommune", "År",
    "Bygningstype", "Sort_Bygningstype",
    col_9gr, sort_col_9gr,
    col_4gr, sort_col_4gr,
    "Antall",
]]

print(df.head())

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "igangsettelser_etter_boligtype.csv"
task_name = "Bolig - Igangsettelser etter boligtype"
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