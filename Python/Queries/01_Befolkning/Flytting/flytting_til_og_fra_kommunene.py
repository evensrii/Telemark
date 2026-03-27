import os
import pandas as pd

from Helper_scripts.utility_functions import fetch_data
from Helper_scripts.email_functions import notify_errors
from Helper_scripts.github_functions import handle_output_data

# Capture the name of the current script
script_name = os.path.basename(__file__)

# List to collect errors during execution
error_messages = []

# ============================================================
# Step 1: Query flytting til og fra Telemark-kommuner (table 13864)
#         Fraflyttingsregion x TilflyttRegion for all
#         Telemark municipalities, latest year.
# ============================================================

TELEMARK_KOMMUNER = [
    "Porsgrunn", "Skien", "Notodden", "Siljan", "Bamble", "Kragerø",
    "Drangedal", "Nome", "Midt-Telemark", "Seljord", "Hjartdal", "Tinn",
    "Kviteseid", "Nissedal", "Fyresdal", "Tokke", "Vinje",
]

GET_URL = (
    "https://data.ssb.no/api/pxwebapi/v2/tables/13864/data?lang=no"
    "&outputFormat=json-stat2"
    "&valuecodes[ContentsCode]=*"
    "&valuecodes[Tid]=2025"
    "&valuecodes[Fraflyttingsregion]=*"
    "&codelist[Fraflyttingsregion]=agg_KommGjeldende"
    "&valuecodes[TilflyttRegion]=*"
    "&codelist[TilflyttRegion]=agg_KommGjeldende"
    "&heading=ContentsCode,Tid,TilflyttRegion"
    "&stub=Fraflyttingsregion"
)

try:
    df = fetch_data(
        url=GET_URL,
        payload=None,
        error_messages=error_messages,
        query_name="Flytting til og fra Telemark-kommuner",
        response_type="json",
    )
except Exception as e:
    print(f"Error occurred: {e}")
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError(
        "A critical error occurred during data fetching, stopping execution."
    )

print(f"Raw data: {len(df)} rows")
print(f"  Columns: {list(df.columns)}")
print(df.head(10))

# ============================================================
# Step 2: Load kommunenummer mapping
# ============================================================

# Read kommuneinndeling metadata to get kommunenummer for each kommune name
metadata_path = os.path.join(
    os.environ["PYTHONPATH"],
    "..",
    "Python",
    "Metadata",
    "ssb_klass_kommuneinndeling_2026.csv",
)
df_komm = pd.read_csv(metadata_path, sep=";", dtype=str, encoding="latin-1")

# Mapping to standardize multi-part SSB/Sami kommune names to Norwegian names
name_replacements = {
    # Remove last part (keep first)
    "Oslo - Oslove": "Oslo",
    "Trondheim - Tråante": "Trondheim",
    "Sortland - Suortá": "Sortland",
    "Nordreisa - Ráisa - Raisi": "Nordreisa",
    "Hammerfest - Hámmerfeasta": "Hammerfest",
    "Steinkjer - Stïentje": "Steinkjer",
    "Namsos - Nåavmesjenjaelmie": "Namsos",
    "Rana - Raane": "Rana",
    "Fauske - Fuossko": "Fauske",
    "Evenes - Evenássi": "Evenes",
    "Harstad - Hárstták": "Harstad",
    "Levanger - Levangke": "Levanger",
    "Porsanger - Porsángu - Porsanki": "Porsanger",
    "Gratangen - Rivtták": "Gratangen",
    "Røros - Rosse": "Røros",
    "Sørfold - Fuolldá": "Sørfold",
    "Lyngen - Ivgu - Yykeä": "Lyngen",
    "Storfjord - Omasvuotna - Omasvuono": "Storfjord",
    # Remove first part (keep last)
    "Dielddanuorri - Tjeldsund": "Tjeldsund",
    "Deatnu - Tana": "Tana",
    "Guovdageaidnu - Kautokeino": "Kautokeino",
    "Aarborte - Hattfjelldal": "Hattfjelldal",
    "Hábmer - Hamarøy": "Hamarøy",
    "Kárásjohka - Karasjok": "Karasjok",
    "Kárá?johka - Karasjok": "Karasjok",
    "Loabák - Lavangen": "Lavangen",
    "Raarvihke - Røyrvik": "Røyrvik",
    "Snåase - Snåsa": "Snåsa",
    "Unjárga - Nesseby": "Nesseby",
    # Remove first and last part (keep middle)
    "Gáivuotna - Kåfjord - Kaivuono": "Kåfjord",
}

# Clean metadata names: apply replacements, then fall back to first part before ' - '
df_komm["name_clean"] = df_komm["name"].replace(name_replacements)
still_multipart = df_komm["name_clean"].str.contains(" - ", na=False)
df_komm.loc[still_multipart, "name_clean"] = (
    df_komm.loc[still_multipart, "name_clean"].str.split(" - ").str[0].str.strip()
)

# Create a mapping from kommune name to kommunenummer (code)
komm_map = dict(zip(df_komm["name_clean"], df_komm["code"]))

# Read fylkesinndeling metadata to map kommunenummer prefix to county name
fylke_path = os.path.join(
    os.environ["PYTHONPATH"],
    "..",
    "Python",
    "Metadata",
    "ssb_klass_fylkesinndeling_2026.csv",
)
df_fylke = pd.read_csv(fylke_path, sep=";", dtype=str, encoding="utf-8-sig")
fylke_map = dict(zip(df_fylke["code"], df_fylke["name"]))

# Read coordinates for each kommune (mean centroid)
coord_path = os.path.join(
    os.environ["PYTHONPATH"],
    "..",
    "Data",
    "01_Befolkning",
    "Flytting",
    "koordinater_gjennomsnitt_kommuner_2026.csv",
)
df_coord = pd.read_csv(coord_path, sep=",", dtype={"kommunenummer": str})

# ============================================================
# Step 3: Clean and reshape the data
# ============================================================

# Filter: keep only rows where fra OR til is a Telemark kommune
is_fra_telemark = df["fraflyttingsregion"].isin(TELEMARK_KOMMUNER)
is_til_telemark = df["tilflyttingsregion"].isin(TELEMARK_KOMMUNER)
df = df[is_fra_telemark | is_til_telemark].copy()

print(f"\nAfter filtering to Telemark: {len(df)} rows")
print(f"  Unique fraflyttingsregion: {sorted(df['fraflyttingsregion'].unique())}")
print(f"  Unique tilflyttingsregion: {sorted(df['tilflyttingsregion'].unique())}")

# Rename columns
df = df.rename(columns={
    "fraflyttingsregion": "Fra kommune",
    "tilflyttingsregion": "Til kommune",
    "statistikkvariabel": "Statistikkvariabel",
    "år": "År",
    "value": "Antall",
})

# Clean kommune names using the name_replacements dict defined in Step 2
df["Fra kommune"] = df["Fra kommune"].replace(name_replacements)
df["Til kommune"] = df["Til kommune"].replace(name_replacements)

# Add kommunenummer for Fra and Til kommune
df["Fra kommunenummer"] = df["Fra kommune"].map(komm_map)
df["Til kommunenummer"] = df["Til kommune"].map(komm_map)

# Add county columns based on first 2 digits of kommunenummer
df["Fra fylke"] = df["Fra kommunenummer"].str[:2].map(fylke_map)
df["Til fylke"] = df["Til kommunenummer"].str[:2].map(fylke_map)

# Merge coordinates for Fra kommune
df = df.merge(
    df_coord.rename(columns={"kommunenummer": "Fra kommunenummer", "MEAN_Y": "Fra lat", "MEAN_X": "Fra lon"}),
    on="Fra kommunenummer",
    how="left",
)

# Merge coordinates for Til kommune
df = df.merge(
    df_coord.rename(columns={"kommunenummer": "Til kommunenummer", "MEAN_Y": "Til lat", "MEAN_X": "Til lon"}),
    on="Til kommunenummer",
    how="left",
)

# Report kommuner missing coordinates (no matching kommunenummer in coordinates file)
missing_from = df[df["Fra lat"].isna()]["Fra kommune"].unique()
missing_to = df[df["Til lat"].isna()]["Til kommune"].unique()
missing_all = sorted(set(missing_from) | set(missing_to))
if missing_all:
    print(f"\nWARNING: {len(missing_all)} kommuner missing coordinates:")
    for name in missing_all:
        knr = komm_map.get(name, "unknown")
        print(f"  {name} (kommunenummer: {knr})")
else:
    print("\nAll kommuner matched to coordinates.")

# Drop statistikkvariabel if only one value
if df["Statistikkvariabel"].nunique() == 1:
    df = df.drop(columns=["Statistikkvariabel"])

df["Antall"] = df["Antall"].fillna(0).astype(int)

# Split into two datasets:
# 1) Fraflytting: rows where a Telemark kommune is the origin (Fra kommune)
# 2) Tilflytting: rows where a Telemark kommune is the destination (Til kommune)
df_fra = df[df["Fra kommune"].isin(TELEMARK_KOMMUNER)].copy()
df_til = df[df["Til kommune"].isin(TELEMARK_KOMMUNER)].copy()

# Remove rows with no flyttinger (Antall = 0)
df_fra = df_fra[df_fra["Antall"] != 0]
df_til = df_til[df_til["Antall"] != 0]

print(f"\nFraflytting fra Telemark-kommuner: {len(df_fra)} rows")
print(df_fra.head(10))
print(f"\nTilflytting til Telemark-kommuner: {len(df_til)} rows")
print(df_til.head(10))

# Reorder columns
df_fra = df_fra[["Fra kommune", "Fra fylke", "Til kommune", "Til fylke", "Antall", "Fra lat", "Fra lon", "Til lat", "Til lon"]]
df_til = df_til[["Fra kommune", "Fra fylke", "Til kommune", "Til fylke", "Antall", "Fra lat", "Fra lon", "Til lat", "Til lon"]]

# ============================================================
# Step 4: Save to CSV, compare and upload to GitHub
# ============================================================

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name1 = "fraflytting_fra_telemarkskommuner.csv"
file_name2 = "tilflytting_til_telemarkskommuner.csv"
task_name = "Flytting - Til og fra kommunene"
github_folder = "Data/01_Befolkning/Flytting"
temp_folder = os.environ.get("TEMP_FOLDER")

# Call the function and get the "New Data" status
is_new_data1 = handle_output_data(df_fra, file_name1, github_folder, temp_folder, keepcsv=True)
is_new_data2 = handle_output_data(df_til, file_name2, github_folder, temp_folder, keepcsv=True)

# Write the "New Data" status to a unique log file
log_dir = os.environ.get("LOG_FOLDER", os.getcwd())
task_name_safe = task_name.replace(".", "_").replace(" ", "_")
new_data_status_file = os.path.join(log_dir, f"new_data_status_{task_name_safe}.log")

with open(new_data_status_file, "w", encoding="utf-8") as log_file:
    log_file.write(f"{task_name_safe},multiple_files,{'Yes' if (is_new_data1 or is_new_data2) else 'No'}\n")

if is_new_data1:
    print(f"New data detected in {file_name1} and pushed to GitHub.")
else:
    print(f"No new data detected in {file_name1}.")

if is_new_data2:
    print(f"New data detected in {file_name2} and pushed to GitHub.")
else:
    print(f"No new data detected in {file_name2}.")

print(f"New data status log written to {new_data_status_file}")
