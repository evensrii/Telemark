import os
import pandas as pd

from Helper_scripts.github_functions import download_github_file
from Helper_scripts.github_functions import handle_output_data

# Capture the name of the current script
script_name = os.path.basename(__file__)

# List to collect errors during execution
error_messages = []

# ============================================================
# Step 1: Read historical and framskrevet data from GitHub
# ============================================================

github_folder = "Data/01_Befolkning/Befolkningsframskrivinger"

df_hist = download_github_file(f"{github_folder}/befolkningsframskrivinger_historiske.csv")
df_fram = download_github_file(f"{github_folder}/befolkningsframskrivinger_siste_tabell.csv")

if df_hist is None or df_fram is None:
    raise RuntimeError("Could not download one or both source files from GitHub.")

print(f"Historical data: {df_hist.shape[0]} rows, years {df_hist['år'].min()}-{df_hist['år'].max()}")
print(f"Framskrevet data: {df_fram.shape[0]} rows, years {df_fram['år'].min()}-{df_fram['år'].max()}")

# ============================================================
# Step 2: Prepare historical data
# ============================================================

# Historical: columns are region, alder, statistikkvariabel, år, value
# Rename to match output: Kommune, Alder, År, Type, Personer
df_hist_clean = df_hist.rename(columns={
    "region": "Kommune",
    "alder": "Alder",
    "år": "År",
    "value": "Personer",
})

df_hist_clean["Type"] = "Historisk"
df_hist_clean = df_hist_clean[["Kommune", "Alder", "År", "Type", "Personer"]]

print(f"\nHistorical data after cleaning: {df_hist_clean.shape[0]} rows")
print(df_hist_clean.head())

# ============================================================
# Step 3: Prepare framskrevet data
# ============================================================

# Keep only the 3 relevant alternatives
keep_types = [
    "Hovedalternativet (MMMM)",
    "Høy nasjonal vekst (HHMH)",
    "Lav nasjonal vekst (LLML)",
]

df_fram_clean = df_fram[df_fram["statistikkvariabel"].isin(keep_types)].copy()

# Rename columns to match output
df_fram_clean = df_fram_clean.rename(columns={
    "region": "Kommune",
    "alder": "Alder",
    "år": "År",
    "statistikkvariabel": "Type",
    "value": "Personer",
})

df_fram_clean = df_fram_clean[["Kommune", "Alder", "År", "Type", "Personer"]]

print(f"\nFramskrevet data after cleaning: {df_fram_clean.shape[0]} rows")
print(f"  Years: {df_fram_clean['År'].min()}-{df_fram_clean['År'].max()}")
print(f"  Types: {df_fram_clean['Type'].unique().tolist()}")
print(df_fram_clean.head())

# ============================================================
# Step 4: Combine, add Kommunenummer and SortColumn
# ============================================================

df_combined = pd.concat([df_hist_clean, df_fram_clean], ignore_index=True)

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
df_combined["Kommunenummer"] = df_combined["Kommune"].map(kommunenummer_map)

# SortColumn: numeric sort order for age groups
age_sort_order = {
    "0 år": 1,
    "1-5 år": 2,
    "6-12 år": 3,
    "13-15 år": 4,
    "16-19 år": 5,
    "20-44 år": 6,
    "45-66 år": 7,
    "67-79 år": 8,
    "80-89 år": 9,
    "90 år eller eldre": 10,
}

df_combined["SortColumn"] = df_combined["Alder"].map(age_sort_order)

# Sort by Kommune, Alder (via SortColumn), År, Type
df_combined = df_combined.sort_values(
    by=["Kommune", "SortColumn", "År", "Type"],
    ignore_index=True,
)

# Convert År to datetime format (YYYY-01-01)
df_combined["År"] = pd.to_datetime(df_combined["År"], format="%Y").dt.strftime("%Y-%m-%d")

# Reorder columns: Kommunenummer first
df_combined = df_combined[["Kommunenummer", "Kommune", "Alder", "År", "Type", "Personer", "SortColumn"]]

# Ensure numeric columns are float64 for GitHub comparison compatibility
df_combined["Personer"] = pd.to_numeric(df_combined["Personer"], errors="coerce")
df_combined["SortColumn"] = df_combined["SortColumn"].astype(float)

print(f"\nCombined data: {df_combined.shape[0]} rows")
print(f"  Years: {df_combined['År'].min()}-{df_combined['År'].max()}")
print(f"  Types: {df_combined['Type'].unique().tolist()}")
print(f"  Columns: {list(df_combined.columns)}")
print(df_combined.head(20))

# ============================================================
# Step 5: Save to CSV, compare and upload to GitHub
# ============================================================

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "befolkning_historisk_og_framskrevet.csv"
task_name = "Befolkning - Historisk og framskrevet"
temp_folder = os.environ.get("TEMP_FOLDER")

# Call the function and get the "New Data" status
is_new_data = handle_output_data(df_combined, file_name, github_folder, temp_folder, keepcsv=True)

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
