import os
import pandas as pd
from pyjstat import pyjstat

# Import the utility functions from the Helper_scripts folder
from Helper_scripts.utility_functions import fetch_data
from Helper_scripts.github_functions import handle_output_data

# Capture the name of the current script
script_name = os.path.basename(__file__)

# List to collect errors during execution
error_messages = []

# ============================================================
# Step 1: Query fritidsbygninger (table 05467) for all Telemark
#         municipality codes (pre-2020, 2020-2023 and current),
#         all years.
#         NB: SSB returns 0 for years when a code did not exist,
#         so the codes can be summed into the current structure.
# ============================================================

GET_URL = (
    "https://data.ssb.no/api/pxwebapi/v2/tables/05467/data?lang=no"
    "&valueCodes[ContentsCode]=Fribygg,FribyggKm"
    "&valueCodes[Tid]=*"
    "&valueCodes[Region]=3806,3807,3808,3812,3813,3814,3815,3816,3817,3818,3819,3820,3821,3822,3823,3824,3825,"
    "4001,4003,4005,4010,4012,4014,4016,4018,4020,4022,4024,4026,4028,4030,4032,4034,4036,"
    "0805,0806,0807,0811,0814,0815,0817,0819,0821,0822,0826,0827,0828,0829,0830,0831,0833,0834,0899"
    "&codelist[Region]=vs_Kommune"
)

try:
    df_raw = fetch_data(
        url=GET_URL,
        payload=None,
        error_messages=error_messages,
        query_name="Antall fritidsboliger",
        response_type="json",
    )
except Exception as e:
    print(f"Error occurred: {e}")
    raise RuntimeError(
        "A critical error occurred during data fetching, stopping execution."
    )

print(f"Raw data: {len(df_raw)} rows")
print(df_raw.head(10))

# ============================================================
# Step 2: Map old municipalities to the current structure
#         (2024-) and aggregate
# ============================================================

# Current municipalities in Telemark
kommunenummer = {
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

# Old municipalities that do not share name with a current one
sammenslaatte = {
    "Bø (Telemark)": "Midt-Telemark",
    "Sauherad": "Midt-Telemark",
}

df = df_raw.copy()
df.columns = ["region", "statistikkvariabel", "år", "value"]
df["value"] = pd.to_numeric(df["value"], errors="coerce").fillna(0)

# Strip SSB's period suffix, e.g. "Porsgrunn (-2019)" / "Skien (2020-2023)"
df["Kommune"] = df["region"].str.replace(r"\s*\((?:\d{4})?-(?:\d{4})?\)$", "", regex=True)
df["Kommune"] = df["Kommune"].replace(sammenslaatte)

# "Uoppgitt kommune Telemark" can't be placed in a current municipality
df = df[df["Kommune"] != "Uoppgitt kommune Telemark"]

unmapped = set(df["Kommune"]) - set(kommunenummer)
if unmapped:
    raise ValueError(f"Unmapped municipalities: {unmapped}")

# One row per original region and year, with count and density side by side
df_wide = df.pivot_table(
    index=["region", "Kommune", "år"], columns="statistikkvariabel", values="value", aggfunc="sum"
).reset_index()
df_wide.columns.name = None
df_wide = df_wide.rename(
    columns={
        "Antall fritidsbygninger": "antall",
        "Antall fritidsbygninger per km²": "per_km2",
    }
)

df_agg = df_wide.groupby(["Kommune", "år"], as_index=False)["antall"].sum()

# Density can't be summed, and SSB's area basis has changed over time
# (e.g. Bø + Sauherad ≈ 547 km² vs. Midt-Telemark ≈ 487 km²). Only use
# SSB's density for the current structure (2024-), left empty before that.
per_km2_current = df_wide[df_wide["region"].isin(kommunenummer) & (df_wide["år"].astype(int) >= 2024)]
df_agg = df_agg.merge(per_km2_current[["Kommune", "år", "per_km2"]], on=["Kommune", "år"], how="left")

df_final = pd.DataFrame(
    {
        "Kommunenummer": df_agg["Kommune"].map(kommunenummer),
        "Kommune": df_agg["Kommune"],
        "År": df_agg["år"].astype(int),
        "Antall fritidsbygninger": df_agg["antall"].astype(int),
        "Fritidsbygninger per km²": df_agg["per_km2"],
    }
)

df_final = df_final.sort_values(["Kommunenummer", "År"]).reset_index(drop=True)

print(df_final.head(20))
print(df_final[df_final["Kommune"] == "Midt-Telemark"].tail(10))
print(f"Years: {df_final['År'].min()}-{df_final['År'].max()}, rows: {len(df_final)}")

# ============================================================
# Step 3: Save to CSV, compare and upload to GitHub
# ============================================================

file_name = "antall_fritidsboliger.csv"
task_name = "Fritidsboliger - Antall fritidsboliger"
github_folder = "Data/10_Areal- og stedsutvikling/Fritidsboliger"
temp_folder = os.environ.get("TEMP_FOLDER")

value_columns = ["Antall fritidsbygninger", "Fritidsbygninger per km²"]

# Call the function and get the "New Data" status
is_new_data = handle_output_data(
    df_final, file_name, github_folder, temp_folder, keepcsv=True, value_columns=value_columns
)

# Write the "New Data" status to a unique log file
log_dir = os.environ.get("LOG_FOLDER", os.getcwd())
task_name_safe = task_name.replace(".", "_").replace(" ", "_")
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
