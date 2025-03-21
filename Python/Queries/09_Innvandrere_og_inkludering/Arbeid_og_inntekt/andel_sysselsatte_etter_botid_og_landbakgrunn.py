import requests
import sys
import os
import glob
from io import BytesIO
from io import StringIO
import pandas as pd
from pyjstat import pyjstat

# Import the utility functions from the Helper_scripts folder
from Helper_scripts.utility_functions import fetch_data
from Helper_scripts.utility_functions import delete_files_in_temp_folder
from Helper_scripts.email_functions import notify_errors
from Helper_scripts.github_functions import upload_github_file
from Helper_scripts.github_functions import download_github_file
from Helper_scripts.github_functions import compare_to_github
from Helper_scripts.github_functions import handle_output_data

# Capture the name of the current script
script_name = os.path.basename(__file__)

# Example list of error messages to collect errors during execution <--- Eksempel på liste for å samle feilmeldinger under kjøring
error_messages = []

# Telemark, siste år

## Beregner ut i fra kommunetallene på IMDI. Har absolutte tall på innvandrere, og andelen de utgjør. Kan da beregne totalt antall innvandrere.
## Kan deretter summere sysselsatte innvandrere og alle innvandrere i hver kommune, og summere disse.
## Til slutt beregner jeg andelen sysselsatte i hele fylket.

# Finner URL vha. "Inspiser side" og fane "Network" (F12)
url = "https://app-simapi-prod.azurewebsites.net/download_csv/k/sysselsatte_botid_land"


## Kjøre spørringer i try-except for å fange opp feil. Quitter hvis feil.

try:
    df = fetch_data(
        url=url,
        error_messages=error_messages,
        query_name="Sysselsatte botid land",
        payload=None,
        response_type="csv",
        delimiter=";",
        encoding="ISO-8859-1",
    )

except Exception as e:
    print(f"Error occurred: {e}")
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError(
        "A critical error occurred during data fetching, stopping execution."
    )


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

## Datasjekk
df.head()
# df.info()

# Print the unique values in the column "Kommunenummer"
# print(df["Kommunenummer"].unique())

# Konvertere kolonne "År" til datetime
df["År"] = pd.to_datetime(df["År"], format="%Y")

# Konverter "Kommunenummer" til string med 4 siffer
df["Kommunenummer"] = df["Kommunenummer"].astype(str).str.pad(width=4, fillchar="0")

# Filter ved hjelp av query basert på keys in kommuner_telemark

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
    "4001": "Porsgrunn",
    "4003": "Skien",
    "4005": "Notodden",
    "4010": "Siljan",
    "4012": "Bamble",
    "4014": "Kragerø",
    "4016": "Drangedal",
    "4018": "Nome",
    "4020": "Midt-Telemark",
    "4022": "Seljord",
    "4024": "Hjartdal",
    "4026": "Tinn",
    "4028": "Kviteseid",
    "4030": "Nissedal",
    "4032": "Fyresdal",
    "4034": "Tokke",
    "4036": "Vinje",
}

## Filtrering av rader hvor "Kommunenummer" er i kommuner_telemark.keys()
df = df[df["Kommunenummer"].isin(kommuner_telemark.keys())]

## Innfylling av manglende kommunenavn
df["Kommune"] = df["Kommune"].fillna(df["Kommunenummer"].map(kommuner_telemark))

# Filter only the most recent data (last year available)
most_recent_year = df["År"].max()
df = df.query("År == @most_recent_year")
print(f"Selected data from year {df['År'].max()}")

# Add the year ({most_recent_year.year}) to the "Botid i Norge" column name
botid_column_name = f"Botid i Norge ({most_recent_year.year})"
df = df.rename(columns={"Botid i Norge": botid_column_name})

# Basic filtering
df = df.query("Verdensregion != 'Alle'")
df = df.query(f'`{botid_column_name}` != "Alle"')

# Rename the country values in the "Verdensregion" column
df["Verdensregion"] = df["Verdensregion"].replace(
    {
        "Øst-Europa utenom EU, Asia (inkl.Tyrkia), Afrika, Sør- og Mellom-Amerika og Oseania utenom Australia og New Zealand": "Gruppe 2-land (Asia, Afrika, Latin-Amerika osv.)",
        "EU/EFTA-land, Nord-Amerika, Australia og New Zealand": "Gruppe 1-land (EU, Storbritannia, USA osv.)",
    }
)

# Pivot table
df_pivot = df.pivot_table(
    index=[
        "År",
        "Kommunenummer",
        "Kommune",
        botid_column_name,
        "Verdensregion",
    ],  # Keep these as index columns
    columns="Enhet",  # Split based on 'Enhet' values
    values="Antall",  # Values to spread across the new columns
    aggfunc="first",  # Use 'first' since there's no aggregation needed if there are no duplicates
).reset_index()

df_pivot.columns.name = None  # Remove the multi-index name for columns

df_pivot["Personer"] = df_pivot["Personer"].astype(int)

# Clean and convert the Prosent column
df_pivot["Prosent"] = df_pivot["Prosent"].replace(":", "0")  # Replace ':' with '0'
df_pivot["Prosent"] = df_pivot["Prosent"].str.replace(",", ".").astype(float)

# Calculate a new column "Totalt", which is "Personer"/"Prosent"*100
# Handle division by zero by replacing inf with 0
df_pivot["Totalt"] = (df_pivot["Personer"] / (df_pivot["Prosent"].replace(0, float('nan'))) * 100)
df_pivot["Totalt"] = df_pivot["Totalt"].fillna(0)  # Replace NaN with 0
df_pivot["Totalt"] = df_pivot["Totalt"].round().astype(int)  # Now safe to convert to int

# Group by 'Botid i Norge' and 'Verdensregion', and sum the 'Personer' and 'Totalt' columns
df_grouped = (
    df_pivot.groupby([botid_column_name, "Verdensregion"])[["Personer", "Totalt"]]
    .sum()
    .reset_index()
)

# Create a column "Andel" which is "Personer"/"Totalt"*100
df_grouped["Andel"] = (df_grouped["Personer"] / df_grouped["Totalt"]) * 100

# Round the "Andel" column to 1 decimal, and use "," as decimal separator
df_grouped["Andel"] = df_grouped["Andel"].round(1).astype(str).str.replace(".", ",")

# Remove columns "Personer" and "Totalt"
df_grouped = df_grouped.drop(columns=["Personer", "Totalt"])

# Pivot the table to split "Verdensregion" into columns and "Andel" as values
df_pivot_grouped = df_grouped.pivot(
    index=botid_column_name, columns="Verdensregion", values="Andel"
).reset_index()

# Remove the "Verdensregion" column
df_pivot_grouped.columns.name = None

# Change the order of the rows, so that "Botid i Norge" is in the desired order
df_pivot_grouped = df_pivot_grouped.reindex([0, 2, 1])

# Reset index
df_pivot_grouped.reset_index(drop=True, inplace=True)

df = df_pivot_grouped

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "sysselsatte_etter_botid_og_landbakgrunn.csv"
task_name = "Innvandrerbefolkningen - Sysselsatte etter botid og bakgrunn"
github_folder = "Data/09_Innvandrere og inkludering/Arbeid og inntekt"
temp_folder = os.environ.get("TEMP_FOLDER")

# Call the function and get the "New Data" status
is_new_data = handle_output_data(df, file_name, github_folder, temp_folder, keepcsv=True)

# Write the "New Data" status to a unique log file
log_dir = os.environ.get("LOG_FOLDER", os.getcwd())  # Default to current working directory
task_name_safe = task_name.replace(".", "_").replace(" ", "_")  # Ensure the task name is file-system safe
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