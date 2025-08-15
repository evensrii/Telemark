import os
import pandas as pd
from pyjstat import pyjstat

# Import the utility functions from the Helper_scripts folder
from Helper_scripts.utility_functions import fetch_data
from Helper_scripts.email_functions import notify_errors
from Helper_scripts.github_functions import handle_output_data

# Capture the name of the current script
script_name = os.path.basename(__file__)

# Example list of error messages to collect errors during execution <--- Eksempel på liste for å samle feilmeldinger under kjøring
error_messages = []

###### Andel innvandrere og øvrig befolkning i lavinntekt ######

# API configuration
base_url = "https://statistikk-data.fhi.no/api/open/v1"
source_id = "nokkel"
table_id = "583"  # Vedvarende lavinntekt

try:
    # Construct data URL
    data_url = f"{base_url}/{source_id}/Table/{table_id}/data"

    # Hentet etter filtrering 
    payload = {"response":{"format":"json-stat2"},"dimensions":[{"filter":"item","values":["0","40","4001","4003","4005","4010","4012","4014","4016","4018","4020","4022","4024","4026","4028","4030","4032","4034","4036"],"code":"GEO"},{"filter":"item","values":["2006_2006","2007_2007","2008_2008","2009_2009","2010_2010","2011_2011","2012_2012","2013_2013","2014_2014","2015_2015","2016_2016","2017_2017","2018_2018","2019_2019","2020_2020","2021_2021","2022_2022","2023_2023"],"code":"AAR"},{"filter":"item","values":["25_120"],"code":"ALDER"},{"filter":"item","values":["2","20"],"code":"INNVKAT"},{"filter":"item","values":["ant_eu60"],"code":"MAAL"},{"filter":"item","values":["RATE"],"code":"MEASURE_TYPE"}]}

    # Use our constructed payload for the data request
    df = fetch_data(
        url=data_url,
        error_messages=error_messages,
        query_name="Lavinntekt",
        payload=payload,
        response_type="json",
    )

except Exception as e:
    print(f"Error occurred: {e}")
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError(
        "A critical error occurred during data fetching, stopping execution."
    )

# Process the data
df.head()

# Add first capital letter in the "Innvandringkategori" column values
df["Innvandringskategori"] = df["Innvandringskategori"].str.capitalize()

#Remove column "Måltall"
df = df.drop(columns=["Måltall"])

#Rename column "value" to "Andel"
df = df.rename(columns={"value": "Andel"})

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "andel_innvandrere_i_lavinntekt_fhi.csv"
task_name = "Innvandrere - Lavinntekt"
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
