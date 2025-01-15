import requests
import sys
import os
import glob
from io import BytesIO
from io import StringIO
import pandas as pd

# Import the utility functions from the Helper_scripts folder
from Helper_scripts.utility_functions import delete_files_in_temp_folder
from Helper_scripts.email_functions import notify_errors
from Helper_scripts.github_functions import upload_github_file, download_github_file, compare_to_github, handle_output_data


# Capture the name of the current script
script_name = os.path.basename(__file__)

# Example list of error messages to collect errors during execution <--- Eksempel på liste for å samle feilmeldinger under kjøring
error_messages = []

################# Spørring #################

# Endepunkt for NVE API
API_URL = "https://api.nve.no/web/Powerplant/GetHydroPowerPlantsInOperation"

def get_hydro_power_plants_in_operation():
    """Fetch and process hydropower plant data from NVE API"""
    try:
        # Make direct request since NVE API returns regular JSON, not JSON-stat2
        response = requests.get(API_URL)
        response.raise_for_status()  # Raise exception for non-200 status codes
        
        # Convert JSON response to DataFrame
        df = pd.DataFrame(response.json())
        
        if df is None or df.empty:
            raise RuntimeError("No data received from NVE API")

        # Process the DataFrame
        selected_columns = [
            "Navn",
            "Fylke",
            "Kommune",
            "KommuneNr",
            "ForsteUtnyttelseAvFalletDato",
            "MidProd_91_20",
        ]

        df_selected = df[selected_columns]
        df_selected = df_selected[df_selected["Fylke"] == "Telemark "]
        df_selected = df_selected.drop(columns=["Fylke", "KommuneNr"])
        df_selected.columns = [
            "Kraftverk",
            "Kommune",
            "Produksjon oppstart",
            "Årlig produksjon (1991-2020)",
        ]

        return df_selected

    except requests.exceptions.RequestException as e:
        error_messages.append(f"Error fetching data from NVE API: {str(e)}")
        notify_errors(error_messages, script_name=script_name)
        raise RuntimeError(
            "A critical error occurred during data fetching, stopping execution."
        )
    except Exception as e:
        error_messages.append(f"Error in get_hydro_power_plants_in_operation: {str(e)}")
        notify_errors(error_messages, script_name=script_name)
        raise RuntimeError(
            "A critical error occurred during data fetching, stopping execution."
        )

try:
    df = get_hydro_power_plants_in_operation()
except Exception as e:
    print(f"Error occurred: {e}")
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError(
        "A critical error occurred during data fetching, stopping execution."
    )

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "vannkraft_telemark.csv"
task_name = "Klima og energi - Vannkraft Telemark"
github_folder= "Data/04_Klima og ressursforvaltning/Kraft og energi/Kraftproduksjon/NVE"
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