# Import the utility functions from the Helper_scripts folder
from Helper_scripts.utility_functions import fetch_data
from Helper_scripts.utility_functions import delete_files_in_temp_folder
from Helper_scripts.email_functions import notify_errors
from Helper_scripts.github_functions import upload_github_file
from Helper_scripts.github_functions import download_github_file

temp_folder = os.environ.get("TEMP_FOLDER")

# Capture the name of the current script
script_name = os.path.basename(__file__)

# Example list of error messages to collect errors during execution <--- Eksempel på liste for å samle feilmeldinger under kjøring
error_messages = []

----------------------------------

## Kjøre spørringer i try-except for å fange opp feil. Quitter hvis feil.

try:
    # Fetch data using the fetch_data function, with separate calls for each request
    df_vtfk = fetch_data(POST_URL, payload_vtfk, error_messages, query_name="VTFK")
    df_tfk = fetch_data(POST_URL, payload_tfk, error_messages, query_name="TFK")

except Exception as e:
    # If any query fails, send the error notification and stop execution
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError(
        "A critical error occurred during data fetching, stopping execution."
    )

----------------------------------

df_filtered.to_csv(
    os.path.join(temp_folder, csv_file_name), index=False
)

# Erstatte csv_file =.... med
csv_file = os.path.join(temp_folder, csv_file_name)

----------------------------------

delete_files_in_temp_folder()