# utility_functions.py

import requests
from pyjstat import pyjstat
import os
import glob

## Funksjon for å kjøre en spørring


def fetch_data(url, payload, error_messages, query_name="Query"):
    """
    Makes a POST request to the provided URL with the given payload.
    Raises an exception and appends an error message to error_messages if the request fails.

    Parameters:
    - url (str): The URL to send the POST request to.
    - payload (dict): The JSON payload for the request.
    - error_messages (list): A list to append error messages to.
    - query_name (str): A name to identify the query in error messages.

    Returns:
    - DataFrame: The resulting DataFrame from the response if successful.
    """
    try:
        response = requests.post(url, json=payload)
        response.raise_for_status()  # Raises an exception for non-200 responses
        dataset = pyjstat.Dataset.read(response.text)
        print(f"{query_name} data loaded successfully")
        return dataset.write("dataframe")
    except requests.exceptions.RequestException as e:
        error_message = f"Error in {query_name}: {str(e)}"
        print(error_message)
        error_messages.append(error_message)
        raise  # Re-raise the exception to stop further processing


## Funksjon for å slette filer i Temp-mappen


def delete_files_in_temp_folder():
    # Retrieve the Temp folder path from the environment variable
    temp_folder = os.environ.get("TEMP_FOLDER")

    if not temp_folder:
        print("TEMP_FOLDER environment variable is not set.")
        return

    # Ensure the Temp folder exists
    if not os.path.exists(temp_folder):
        print(f"The folder does not exist: {temp_folder}")
        return

    # Construct the path pattern to match all files in the folder
    files = glob.glob(os.path.join(temp_folder, "*"))

    # Iterate over the list of files and delete each one
    for file_path in files:
        try:
            os.remove(file_path)
            print(f"Deleted file: {file_path}")
        except Exception as e:
            print(f"Error deleting file {file_path}: {e}")
