# utility_functions.py

import requests
from pyjstat import pyjstat
import os
import glob
from io import BytesIO
import pandas as pd
from Helper_scripts.github_functions import get_current_file
import time

## Funksjon for å kjøre en spørring


def fetch_data(
    url,
    payload=None,
    error_messages=None,
    query_name="Query",
    response_type="json",
    delimiter=";",
    encoding="ISO-8859-1",
):
    """
    Fetches data using POST or GET requests and processes the response as JSON or CSV.

    Parameters:
    - url (str): The URL for the request.
    - payload (dict or None): The JSON payload for POST requests. If None, a GET request is used.
    - error_messages (list or None): A list to append error messages to (optional).
    - query_name (str): A name to identify the query in error messages.
    - response_type (str): The expected response type, either 'json' or 'csv'.
    - delimiter (str): The delimiter for CSV data (default: ';').
    - encoding (str): The encoding for CSV data (default: 'ISO-8859-1').

    Returns:
    - DataFrame: A Pandas DataFrame containing the response data if successful.
    """

    try:
        # Make the request (POST if payload is provided, otherwise GET)
        if payload:
            response = requests.post(url, json=payload)
        else:
            response = requests.get(url)

        # Raise an exception for non-successful responses
        response.raise_for_status()

        # Process the response based on the expected response type
        if response_type == "json":
            try:
                # Use pyjstat for JSON-stat2 data
                dataset = pyjstat.Dataset.read(response.text)
                print(f"{query_name} JSON data loaded successfully.")
                return dataset.write("dataframe")
            except Exception as e:
                raise ValueError(
                    f"Error processing JSON response for {query_name}: {e}"
                )

        elif response_type == "csv":
            try:
                # Load CSV data into a Pandas DataFrame
                data = pd.read_csv(
                    BytesIO(response.content), delimiter=delimiter, encoding=encoding
                )
                print(f"{query_name} CSV data loaded successfully.")
                return data
            except Exception as e:
                raise ValueError(f"Error processing CSV response for {query_name}: {e}")

        else:
            raise ValueError(
                f"Unsupported response type '{response_type}' for {query_name}."
            )

    except requests.exceptions.RequestException as e:
        error_message = f"Request error in {query_name}: {str(e)}"
        print(error_message)
        error_messages.append(error_message)
        raise  # Re-raise the exception to propagate the error


## Funksjon for å slette filer i Temp-mappen, unntatt "readme.txt" (Er Temp-mappen tom "forsvinner" den fra Github, noe som kan skape krøll.)


def delete_files_in_temp_folder():
    """Delete only the current file being processed from the temp folder"""
    # Retrieve the Temp folder path from the environment variable
    temp_folder = os.environ.get("TEMP_FOLDER")

    if not temp_folder:
        print("TEMP_FOLDER environment variable is not set.")
        return

    # Ensure the Temp folder exists
    if not os.path.exists(temp_folder):
        print(f"The folder does not exist: {temp_folder}")
        return

    # Get the current file being processed
    current_file = get_current_file()
    if not current_file:
        return

    # Construct the full path to the file
    file_path = os.path.join(temp_folder, current_file)

    # Only attempt to delete if file exists
    if os.path.exists(file_path):
        max_retries = 3
        retry_delay = 1  # seconds
        
        for attempt in range(max_retries):
            try:
                os.remove(file_path)
                print(f"Deleted file from temp folder: {current_file}")
                break
            except Exception as e:
                if attempt < max_retries - 1:
                    print(f"Attempt {attempt + 1}: Error deleting file {current_file}. Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    print(f"Error deleting file {current_file}: {e}")
