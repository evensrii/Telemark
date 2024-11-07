# utility_functions.py

import requests
from pyjstat import pyjstat

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
