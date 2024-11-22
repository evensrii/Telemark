# email_functions.py

import requests
import sys
import os
from dotenv import load_dotenv

##### TO funksjoner: En for hvis nye data, og en for hvis ikke error eller ikke 200.


### EPOST VED FEILMELDINGER

# Retrieve PYTHONPATH environment variable
pythonpath = os.environ.get("PYTHONPATH")

if pythonpath:
    # Construct the full path to the Queries/token.env
    env_file_path = os.path.join(pythonpath, "Queries", "token.env")

    if os.path.exists(env_file_path):
        # Load the .env file
        load_dotenv(env_file_path)
        print(f"Loaded .env file from: {env_file_path}")
    else:
        print(f"token.env not found in: {env_file_path}")
else:
    print("PYTHONPATH environment variable is not set.")

# Get the X-FUNCTIONS-KEY from the environment
X_FUNCTIONS_KEY = os.getenv("X_FUNCTIONS_KEY")
if not X_FUNCTIONS_KEY:
    raise ValueError("X-FUNCTIONS-KEY not found in the loaded .env file.")

print("X_FUNCTIONS_KEY loaded successfully.")


def notify_errors(error_messages, script_name="Unknown Script"):

    # Sends an email notification if there are any errors in the error_messages list.

    # Parameters:
    # - error_messages (list): A list of error messages.
    # - script_name (str): The name of the script where the errors occurred.

    if error_messages:
        error_details = "\n".join(error_messages)
        payload = {
            "to": ["even.sannes.riiser@telemarkfylke.no"],
            # "cc": [kjersti.aase@telemarkfylke.no],
            "from": "Analyse: Statusoppdatering <analyse@telemarkfylke.no>",
            "subject": f"Spørring feilet i script {script_name}",
            "text": f"The following errors were found in {script_name}:\n{error_details}",
            "html": f"<b>The following errors were found in {script_name}:</b><br>{'<br>'.join(error_messages)}",
        }

        url = "https://mail.api.telemarkfylke.no/api/mail"
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "insomnia/10.1.1",
            "x-functions-key": X_FUNCTIONS_KEY,
        }

        email_response = requests.post(url, headers=headers, json=payload)

        if email_response.status_code == 200:
            print("Error notification email sent successfully.")
        else:
            print(
                f"Failed to send error notification email. Status code: {email_response.status_code}"
            )
    else:
        print("All requests were successful. No email sent.")


### EPOST VED NYE DATA


def notify_updated_data(file_name, script_name="Unknown Script"):
    """
    Sends an email notification when new data is detected in the GitHub comparison.

    Parameters:
        file_name (str): The name of the updated file.
        script_name (str): The name of the script where the update occurred.
    """
    payload = {
        "to": ["even.sannes.riiser@telemarkfylke.no"],
        # "cc": ["optional.cc.email@telemarkfylke.no"],
        "from": "Analyse: Statusoppdatering <analyse@telemarkfylke.no>",
        "subject": f"New data detected in {script_name}",
        "text": f"New data has been detected and updated for the file: {file_name}",
        "html": f"<b>New data has been detected and updated for the file:</b><br>{file_name}",
    }

    url = "https://mail.api.telemarkfylke.no/api/mail"
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "insomnia/10.1.1",
        "x-functions-key": X_FUNCTIONS_KEY,
    }

    try:
        email_response = requests.post(url, headers=headers, json=payload)

        if email_response.status_code == 200:
            print("Update notification email sent successfully.")
        else:
            print(
                f"Failed to send update notification email. Status code: {email_response.status_code}"
            )
    except Exception as e:
        print(f"An error occurred while sending the email: {e}")
