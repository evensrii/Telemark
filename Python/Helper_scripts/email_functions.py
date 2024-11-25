# email_functions.py

import requests
import sys
import os
import inspect
from dotenv import load_dotenv

### EPOST CONFIGURATION

# Retrieve PYTHONPATH environment variable
pythonpath = os.environ.get("PYTHONPATH")

if pythonpath:
    # Construct the full path to the Queries/token.env
    env_file_path = os.path.join(pythonpath, "token.env")

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


### 1) EPOST VED MISLYKKEDE SPØRRINGER


def notify_errors(error_messages, script_name="Unknown Script"):

    # Sends an email notification if there are any errors in the error_messages list.

    # Parameters:
    # - error_messages (list): A list of error messages.
    # - script_name (str): The name of the script where the errors occurred.

    if error_messages:
        error_details = "\n".join(error_messages)
        payload = {
            "to": ["even.sannes.riiser@telemarkfylke.no", "even.s.riiser@gmail.com"],
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


### 2) EPOST VED NYE DATA


def notify_updated_data(file_name, diff_lines=None, reason=""):
    """
    Sends an email notification when new data is detected in the GitHub comparison.

    Parameters:
        file_name (str): The name of the updated file.
        diff_lines (list): A list of dictionaries containing the differences (optional).
        reason (str): The reason for the update (optional).
    """
    # Get the name of the script that called compare_to_github()
    frame = inspect.stack()[2]  # Two levels up in the call stack
    script_name = (
        os.path.basename(frame.filename)
        if frame.filename
        else "Interactive Environment"
    )

    # Adjust the script name for IPython or Jupyter cases
    if "ipython-input" in script_name or script_name.endswith(".py") is False:
        script_name = "Jupyter Notebook or IPython"

    # Format the differences for the email
    formatted_diff = ""
    if diff_lines:
        formatted_diff = "<br>".join(
            [f"Old: {line}" for line in diff_lines[: len(diff_lines) // 2]]
            + [f"New: {line}" for line in diff_lines[len(diff_lines) // 2 :]]
        )

    payload = {
        "to": ["even.sannes.riiser@telemarkfylke.no"],
        "from": "Analyse: Statusoppdatering <analyse@telemarkfylke.no>",
        "subject": f"New data detected based on {script_name}",
        "text": (
            f"New data has been detected and updated for the file: {file_name}\n\n"
            f"Reason: {reason}\n\n"
            f"Changes:\n{formatted_diff if formatted_diff else 'N/A'}"
        ),
        "html": (
            f"<b>New data has been detected and updated for the file:</b><br>{file_name}<br><br>"
            f"<b>Reason:</b> {reason}<br><br>"
            f"<b>Changes:</b><br>{formatted_diff if formatted_diff else 'N/A'}"
        ),
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
