# email_functions.py

import requests
import os
from dotenv import load_dotenv

### TO funksjoner: En for hvis nye data, og en for hvis ikke error eller ikke 200.

# Load GitHub token from .env file
dotenv_path = os.path.join(os.path.dirname(__file__), "token.env")
load_dotenv(dotenv_path)
X_FUNCTIONS_KEY = os.getenv("X_FUNCTIONS_KEY")

if X_FUNCTIONS_KEY is None:
    raise ValueError("X-Functions-Key is not found in 'token.env'")


def notify_errors(error_messages, script_name="Unknown Script"):
    """
    Sends an email notification if there are any errors in the error_messages list.

    Parameters:
    - error_messages (list): A list of error messages.
    - script_name (str): The name of the script where the errors occurred.
    """
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
            "x-functions-key": "your_x_functions_key",  # Replace with your actual key
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
