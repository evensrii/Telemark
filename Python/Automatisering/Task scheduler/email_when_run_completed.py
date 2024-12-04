import requests
import sys
import os
import inspect
from dotenv import load_dotenv

### EMAIL CONFIGURATION

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

#### SEND EMAIL

payload = {
            "to": ["even.sannes.riiser@telemarkfylke.no"],
            # "cc": [kjersti.aase@telemarkfylke.no],
            "from": "Analyse: Statusoppdatering <analyse@telemarkfylke.no>",
            "subject": f"....",
            "text": f"T...",
            "html": f"<b>....</b>"
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