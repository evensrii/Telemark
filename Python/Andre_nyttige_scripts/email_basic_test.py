import requests
from dotenv import load_dotenv
import os

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
print(f"X_FUNCTIONS_KEY: {X_FUNCTIONS_KEY}")


url = "https://mail.api.telemarkfylke.no/api/mail"

payload = {
    "to": ["even.sannes.riiser@telemarkfylke.no"],
    # "cc": ["even.sannes.riiser@telemarkfylke.no"],
    # "bcc": ["even.sannes.riiser@telemarkfylke.no"],
    "from": "Ola Nordmann <ola@nordmann.no>",
    "subject": "Test",
    "text": "Heihei",
    "html": "<b>Heihei</b>",
}

headers = {
    "Content-Type": "application/json",
    "User-Agent": "insomnia/10.1.1",
    "x-functions-key": X_FUNCTIONS_KEY,
}

# Send the POST request
try:
    response = requests.post(
        url,
        headers=headers,  # Use the headers to pass the API key
        json=payload,  # Use the body as the JSON payload
    )

    # Raise an error if the request was unsuccessful
    response.raise_for_status()

    print("Email sent successfully!")
    print(f"Response Status Code: {response.status_code}")
    print(f"Response Text: {response.text}")

except requests.exceptions.HTTPError as http_err:
    print(f"HTTP error occurred: {http_err}")  # Specific HTTP error details
except Exception as e:
    print(f"An error occurred: {e}")
