import requests
import os
from dotenv import load_dotenv

### EMAIL CONFIGURATION ###

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

### READ MASTER LOG FILE ###

# Locate the log file relative to the script
script_dir = os.path.dirname(os.path.abspath(__file__))  # Folder containing this script
os.chdir(script_dir)  # Ensure CWD is the script's directory

log_file_path = os.path.join(script_dir, "./logs/00_master_run_log.txt")
print(f"Resolved log file path: {log_file_path}")

# Check if the file exists
if not os.path.exists(log_file_path):
    raise FileNotFoundError(f"Log file not found: {log_file_path}")

# Read the log file content
with open(log_file_path, "r") as log_file:
    log_content = log_file.read()
    
### SEND EMAIL ###

# Define the email payload
payload = {
    "to": ["even.sannes.riiser@telemarkfylke.no"],
    # "cc": ["kjersti.aase@telemarkfylke.no"],  # Uncomment if needed
    "from": "Analyse: Statusoppdatering <analyse@telemarkfylke.no>",
    "subject": "Statusoppdatering: Batch Run Completed",
    "text": log_content,  # Plain text version of the log
    "html": f"<pre>{log_content}</pre>",  # HTML version, formatted for better readability
}

# API endpoint for sending emails
url = "https://mail.api.telemarkfylke.no/api/mail"
headers = {
    "Content-Type": "application/json",
    "User-Agent": "insomnia/10.1.1",
    "x-functions-key": X_FUNCTIONS_KEY,
}

# Send the email
response = requests.post(url, headers=headers, json=payload)

# Handle response
if response.status_code == 200:
    print("Run completion email sent successfully.")
else:
    print(
        f"Failed to send run completion email. Status code: {response.status_code}, Response: {response.text}"
    )
