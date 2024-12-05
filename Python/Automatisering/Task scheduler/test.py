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

print(log_content)


# Convert log content into an HTML table
def format_log_as_html_table(log_content):
    # Split log content into lines
    log_lines = log_content.split("\n")

    # Create HTML table rows
    rows = ""
    for line in log_lines:
        if line.strip():  # Ignore empty lines
            # Extract timestamp and log message
            try:
                timestamp, message = line.split("]", 1)
                timestamp = timestamp.strip("[")  # Remove the leading '['
                message = message.strip()  # Remove extra whitespace
                rows += f"<tr><td>{timestamp}</td><td>{message}</td></tr>"
            except ValueError:
                # If the line doesn't have a timestamp (e.g., plain text), place it in the message column
                rows += f"<tr><td></td><td>{line.strip()}</td></tr>"

    # Wrap rows in a table
    html_table = f"""
    <table border="1" style="border-collapse: collapse; width: 100%;">
        <thead>
            <tr>
                <th>Time</th>
                <th>Log Message</th>
            </tr>
        </thead>
        <tbody>
            {rows}
        </tbody>
    </table>
    """
    return html_table


# Generate the HTML table from log content
html_table = format_log_as_html_table(log_content)


# Define the email payload
payload = {
    "to": ["even.sannes.riiser@telemarkfylke.no"],
    "from": "Analyse: Statusoppdatering <analyse@telemarkfylke.no>",
    "subject": "Hei",
    "text": log_content,  # Fallback for plain text
    "html": html_table,  # HTML table for modern email clients
}

# API endpoint and headers
url = "https://mail.api.telemarkfylke.no/api/mail"
headers = {
    "Content-Type": "application/json",
    "User-Agent": "insomnia/10.1.1",
    "x-functions-key": X_FUNCTIONS_KEY,
}

# Send the email
response = requests.post(url, headers=headers, json=payload)


# Check the response
if response.status_code == 200:
    print(f"Email sent successfully to {recipient['name']} ({recipient['email']}).")
else:
    print(
        f"Failed to send email to {recipient['name']} ({recipient['email']}). "
        f"Status code: {response.status_code}, Response: {response.text}"
    )


### SEND EMAIL ###

# Email sending logic
for recipient in recipients:
    # Personalize the subject
    subject = f"God morgen, {recipient['name']}! Her er nattens kjøringer."

    # Define the email payload
    payload = {
        "to": [recipient["email"]],
        "from": "Analyse: Statusoppdatering <analyse@telemarkfylke.no>",
        "subject": subject,
        "text": log_content,
        "html": f"<pre>{log_content}</pre>",
    }

    # API endpoint and headers
    url = "https://mail.api.telemarkfylke.no/api/mail"
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "insomnia/10.1.1",
        "x-functions-key": X_FUNCTIONS_KEY,
    }

    # Send the email
    response = requests.post(url, headers=headers, json=payload)
