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


### RECIPIENTS ###

# Recipients and their corresponding names
recipients = [
    {"email": "even.sannes.riiser@telemarkfylke.no", "name": "Even"},
    {"email": "kjersti.aase@telemarkfylke.no", "name": "Kjersti"},
]

### READ MASTER LOG FILE ###

# Locate the log file relative to the script
script_dir = os.path.dirname(os.path.abspath(__file__))  # Folder containing this script
os.chdir(script_dir)  # Ensure CWD is the script's directory

log_file_path = os.path.join(script_dir, "./logs/00_master_run.log")
print(f"Resolved log file path: {log_file_path}")

# Check if the file exists
if not os.path.exists(log_file_path):
    raise FileNotFoundError(f"Log file not found: {log_file_path}")

# Read the log file content with UTF-8 encoding
with open(log_file_path, "r", encoding="utf-8") as log_file:
    log_content = log_file.read()


### FORMAT LOG CONTENT INTO HTML TABLE ###

def format_log_as_html_table(log_content):
    # Split log content into lines
    log_lines = log_content.split("\n")

    # Create HTML table rows
    rows = ""
    for idx, line in enumerate(log_lines):
        if line.strip():  # Ignore empty lines
            try:
                # Split line into components
                timestamp_task_status, new_data_status = line.rsplit(",", 1)
                timestamp, task_status = timestamp_task_status.split("]", 1)
                timestamp = timestamp.strip("[")
                task, script_status = task_status.split(":", 1)
                script, status = script_status.split(":", 1)

                # Format the "New Data" status
                new_data = "Ja" if new_data_status.strip() == "Ja" else ""

                # Add table row
                rows += f"""
                <tr>
                    <td>{timestamp}</td>
                    <td>{task.strip()}</td>
                    <td>{script.strip()}</td>
                    <td>{status.strip()}</td>
                    <td>{new_data}</td>
                </tr>
                """
            except ValueError:
                continue

    # Wrap rows in a styled HTML table
    html_table = f"""
    <table>
        <thead>
            <tr>
                <th>Dato</th>
                <th>Oppgave</th>
                <th>Script</th>
                <th>Status</th>
                <th>New Data</th>
            </tr>
        </thead>
        <tbody>
            {rows}
        </tbody>
    </table>
    """
    return html_table


# Generate the HTML table
html_table = format_log_as_html_table(log_content)


### SEND EMAIL ###

# Email sending logic
for recipient in recipients:
    # Personalize the subject
    subject = f"God morgen, {recipient['name']}! Her er nattens kjøringer."

    # Define the email payload
    payload = {
        "to": [recipient["email"]],
        "from": "Analyse TFK <analyse@telemarkfylke.no>",
        "subject": subject,
        "text": log_content,
        "html": html_table,
    }

    # API endpoint and headers
    url = "https://mail.api.telemarkfylke.no/api/mail"
    headers = {
        "Content-Type": "application/json; charset=utf-8",  # Explicitly set charset
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
