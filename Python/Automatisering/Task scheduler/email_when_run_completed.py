import requests
import os
from dotenv import load_dotenv
from datetime import datetime
import base64
import json

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
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")

if not X_FUNCTIONS_KEY:
    raise ValueError("X-FUNCTIONS-KEY not found in the loaded .env file.")
if not GITHUB_TOKEN:
    raise ValueError("GITHUB_TOKEN not found in the loaded .env file.")

print("X_FUNCTIONS_KEY loaded successfully.")

### GITHUB CONFIGURATION ###
GITHUB_API = "https://api.github.com"
REPO_OWNER = "evensrii"
REPO_NAME = "telemark"
LOGS_PATH = "Python/Automatisering/Task scheduler/logs_for_email_table"

def push_logs_to_github():
    """
    Push all log files to GitHub and return a dictionary mapping task names to their GitHub URLs.
    """
    log_urls = {}
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json"
    }

    # Get the current date for the commit message
    current_date = datetime.now().strftime("%Y-%m-%d")

    # Iterate through log files
    script_dir = os.path.dirname(os.path.abspath(__file__))
    logs_dir = os.path.join(script_dir, "logs")
    
    for log_file in os.listdir(logs_dir):
        if log_file.endswith(".log"):
            log_path = os.path.join(logs_dir, log_file)
            task_name = os.path.splitext(log_file)[0]  # Remove .log extension
            
            try:
                # Read the log file content
                with open(log_path, 'r', encoding='utf-8') as f:
                    content = f.read()

                # Encode content to base64
                content_bytes = content.encode('utf-8')
                content_base64 = base64.b64encode(content_bytes).decode('utf-8')

                # Prepare the file path in the repository
                file_path = f"{LOGS_PATH}/{log_file}"

                # Check if file exists in repo
                check_url = f"{GITHUB_API}/repos/{REPO_OWNER}/{REPO_NAME}/contents/{file_path}"
                response = requests.get(check_url, headers=headers)
                
                if response.status_code == 200:
                    # File exists, get its SHA
                    file_sha = response.json()["sha"]
                    
                    # Update file
                    update_data = {
                        "message": f"Update {log_file} for {current_date}",
                        "content": content_base64,
                        "sha": file_sha
                    }
                else:
                    # Create new file
                    update_data = {
                        "message": f"Add {log_file} for {current_date}",
                        "content": content_base64
                    }

                # Push to GitHub
                response = requests.put(check_url, headers=headers, json=update_data)
                
                if response.status_code in [200, 201]:
                    # Generate the URL to the raw content
                    raw_url = f"https://raw.githubusercontent.com/{REPO_OWNER}/{REPO_NAME}/main/{LOGS_PATH}/{log_file}"
                    log_urls[task_name] = raw_url
                else:
                    print(f"Failed to push {log_file} to GitHub. Status: {response.status_code}, Response: {response.text}")

            except Exception as e:
                print(f"Error processing {log_file}: {str(e)}")
                continue

    return log_urls

### RECIPIENTS ###

# Recipients and their corresponding names
recipients = [
    {"email": "even.sannes.riiser@telemarkfylke.no", "name": "Even"},
    #{"email": "kjersti.aase@telemarkfylke.no", "name": "Kjersti"},
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

# Read the log file content with UTF-8 encoding and error handling
with open(log_file_path, "r", encoding="utf-8", errors='replace') as log_file:
    log_content = log_file.read()

# Push logs to GitHub and get URLs
log_urls = push_logs_to_github()

def format_log_as_html_table(log_content, log_urls):
    """
    Formats the log content into an HTML table with separate "Dato" and "Tid" columns.

    Args:
        log_content (str): The content of the master log file.
        log_urls (dict): Dictionary mapping task names to their GitHub URLs.

    Returns:
        str: HTML string for the table.
    """
    # Split log content into lines
    log_lines = log_content.split("\n")

    # Prepare separate lists for rows with "Ja", "Feilet", and others
    rows_ja = []
    rows_feilet = []
    rows_other = []

    # Process each log line
    for idx, line in enumerate(log_lines):
        if line.strip() and "Daily run completed" not in line:  # Ignore empty lines and summary lines
            try:
                # Remove "Processing line: " if present
                if "Processing line: " in line:
                    line = line.replace("Processing line: ", "")

                # Split into timestamp and the rest of the line
                timestamp, rest = line.split("]", 1)
                timestamp = timestamp.strip("[")[:-3]  # Remove leading "[" and last 3 characters ",XX"

                # Split the timestamp into date and time
                date, time = timestamp.split(maxsplit=1)

                # Split the rest into task, script, status, and new data
                task_part, details_part = rest.split(":", 1)
                task = task_part.strip()

                # Extract script, status, and new_data_status from details
                script_part, status_and_new_data = details_part.rsplit(":", 1)
                script = script_part.strip()
                status, new_data_status = map(str.strip, status_and_new_data.split(",", 1))
                
                # Map status ("Completed" or "Failed") to "Fullført" or "Feilet"
                status = "Fullført" if status.lower() == "completed" else "Feilet"

                # Map new_data_status ("Yes" or "No") to "Ja" or "Nei"
                new_data = "Ja" if new_data_status == "Yes" else "Nei"

                # Determine "Status" badge style
                if status == "Fullført":
                    status_badge = f"<span style='color: #0bb30b; font-size: 22px; font-weight: 900;'>✓</span>"
                else:
                    status_badge = f"<span style='color: #FF4500; font-size: 22px; font-weight: 900;'>✗</span>"

                # Determine "New Data" badge style
                if new_data == "Ja":
                    new_data_badge = f"<span style='background-color: #faf334; color: black; font-weight: bold; border-radius: 8px; padding: 2px 5px; display: inline-block;'>Ja</span>"
                else:
                    new_data_badge = f"<span style='background-color: transparent; color: black; border-radius: 8px; padding: 2px 5px; display: inline-block;'>Nei</span>"

                # Create the row with GitHub log link if available
                background_color = "#f2f2f2" if idx % 2 == 0 else "#ffffff"
                task_url = log_urls.get(task, "#")
                row = f"""
                <tr style='background-color: {background_color};'>
                    <td>{date}</td>
                    <td>{time}</td>
                    <td style='text-align: left; padding-left: 20px; vertical-align: middle;'><a href='{task_url}' style='color: #00008B; text-decoration: none;'>{task}</a></td>
                    <td style='text-align: left; padding-left: 20px; vertical-align: middle;'>{script}</td>
                    <td>{status_badge}</td>
                    <td>{new_data_badge}</td>
                </tr>
                """

                # Sort rows into "Ja", "Feilet", or others
                if new_data == "Ja":
                    rows_ja.append(row)
                elif status == "Feilet":
                    rows_feilet.append(row)
                else:
                    rows_other.append(row)

            except Exception as e:
                # Debugging: Print the error and the line that caused it
                print(f"Error processing line: {line}. Error: {e}")
                continue

    # Combine rows: "Ja" first, then "Feilet," then others
    all_rows = rows_ja + rows_feilet + rows_other

    # Join all rows
    rows = "".join(all_rows)

    # Wrap rows in a styled HTML table
    html_table = f"""
    <style>
        @import url('https://fonts.googleapis.com/css?family=Source+Sans+Pro:400,700');

        body {{
            font-family: 'Source Sans Pro', sans-serif;
        }}

        table {{
            width: 100%;
            border-collapse: collapse;
            margin: 8px 0;
            font-size: 14px;
        }}

        th, td {{
            border: 1px solid #ddd;
            padding: 8px;
            text-align: center;
        }}

        th {{
            background-color: #000;
            color: white;
            text-transform: uppercase;
        }}

        td {{
            vertical-align: middle;
        }}

        tr:hover {{
            background-color: #ddd;
        }}
    </style>
    <table>
        <thead>
            <tr>
                <th>Dato</th>
                <th>Tid</th>
                <th>Oppgave</th>
                <th>Script</th>
                <th>Status</th>
                <th>Nye data?</th>
            </tr>
        </thead>
        <tbody>
            {rows}
        </tbody>
    </table>
    """
    return html_table


# Generate the HTML table
html_table = format_log_as_html_table(log_content, log_urls)


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
