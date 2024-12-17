import requests
import os
from dotenv import load_dotenv
import base64
from datetime import datetime

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
    #{"email": "kjersti.aase@telemarkfylke.no", "name": "Kjersti"},
]

def push_logs_to_github():
    """
    Push all log files to GitHub repository.
    Returns a dictionary mapping script names to their GitHub log URLs.
    """
    # Get GitHub token
    GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
    if not GITHUB_TOKEN:
        print("GITHUB_TOKEN not found in environment variables")
        return {}

    # GitHub API configuration
    owner = "evensrii"
    repo = "Telemark"
    branch = "main"
    base_url = f"https://api.github.com/repos/{owner}/{repo}/contents"
    logs_path = "Python/Automatisering/Task scheduler/logs"
    
    # Dictionary to store script names and their log URLs
    log_urls = {}
    
    # Get list of log files
    script_dir = os.path.dirname(os.path.abspath(__file__))  # Folder containing this script
    log_dir = os.path.join(script_dir, "logs")
    for filename in os.listdir(log_dir):
        if filename.endswith('.log') and filename != "00_email.log" and filename != "00_master_run.log":
            file_path = os.path.join(log_dir, filename)
            
            try:
                # Read file content with explicit UTF-8 encoding and error handling
                with open(file_path, 'r', encoding='utf-8', errors='replace') as file:
                    content = file.read()
                
                # Prepare the file content for GitHub
                encoded_content = base64.b64encode(content.encode('utf-8')).decode('utf-8')
                
                # GitHub API endpoint for the specific file
                github_path = f"{logs_path}/{filename}"
                url = f"{base_url}/{github_path}"
                
                # Headers for GitHub API
                headers = {
                    "Authorization": f"token {GITHUB_TOKEN}",
                    "Accept": "application/vnd.github.v3+json"
                }
                
                try:
                    # Check if file exists on GitHub
                    response = requests.get(url, headers=headers)
                    
                    if response.status_code == 200:
                        # File exists, get its SHA
                        file_sha = response.json()["sha"]
                        
                        # Update file
                        data = {
                            "message": f"Update {filename} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                            "content": encoded_content,
                            "sha": file_sha,
                            "branch": branch
                        }
                    else:
                        # Create new file
                        data = {
                            "message": f"Add {filename} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                            "content": encoded_content,
                            "branch": branch
                        }
                    
                    # Push to GitHub
                    response = requests.put(url, headers=headers, json=data)
                    
                    if response.status_code in [200, 201]:
                        # Store the raw GitHub URL for the file
                        raw_url = f"https://github.com/{owner}/{repo}/blob/{branch}/{logs_path}/{filename}"
                        script_name = filename.replace('.log', '')
                        log_urls[script_name] = raw_url
                        print(f"Successfully pushed {filename} to GitHub")
                    else:
                        print(f"Failed to push {filename}. Status code: {response.status_code}")
                        
                except Exception as e:
                    print(f"Error pushing {filename}: {str(e)}")
                    
            except Exception as e:
                print(f"Error processing {filename}: {str(e)}")
                continue
    
    return log_urls

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

### FORMAT LOG CONTENT INTO HTML TABLE ###

def format_log_as_html_table(log_content):
    """
    Formats the log content into an HTML table with separate "Dato" and "Tid" columns.

    Args:
        log_content (str): The content of the master log file.

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
                # Debugging: Print the line being processed
                print(f"Processing line: {line}")

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

                # Create the row
                background_color = "#f2f2f2" if idx % 2 == 0 else "#ffffff"
                row = f"""
                <tr style='background-color: {background_color};'>
                    <td>{date}</td>
                    <td>{time}</td>
                    <td style='text-align: left; padding-left: 20px; vertical-align: middle;'><a href='{log_urls.get(task, "#")}' style='color: #00008B; text-decoration: none;'>{task}</a></td>
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
