import requests
import os
from dotenv import load_dotenv
import base64
from datetime import datetime
import urllib.parse
import sys

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

# Paths and configurations
base_path = os.getenv("PYTHONPATH")
if base_path is None:
    raise ValueError("PYTHONPATH environment variable is not set")
LOG_DIR = os.path.join(base_path, "Automatisering", "Task scheduler", "logs")

### RECIPIENTS ###

# Recipients and their corresponding names
recipients = [
    {"email": "even.sannes.riiser@telemarkfylke.no", "name": "Even"},
    #{"email": "even.s.riiser@gmail.com", "name": "Evensen"},
    #{"email": "kjersti.aase@telemarkfylke.no", "name": "Kjersti"},
 ]

def push_logs_to_github():
    """
    Push all log files to GitHub repository.
    Returns a dictionary mapping script names to their GitHub log URLs.
    """
    log_urls = {}

    # GitHub token and configuration
    github_token = os.getenv("GITHUB_TOKEN")
    if not github_token:
        print("GITHUB_TOKEN not found in environment variables")
        return log_urls

    owner = "evensrii"
    repo = "Telemark"
    branch = "main"
    base_url = f"https://github.com/{owner}/{repo}/blob/{branch}"
    logs_path = "Python/Automatisering/Task scheduler/logs_for_email_links"

    for filename in os.listdir(LOG_DIR):
        if filename.endswith(".log") and filename not in ["00_email.log", "00_email_weekly.log", "00_master_run.log", "00_master_run_weekly.log", "readme.txt"]:
            local_file_path = os.path.join(LOG_DIR, filename)

            # Read local log file content
            try:
                with open(local_file_path, "r", encoding="utf-8") as file:
                    local_content = file.read()
            except Exception as e:
                print(f"Error reading {filename}: {e}")
                continue

            # GitHub API endpoint
            github_file_path = f"{logs_path}/{filename}"
            url = f"https://api.github.com/repos/{owner}/{repo}/contents/{github_file_path}"
            headers = {
                "Authorization": f"token {github_token}",
                "Accept": "application/vnd.github.v3+json",
            }

            # Check if the file exists on GitHub
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                # File exists, check if content differs
                github_content = base64.b64decode(response.json()["content"]).decode("utf-8")
                sha = response.json()["sha"]

                if local_content.strip() == github_content.strip():
                    generated_url = f"{base_url}/{github_file_path}"
                    log_urls[filename] = generated_url
                    print(f"No changes detected for {filename}. Skipping upload. URL: {generated_url}")
                    continue

            # Upload or update the file on GitHub
            payload = {
                "message": f"Update {filename} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                "content": base64.b64encode(local_content.encode("utf-8")).decode("utf-8"),
                "branch": branch,
            }
            if response.status_code == 200:
                payload["sha"] = response.json()["sha"]

            response = requests.put(url, json=payload, headers=headers)
            if response.status_code in [200, 201]:
                generated_url = f"{base_url}/{github_file_path}"
                log_urls[filename] = generated_url
                print(f"Successfully uploaded log file {filename}. URL: {generated_url}")
            else:
                log_urls[filename] = f"Failed: {response.json()}"
                print(f"Failed to upload {filename}: {response.json()}")

    # Print all generated URLs
    #print("Generated log URLs:")
    #for filename, url in log_urls.items():
    #    print(f"{filename}: {url}")

    return log_urls



### READ MASTER LOG FILE ###

# Locate the log file relative to the script
script_dir = os.path.dirname(os.path.abspath(__file__))  # Folder containing this script
os.chdir(script_dir)  # Ensure CWD is the script's directory

# Determine if this is a weekly or daily run based on the log file
is_weekly = any(arg == "--weekly" for arg in sys.argv)
log_file_path = os.path.join(script_dir, "./logs/00_master_run_weekly.log" if is_weekly else "./logs/00_master_run.log")
print(f"Resolved log file path: {log_file_path}")

# Check if the file exists
if not os.path.exists(log_file_path):
    raise FileNotFoundError(f"Log file not found: {log_file_path}")

# Read the log file
try:
    with open(log_file_path, 'r', encoding='utf-8') as file:
        log_content = file.read()
    print(f"Successfully read the master run log file: {log_file_path}")
except Exception as e:
    print(f"Error reading log file: {e}")
    sys.exit(1)

# Push logs to GitHub and get URLs
log_urls = push_logs_to_github()

### FORMAT LOG CONTENT INTO HTML TABLE ###

def generate_raw_github_url(task_name):
    """
    Generate a raw GitHub URL for a task's log file.
    
    Args:
        task_name (str): Name of the task
        
    Returns:
        str: Raw GitHub URL for the log file
    """
    # Base URL for raw GitHub content with refs/heads/
    base_url = "https://raw.githubusercontent.com/evensrii/Telemark/refs/heads/main/Python/Automatisering/Task%20scheduler/logs_for_email_links"
    
    # Create the file name by replacing spaces with %20 and adding .log extension
    file_name = task_name.replace(" ", "%20") + ".log"
    
    # Combine base URL and file name
    url = f"{base_url}/{file_name}"
    
    # Log the generated URL for debugging
    print(f"\nDEBUG - Task: {task_name}")
    print(f"DEBUG - Generated URL: {url}")
    
    return url

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
    rows_other = []
    rows_feilet = []

    # Process each log line
    for idx, line in enumerate(log_lines):
        if line.strip() and "Daily run completed" not in line:  # Ignore empty lines and summary lines
            try:
                # Split into timestamp and the rest of the line
                timestamp, rest = line.split("]", 1)
                timestamp = timestamp.strip("[")[:-3]  # Remove leading "[" and last 3 characters ",XX"

                # Split the timestamp into date and time
                date, time = timestamp.split(maxsplit=1)

                # Check if this is a script execution line or an administrative line
                if ":" in rest and len(rest.split(":")) >= 3:
                    # Split the rest into task, script, status, new data, and last commit
                    task_part, details_part = rest.split(":", 1)
                    task = task_part.strip()

                    # Extract script, status, new_data_status, and last_commit from details
                    parts = details_part.split(":", 1)[1].strip().split(",")
                    script = details_part.split(":")[0].strip()
                    status = parts[0].strip()
                    new_data_status = parts[1].strip()
                    last_commit = parts[2].strip() if len(parts) > 2 else None
                    
                    # Map status ("Completed" or "Failed") to "Fullført" or "Feilet"
                    status = "Fullført" if status.lower() == "completed" else "Feilet"

                    # Map new_data_status ("Yes" or "No") to "Ja" or "Nei"
                    new_data = "Ja" if new_data_status == "Yes" else "Nei"

                    # Create row data
                    row_data = {
                        "date": date,
                        "time": time,
                        "task": task,
                        "script": script,
                        "status": status,
                        "new_data": new_data,
                        "last_commit": last_commit
                    }

                    # Add to appropriate list based on status and new_data value
                    if status == "Feilet":
                        rows_feilet.append(row_data)
                    elif new_data == "Ja":
                        rows_ja.append(row_data)
                    else:
                        rows_other.append(row_data)
                else:
                    # This is an administrative line, skip it
                    continue

            except Exception as e:
                # Print the error and continue with the next line
                print(f"Error processing line: {line}. Error: {e}")
                continue

    # Sort each list by last_commit time (most recent first)
    def sort_by_last_commit(row):
        last_commit = row.get("last_commit", "")
        if not last_commit or last_commit == "N/A":
            return datetime.min  # Put entries with no last_commit at the bottom
        try:
            return datetime.strptime(last_commit, '%Y-%m-%d %H:%M:%S')
        except:
            return datetime.min

    for row_list in [rows_ja, rows_other, rows_feilet]:
        row_list.sort(key=sort_by_last_commit, reverse=True)  # reverse=True for most recent first

    # Combine rows: "Ja" first, then "Feilet", then successful runs without new data
    all_rows = rows_ja + rows_feilet + rows_other
    
    # Create HTML rows
    html_rows = []
    for row in all_rows:
        background_color = "#f2f2f2" if all_rows.index(row) % 2 == 0 else "#ffffff"
        status_badge = f"<span style='color: #0bb30b; font-size: 22px; font-weight: 900;'>✓</span>" if row["status"] == "Fullført" else f"<span style='color: #FF4500; font-size: 22px; font-weight: 900;'>✗</span>"
        new_data_badge = f"<span style='background-color: #faf334; color: black; font-weight: bold; border-radius: 8px; padding: 2px 5px; display: inline-block;'>Ja</span>" if row["new_data"] == "Ja" else f"<span style='background-color: transparent; color: black; border-radius: 8px; padding: 2px 5px; display: inline-block;'>Nei</span>"
        
        # Format last commit time if available
        last_commit = row.get("last_commit", "")
        if last_commit:
            try:
                # Convert to datetime for formatting
                dt = datetime.strptime(last_commit, '%Y-%m-%d %H:%M:%S')
                last_commit = dt.strftime('%d.%m.%Y %H:%M')
            except:
                last_commit = "N/A"
        else:
            last_commit = "N/A"
        
        # Generate raw GitHub URL for the task
        log_url = generate_raw_github_url(row["task"])
        
        html_row = f"""
        <tr style='background-color: {background_color};'>
            <td>{row["date"]}</td>
            <td>{row["time"]}</td>
            <td style='text-align: left; padding-left: 20px; vertical-align: middle;'>
                <a href='{log_url}' target='_blank' style='color: #00008B; text-decoration: none;'>{row["task"]}</a>
            </td>
            <td style='text-align: left; padding-left: 20px; vertical-align: middle;'>{row["script"]}</td>
            <td>{status_badge}</td>
            <td>{new_data_badge}</td>
            <td>{last_commit}</td>
        </tr>
        """
        html_rows.append(html_row)

    # Join all rows
    rows = "".join(html_rows)

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
                <th>Fullført</th>
                <th>Nye data?</th>
                <th>Sist oppdatert</th>
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
    subject = f"God morgen, {recipient['name']}! Her er {'de ukentlige kjøringene.' if is_weekly else 'dagens kjøringer.'}"

    # Define the email payload
    payload = {
        "to": [recipient["email"]],
        "from": "Analyse TFK <analyse@telemarkfylke.no>",
        "subject": subject,
        "text": log_content,
        "html": html_table,
    }

    # API endpoint and headers
    url = "https://email.api.telemarkfylke.no/api/send"
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