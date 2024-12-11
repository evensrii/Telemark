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

# Read the log file content with UTF-8 encoding
with open(log_file_path, "r", encoding="utf-8") as log_file:
    log_content = log_file.read()


### FORMAT LOG CONTENT INTO HTML TABLE ###

def format_log_as_html_table(log_content):
    """
    Formats the log content into an HTML table with proper styling.

    Args:
        log_content (str): The content of the master log file.

    Returns:
        str: HTML string for the table.
    """
    # Split log content into lines
    log_lines = log_content.split("\n")

    # Create HTML table rows
    rows = ""
    for idx, line in enumerate(log_lines):
        if line.strip():  # Ignore empty lines
            try:
                # Parse the line into components
                timestamp_task_status, new_data_status = line.rsplit(",", 1)
                timestamp, task_status = timestamp_task_status.split("]", 1)
                timestamp = timestamp.strip("[")
                task, script_status = task_status.split(":", 1)
                script, status = script_status.split(":", 1)

                # Format the "New Data" status
                new_data = "Ja" if new_data_status.strip() == "New Data" else ""

                # Determine status badge style
                if status.strip().lower() == "completed":
                    status_badge = f"<span style='background-color: #32CD32; color: white; border-radius: 8px; padding: 2px 5px; display: inline-block;'>Completed</span>"
                else:
                    status_badge = f"<span style='background-color: #FF4500; color: white; border-radius: 8px; padding: 2px 5px; display: inline-block;'>Failed</span>"

                # Apply alternating row colors
                background_color = "#f2f2f2" if idx % 2 == 0 else "#ffffff"

                # Add table row
                rows += f"""
                <tr style='background-color: {background_color};'>
                    <td>{timestamp}</td>
                    <td style='text-align: left; padding-left: 10px;'>{task.strip()}</td>
                    <td>{script.strip()}</td>
                    <td>{status_badge}</td>
                    <td>{new_data}</td>
                </tr>
                """
            except ValueError:
                # Skip lines that don't conform to the expected format
                continue

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

        tr:hover {{
            background-color: #ddd;
        }}
    </style>
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
