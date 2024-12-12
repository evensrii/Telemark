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

    # Prepare separate lists for rows with "Ja" and "Nei"
    rows_ja = []
    rows_nei = []

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
                    status_badge = f"<span style='background-color: #139c13; color: white; border-radius: 8px; padding: 2px 5px; display: inline-block;'>Fullført</span>"
                else:
                    status_badge = f"<span style='background-color: #FF4500; color: white; border-radius: 8px; padding: 2px 5px; display: inline-block;'>Feilet</span>"

                # Determine "New Data" badge style
                if new_data == "Ja":
                    new_data_badge = f"<span style='background-color: #f0e80a; color: black; font-weight: bold; border-radius: 8px; padding: 2px 5px; display: inline-block;'>Ja</span>"
                else:
                    new_data_badge = f"<span style='background-color: transparent; color: black; border-radius: 8px; padding: 2px 5px; display: inline-block;'>Nei</span>"

                # Apply alternating row colors
                background_color = "#f2f2f2" if idx % 2 == 0 else "#ffffff"

                # Create the row
                row = f"""
                <tr style='background-color: {background_color};'>
                    <td>{date}</td>
                    <td>{time}</td>
                    <td style='text-align: left; padding-left: 20px; vertical-align: middle;'>{task}</td>
                    <td style='text-align: left; padding-left: 20px; vertical-align: middle;'>{script}</td>
                    <td>{status_badge}</td>
                    <td>{new_data_badge}</td>
                </tr>
                """

                # Sort rows into "Ja" or "Nei" groups
                if new_data == "Ja":
                    rows_ja.append(row)
                else:
                    rows_nei.append(row)

            except Exception as e:
                # Debugging: Print the error and the line that caused it
                print(f"Error processing line: {line}. Error: {e}")
                continue

    # Combine "Ja" rows first, followed by "Nei" rows
    rows = "".join(rows_ja + rows_nei)

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
