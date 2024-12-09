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
    # Split log content into lines
    log_lines = log_content.split("\n")

    # Create HTML table rows
    rows = ""
    for idx, line in enumerate(log_lines):
        if line.strip():  # Ignore empty lines
            try:
                # Split into timestamp, rest
                timestamp, rest = line.split("]", 1)
                timestamp = timestamp.strip("[")[:-3]  # Remove leading "[" and last 3 characters ",XX"

                # Further split the timestamp into date and time
                date, time = timestamp.split(maxsplit=1)

                # Split rest into task and details
                task, details = rest.split(":", 1)
                task = task.strip()

                # Split details into script, status, and new data
                script, status, new_data = details.split(":", 2)
                script = script.strip()
                status = status.strip()
                new_data = new_data.split("New Data:")[-1].strip()  # Extract "True" or "False"

                # Format "Nye data" for the table
                nye_data_formatted = (
                    f"<span style='background-color: #1E90FF; color: white; border-radius: 8px; padding: 2px 5px; display: inline-block; font-size: 14px;'>Ja</span>"
                    if new_data.lower() == "true"
                    else ""
                )

                # Determine status badge style
                if status.lower() == "completed":
                    status_badge = f"<span style='background-color: #32CD32; color: white; border-radius: 8px; padding: 2px 5px; display: inline-block; font-size: 14px;'>{status}</span>"
                else:
                    status_badge = f"<span style='background-color: #FF4500; color: white; border-radius: 8px; padding: 2px 5px; display: inline-block; font-size: 14px;'>{status}</span>"

                # Apply alternating background colors manually
                background_color = "#f2f2f2" if idx % 2 == 0 else "#ffffff"
                rows += (
                    f"<tr style='background-color: {background_color};'>"
                    f"<td>{date}</td>"
                    f"<td>{time}</td>"
                    f"<td style='text-align: left; padding-left: 20px; vertical-align: middle;'>{task}</td>"
                    f"<td style='text-align: left; padding-left: 20px; vertical-align: middle;'>{script}</td>"
                    f"<td>{status_badge}</td>"
                    f"<td style='text-align: center; vertical-align: middle;'>{nye_data_formatted}</td>"
                    f"</tr>"
                )
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
            padding: 2px;
            text-align: center;
            vertical-align: middle;
        }}

        th {{
            background-color: #000;
            color: white;
            text-transform: uppercase;
            font-size: 14px;
            height: 40px;
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
                <th>Nye data</th>
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
