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


### RECIPIENTS

# Recipients and their corresponding names
recipients = [
    {"email": "even.sannes.riiser@telemarkfylke.no", "name": "Even"},
    {"email": "even.s.riiser@gmail.com", "name": "Evenmann"},
]

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
                date, time = timestamp.split(" ")

                # Split rest into task and details
                task, details = rest.split(":", 1)
                task = task.strip()

                # Split details into script and status
                script, status = details.split(":", 1)
                script = script.strip()
                status = status.strip()

                # Determine status badge style
                if status.lower() == "completed":
                    status_badge = f"<span style='background-color: #32CD32; color: white; border-radius: 8px; padding: 2px 5px; display: inline-block; font-size: 14px;'>{status}</span>"
                else:
                    status_badge = f"<span style='background-color: #FF4500; color: white; border-radius: 8px; padding: 2px 5px; display: inline-block; font-size: 14px;'>{status}</span>"

                # Apply alternating background colors manually
                # Apply alternating background colors manually
                background_color = "#f2f2f2" if idx % 2 == 0 else "#ffffff"
                rows += (
                    f"<tr style='background-color: {background_color};'>"
                    f"<td>{date}</td>"
                    f"<td>{time}</td>"
                    f"<td style='text-align: left; padding-left: 20px; vertical-align: middle; display: block; margin: 0 auto;'>{task}</td>"  # Proper alignment for Oppgave
                    f"<td style='text-align: left; padding-left: 20px; vertical-align: middle; display: block; margin: 0 auto;'>{script}</td>"  # Proper alignment for Script
                    f"<td>{status_badge}</td>"
                    f"</tr>"
                )
            except ValueError:
                # Handle lines that don't conform to the expected format
                rows += f"<tr><td colspan='5'>{line.strip()}</td></tr>"

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
            font-size: 14px; /* Increased font size */
        }}

        th, td {{
            border: 1px solid #ddd;
            padding: 2px; /* Keep compact padding */
            text-align: center; /* Default for all cells */
            vertical-align: middle; /* Align content vertically */
        }}

        th {{
            background-color: #000;
            color: white;
            text-transform: uppercase;
            font-size: 14px; /* Increased font size */
        }}

        tr:hover {{
            background-color: #ddd; /* Highlight on hover */
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
        "from": "Analyse: Statusoppdatering <analyse@telemarkfylke.no>",
        "subject": subject,
        "text": log_content,
        "html": html_table,
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