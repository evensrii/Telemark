"""
Test script to verify email sending functionality
"""

import os
import requests
from dotenv import load_dotenv

# Load environment variables from token.env
pythonpath = os.environ.get("PYTHONPATH")

if pythonpath:
    # Construct the full path to token.env
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

print("Environment variable loaded successfully.")
print(f"API Key found: {'*' * 10}{X_FUNCTIONS_KEY[-4:]}")  # Show only last 4 chars

# Define test recipient
test_recipient = {
    "name": "Even",
    "email": "even.sannes.riiser@telemarkfylke.no"
}

# Define the email payload
subject = "Test Email - Email Function Verification"
text_content = "This is a test email to verify the email sending functionality works correctly."
html_content = "<h2>Test Email</h2><p>This is a test email to verify the email sending functionality works correctly.</p>"

payload = {
    "to": [test_recipient["email"]],
    "from": "Analyse TFK <analyse@telemarkfylke.no>",
    "subject": subject,
    "text": text_content,
    "html": html_content,
}

# API endpoint and headers
url = "https://email.api.telemarkfylke.no/api/send"
headers = {
    "Content-Type": "application/json; charset=utf-8",
    "User-Agent": "insomnia/10.1.1",
    "x-functions-key": X_FUNCTIONS_KEY,
}

print(f"\nAttempting to send test email to: {test_recipient['email']}")

# Send the email
try:
    response = requests.post(url, headers=headers, json=payload)
    
    # Check the response
    if response.status_code == 200:
        print(f"✓ Email sent successfully to {test_recipient['name']} ({test_recipient['email']}).")
        print(f"Response: {response.text}")
    else:
        print(
            f"✗ Failed to send email to {test_recipient['name']} ({test_recipient['email']}). "
            f"Status code: {response.status_code}, Response: {response.text}"
        )
except Exception as e:
    print(f"✗ Error occurred while sending email: {str(e)}")
