import requests
import os
from datetime import datetime
from dotenv import load_dotenv

### EPOST CONFIGURATION

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

def test_email_api():
    """
    Test the email API functionality with a simple test message.
    """
    # Test payload
    payload = {
        "to": ["even.sannes.riiser@telemarkfylke.no"],
        "from": "API Test <analyse@telemarkfylke.no>",
        "subject": "Test Email API",
        "text": (
            f"This is a test email sent at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
            "Testing plain text formatting:\n"
            "- Line 1\n"
            "- Line 2\n"
        ),
        "html": (
            f"<h2>This is a test email</h2>"
            f"<p>Sent at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>"
            "<p>Testing HTML formatting:</p>"
            "<ul>"
            "<li>Line 1</li>"
            "<li>Line 2</li>"
            "</ul>"
        ),
    }

    url = "https://email.api.telemarkfylke.no/api/send"
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "insomnia/10.1.1",
        "x-functions-key": X_FUNCTIONS_KEY,
    }

    try:
        print("Sending test email...")
        email_response = requests.post(url, headers=headers, json=payload)
        
        print(f"\nResponse status code: {email_response.status_code}")
        print(f"Response body: {email_response.text}")

        if email_response.status_code == 200:
            print("\nSuccess: Test email sent successfully!")
        else:
            print(f"\nError: Failed to send test email. Status code: {email_response.status_code}")
            
    except requests.exceptions.RequestException as e:
        print(f"\nNetwork Error: An error occurred while sending the email: {e}")
    except Exception as e:
        print(f"\nUnexpected Error: {e}")

if __name__ == "__main__":
    test_email_api()
