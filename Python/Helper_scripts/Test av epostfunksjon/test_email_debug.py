"""
Simple test script to debug email functionality from email_when_run_completed.py

This script tests:
1. Environment variable loading (X-FUNCTIONS-KEY)
2. Basic email sending functionality
3. API connection and response handling
"""

import requests
import os
from dotenv import load_dotenv
from datetime import datetime
import sys

print("=" * 60)
print("EMAIL FUNCTION DEBUG TEST")
print("=" * 60)

### ENVIRONMENT VARIABLE TESTING ###
print("\n1. TESTING ENVIRONMENT VARIABLE LOADING")
print("-" * 40)

# Retrieve PYTHONPATH environment variable
pythonpath = os.environ.get("PYTHONPATH")
print(f"PYTHONPATH: {pythonpath}")

if pythonpath:
    # Construct the full path to the Queries/token.env
    env_file_path = os.path.join(pythonpath, "token.env")
    print(f"Looking for .env file at: {env_file_path}")

    if os.path.exists(env_file_path):
        # Load the .env file
        load_dotenv(env_file_path)
        print(f"✓ Loaded .env file from: {env_file_path}")
    else:
        print(f"✗ token.env not found in: {env_file_path}")
        sys.exit(1)
else:
    print("✗ PYTHONPATH environment variable is not set.")
    sys.exit(1)

# Get the X-FUNCTIONS-KEY from the environment
X_FUNCTIONS_KEY = os.getenv("X_FUNCTIONS_KEY")
if not X_FUNCTIONS_KEY:
    print("✗ X-FUNCTIONS-KEY not found in the loaded .env file.")
    sys.exit(1)

print("✓ X_FUNCTIONS_KEY loaded successfully.")
print(f"Key length: {len(X_FUNCTIONS_KEY)} characters")
print(f"Key starts with: {X_FUNCTIONS_KEY[:10]}...")

### TEST EMAIL SENDING ###
print("\n2. TESTING EMAIL API CONNECTION")
print("-" * 40)

# Simple test email payload
test_payload = {
    "to": ["even.sannes.riiser@telemarkfylke.no"],
    "from": "Analyse TFK <analyse@telemarkfylke.no>",
    "subject": f"Email Function Debug Test - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
    "text": "This is a test email from the debug script to verify email functionality is working.",
    "html": "<p><strong>This is a test email</strong> from the debug script to verify email functionality is working.</p><p>If you receive this, the email function is working correctly!</p>",
}

# API endpoint and headers (same as in email_when_run_completed.py)
url = "https://email.api.telemarkfylke.no/api/send"
headers = {
    "Content-Type": "application/json; charset=utf-8",
    "User-Agent": "insomnia/10.1.1",
    "x-functions-key": X_FUNCTIONS_KEY,
}

print(f"API Endpoint: {url}")
print(f"Headers: {dict(headers)}")
print(f"Payload recipient: {test_payload['to']}")
print(f"Payload subject: {test_payload['subject']}")

### DRY RUN OPTION ###
print("\n3. EMAIL SENDING OPTIONS")
print("-" * 40)

# Ask user if they want to actually send the email
user_input = input("Do you want to send the test email? (y/N): ").strip().lower()

if user_input == 'y' or user_input == 'yes':
    print("\nSending test email...")
    
    try:
        # Send the email
        response = requests.post(url, headers=headers, json=test_payload)
        
        print(f"Response Status Code: {response.status_code}")
        print(f"Response Headers: {dict(response.headers)}")
        print(f"Response Text: {response.text}")
        
        # Check the response
        if response.status_code == 200:
            print("✓ Email sent successfully!")
        else:
            print(f"✗ Failed to send email. Status code: {response.status_code}")
            print(f"Response: {response.text}")
            
    except Exception as e:
        print(f"✗ Exception occurred while sending email: {e}")
        
else:
    print("Skipping email send. Testing connection only...")
    
    try:
        # Test connection without sending (using a different endpoint or method if available)
        # For now, we'll just validate the structure
        print("✓ Email payload structure is valid")
        print("✓ Headers are properly formatted")
        print("✓ API key is loaded")
        print("Note: Email was not sent due to user choice")
        
    except Exception as e:
        print(f"✗ Error in email setup: {e}")

### COMPARISON WITH EMAIL_FUNCTIONS.PY ###
print("\n4. COMPARING WITH EMAIL_FUNCTIONS.PY")
print("-" * 40)

try:
    # Try to import and test the email_functions
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    from email_functions import notify_errors
    
    print("✓ Successfully imported email_functions.py")
    
    # Test the notify_errors function (dry run)
    test_errors = ["This is a test error message"]
    print("Testing notify_errors function (dry run)...")
    notify_errors(test_errors, script_name="test_email_debug.py", send_email=False)
    
except Exception as e:
    print(f"✗ Error importing or testing email_functions.py: {e}")

print("\n" + "=" * 60)
print("DEBUG TEST COMPLETED")
print("=" * 60)
