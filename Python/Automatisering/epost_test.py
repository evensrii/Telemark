import requests

# Finner URL vha. "Inspiser side" og fane "Network"
url = "https://mail-test.api.telemarkfylke.no/api/mail"
headers = {
    "Content-Type": "application/json",  # Ensure JSON content type
}

# Email details (customize as per your API's requirements)
body = {
    "to": ["even.sannes.riiser@telemarkfylke.no"],
    # You can add more recipients to the "to" list if needed
    "from": "task-server@telemarkfylke.no",
    "subject": "Test Email",
    "text": "This is a test email sent via the API.",
    # Optional HTML content
    # "html": "<h1>This is a test email</h1><p>More details here...</p>"
}

# Send the POST request
try:
    response = requests.post(
        url,
        headers=headers,  # Use the headers to pass the API key
        json=body,  # Use the body as the JSON payload
    )

    # Raise an error if the request was unsuccessful
    response.raise_for_status()

    print("Email sent successfully!")
    print(f"Response Status Code: {response.status_code}")
    print(f"Response Text: {response.text}")

except requests.exceptions.HTTPError as http_err:
    print(f"HTTP error occurred: {http_err}")  # Specific HTTP error details
except Exception as e:
    print(f"An error occurred: {e}")
