## Function to fetch the GITHUB_TOKEN (environment variable) from the token.env file
def get_github_token():
    """
    Loads the GITHUB_TOKEN from a token.env file located in the PYTHONPATH/Queries directory.

    Returns:
        str: The value of the GITHUB_TOKEN environment variable.

    Raises:
        ValueError: If PYTHONPATH is not set, token.env is not found, or GITHUB_TOKEN is not in the .env file.
    """
    # Retrieve PYTHONPATH environment variable
    pythonpath = os.environ.get("PYTHONPATH")
    if not pythonpath:
        raise ValueError("PYTHONPATH environment variable is not set.")

    # Construct the full path to the Queries/token.env
    env_file_path = os.path.join(pythonpath, "token.env")
    if not os.path.exists(env_file_path):
        raise ValueError(f"token.env file not found in: {env_file_path}")

    # Load the .env file
    load_dotenv(env_file_path)
    print(f"Loaded .env file from: {env_file_path}")

    # Get the GITHUB_TOKEN from the environment
    github_token = os.getenv("GITHUB_TOKEN")
    if not github_token:
        raise ValueError("GITHUB_TOKEN not found in the loaded .env file.")

    print("GITHUB_TOKEN loaded successfully.")
    return github_token


GITHUB_TOKEN = get_github_token()

def download_github_file(file_path):
    """Download a file from GitHub."""
    url = f"https://api.github.com/repos/evensrii/Telemark/contents/{file_path}?ref=main"
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3.raw",
    }
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        # Return the content as a Pandas DataFrame
        from io import StringIO
        
        df = pd.read_csv(BytesIO(response.content))
        
        # Clean up the data
        for col in df.columns:
            df[col] = df[col].astype(str).str.strip()
            # Skip numeric conversion for NACE columns
            if 'nace' not in col.lower():
                try:
                    # Try converting to float if it's not a NACE column
                    df[col] = pd.to_numeric(df[col], errors='raise')
                except (ValueError, TypeError):
                    # If conversion fails, keep as string
                    pass
        
        return df
    elif response.status_code == 404:
        print(f"File not found on GitHub: {file_path}")
        return None
    else:
        print(f"Failed to download file: {file_path}, Status Code: {response.status_code}")
        return None