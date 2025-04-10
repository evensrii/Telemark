import pandas as pd
import matplotlib.pyplot as plt
import requests
from dotenv import load_dotenv
import os

def get_github_file():
    # Load environment variables
    load_dotenv()
    
    # Get GitHub token from environment variable
    github_token = os.getenv('GITHUB_TOKEN')
    if not github_token:
        raise ValueError("GitHub token not found in environment variables")
    
    # GitHub raw content URL for the CSV file
    url = "https://raw.githubusercontent.com/evensrii/Telemark/main/Data/01_Befolkning/Befolkningsendringer_VT_2010-2020.csv"
    
    # Set up headers with authentication
    headers = {
        "Authorization": f"token {github_token}",
        "Accept": "application/vnd.github.v3.raw"
    }
    
    # Get file content from GitHub
    response = requests.get(url, headers=headers)
    response.raise_for_status()  # Raise exception for bad status codes
    
    # Create DataFrame from CSV content
    df = pd.read_csv(pd.StringIO(response.text))
    return df

def analyze_population_changes():
    try:
        # Get data from GitHub
        print("Fetching data from GitHub...")
        df = get_github_file()
        
        # Display basic information about the dataset
        print("\nDataset Info:")
        print("-" * 50)
        print(df.info())
        
        # Display the first few rows
        print("\nFirst few rows of data:")
        print("-" * 50)
        print(df.head())
        
        # Display basic statistics
        print("\nBasic statistics:")
        print("-" * 50)
        print(df.describe())
        
        # If you want to save the processed data
        output_path = "processed_population_data.csv"
        df.to_csv(output_path, index=False)
        print(f"\nProcessed data saved to: {output_path}")
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    analyze_population_changes()