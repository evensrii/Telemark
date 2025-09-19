"""
Script for importing sentrumssoner_100m.csv from GitHub and converting to pandas DataFrame

This script:
1. Imports the CSV file directly from GitHub
2. Converts it to a pandas DataFrame
3. Displays basic information about the dataset
4. Allows for further processing and analysis

Author: Data Engineering Team
Date: 2025-09-19
"""

import pandas as pd
import requests
from io import StringIO
import sys
import os

#### Datasettet inneholder alle ruter i Skien og Porsgrunn, uvhengig av om de er i sentrum. Filtrerer disse til slutt.

# Add helper scripts to path
sys.path.append(r'C:\Users\eve1509\OneDrive - Telemark fylkeskommune\Github\Telemark\Python\Helper_scripts')

def import_sentrumssoner_data():
    """
    Import sentrumssoner_100m.csv from GitHub and convert to pandas DataFrame
    
    Returns:
        pd.DataFrame: The imported data as a pandas DataFrame
    """
    
    # GitHub URL for the CSV file
    github_url = "https://raw.githubusercontent.com/evensrii/Telemark/refs/heads/main/Data/Bystrategi_Grenland/Areal_og_byutvikling/Sentrumssoner/sentrumssoner_100m.csv"
    
    try:
        print("Importing sentrumssoner_100m.csv from GitHub...")
        print(f"URL: {github_url}")
        
        # Download the CSV file from GitHub
        response = requests.get(github_url)
        response.raise_for_status()  # Raise an exception for bad status codes
        
        # Convert the response content to a pandas DataFrame
        # Using StringIO to treat the string content as a file-like object
        csv_content = StringIO(response.text)
        
        # Read CSV with pandas - assuming semicolon separator based on your workflow
        df = pd.read_csv(csv_content, sep=';', encoding='utf-8')
        
        print(f"Successfully imported data with shape: {df.shape}")
        print(f"Columns: {list(df.columns)}")
        
        return df
        
    except requests.exceptions.RequestException as e:
        print(f"Error downloading file from GitHub: {e}")
        return None
    except pd.errors.EmptyDataError:
        print("Error: The CSV file is empty")
        return None
    except pd.errors.ParserError as e:
        print(f"Error parsing CSV file: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error: {e}")
        return None

def analyze_data(df):
    """
    Perform basic analysis of the imported DataFrame
    
    Args:
        df (pd.DataFrame): The DataFrame to analyze
    """
    if df is None:
        print("No data to analyze")
        return
    
    print("\n" + "="*50)
    print("DATA ANALYSIS")
    print("="*50)
    
    print(f"\nDataset shape: {df.shape}")
    print(f"Number of rows: {df.shape[0]}")
    print(f"Number of columns: {df.shape[1]}")
    
    print(f"\nColumn names and types:")
    for col in df.columns:
        print(f"  {col}: {df[col].dtype}")
    
    print(f"\nFirst 5 rows:")
    print(df.head())
    
    print(f"\nBasic statistics:")
    print(df.describe(include='all'))
    
    print(f"\nMissing values:")
    missing_values = df.isnull().sum()
    if missing_values.sum() > 0:
        print(missing_values[missing_values > 0])
    else:
        print("No missing values found")

def main():
    """
    Main function to execute the import and analysis
    """
    print("Starting sentrumssoner data import...")
    
    # Import the data
    df = import_sentrumssoner_data()
    
    if df is not None:
        # Analyze the data
        analyze_data(df)
        
        print(f"\nData successfully imported and ready for further processing!")
        print(f"DataFrame variable 'df' contains the sentrumssoner data")
        
        # Return the DataFrame for interactive use
        return df
    else:
        print("Failed to import data")
        return None

if __name__ == "__main__":
    # Execute main function
    df = main()


# ===================================================================
# MANUAL DATAFRAME MODIFICATIONS
# Run these lines individually in your Jupyter interactive window
# ===================================================================

# Copy the merged DataFrame to a new variable
df_edit = df.copy()

# Print all column names, no shortening
print(df_edit.columns)

# Remove columns "geometry_b", "geometry_1", "geometry_2", "geometry_3", "Shape_Length", "Shape_Area", "ssbid250m"
df_edit = df_edit.drop(columns=['gridx', 'gridy', 'fylke_id', 'fylkesnavn'])

# Convert column "Antall innbyggere" to int
merged_df_edit['Antall innbyggere'] = merged_df_edit['Antall innbyggere'].astype(int)

# Convert column "År" to datetime format (year only)
merged_df_edit['År'] = pd.to_datetime(merged_df_edit['År'], format='%Y')

# Sort df by year
merged_df_edit = merged_df_edit.sort_values(by='År')

# List unique values in "Kommune"
print(f"Unique municipalities: {merged_df_edit['Kommune'].unique()}")

# Filter to keep only rows that have values in center zone columns
center_columns = ['SSB_sentrum2019_intersect', 'SSB_sentrum2019_centroid', 
                 'SSB_sentrum2024_intersect', 'SSB_sentrum2024_centroid',
                 'Hovedsentrum2019_intersect', 'Hovedsentrum2019_centroid',
                 'Hovedsentrum2024_intersect', 'Hovedsentrum2024_centroid']

print(f"Before center zone filter: {len(merged_df_edit)} rows")

# Create a mask for rows that have non-null values in any of the center columns
mask = merged_df_edit[center_columns].notna().any(axis=1)
merged_df_edit = merged_df_edit[mask]

print(f"After center zone filter: {len(merged_df_edit)} rows")
print(f"Unique municipalities: {merged_df_edit['Kommune'].unique()}")
print(f"Filtered out {len(merged_df_edit[~mask])} rows with no center zone values")

output_path = r'c:\Users\eve1509\OneDrive - Telemark fylkeskommune\Github\Telemark\Data\Bystrategi_Grenland\Areal_og_byutvikling\Sentrumssoner\sentrumssoner_med_befolkning.csv'
print(f"Saving modified data to: {output_path}")
merged_df_edit.to_csv(output_path, index=False, sep=';')
print(f"File saved successfully!")
