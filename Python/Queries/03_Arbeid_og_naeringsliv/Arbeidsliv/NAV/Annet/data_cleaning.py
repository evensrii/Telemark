import pandas as pd
import glob
import os

# Define the data directory
data_dir = "2010-2024"

# Define string replacements
replacements = {
    "I alt": "Landet",
    "Trøndelag - Trööndelage": "Trøndelag",
    "Troms og Finnmark - Romsa ja Finnmárku": "Troms og Finnmark",
    "Riket": "Landet",
    "Ikke i et fylkesdistrikt": "Øvrige områder",
    "Nordland - Nordlánnda" : "Nordland",
    "Troms - Romsa - Tromssa " : "Troms",
    "Finnmark - Finnmárku - Finmarkku" : "Finnmark"
    }

# Define month order
month_order = {
    'Januar': 1,
    'Februar': 2,
    'Mars': 3,
    'April': 4,
    'Mai': 5,
    'Juni': 6,
    'Juli': 7,
    'August': 8,
    'September': 9,
    'Oktober': 10,
    'November': 11,
    'Desember': 12
}

def clean_dataframe(df):
    """Clean a single dataframe according to requirements"""
    # Make a copy to avoid modifying the original
    df = df.copy()
    
    # 1. Remove rows where Fylke column contains month and year pattern
    # or starts with the NAV note
    df = df[
        (~df['Fylke'].str.contains(r'^[A-Za-zæøåÆØÅ]+ \d{4}$', regex=True, na=False)) &
        (~df['Fylke'].str.startswith('1) NAV har endret variabelen for fordeling på fylke', na=False))
    ]
    
    # 2. Round numeric columns to 1 decimal
    numeric_columns = [
        'Antall',
        'Prosent av arbeidsstyrken',
        'Endring fra i fjor (antall)',
        'Endring fra i fjor (prosent)',
        'Prosent av arbeidsstyrken i fjor'
    ]
    
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            df[col] = df[col].round(1)
    
    # 3. Capitalize month names
    if 'Måned' in df.columns:
        df['Måned'] = df['Måned'].str.capitalize()
    
    # 4. Replace strings according to mapping
    df['Fylke'] = df['Fylke'].replace(replacements)
    
    return df

# Get all CSV files in the directory
csv_files = glob.glob(os.path.join(data_dir, "arbeidsledighet_*.csv"))

if not csv_files:
    raise ValueError(f"No CSV files found in {data_dir}")

print(f"Found {len(csv_files)} CSV files")

# Read and clean all dataframes
all_dfs = []
for file in csv_files:
    print(f"Processing {file}")
    try:
        df = pd.read_csv(file)
        cleaned_df = clean_dataframe(df)
        all_dfs.append(cleaned_df)
    except Exception as e:
        print(f"Error processing {file}: {e}")
        continue

# Combine all dataframes
if not all_dfs:
    raise ValueError("No dataframes were successfully processed")

combined_df = pd.concat(all_dfs, ignore_index=True)

# Ensure all required columns exist
required_columns = [
    'Fylke',
    'Antall',
    'Prosent av arbeidsstyrken',
    'Endring fra i fjor (antall)',
    'Endring fra i fjor (prosent)',
    'Prosent av arbeidsstyrken i fjor',
    'År',
    'Måned'
]

for col in required_columns:
    if col not in combined_df.columns:
        print(f"Adding missing column: {col}")
        combined_df[col] = ''

# Create a month number column for sorting
combined_df['month_num'] = combined_df['Måned'].map(month_order)

# Sort by year, month number, and fylke
combined_df = combined_df.sort_values(['År', 'month_num', 'Fylke'])

# Drop the temporary month number column
combined_df = combined_df.drop('month_num', axis=1)

# Export to CSV
output_file = "arbeidsledighet_2010_2024.csv"
combined_df.to_csv(output_file, index=False)
print(f"\nExported combined data to {output_file}")

# Print some statistics
print("\nDataset statistics:")
print(f"Total rows: {len(combined_df)}")
print(f"Years covered: {combined_df['År'].unique()}")
print(f"Number of fylker: {combined_df['Fylke'].nunique()}")
print("\nSample of the data:")
print(combined_df.head())