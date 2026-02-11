"""
Script to combine _Clip.csv files into a single long format CSV file
with location information in an 'Område' column.
"""

import pandas as pd
import glob
import os
import re

# Get the directory where this script is located
script_dir = os.path.dirname(os.path.abspath(__file__))

# Find all _Clip.csv files in the current directory
csv_files = glob.glob(os.path.join(script_dir, "*_Clip.csv"))

if not csv_files:
    print("No _Clip.csv files found in the directory.")
else:
    print(f"Found {len(csv_files)} _Clip.csv files:")
    for file in csv_files:
        print(f"  - {os.path.basename(file)}")

# Initialize list to store dataframes
combined_data = []

# Process each file
for file_path in csv_files:
    try:
        # Extract location name from filename
        filename = os.path.basename(file_path)
        # Pattern: Gronnstruktur_[Location]_Clip.csv
        match = re.search(r'Gronnstruktur_(.+?)_Clip\.csv', filename)
        if match:
            location = match.group(1)
        else:
            # Fallback: use the part before _Clip.csv
            location = filename.replace('_Clip.csv', '').split('_')[-1]
        
        print(f"\nProcessing {filename} (Location: {location})")
        
        # Read the CSV file with semicolon separator
        df = pd.read_csv(file_path, sep=';')
        
        # Standardize column names - handle SUM_Tre_area/SUM_Tre_Area inconsistency
        df.columns = df.columns.str.replace('SUM_Tre_Area', 'SUM_Tre_area')
        
        # Add the 'Område' column
        df['Område'] = location
        
        # Remove the first column if it contains the location name
        # (since we now have it in the 'Område' column)
        if df.columns[0] == location or df.iloc[0, 0] == location:
            df = df.drop(df.columns[0], axis=1)
        
        # Convert numeric columns to float64 for consistency
        numeric_columns = ['FREQUENCY', 'SUM_Bunn_area', 'SUM_Busk_area', 'SUM_Tre_area', 'SUM_Shape_Area']
        for col in numeric_columns:
            if col in df.columns:
                # Handle comma as decimal separator
                if df[col].dtype == 'object':
                    df[col] = df[col].astype(str).str.replace(',', '.').astype(float)
                df[col] = df[col].astype('float64')
        
        combined_data.append(df)
        print(f"  Added {len(df)} rows from {location}")
        
    except Exception as e:
        print(f"Error processing {file_path}: {str(e)}")
        continue

# Check if we have data to work with
if not combined_data:
    print("No data was successfully processed.")
else:
    # Combine all dataframes
    final_df = pd.concat(combined_data, ignore_index=True)
    
    # =================================================================
    # MANUAL MODIFICATION SECTION - You can inspect and modify final_df here
    # =================================================================
    
    # Inspect the data
    print(f"\n📊 Data Summary:")
    print(f"Total rows: {len(final_df)}")
    print(f"Columns: {list(final_df.columns)}")
    print(f"Areas included: {sorted(final_df['Område'].unique())}")
    
    # Display first few rows
    print(f"\nFirst few rows:")
    print(final_df.head())
    
    # Display data types
    print(f"\nData types:")
    print(final_df.dtypes)
    
    # Data transformations
    
    # Delete the FREQUENCY column
    if 'FREQUENCY' in final_df.columns:
        final_df = final_df.drop('FREQUENCY', axis=1)
        print("Deleted FREQUENCY column")
    
    # Rename columns
    column_mapping = {
        'SUM_Bunn_area': 'Kvm_bunnsjikt',
        'SUM_Busk_area': 'Kvm_busksjikt', 
        'SUM_Tre_area': 'Kvm_tresjikt',
        'SUM_Shape_Area': 'Kvm_tettsted'
    }
    final_df = final_df.rename(columns=column_mapping)
    print(f"Renamed columns: {column_mapping}")
    
    # Calculate proportions (andel columns)
    final_df['Andel_bunnsjikt'] = final_df['Kvm_bunnsjikt'] / final_df['Kvm_tettsted']
    final_df['Andel_busksjikt'] = final_df['Kvm_busksjikt'] / final_df['Kvm_tettsted']
    final_df['Andel_tresjikt'] = final_df['Kvm_tresjikt'] / final_df['Kvm_tettsted']
    print("Calculated proportion columns")
    
    # Transform to long format
    # First, create separate dataframes for Kvm and Andel data
    kvm_data = final_df[['Område', 'Kvm_bunnsjikt', 'Kvm_busksjikt', 'Kvm_tresjikt']].melt(
        id_vars=['Område'], 
        value_vars=['Kvm_bunnsjikt', 'Kvm_busksjikt', 'Kvm_tresjikt'],
        var_name='Kategori_temp', 
        value_name='Kvm'
    )
    
    andel_data = final_df[['Område', 'Andel_bunnsjikt', 'Andel_busksjikt', 'Andel_tresjikt']].melt(
        id_vars=['Område'], 
        value_vars=['Andel_bunnsjikt', 'Andel_busksjikt', 'Andel_tresjikt'],
        var_name='Kategori_temp', 
        value_name='Andel'
    )
    
    # Clean up category names
    kvm_data['Kategori'] = kvm_data['Kategori_temp'].str.replace('Kvm_', '').str.replace('sjikt', 'sjikt').str.title()
    andel_data['Kategori'] = andel_data['Kategori_temp'].str.replace('Andel_', '').str.replace('sjikt', 'sjikt').str.title()
    
    # Merge the two datasets
    final_df = pd.merge(kvm_data[['Område', 'Kategori', 'Kvm']], 
                       andel_data[['Område', 'Kategori', 'Andel']], 
                       on=['Område', 'Kategori'])
    
    print("Transformed to long format")
    print(f"New shape: {final_df.shape}")
    print(f"Categories: {sorted(final_df['Kategori'].unique())}")
    
    # =================================================================
    # END MANUAL MODIFICATION SECTION
    # =================================================================
    
    # Reorder columns to put 'Område' first
    cols = ['Område'] + [col for col in final_df.columns if col != 'Område']
    final_df = final_df[cols]
    
    # Save the combined data
    output_file = os.path.join(script_dir, 'gronnstruktur_kombinert.csv')
    final_df.to_csv(output_file, sep=';', index=False)
    
    print(f"\n✅ Successfully combined {len(csv_files)} files into '{os.path.basename(output_file)}'")
    print(f"Final data shape: {final_df.shape}")
    
    # Final display
    print(f"\nFinal combined data:")
    print(final_df.head())