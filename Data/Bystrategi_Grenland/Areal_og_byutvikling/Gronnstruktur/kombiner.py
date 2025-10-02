"""
Script to combine _Clip.csv files into a single long format CSV file
with location information in an 'Område' column.
"""

import pandas as pd
import glob
import os
import re

def main():
    # Get the directory where this script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Find all _Clip.csv files in the current directory
    csv_files = glob.glob(os.path.join(script_dir, "*_Clip.csv"))
    
    if not csv_files:
        print("No _Clip.csv files found in the directory.")
        return
    
    print(f"Found {len(csv_files)} _Clip.csv files:")
    for file in csv_files:
        print(f"  - {os.path.basename(file)}")
    
    combined_data = []
    
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
    
    if not combined_data:
        print("No data was successfully processed.")
        return
    
    # Combine all dataframes
    final_df = pd.concat(combined_data, ignore_index=True)
    
    # Reorder columns to put 'Område' first
    cols = ['Område'] + [col for col in final_df.columns if col != 'Område']
    final_df = final_df[cols]
    
    # Save the combined data
    output_file = os.path.join(script_dir, 'gronnstruktur_kombinert.csv')
    final_df.to_csv(output_file, sep=';', index=False)
    
    print(f"\n✅ Successfully combined {len(csv_files)} files into '{os.path.basename(output_file)}'")
    print(f"Total rows: {len(final_df)}")
    print(f"Columns: {list(final_df.columns)}")
    print(f"Areas included: {sorted(final_df['Område'].unique())}")
    
    # Display first few rows
    print(f"\nFirst few rows of combined data:")
    print(final_df.head())

if __name__ == "__main__":
    main()