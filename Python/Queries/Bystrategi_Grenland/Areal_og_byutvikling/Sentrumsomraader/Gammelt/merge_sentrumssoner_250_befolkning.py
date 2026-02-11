"""
Script for merging sentrumssoner data with befolkning data based on ssbid.

This script:
1. Reads sentrumssoner_grenland.csv (list of sentrumssoner)
2. Reads befolkning_i_rutenett.csv (population data for grid cells)
3. Merges them based on the common 'ssbid' column
4. Saves the result as sentrumssoner_med_befolkning.csv

"""

import pandas as pd
import os
import sys

# Add helper scripts to path
sys.path.append(r'c:\Users\eve1509\OneDrive - Telemark fylkeskommune\Github\Telemark\Python\Helper_scripts')
from github_functions import handle_output_data

def merge_sentrumssoner_befolkning():
    """
    Merge sentrumssoner data with befolkning data based on ssbid column.
    """
    
    # Define file paths
    base_path = r'c:\Users\eve1509\OneDrive - Telemark fylkeskommune\Github\Telemark\Data\Bystrategi_Grenland\Areal_og_byutvikling\Sentrumssoner'
    sentrumssoner_file = os.path.join(base_path, 'sentrumssoner_grenland.csv')
    befolkning_file = os.path.join(base_path, 'befolkning_i_rutenett.csv')
    
    print("Starting merge process...")
    print(f"Reading sentrumssoner from: {sentrumssoner_file}")
    print(f"Reading befolkning data from: {befolkning_file}")
    
    try:
        # Read sentrumssoner file (semicolon separated)
        print("Loading sentrumssoner data...")
        sentrumssoner_df = pd.read_csv(sentrumssoner_file, sep=';', dtype={'ssbid': str})
        print(f"Loaded {len(sentrumssoner_df)} sentrumssoner records")
        print(f"Sentrumssoner columns: {list(sentrumssoner_df.columns)}")
        
        # Read befolkning file (semicolon separated)
        print("Loading befolkning data...")
        befolkning_df = pd.read_csv(befolkning_file, sep=';', dtype={'ssbid': str})
        print(f"Loaded {len(befolkning_df)} befolkning records")
        print(f"Befolkning columns: {list(befolkning_df.columns)}")
        
        # Convert ssbid to string format for proper matching
        print("Converting ssbid to string format...")
        
        # Check for missing values before conversion
        print(f"Missing values in sentrumssoner ssbid: {sentrumssoner_df['ssbid'].isna().sum()}")
        print(f"Missing values in befolkning ssbid: {befolkning_df['ssbid'].isna().sum()}")
        
        # Display sample values to understand the data format
        print(f"Sample sentrumssoner ssbids (raw): {sentrumssoner_df['ssbid'].head().tolist()}")
        print(f"Sample befolkning ssbids (raw): {befolkning_df['ssbid'].head().tolist()}")
        
        # Keep ssbid as strings since they're too large for standard integers
        # Handle missing values by dropping them
        sentrumssoner_df = sentrumssoner_df.dropna(subset=['ssbid'])
        sentrumssoner_df['ssbid'] = sentrumssoner_df['ssbid'].astype(str)
        
        # For befolkning: convert to string, handling missing values
        befolkning_df = befolkning_df.dropna(subset=['ssbid'])
        befolkning_df['ssbid'] = befolkning_df['ssbid'].astype(str)
        
        # Remove any '.0' suffix that might come from float conversion
        befolkning_df['ssbid'] = befolkning_df['ssbid'].str.replace('.0', '', regex=False)
        
        print(f"After cleaning - sentrumssoner records: {len(sentrumssoner_df)}")
        print(f"After cleaning - befolkning records: {len(befolkning_df)}")
        print(f"Sample sentrumssoner ssbids after conversion: {sentrumssoner_df['ssbid'].head().tolist()}")
        print(f"Sample befolkning ssbids after conversion: {befolkning_df['ssbid'].head().tolist()}")
        
        # Analyze the ssbid patterns
        print("\n=== SSBID ANALYSIS ===")
        sentrumssoner_ids = set(sentrumssoner_df['ssbid'])
        befolkning_ids = set(befolkning_df['ssbid'])
        
        # Check prefixes (ssbids are now strings)
        sentrumssoner_prefixes = set([id[:3] for id in sentrumssoner_ids])
        befolkning_prefixes = set([id[:3] for id in befolkning_ids])
        
        print(f"Sentrumssoner ssbid prefixes: {sorted(sentrumssoner_prefixes)}")
        print(f"Befolkning ssbid prefixes: {sorted(befolkning_prefixes)}")
        print(f"Sentrumssoner ssbid range: {min(sentrumssoner_ids)} to {max(sentrumssoner_ids)}")
        print(f"Befolkning ssbid range: {min(befolkning_ids)} to {max(befolkning_ids)}")
        
        # Check for any overlapping ssbid values
        common_ids = sentrumssoner_ids.intersection(befolkning_ids)
        print(f"Number of common ssbid values: {len(common_ids)}")
        
        if len(common_ids) == 0:
            print("\n❌ WARNING: No matching ssbid values found between the two datasets!")
            print("This suggests the datasets use different coordinate systems or geographic areas.")
            print("\nPossible solutions:")
            print("1. Check if there's a coordinate transformation needed")
            print("2. Verify that both datasets cover the same geographic area")
            print("3. Look for alternative joining keys (coordinates, area names, etc.)")
            print("4. Check if one dataset needs to be filtered to match the other's area")
            return None
        else:
            print(f"✅ Found {len(common_ids)} matching ssbid values!")
            if len(common_ids) < 10:
                print(f"Common ssbids: {sorted(list(common_ids))}")
        
        # Check if ssbid column exists in both files
        if 'ssbid' not in sentrumssoner_df.columns:
            raise ValueError("Column 'ssbid' not found in sentrumssoner file")
        if 'ssbid' not in befolkning_df.columns:
            raise ValueError("Column 'ssbid' not found in befolkning file")
        
        # Perform inner join to get only matching records
        print("Merging data based on ssbid...")
        merged_df = pd.merge(
            sentrumssoner_df, 
            befolkning_df, 
            on='ssbid', 
            how='inner',
            suffixes=('_sentrumssoner', '_befolkning')
        )
        
        print(f"Merged dataset contains {len(merged_df)} records")
        print(f"Final columns: {list(merged_df.columns)}")
        
        # Convert numeric columns to float64 for github_functions compatibility
        numeric_columns = merged_df.select_dtypes(include=['int32', 'int64', 'float32']).columns
        for col in numeric_columns:
            merged_df[col] = merged_df[col].astype('float64')
        
        # Check for matches before merging - debugging
        print(f"\nDebugging - checking for matching ssbid values...")
        sentrumssoner_ssbids = set(sentrumssoner_df['ssbid'].unique())
        befolkning_ssbids = set(befolkning_df['ssbid'].unique())
        
        print(f"Unique ssbids in sentrumssoner: {len(sentrumssoner_ssbids)}")
        print(f"Unique ssbids in befolkning: {len(befolkning_ssbids)}")
        print(f"Sample sentrumssoner ssbids: {list(sentrumssoner_ssbids)[:5]}")
        print(f"Sample befolkning ssbids: {list(befolkning_ssbids)[:5]}")
        
        common_ssbids = sentrumssoner_ssbids.intersection(befolkning_ssbids)
        print(f"Common ssbids found: {len(common_ssbids)}")
        
        if len(merged_df) == 0:
            print("WARNING: No matching records found!")
            print("This might be due to different ssbid formats between the files.")
            return None
        
        # Work with the merged DataFrame
        print("\n=== WORKING WITH MERGED DATAFRAME ===")
        print(f"DataFrame shape: {merged_df.shape}")
        print(f"DataFrame columns: {list(merged_df.columns)}")
        print(f"DataFrame data types:")
        print(merged_df.dtypes)
        
        # Display basic statistics
        print(f"\n=== BASIC STATISTICS ===")
        print(f"Total records: {len(merged_df):,}")
        print(f"Unique ssbid values: {merged_df['ssbid'].nunique():,}")
        print(f"Year range: {merged_df['År'].min():.0f} - {merged_df['År'].max():.0f}")
        print(f"Population range: {merged_df['pop_tot'].min():.0f} - {merged_df['pop_tot'].max():.0f}")
        print(f"Total population across all years: {merged_df['pop_tot'].sum():,.0f}")
        
        # Show municipalities
        if 'Kommune' in merged_df.columns:
            print(f"Municipalities: {sorted(merged_df['Kommune'].unique())}")
            print(f"Records per municipality:")
            print(merged_df['Kommune'].value_counts())
        
        # Show population by year
        print(f"\nPopulation by year:")
        yearly_pop = merged_df.groupby('År')['pop_tot'].sum().sort_index()
        print(yearly_pop)
        
        # Display sample of merged data
        print("\n=== SAMPLE DATA ===")
        print(merged_df.head(10))
        
        # Check for missing values
        print(f"\n=== MISSING VALUES CHECK ===")
        missing_counts = merged_df.isnull().sum()
        missing_counts = missing_counts[missing_counts > 0]
        if len(missing_counts) > 0:
            print("Missing values found:")
            print(missing_counts)
        else:
            print("No missing values found in the merged DataFrame")
        
        # The DataFrame is now ready for further analysis or saving
        print(f"\n✅ Merged DataFrame is ready for use!")
        print(f"Function will return the DataFrame for manual modifications.")
        
        return merged_df
        
    except FileNotFoundError as e:
        print(f"Error: File not found - {e}")
        return None
    except Exception as e:
        print(f"Error during merge process: {e}")
        return None

if __name__ == "__main__":
    # Run the merge function
    merged_df = merge_sentrumssoner_befolkning()
    
    if merged_df is not None:
        print("Merge completed successfully!")
        print(f"DataFrame available as 'merged_df' with shape: {merged_df.shape}")
    else:
        print("Merge failed - please check error messages above")

# ===================================================================
# MANUAL DATAFRAME MODIFICATIONS
# Run these lines individually in your Jupyter interactive window
# ===================================================================

# Copy the merged DataFrame to a new variable
merged_df_edit = merged_df.copy()

# Remove columns "geometry_b", "geometry_1", "geometry_2", "geometry_3", "Shape_Length", "Shape_Area", "ssbid250m"
merged_df_edit = merged_df_edit.drop(columns=['geometry_b', 'geometry_1', 'geometry_2', 'geometry_3', 'Shape_Length', 'Shape_Area', 'ssbid250m'])

# Rename columns "pop_tot" to "Antall innbyggere"
merged_df_edit = merged_df_edit.rename(columns={'pop_tot': 'Antall innbyggere'})

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
