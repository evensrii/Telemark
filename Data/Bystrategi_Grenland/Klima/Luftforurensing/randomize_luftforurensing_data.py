import pandas as pd
import numpy as np
import os

def randomize_luftforurensing_data():
    """
    Randomize numeric values in the luftforurensing CSV file while preserving
    Fra-tid, Til-tid, and Datadekning (%) columns.
    """
    
    # Define input and output file paths
    input_file = "Luftforurensning Grenland PM10 og NO2 Døgn Juni_dummy.csv"
    output_file = "Luftforurensning Grenland PM10 og NO2 Døgn Juni_dummy_randomized.csv"
    
    print(f"Reading data from: {input_file}")
    
    # Read the CSV file with semicolon separator
    df = pd.read_csv(input_file, sep=';', encoding='utf-8-sig')
    
    print(f"Original data shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")
    
    # Identify columns to preserve (time columns and Datadekning columns)
    # Note: pandas renames duplicate columns, so we need to check for variations
    preserve_columns = ['Fra-tid', 'Til-tid']
    preserve_columns.extend([col for col in df.columns if 'Datadekning (%)' in col])
    
    print(f"Columns to preserve: {preserve_columns}")
    
    # Identify columns to randomize (all other columns that contain numeric data)
    columns_to_randomize = []
    for col in df.columns:
        if col not in preserve_columns:
            # Check if the column contains numeric data (PM10 or NO2 measurements)
            if any(keyword in col for keyword in ['PM10', 'NO2']) and 'µg/m³' in col:
                columns_to_randomize.append(col)
    
    print(f"Columns to randomize: {columns_to_randomize}")
    
    # Create a copy of the dataframe for modification
    df_randomized = df.copy()
    
    # Set random seed for reproducibility (optional)
    np.random.seed(42)
    
    # Randomize each numeric column
    for col in columns_to_randomize:
        print(f"\nProcessing column: {col}")
        print(f"  Original dtype: {df[col].dtype}")
        print(f"  Sample original values: {df[col].head(3).tolist()}")
        
        # Convert comma-separated decimals to proper floats
        # Handle the European decimal format (comma instead of dot)
        if df[col].dtype == 'object':  # String column
            # Replace comma with dot for decimal conversion
            df_randomized[col] = df[col].astype(str).str.replace(',', '.', regex=False)
            df_randomized[col] = pd.to_numeric(df_randomized[col], errors='coerce')
        else:
            df_randomized[col] = pd.to_numeric(df[col], errors='coerce')
        
        print(f"  After conversion dtype: {df_randomized[col].dtype}")
        print(f"  Sample converted values: {df_randomized[col].head(3).tolist()}")
        
        # Get the original values (excluding NaN)
        original_values = df_randomized[col].dropna().values
        
        if len(original_values) > 0:
            # Calculate statistics for realistic randomization
            min_val = float(original_values.min())
            max_val = float(original_values.max())
            mean_val = float(original_values.mean())
            std_val = float(original_values.std())
            
            print(f"Randomizing {col}: min={min_val:.3f}, max={max_val:.3f}, mean={mean_val:.3f}, std={std_val:.3f}")
            
            # Generate random values with similar distribution
            # Use normal distribution centered around the mean with similar std
            random_values = np.random.normal(mean_val, std_val, len(df))
            
            # Clip values to stay within reasonable bounds (min to max range)
            random_values = np.clip(random_values, min_val, max_val)
            
            # Apply randomization only to non-NaN values
            mask = df[col].notna()
            df_randomized.loc[mask, col] = random_values[mask]
            
            print(f"  New range: {random_values[mask].min():.3f} to {random_values[mask].max():.3f}")
            
            # Show a sample of original vs randomized values
            print(f"  Sample original: {original_values[:3]}")
            print(f"  Sample randomized: {random_values[mask][:3]}")
    
    # Convert back to string format with comma as decimal separator for output
    for col in columns_to_randomize:
        if df_randomized[col].dtype in ['float64', 'float32']:
            # Format to match original precision and use comma as decimal separator
            df_randomized[col] = df_randomized[col].round(8).astype(str).str.replace('.', ',', regex=False)
    
    # Save the randomized data
    df_randomized.to_csv(output_file, sep=';', index=False, encoding='utf-8-sig')
    
    print(f"\nRandomized data saved to: {output_file}")
    print(f"Output data shape: {df_randomized.shape}")
    
    # Display first few rows for verification
    print("\nFirst 5 rows of randomized data:")
    print(df_randomized.head())
    
    return df_randomized

if __name__ == "__main__":
    # Set working directory to script location
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)
    
    # Run the randomization
    randomized_df = randomize_luftforurensing_data()
