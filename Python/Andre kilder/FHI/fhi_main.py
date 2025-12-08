import json
import os
import sys
import pandas as pd
from pathlib import Path

# Get PYTHONPATH and add to sys.path
pythonpath = os.environ.get("PYTHONPATH")
if not pythonpath:
    # Fallback if PYTHONPATH not in environment
    pythonpath = str(Path(__file__).parent.parent.parent)
    os.environ["PYTHONPATH"] = pythonpath  # Set in environment for other modules
    
sys.path.append(pythonpath)

from Helper_scripts.utility_functions import fetch_data
from Helper_scripts.github_functions import handle_output_data


def get_data_from_fhi(url, query, error_messages):
    """
    Fetch data from FHI API using the utility function
    """
    # Keep the query as-is (don't modify it)
    df = fetch_data(
        url=url,
        payload=query,
        error_messages=error_messages,
        query_name="FHI Query",
        response_type="json"
    )
    return df


def load_query_file(file_path):
    """
    Load URL and query from a query file
    Format:
    - Line 1: API endpoint URL
    - Line 2: JSON query
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    # Parse the two-line format
    url = lines[0].strip()
    query_lines = [line.strip() for line in lines[1:] if line.strip()]
    query = json.loads('\n'.join(query_lines))
    
    return url, query


def process_query_file(query_file_path, output_dir, error_messages):
    """
    Process a single query file and save CSV output
    """
    try:
        print(f"\nProcessing {Path(query_file_path).name}...")
        
        # Load URL and query
        url, query = load_query_file(query_file_path)
        
        # Get data from FHI API
        df = get_data_from_fhi(url, query, error_messages)
        
        if df is None or df.empty:
            print(f"No data returned for {query_file_path}")
            return False
        
        # Generate output filename using the txt filename (without extension)
        output_name = Path(query_file_path).stem  # Gets filename without extension
        output_filename = f"{output_name}.csv"
        output_path = os.path.join(output_dir, output_filename)
        
        # Save to output directory
        df.to_csv(output_path, index=False, encoding='utf-8')
        print(f"✓ Saved {output_filename} with {len(df)} rows and {len(df.columns)} columns")
        print(f"  Columns: {', '.join(df.columns.tolist())}")
        
        return True
        
    except Exception as e:
        print(f"✗ Error processing {Path(query_file_path).name}: {e}")
        error_messages.append(str(e))
        return False


def main():
    """
    Main function to process all query files
    """
    # Set up paths
    script_dir = os.path.dirname(__file__)
    queries_dir = os.path.join(script_dir, 'queries')
    output_dir = os.path.join(script_dir, 'output')
    
    # Error tracking
    error_messages = []
    
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Find all query files (.txt files)
    query_files = []
    for file in os.listdir(queries_dir):
        if file.endswith('.txt'):
            query_files.append(os.path.join(queries_dir, file))
    
    if not query_files:
        print("No query files found in queries directory")
        return
    
    print(f"\n{'='*60}")
    print(f"FHI Data Extraction")
    print(f"{'='*60}")
    print(f"Found {len(query_files)} query files to process\n")
    
    # Process each query file
    results = []
    for query_file in sorted(query_files):
        result = process_query_file(query_file, output_dir, error_messages)
        results.append((query_file, result))
    
    # Summary
    successful = sum(1 for _, result in results if result)
    print(f"\n{'='*60}")
    print(f"Processing complete: {successful}/{len(query_files)} files successful")
    print(f"{'='*60}")
    
    for query_file, success in results:
        status = "✓" if success else "✗"
        print(f"{status} {os.path.basename(query_file)}")
    
    if error_messages:
        print(f"\nErrors encountered:")
        for error in error_messages:
            print(f"  - {error}")


# Run the main function
main()
