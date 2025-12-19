"""
FHI Script Generator
====================
Automatically generates individual Python scripts for each FHI query file (.txt).

This script:
1. Scans FHI/queries/ for .txt files
2. Generates corresponding .py scripts in FHI/scripts/ (if they don't exist)
3. Updates master_script.py with new script entries
4. Maintains folder structure across queries/scripts/output

Usage:
    Run manually when new .txt query files are added by the health department.
    Should be run BEFORE master_script.py.

Author: Auto-generated
Date: 2025-12-19
"""

import os
import sys
import re
from pathlib import Path
from datetime import datetime

# Get PYTHONPATH
pythonpath = os.environ.get("PYTHONPATH")
if not pythonpath:
    raise ValueError("PYTHONPATH environment variable is not set")

# Paths
FHI_BASE = os.path.join(pythonpath, "Queries", "08_Folkehelse_og_levekår", "FHI")
QUERIES_DIR = os.path.join(FHI_BASE, "queries")
SCRIPTS_DIR = os.path.join(FHI_BASE, "scripts")
MASTER_SCRIPT = os.path.join(pythonpath, "Automatisering", "Task scheduler", "master_script.py")
DATA_BASE = os.path.join(pythonpath, "Data", "08_Folkehelse og levekår")


def sanitize_filename(filename):
    """
    Sanitize filename by removing special characters and replacing spaces.
    
    Args:
        filename: Original filename (without extension)
    
    Returns:
        Sanitized filename suitable for Python script names
    
    Examples:
        "Hjerte- og karsykdom, ettårige tall" -> "hjerte_og_karsykdom_ettaarige_tall"
        "Dødsårsaker, ettårig" -> "dodsaarsaker_ettaarig"
    """
    # Convert to lowercase
    name = filename.lower()
    
    # Replace Norwegian characters
    replacements = {
        'å': 'aa',
        'æ': 'ae',
        'ø': 'oe',
        'é': 'e',
        'è': 'e',
        'ê': 'e',
        'ü': 'u',
        'ö': 'o',
        'ä': 'a'
    }
    
    for old, new in replacements.items():
        name = name.replace(old, new)
    
    # Remove special characters (keep only alphanumeric and spaces)
    name = re.sub(r'[^a-z0-9\s]', '', name)
    
    # Replace multiple spaces with single space
    name = re.sub(r'\s+', ' ', name)
    
    # Replace spaces with underscores
    name = name.replace(' ', '_')
    
    # Remove leading/trailing underscores
    name = name.strip('_')
    
    return name


def generate_script_template(query_file_path, script_file_path, queries_base_dir):
    """
    Generate a Python script template for a query file.
    
    Args:
        query_file_path: Full path to the .txt query file
        script_file_path: Full path where the .py script should be created
        queries_base_dir: Base directory for queries
    
    Returns:
        String containing the complete Python script
    """
    # Get relative paths for documentation
    rel_query_path = os.path.relpath(query_file_path, queries_base_dir)
    query_filename = Path(query_file_path).name
    script_filename = Path(script_file_path).stem
    
    # Determine subfolder structure
    query_dir = os.path.dirname(query_file_path)
    relative_subfolder = os.path.relpath(query_dir, queries_base_dir)
    
    # GitHub folder path
    if relative_subfolder == '.':
        github_folder = "Data/08_Folkehelse og levekår"
        data_subfolder = ""
    else:
        github_folder = f"Data/08_Folkehelse og levekår/{relative_subfolder}"
        data_subfolder = relative_subfolder
    
    # Output filename (same as script name)
    output_filename = f"{script_filename}.csv"
    
    # Use raw string for docstring to avoid escape sequence warnings
    rel_query_path_escaped = rel_query_path.replace('\\', '/')
    
    template = f'''"""
FHI Query Script: {query_filename}
{'=' * (18 + len(query_filename))}

Auto-generated script for processing FHI query data.
Query file: {rel_query_path_escaped}

This script:
1. Loads query from .txt file
2. Fetches data from FHI API
3. Processes data (EDITABLE SECTION - outside main() for Jupyter interactive use)
4. Compares with GitHub and uploads if changed
5. Saves to CSV output

Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""

import json
import os
import sys
import pandas as pd
from pathlib import Path

# Get PYTHONPATH and add to sys.path
pythonpath = os.environ.get("PYTHONPATH")
if not pythonpath:
    pythonpath = str(Path(__file__).parent.parent.parent.parent.parent)
    os.environ["PYTHONPATH"] = pythonpath

sys.path.append(pythonpath)

from Helper_scripts.utility_functions import fetch_data
from Helper_scripts.github_functions import handle_output_data

# Capture the name of the current script
script_name = os.path.basename(__file__)

# Paths
query_file = os.path.join(
    pythonpath, 
    "Queries", 
    "08_Folkehelse_og_levekår", 
    "FHI", 
    "queries",
    "{data_subfolder}",
    "{query_filename}"
)

# Output configuration
output_filename = "{output_filename}"
github_folder = "{github_folder}"

# Get temp folder
temp_folder = os.environ.get("TEMP_FOLDER")
if not temp_folder:
    temp_folder = os.path.join(pythonpath, "Temp")


def load_query_file(file_path):
    """
    Load URL and query from the query file.
    
    Returns:
        tuple: (url, query_dict)
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    url = lines[0].strip()
    query_lines = [line.strip() for line in lines[1:] if line.strip()]
    query = json.loads(''.join(query_lines))
    
    return url, query


# %%
print(f"\\n{{'=' * 70}}")
print(f"FHI Query: {query_filename}")
print(f"{{'=' * 70}}\\n")

# Load query from file
print("Loading query from file...")
url, query = load_query_file(query_file)
print(f"  ✓ Query loaded")
print(f"  URL: {{url}}")

# %%
# Fetch data from FHI API
print("\\nFetching data from FHI API...")
error_messages = []
df = fetch_data(
    url=url,
    payload=query,
    error_messages=error_messages,
    query_name="FHI Query",
    response_type="json"
)

if df is None or df.empty:
    print("  ✗ No data returned from API")
    if error_messages:
        for error in error_messages:
            print(f"    Error: {{error}}")
    sys.exit(1)

print(f"  ✓ Fetched {{len(df)}} rows and {{len(df.columns)}} columns")
print(f"  Columns: {{', '.join(df.columns.tolist())}}")

# %%
####################################################################
### EDITABLE SECTION START                                       ###
### Add your data transformations and processing here            ###
####################################################################

# Example transformations (uncomment and modify as needed):
# df = df[df['value'] > 0]  # Filter rows
# df['new_column'] = df['old_column'] * 2  # Create new column
# df = df.rename(columns={{'old_name': 'new_name'}})  # Rename columns
# df = df.sort_values('column_name')  # Sort data

####################################################################
### EDITABLE SECTION END                                         ###
####################################################################

print(f"\\nAfter processing: {{len(df)}} rows and {{len(df.columns)}} columns")

# %%
# Compare with GitHub and upload if changed
print("\\nComparing with GitHub...")
has_changes = handle_output_data(
    df=df,
    file_name=output_filename,
    github_folder=github_folder,
    temp_folder=temp_folder,
    keepcsv=True
)

if has_changes:
    print("  ✓ New data detected and uploaded to GitHub")
    print("New data detected")  # For master_script.py parsing
else:
    print("  ✓ No changes detected")

# Save to local output directory
output_dir = os.path.join(pythonpath, "Data", "08_Folkehelse og levekår"{', "' + data_subfolder + '"' if data_subfolder else ''})
os.makedirs(output_dir, exist_ok=True)
output_path = os.path.join(output_dir, output_filename)
df.to_csv(output_path, index=False, encoding='utf-8')
print(f"\\n  ✓ Saved to: {{output_path}}")

print(f"\\n{{'=' * 70}}")
print("Processing complete")
print(f"{{'=' * 70}}\\n")
'''
    
    return template


def find_all_query_files(queries_dir):
    """
    Recursively find all .txt query files.
    
    Returns:
        List of tuples: (full_path, relative_path, subfolder)
    """
    query_files = []
    for root, dirs, files in os.walk(queries_dir):
        for file in files:
            if file.endswith('.txt'):
                full_path = os.path.join(root, file)
                rel_path = os.path.relpath(full_path, queries_dir)
                subfolder = os.path.relpath(root, queries_dir)
                if subfolder == '.':
                    subfolder = ''
                query_files.append((full_path, rel_path, subfolder))
    
    return sorted(query_files)


def sync_master_script(all_script_entries):
    """
    Synchronize master_script.py with actual FHI scripts.
    - Removes entries for deleted scripts
    - Adds entries for new/missing scripts
    - Maintains correct order
    
    Args:
        all_script_entries: List of tuples (script_path, task_name) for ALL existing scripts
    """
    # Read current master_script.py
    with open(MASTER_SCRIPT, 'r', encoding='utf-8') as f:
        content = f.read()
    
    lines = content.split('\n')
    
    # Find the FHI section
    fhi_section_marker = "## Folkehelse og levekår - FHI"
    fhi_section_start = None
    fhi_section_end = None
    
    for i, line in enumerate(lines):
        if fhi_section_marker in line:
            fhi_section_start = i
            # Find end of FHI section (next ## or end of SCRIPTS list)
            for j in range(i + 1, len(lines)):
                if lines[j].strip().startswith('##') or lines[j].strip() == ']':
                    fhi_section_end = j
                    break
            break
    
    # Extract existing FHI entries
    existing_fhi_paths = set()
    if fhi_section_start is not None and fhi_section_end is not None:
        for i in range(fhi_section_start + 1, fhi_section_end):
            line = lines[i].strip()
            if 'FHI/scripts/' in line:
                # Extract the script path from the line
                match = line.split('"')[1] if '"' in line else None
                if match and 'FHI/scripts/' in match:
                    existing_fhi_paths.add(match)
    
    # Compare with actual scripts
    actual_script_paths = {path for path, _ in all_script_entries}
    
    # Find differences
    to_add = actual_script_paths - existing_fhi_paths
    to_remove = existing_fhi_paths - actual_script_paths
    
    # Report changes
    if to_remove:
        print(f"\n⚠ Removing {len(to_remove)} deleted scripts from master_script.py:")
        for path in sorted(to_remove):
            script_name = os.path.basename(path)
            print(f"  - {script_name}")
    
    if to_add:
        print(f"\n+ Adding {len(to_add)} missing scripts to master_script.py:")
        for path in sorted(to_add):
            script_name = os.path.basename(path)
            print(f"  + {script_name}")
    
    if not to_add and not to_remove:
        print("\n✓ master_script.py is already in sync with FHI scripts")
        return
    
    # Rebuild FHI section
    if fhi_section_start is None:
        # FHI section doesn't exist, create it
        insert_after = '## Idrett, friluftsliv og frivillighet (husk, ingen komma i oppgavenavn)'
        insert_index = None
        
        for i, line in enumerate(lines):
            if insert_after in line:
                # Find next section or closing bracket
                for j in range(i + 1, len(lines)):
                    if lines[j].strip().startswith('##') or lines[j].strip() == ']':
                        insert_index = j
                        break
                break
        
        if insert_index:
            new_section = [
                f"    {fhi_section_marker}",
            ]
            
            for script_path, task_name in sorted(all_script_entries, key=lambda x: x[0]):
                new_section.append(f'    (os.path.join(PYTHON_PATH, "{script_path}"), "{task_name}"),')
            
            # Add blank line after section
            new_section.append("")
            
            lines = lines[:insert_index] + new_section + lines[insert_index:]
    else:
        # Replace existing FHI section
        new_fhi_lines = [f"    {fhi_section_marker}"]
        
        for script_path, task_name in sorted(all_script_entries, key=lambda x: x[0]):
            new_fhi_lines.append(f'    (os.path.join(PYTHON_PATH, "{script_path}"), "{task_name}"),')
        
        # Add blank line after section
        new_fhi_lines.append("")
        
        # Replace the section
        lines = lines[:fhi_section_start] + new_fhi_lines + lines[fhi_section_end:]
    
    # Write updated content
    content = '\n'.join(lines)
    with open(MASTER_SCRIPT, 'w', encoding='utf-8') as f:
        f.write(content)
    
    print(f"\n✓ Synchronized master_script.py with {len(all_script_entries)} FHI scripts")


def find_orphaned_scripts(queries_dir, scripts_dir):
    """
    Find scripts that don't have corresponding query files.
    
    Returns:
        List of tuples: (script_path, script_name)
    """
    orphaned = []
    
    # Get all query file names (sanitized)
    query_files = find_all_query_files(queries_dir)
    expected_scripts = set()
    
    for query_path, rel_path, subfolder in query_files:
        query_name = Path(query_path).stem
        sanitized_name = sanitize_filename(query_name)
        expected_scripts.add((subfolder, sanitized_name))
    
    # Find all existing scripts
    for root, dirs, files in os.walk(scripts_dir):
        for file in files:
            if file.endswith('.py'):
                script_path = os.path.join(root, file)
                script_name = Path(file).stem
                subfolder = os.path.relpath(root, scripts_dir)
                if subfolder == '.':
                    subfolder = ''
                
                # Check if this script has a corresponding query file
                if (subfolder, script_name) not in expected_scripts:
                    orphaned.append((script_path, script_name))
    
    return orphaned


def main():
    """Main execution function."""
    
    print("\n" + "=" * 70)
    print("FHI Script Generator")
    print("=" * 70)
    print(f"Queries directory: {QUERIES_DIR}")
    print(f"Scripts directory: {SCRIPTS_DIR}")
    print("=" * 70 + "\n")
    
    # Ensure scripts directory exists
    os.makedirs(SCRIPTS_DIR, exist_ok=True)
    
    # Find all query files
    query_files = find_all_query_files(QUERIES_DIR)
    print(f"Found {len(query_files)} query files\n")
    
    # Check for orphaned scripts (scripts without query files)
    orphaned_scripts = find_orphaned_scripts(QUERIES_DIR, SCRIPTS_DIR)
    
    if orphaned_scripts:
        print(f"⚠ Found {len(orphaned_scripts)} orphaned scripts (no corresponding .txt query file):")
        for script_path, script_name in orphaned_scripts:
            print(f"  - {script_name}.py")
        print("\nDeleting orphaned scripts...")
        
        for script_path, script_name in orphaned_scripts:
            try:
                os.remove(script_path)
                print(f"  ✓ Deleted: {script_name}.py")
            except Exception as e:
                print(f"  ✗ Failed to delete {script_name}.py: {e}")
        print()
    
    # Track all scripts and new scripts
    all_script_entries = []
    new_scripts = []
    existing_scripts = []
    
    # Process each query file
    for query_path, rel_path, subfolder in query_files:
        query_filename = Path(query_path).name
        query_name = Path(query_path).stem
        
        # Sanitize filename
        sanitized_name = sanitize_filename(query_name)
        
        # Determine script path
        if subfolder:
            script_dir = os.path.join(SCRIPTS_DIR, subfolder)
            script_rel_path = f"Queries/08_Folkehelse_og_levekår/FHI/scripts/{subfolder}/{sanitized_name}.py"
        else:
            script_dir = SCRIPTS_DIR
            script_rel_path = f"Queries/08_Folkehelse_og_levekår/FHI/scripts/{sanitized_name}.py"
        
        script_path = os.path.join(script_dir, f"{sanitized_name}.py")
        
        # Task name for master_script.py
        task_name = f"Folkehelse - {query_name}"
        
        # Check if script already exists
        if os.path.exists(script_path):
            existing_scripts.append((query_filename, sanitized_name))
            all_script_entries.append((script_rel_path, task_name))
            print(f"[EXISTS] {rel_path}")
            print(f"         → {sanitized_name}.py")
        else:
            # Create script directory if needed
            os.makedirs(script_dir, exist_ok=True)
            
            # Generate script
            print(f"[NEW]    {rel_path}")
            print(f"         → {sanitized_name}.py")
            
            script_content = generate_script_template(query_path, script_path, QUERIES_DIR)
            
            with open(script_path, 'w', encoding='utf-8') as f:
                f.write(script_content)
            
            new_scripts.append((script_rel_path, task_name))
            all_script_entries.append((script_rel_path, task_name))
    
    # Summary
    print("\n" + "=" * 70)
    print("Summary")
    print("=" * 70)
    print(f"Total query files: {len(query_files)}")
    print(f"Existing scripts: {len(existing_scripts)}")
    print(f"New scripts generated: {len(new_scripts)}")
    print("=" * 70 + "\n")
    
    if existing_scripts:
        print("Existing scripts:")
        for query_name, script_name in existing_scripts:
            print(f"  ✓ {script_name}.py")
        print()
    
    if new_scripts:
        print("New scripts generated:")
        for script_path, task_name in new_scripts:
            print(f"  + {Path(script_path).name}")
        print()
    
    # Always sync master_script.py with all scripts
    print("Synchronizing master_script.py...")
    sync_master_script(all_script_entries)
    print()
    
    print("=" * 70)
    print("Script generation complete!")
    print("=" * 70)
    
    if new_scripts:
        print("\nNext steps:")
        print("1. Review and edit generated scripts in FHI/scripts/ as needed")
        print("2. Run master_script.py to execute all scripts")
    else:
        print("\nAll query files have corresponding scripts.")


if __name__ == "__main__":
    main()
