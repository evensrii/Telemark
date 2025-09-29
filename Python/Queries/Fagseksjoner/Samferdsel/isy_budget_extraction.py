"""
ISY API Budget Data Extraction Script
====================================

This script extracts monthly budget data for all projects from the ISY API
and creates a comprehensive pandas DataFrame with all budget information.

Author: Data Engineering Team
Date: 2025-09-25
"""

import os
import sys
import requests
import json
import pandas as pd
from datetime import datetime
from pathlib import Path

# Add helper scripts to path
sys.path.append(str(Path(__file__).parent.parent.parent / "Helper_scripts"))

def load_environment():
    """Load environment variables from token.env file"""
    token_file = Path(__file__).parent.parent.parent / "token.env"
    
    if not token_file.exists():
        raise FileNotFoundError(f"Token file not found: {token_file}")
    
    # Read the token.env file and set environment variables
    with open(token_file, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                # Remove quotes from value if present
                value = value.strip()
                if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
                    value = value[1:-1]
                os.environ[key.strip()] = value

class ISYAPIClient:
    """Client for accessing ISY API with OAuth 2.0 authentication"""
    
    def __init__(self):
        self.client_id = "telemarkfk_bi_client_temp"
        self.client_secret = os.getenv("ISY_CLIENT_SECRET")
        self.token_url = "https://po.isy.no/isypo_api_telemarkfk/connect/token"
        self.base_url = "https://po.isy.no/ISYPO_API_telemarkFK"
        self.access_token = None
        
        if not self.client_secret:
            raise ValueError("ISY_CLIENT_SECRET not found in environment variables")
    
    def get_access_token(self):
        """Obtain access token using OAuth 2.0 client credentials flow"""
        scopes = ["ProjectRead", "PortfolioRead"]
        
        token_data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "scope": " ".join(scopes)
        }
        
        headers = {
            "Content-Type": "application/x-www-form-urlencoded"
        }
        
        try:
            response = requests.post(self.token_url, data=token_data, headers=headers)
            response.raise_for_status()
            
            token_info = response.json()
            self.access_token = token_info.get("access_token")
            
            print(f"✅ Successfully obtained access token")
            return self.access_token
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Error obtaining access token: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"Response content: {e.response.text}")
            raise
    
    def make_authenticated_request(self, endpoint, method="GET", params=None, data=None):
        """Make an authenticated request to the ISY API"""
        if not self.access_token:
            self.get_access_token()
        
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Accept": "application/json"
        }
        
        try:
            response = requests.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                json=data
            )
            
            response.raise_for_status()
            return response.json() if response.content else None
            
        except requests.exceptions.RequestException as e:
            print(f"❌ API request failed for {endpoint}: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"Response status: {e.response.status_code}")
                print(f"Response content: {e.response.text}")
            return None

def get_all_projects(client):
    """Get all active projects from the ISY API"""
    print("📋 Fetching all projects...")
    
    try:
        projects = client.make_authenticated_request("api/Project/GetProjects")
        if projects:
            print(f"✅ Found {len(projects)} projects")
            return projects
        else:
            print("❌ No projects found")
            return []
    except Exception as e:
        print(f"❌ Error fetching projects: {e}")
        return []

def get_project_budget(client, project_number, revision_type="referencebudget"):
    """Get budget data for a specific project"""
    try:
        endpoint = f"api/Budget/{project_number}/{revision_type}"
        budget_data = client.make_authenticated_request(endpoint)
        return budget_data
    except Exception as e:
        print(f"❌ Error fetching budget for project {project_number}: {e}")
        return None

def process_budget_data(project_info, budget_data):
    """Process budget data into a list of records for DataFrame creation"""
    records = []
    
    if not budget_data or 'budgetPosts' not in budget_data:
        return records
    
    project_number = project_info.get('projectNumber', '')
    project_name = project_info.get('name', '')
    project_rec = project_info.get('projectRec', '')
    start_date = project_info.get('startDate', '')
    end_date = project_info.get('endDate', '')
    responsible = project_info.get('responsible', '')
    inactive = project_info.get('inactive', False)
    finished = project_info.get('finished', False)
    
    # Extract revision information properly
    revision_info = budget_data.get('revision', {})
    if isinstance(revision_info, dict):
        revision_type = revision_info.get('type', 'referencebudget')
        revision_description = revision_info.get('description', '')
        revision_date = revision_info.get('date', '')
        revision_cost_year = revision_info.get('costYear', '')
    else:
        revision_type = str(revision_info) if revision_info else 'referencebudget'
        revision_description = ''
        revision_date = ''
        revision_cost_year = ''
    
    for budget_post in budget_data.get('budgetPosts', []):
        # Get budget post information
        post_info = {
            'project_number': project_number,
            'project_name': project_name,
            'project_rec': project_rec,
            'start_date': start_date,
            'end_date': end_date,
            'responsible': responsible,
            'inactive': inactive,
            'finished': finished,
            'revision_type': revision_type,
            'revision_description': revision_description,
            'revision_date': revision_date,
            'revision_cost_year': revision_cost_year
        }
        
        # Add budget post specific fields if they exist, handling nested structures properly
        simple_fields = ['budgetPostRec', 'budgetPostNumber', 'description', 'responsible',
                        'lastSum', 'lastCode', 'startPeriod', 'endPeriod']
        
        for field in simple_fields:
            if field in budget_post:
                post_info[f'budget_post_{field}'] = budget_post[field]
        
        # Handle addendumValues separately (it's a nested structure)
        if 'addendumValues' in budget_post:
            addendum_values = budget_post['addendumValues']
            if isinstance(addendum_values, dict):
                # Extract useful information from addendumValues without storing the whole dict
                addendum_fields = addendum_values.get('addendumFields', {})
                addendum_groups = addendum_values.get('addendumGroups', [])
                post_info['budget_post_addendum_fields_count'] = len(addendum_fields) if addendum_fields else 0
                post_info['budget_post_addendum_groups_count'] = len(addendum_groups) if addendum_groups else 0
            else:
                post_info['budget_post_addendum_fields_count'] = 0
                post_info['budget_post_addendum_groups_count'] = 0
        
        # Process period data (monthly data)
        if 'periodData' in budget_post:
            for period in budget_post['periodData']:
                record = post_info.copy()
                record.update({
                    'year': period.get('year'),
                    'month': period.get('month'),
                    'net_amount': period.get('netAmount'),
                    'tax_amount': period.get('taxAmount'),
                    'gross_amount': (period.get('netAmount', 0) + period.get('taxAmount', 0)) if period.get('netAmount') is not None and period.get('taxAmount') is not None else None
                })
                
                # Create a date column for easier analysis
                if record['year'] and record['month']:
                    try:
                        record['date'] = pd.to_datetime(f"{record['year']}-{record['month']:02d}-01")
                    except:
                        record['date'] = None
                
                records.append(record)
        else:
            # If no period data, still add the project/budget post info
            records.append(post_info)
    
    return records

def extract_all_budget_data():
    """Main function to extract all budget data and create DataFrame"""
    print("=" * 80)
    print("ISY API Budget Data Extraction")
    print("=" * 80)
    
    # Initialize API client
    try:
        load_environment()
        client = ISYAPIClient()
        print("✅ ISY API client initialized")
    except Exception as e:
        print(f"❌ Failed to initialize API client: {e}")
        return None
    
    # Get all projects
    projects = get_all_projects(client)
    if not projects:
        print("❌ No projects found. Exiting.")
        return None
    
    # Extract budget data for all projects
    all_records = []
    successful_projects = 0
    failed_projects = 0
    
    print(f"\n📊 Processing budget data for {len(projects)} projects...")
    print("-" * 80)
    
    for i, project in enumerate(projects, 1):
        project_number = project.get('projectNumber', 'Unknown')
        project_name = project.get('name', 'Unknown')
        
        print(f"[{i:2d}/{len(projects)}] Processing: {project_number} - {project_name[:50]}...")
        
        # Get budget data for this project
        budget_data = get_project_budget(client, project_number)
        
        if budget_data:
            # Process the budget data into records
            records = process_budget_data(project, budget_data)
            all_records.extend(records)
            successful_projects += 1
            print(f"         ✅ Added {len(records)} budget records")
        else:
            failed_projects += 1
            print(f"         ❌ No budget data available")
    
    print("-" * 80)
    print(f"📈 Processing Summary:")
    print(f"   ✅ Successful projects: {successful_projects}")
    print(f"   ❌ Failed projects: {failed_projects}")
    print(f"   📊 Total budget records: {len(all_records)}")
    
    if not all_records:
        print("❌ No budget records found. Exiting.")
        return None
    
    # Create DataFrame
    print(f"\n📋 Creating pandas DataFrame...")
    df = pd.DataFrame(all_records)
    
    # Convert data types
    numeric_columns = ['year', 'month', 'net_amount', 'tax_amount', 'gross_amount', 
                      'revision_cost_year', 'budget_post_addendum_fields_count', 
                      'budget_post_addendum_groups_count']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Convert boolean columns
    boolean_columns = ['inactive', 'finished']
    for col in boolean_columns:
        if col in df.columns:
            df[col] = df[col].astype(bool)
    
    # Convert date columns
    date_columns = ['start_date', 'end_date', 'revision_date']
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # Sort by project and date
    if 'date' in df.columns:
        df = df.sort_values(['project_number', 'date']).reset_index(drop=True)
    else:
        df = df.sort_values(['project_number', 'year', 'month']).reset_index(drop=True)
    
    print(f"✅ DataFrame created with {len(df)} rows and {len(df.columns)} columns")
    print(f"\nDataFrame Info:")
    print(f"   Shape: {df.shape}")
    print(f"   Columns: {list(df.columns)}")
    
    # Show sample data
    if len(df) > 0:
        print(f"\nSample data (first 3 rows):")
        print(df.head(3).to_string())
    
    return df

def save_to_csv(df, filename=None):
    """Save DataFrame to CSV file"""
    if df is None or len(df) == 0:
        print("❌ No data to save")
        return None
    
    if filename is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"isy_budget_data_{timestamp}.csv"
    
    # Save to same folder as script
    script_folder = Path(__file__).parent
    filepath = script_folder / filename
    
    try:
        df.to_csv(filepath, index=False, encoding='utf-8')
        print(f"✅ Data saved to: {filepath}")
        print(f"   File size: {filepath.stat().st_size / 1024 / 1024:.2f} MB")
        return filepath
    except Exception as e:
        print(f"❌ Error saving CSV: {e}")
        return None

def main():
    """Main execution function"""
    print(f"ISY Budget Data Extraction - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Extract all budget data
    df = extract_all_budget_data()
    
    if df is not None:
        # Save to CSV
        filepath = save_to_csv(df)
        
        if filepath:
            print(f"\n🎉 Budget data extraction completed successfully!")
            print(f"📁 Output file: {filepath}")
            print(f"📊 Total records: {len(df)}")
            
            # Show summary statistics
            if 'net_amount' in df.columns:
                total_net = df['net_amount'].sum()
                print(f"💰 Total net amount: {total_net:,.2f}")
            
            if 'project_number' in df.columns:
                unique_projects = df['project_number'].nunique()
                print(f"🏗️  Unique projects: {unique_projects}")
            
            if 'date' in df.columns:
                date_range = df['date'].dropna()
                if len(date_range) > 0:
                    print(f"📅 Date range: {date_range.min().strftime('%Y-%m')} to {date_range.max().strftime('%Y-%m')}")
        else:
            print(f"\n❌ Failed to save data to CSV")
    else:
        print(f"\n❌ Budget data extraction failed")

if __name__ == "__main__":
    main()
