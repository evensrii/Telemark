"""
ISY API Access Script
=====================

This script tests OAuth 2.0 authentication and basic API access to the ISY API.
It uses client credentials flow with the provided ClientId and ClientSecret.

Author: Data Engineering Team
Date: 2025-09-25
"""

import os
import sys
import requests
import json
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
        self.token_expires_at = None
        
        if not self.client_secret:
            raise ValueError("ISY_CLIENT_SECRET not found in environment variables")
    
    def get_access_token(self, scopes=None, auth_method="form"):
        """
        Obtain access token using OAuth 2.0 client credentials flow
        
        Args:
            scopes (list): List of scopes to request (default: ["ProjectRead", "PortfolioRead"])
            auth_method (str): Authentication method - "form", "basic", or "header"
        """
        if scopes is None:
            scopes = ["ProjectRead", "PortfolioRead"]
        
        print(f"Requesting access token for scopes: {', '.join(scopes)}")
        print(f"Using authentication method: {auth_method}")
        print(f"Client ID: {self.client_id}")
        print(f"Token URL: {self.token_url}")
        
        # Try different authentication methods
        if auth_method == "form":
            # Method 1: Client credentials in form data (most common)
            token_data = {
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "scope": " ".join(scopes)
            }
            headers = {
                "Content-Type": "application/x-www-form-urlencoded"
            }
            auth = None
            
        elif auth_method == "basic":
            # Method 2: HTTP Basic Authentication
            token_data = {
                "grant_type": "client_credentials",
                "scope": " ".join(scopes)
            }
            headers = {
                "Content-Type": "application/x-www-form-urlencoded"
            }
            auth = (self.client_id, self.client_secret)
            
        elif auth_method == "header":
            # Method 3: Client credentials in Authorization header
            import base64
            credentials = base64.b64encode(f"{self.client_id}:{self.client_secret}".encode()).decode()
            token_data = {
                "grant_type": "client_credentials",
                "scope": " ".join(scopes)
            }
            headers = {
                "Content-Type": "application/x-www-form-urlencoded",
                "Authorization": f"Basic {credentials}"
            }
            auth = None
        
        try:
            print(f"Request data: {token_data}")
            print(f"Request headers: {headers}")
            
            response = requests.post(
                self.token_url, 
                data=token_data, 
                headers=headers,
                auth=auth
            )
            
            print(f"Response status: {response.status_code}")
            print(f"Response headers: {dict(response.headers)}")
            
            response.raise_for_status()
            
            token_info = response.json()
            self.access_token = token_info.get("access_token")
            expires_in = token_info.get("expires_in", 3600)
            
            print(f"✅ Successfully obtained access token")
            print(f"Token expires in: {expires_in} seconds")
            print(f"Token type: {token_info.get('token_type', 'Bearer')}")
            
            return self.access_token
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Error obtaining access token: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"Response status: {e.response.status_code}")
                print(f"Response content: {e.response.text}")
                try:
                    error_json = e.response.json()
                    print(f"Error details: {json.dumps(error_json, indent=2)}")
                except:
                    pass
            raise
    
    def make_authenticated_request(self, endpoint, method="GET", params=None, data=None):
        """
        Make an authenticated request to the ISY API
        
        Args:
            endpoint (str): API endpoint (relative to base URL)
            method (str): HTTP method (GET, POST, etc.)
            params (dict): Query parameters
            data (dict): Request body data
        """
        if not self.access_token:
            print("No access token available. Obtaining token...")
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
            
            print(f"Request: {method} {url}")
            print(f"Status: {response.status_code}")
            
            response.raise_for_status()
            return response.json() if response.content else None
            
        except requests.exceptions.RequestException as e:
            print(f"❌ API request failed: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"Response status: {e.response.status_code}")
                print(f"Response content: {e.response.text}")
            raise

def test_connection():
    """Test basic connection and authentication"""
    print("=" * 60)
    print("ISY API Connection Test")
    print("=" * 60)
    
    try:
        # Load environment variables
        load_environment()
        print("✅ Environment variables loaded")
        
        # Initialize client
        client = ISYAPIClient()
        print("✅ ISY API client initialized")
        
        # Test different authentication methods
        auth_methods = ["form", "basic", "header"]
        
        for method in auth_methods:
            print(f"\n--- Testing authentication method: {method} ---")
            try:
                token = client.get_access_token(auth_method=method)
                if token:
                    print(f"✅ OAuth 2.0 authentication successful with method: {method}")
                    return client
                else:
                    print(f"❌ Failed to obtain access token with method: {method}")
            except Exception as e:
                print(f"❌ Authentication failed with method {method}: {e}")
                continue
        
        print("❌ All authentication methods failed")
        return None
            
    except Exception as e:
        print(f"❌ Connection test failed: {e}")
        return None

def verify_credentials():
    """Verify that credentials are loaded correctly"""
    print("=" * 60)
    print("Credential Verification")
    print("=" * 60)
    
    try:
        load_environment()
        print("✅ Environment variables loaded")
        
        client_id = "telemarkfk_bi_client_temp"
        client_secret = os.getenv("ISY_CLIENT_SECRET")
        
        print(f"Client ID: {client_id}")
        print(f"Client Secret loaded: {'Yes' if client_secret else 'No'}")
        if client_secret:
            print(f"Client Secret length: {len(client_secret)} characters")
            print(f"Client Secret starts with: {client_secret[:5]}...")
            print(f"Client Secret ends with: ...{client_secret[-5:]}")
        else:
            print("❌ ISY_CLIENT_SECRET not found in environment!")
            
    except Exception as e:
        print(f"❌ Error verifying credentials: {e}")

def test_single_auth_method(auth_method="basic"):
    """Test a single authentication method with detailed output"""
    print("=" * 60)
    print(f"ISY API Single Auth Test - Method: {auth_method}")
    print("=" * 60)
    
    try:
        # Load environment variables
        load_environment()
        print("✅ Environment variables loaded")
        
        # Initialize client
        client = ISYAPIClient()
        print("✅ ISY API client initialized")
        
        # Test token acquisition with specific method
        token = client.get_access_token(auth_method=auth_method)
        if token:
            print(f"✅ OAuth 2.0 authentication successful")
            return client
        else:
            print("❌ Failed to obtain access token")
            return None
            
    except Exception as e:
        print(f"❌ Connection test failed: {e}")
        return None

def analyze_project_structure(projects_data):
    """Analyze the structure of project data to find the right identifier for budget API"""
    if isinstance(projects_data, list) and len(projects_data) > 0:
        first_project = projects_data[0]
        print(f"\n📋 Project Data Structure Analysis:")
        print(f"   Sample project keys: {list(first_project.keys())}")
        
        # Show values for potential project identifier fields
        identifier_fields = ['projectRec', 'projectNumber', 'projectId', 'id', 'number', 'name']
        for field in identifier_fields:
            if field in first_project:
                value = first_project[field]
                print(f"   {field}: {value} (type: {type(value).__name__})")
        
        return first_project
    return None

def extract_project_numbers(projects_data, field_name='projectNumber'):
    """Extract project numbers from the API response using specified field"""
    project_numbers = []
    
    if isinstance(projects_data, list):
        for project in projects_data:
            if isinstance(project, dict):
                if field_name in project and project[field_name]:
                    project_numbers.append(str(project[field_name]))
    elif isinstance(projects_data, dict):
        # If it's a dict, look for a list of projects inside
        for key, value in projects_data.items():
            if isinstance(value, list):
                project_numbers.extend(extract_project_numbers(value, field_name))
    
    return project_numbers

def test_project_budgets(client, project_numbers, revision_type="referencebudget"):
    """Test budget endpoints for all discovered projects"""
    print(f"\n" + "=" * 60)
    print(f"Testing Budget Data - Revision Type: {revision_type}")
    print("=" * 60)
    
    budget_results = {}
    successful_budgets = 0
    failed_budgets = 0
    
    for i, project_rec in enumerate(project_numbers[:10]):  # Test first 10 projects to avoid overwhelming output
        try:
            print(f"\n[{i+1}/10] Testing project: {project_rec[:8]}...")
            endpoint = f"api/Budget/{project_rec}/{revision_type}"
            
            response = client.make_authenticated_request(endpoint)
            
            if response is not None:
                print(f"✅ Success - Budget data found")
                if isinstance(response, dict):
                    print(f"   Keys: {list(response.keys())}")
                elif isinstance(response, list):
                    print(f"   Items: {len(response)}")
                
                budget_results[project_rec] = {
                    "status": "success",
                    "response": response
                }
                successful_budgets += 1
            else:
                print("✅ Success - Empty response")
                budget_results[project_rec] = {
                    "status": "success",
                    "response": None
                }
                successful_budgets += 1
                
        except Exception as e:
            print(f"❌ Failed: {e}")
            budget_results[project_rec] = {
                "status": "error",
                "error": str(e)
            }
            failed_budgets += 1
    
    print(f"\n" + "=" * 60)
    print(f"Budget Test Summary")
    print("=" * 60)
    print(f"✅ Successful budget requests: {successful_budgets}")
    print(f"❌ Failed budget requests: {failed_budgets}")
    print(f"📊 Total projects tested: {len(project_numbers[:10])}")
    
    if len(project_numbers) > 10:
        print(f"📝 Note: Only tested first 10 of {len(project_numbers)} total projects")
    
    return budget_results

def test_api_endpoints(client):
    """Test various API endpoints to verify access"""
    print("\n" + "=" * 60)
    print("API Endpoints Test")
    print("=" * 60)
    
    # Test the actual endpoints from the Swagger documentation
    test_endpoints = [
        # Project discovery endpoint - the correct one from Swagger
        ("api/Project/GetProjects", "Get all active projects", "GET", None),
        
        # You can uncomment and modify the POST endpoint when you know the correct parameters
        # ("api/Portfolio/GetPortfolioData", "Get Portfolio Data", "POST", {
        #     "period": {
        #         "year": 2024,
        #         "months": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
        #     },
        #     "includeInactive": True,
        #     "selectedPortfolio": "Porteføljeoversikt",
        #     "source": 0  # Try numeric value instead of string
        # }),
    ]
    
    results = {}
    
    for endpoint_info in test_endpoints:
        if len(endpoint_info) == 4:
            endpoint, description, method, request_body = endpoint_info
        else:
            endpoint, description = endpoint_info[:2]
            method, request_body = "GET", None
            
        try:
            print(f"\nTesting: {description}")
            print(f"Endpoint: {method} {endpoint}")
            if request_body:
                print(f"Request body: {json.dumps(request_body, indent=2)}")
            
            response = client.make_authenticated_request(endpoint, method=method, data=request_body)
            
            if response is not None:
                print(f"✅ Success - Response type: {type(response)}")
                if isinstance(response, list):
                    print(f"   Items returned: {len(response)}")
                elif isinstance(response, dict):
                    print(f"   Keys in response: {list(response.keys())}")
                
                # Try to extract project numbers if this looks like a project list
                if "project" in description.lower():
                    # First analyze the project structure
                    sample_project = analyze_project_structure(response)
                    
                    # Try extracting with projectNumber field instead of projectRec
                    project_numbers = extract_project_numbers(response, 'projectNumber')
                    if project_numbers:
                        print(f"   🎯 Found {len(project_numbers)} project numbers (using projectNumber field)!")
                        print(f"   First 5 projects: {project_numbers[:5]}")
                        # Save project numbers for later use
                        results[f"{endpoint}_project_numbers"] = project_numbers
                        
                        # Test budget data for all projects using projectNumber
                        budget_results = test_project_budgets(client, project_numbers, "referencebudget")
                        results["budget_test_results"] = budget_results
                    else:
                        # Fallback to projectRec if projectNumber is empty
                        project_recs = extract_project_numbers(response, 'projectRec')
                        if project_recs:
                            print(f"   🎯 Found {len(project_recs)} project RECs (using projectRec field)!")
                            print(f"   First 5 projects: {project_recs[:5]}")
                            results[f"{endpoint}_project_recs"] = project_recs
                
                results[endpoint] = {
                    "status": "success",
                    "response": response
                }
            else:
                print("✅ Success - Empty response")
                results[endpoint] = {
                    "status": "success",
                    "response": None
                }
                
        except Exception as e:
            print(f"❌ Failed: {e}")
            results[endpoint] = {
                "status": "error",
                "error": str(e)
            }
    
    return results

def main():
    """Main function to run all tests"""
    print(f"ISY API Test Script - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Test connection
    client = test_connection()
    
    if client:
        # Test API endpoints
        results = test_api_endpoints(client)
        
        # Summary
        print("\n" + "=" * 60)
        print("Test Summary")
        print("=" * 60)
        
        successful_endpoints = [ep for ep, result in results.items() if isinstance(result, dict) and result.get("status") == "success"]
        failed_endpoints = [ep for ep, result in results.items() if isinstance(result, dict) and result.get("status") == "error"]
        
        print(f"✅ Successful endpoints: {len(successful_endpoints)}")
        for ep in successful_endpoints:
            print(f"   - {ep}")
        
        if failed_endpoints:
            print(f"❌ Failed endpoints: {len(failed_endpoints)}")
            for ep in failed_endpoints:
                print(f"   - {ep}: {results[ep]['error']}")
        
        print(f"\nTotal endpoints tested: {len(results)}")
        
        # Save results for further analysis
        output_file = Path(__file__).parent / f"isy_api_test_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        print(f"\nDetailed results saved to: {output_file}")
        
    else:
        print("\n❌ Connection test failed. Cannot proceed with endpoint testing.")

if __name__ == "__main__":
    main()
