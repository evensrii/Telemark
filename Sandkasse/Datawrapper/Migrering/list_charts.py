import os
import requests
from dotenv import load_dotenv

## Migrering: list charts in Datawrapper via the API (https://developer.datawrapper.de/reference/getcharts)

API_BASE = "https://api.datawrapper.de/v3"
TEAM_NAME = "Team analyse"


## Load the Datawrapper token from token.env (same location as GITHUB_TOKEN)
def get_datawrapper_token():
    pythonpath = os.environ.get("PYTHONPATH")
    if not pythonpath:
        raise ValueError("PYTHONPATH environment variable is not set.")

    env_file_path = os.path.join(pythonpath, "token.env")
    if not os.path.exists(env_file_path):
        raise ValueError(f"token.env file not found in: {env_file_path}")

    load_dotenv(env_file_path)
    token = os.getenv("DATAWRAPPER_CREATE_TOKEN")
    if not token:
        raise ValueError("DATAWRAPPER_CREATE_TOKEN not found in the loaded .env file.")
    return token


token = get_datawrapper_token()
session = requests.Session()
session.headers.update({"Authorization": f"Bearer {token}"})


## Who am I? (verifies the token)
response = session.get(f"{API_BASE}/me")
response.raise_for_status()
me = response.json()
print(f"Logged in as: {me.get('name') or me.get('email')} (user id {me.get('id')})")


## Find the team id for TEAM_NAME
response = session.get(f"{API_BASE}/teams")
response.raise_for_status()
teams = response.json().get("list", [])

print("\nTeams available to this token:")
for team in teams:
    print(f"  {team['id']:<20} {team['name']}")

team_id = next((t["id"] for t in teams if t["name"] == TEAM_NAME), None)
if team_id is None:
    raise ValueError(f"Team '{TEAM_NAME}' not found among the teams listed above.")


## Fetch all charts in the team (the API is paginated)
def get_all_charts(team_id, page_size=100):
    charts = []
    offset = 0
    while True:
        params = {
            "teamId": team_id,
            "limit": page_size,
            "offset": offset,
            "orderBy": "lastModifiedAt",
            "order": "DESC",
        }
        response = session.get(f"{API_BASE}/charts", params=params)
        response.raise_for_status()
        result = response.json()
        page = result.get("list", [])
        charts.extend(page)
        offset += len(page)
        if not page or offset >= result.get("total", 0):
            break
    return charts


charts = get_all_charts(team_id)


## Print to stdout
print(f"\n{len(charts)} charts in '{TEAM_NAME}':\n")
print(f"{'ID':<8} {'Type':<20} {'Last modified':<20} {'Published':<10} Title")
print("-" * 100)
for chart in charts:
    last_modified = (chart.get("lastModifiedAt") or "")[:16].replace("T", " ")
    published = "yes" if chart.get("publishedAt") else "no"
    print(
        f"{chart['id']:<8} {chart.get('type', ''):<20} {last_modified:<20} {published:<10} {chart.get('title', '')}"
    )
