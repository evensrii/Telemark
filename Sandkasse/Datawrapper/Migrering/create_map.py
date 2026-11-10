from datawrapper_generelt import get_session
from datawrapper_map import create_map

## Migrering: create (or update) one live choropleth map of the municipalities in Telemark in Datawrapper,
## formatted according to Formatering/formatering_map.txt. The map logic is in datawrapper_map.py
## (shared with create_maps_from_list.py, which creates several maps from a list).

CSV_URL = "https://raw.githubusercontent.com/evensrii/Telemark/refs/heads/main/Data/01_Befolkning/Husholdninger/dw_aleneboende.csv"
CHART_TITLE = "Andel aleneboende (test av dw_-fil)"

# Description (subtitle below the title), "" for none
DESCRIPTION = ""

# Set to an existing chart id to update it, or None to create a new chart
CHART_ID = "pFsxq"

# Query script that fetches the data (relative to PYTHONPATH), used to find the data source.
# None if the data has no query script (e.g. dummy data): the source is left empty.
QUERY_SCRIPT = "Queries/01_Befolkning/Husholdninger/aleneboende.py"

# Number of color steps: 5 (standard); 3 and 2 have fixed colors, other numbers are calculated
NUM_STEPS = 5

# Datawrapper folder id to put the map in, or None to leave it where it is
FOLDER_ID = None

# Publish the map (Datawrapper only refreshes live data automatically after the first publish)
PUBLISH = False


session = get_session()
chart_id = create_map(
    session,
    csv_url=CSV_URL,
    title=CHART_TITLE,
    chart_id=CHART_ID,
    query_script=QUERY_SCRIPT,
    num_steps=NUM_STEPS,
    description=DESCRIPTION,
    folder_id=FOLDER_ID,
    publish=PUBLISH,
)
