import os
import json
import pandas as pd

# ============================================================
# Configuration
# ============================================================

TELEMARK_KOMMUNER = [
    "Porsgrunn", "Skien", "Notodden", "Siljan", "Bamble", "Kragerø",
    "Drangedal", "Nome", "Midt-Telemark", "Seljord", "Hjartdal", "Tinn",
    "Kviteseid", "Nissedal", "Fyresdal", "Tokke", "Vinje",
]

# Approximate county center coordinates for the county-level map view
FYLKE_COORDS = {
    "Oslo": (59.91, 10.75),
    "Rogaland": (59.00, 6.00),
    "Møre og Romsdal": (62.50, 7.10),
    "Nordland": (67.00, 15.00),
    "Østfold": (59.30, 11.20),
    "Akershus": (59.90, 11.20),
    "Buskerud": (60.10, 9.50),
    "Innlandet": (61.20, 10.50),
    "Vestfold": (59.25, 10.20),
    "Telemark": (59.40, 9.00),
    "Agder": (58.35, 7.80),
    "Vestland": (60.70, 5.80),
    "Trøndelag": (63.80, 11.50),
    "Troms": (69.30, 19.00),
    "Finnmark": (70.20, 25.50),
}

# Colors
COLOR_TILFLYTTING = "#2166ac"   # Blue for inflow
COLOR_FRAFLYTTING = "#d6604d"   # Red/orange for outflow
COLOR_TELEMARK = "#005260"      # Telemark fylkeskommune color

# Map center (roughly center of southern Norway)
MAP_CENTER = [62.0, 10.0]
MAP_ZOOM = 5

# ============================================================
# Step 1: Load data
# ============================================================

script_dir = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.abspath(os.path.join(script_dir, "..", ".."))
data_dir = os.path.join(repo_root, "Python", "Temp")

df_fra = pd.read_csv(os.path.join(data_dir, "fraflytting_fra_telemarkskommuner.csv"))
df_til = pd.read_csv(os.path.join(data_dir, "tilflytting_til_telemarkskommuner.csv"))

print(f"Loaded fraflytting: {len(df_fra)} rows")
print(f"Loaded tilflytting: {len(df_til)} rows")
print(f"  Columns: {list(df_fra.columns)}")

# ============================================================
# Step 2: Drop rows with missing coordinates
# ============================================================

df_fra = df_fra.dropna(subset=["Fra lat", "Fra lon", "Til lat", "Til lon"])
df_til = df_til.dropna(subset=["Fra lat", "Fra lon", "Til lat", "Til lon"])

print(f"After dropping missing coords - fraflytting: {len(df_fra)}, tilflytting: {len(df_til)}")

# ============================================================
# Step 3: Tag internal vs external moves
# ============================================================

df_fra["internal"] = df_fra["Til kommune"].isin(TELEMARK_KOMMUNER)
df_til["internal"] = df_til["Fra kommune"].isin(TELEMARK_KOMMUNER)

# ============================================================
# Step 4: Aggregate county-level data
# ============================================================

# Fraflytting aggregated by county (from Telemark to other counties)
df_fra_fylke = (
    df_fra[~df_fra["internal"]]
    .groupby("Til fylke", as_index=False)["Antall"]
    .sum()
    .rename(columns={"Til fylke": "Fylke", "Antall": "Antall"})
)

# Tilflytting aggregated by county (from other counties to Telemark)
df_til_fylke = (
    df_til[~df_til["internal"]]
    .groupby("Fra fylke", as_index=False)["Antall"]
    .sum()
    .rename(columns={"Fra fylke": "Fylke", "Antall": "Antall"})
)

# Add coordinates for each county
df_fra_fylke["lat"] = df_fra_fylke["Fylke"].map(lambda f: FYLKE_COORDS.get(f, (0, 0))[0])
df_fra_fylke["lon"] = df_fra_fylke["Fylke"].map(lambda f: FYLKE_COORDS.get(f, (0, 0))[1])
df_til_fylke["lat"] = df_til_fylke["Fylke"].map(lambda f: FYLKE_COORDS.get(f, (0, 0))[0])
df_til_fylke["lon"] = df_til_fylke["Fylke"].map(lambda f: FYLKE_COORDS.get(f, (0, 0))[1])

print(f"\nCounty-level fraflytting: {len(df_fra_fylke)} counties")
print(f"County-level tilflytting: {len(df_til_fylke)} counties")

# ============================================================
# Step 5: Load Telemark kommune GeoJSON for borders
# ============================================================

geojson_path = os.path.join(
    repo_root, "Kart", "Kartfiler", "Telemark_kommuner_WGS1984.geojson"
)
with open(geojson_path, "r", encoding="utf-8") as f:
    telemark_geojson = json.load(f)

telemark_geojson_json = json.dumps(telemark_geojson, ensure_ascii=False)
print(f"Loaded Telemark GeoJSON: {len(telemark_geojson['features'])} features")

# ============================================================
# Step 6: Prepare data as JSON for embedding in HTML
# ============================================================

def df_to_records(df):
    """Convert DataFrame to list of dicts for JSON serialization."""
    return df.to_dict(orient="records")

data_fra_json = json.dumps(df_to_records(df_fra), ensure_ascii=False)
data_til_json = json.dumps(df_to_records(df_til), ensure_ascii=False)
data_fra_fylke_json = json.dumps(df_to_records(df_fra_fylke), ensure_ascii=False)
data_til_fylke_json = json.dumps(df_to_records(df_til_fylke), ensure_ascii=False)
telemark_list_json = json.dumps(TELEMARK_KOMMUNER, ensure_ascii=False)
fylke_coords_json = json.dumps(FYLKE_COORDS, ensure_ascii=False)

# ============================================================
# Step 7: Generate the full interactive HTML
# ============================================================

full_html = f"""<!DOCTYPE html>
<html lang="no">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Flyttestrømmer - Telemark</title>
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
    <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: #f5f5f5;
            color: #333;
        }}
        .header {{
            background: {COLOR_TELEMARK};
            color: white;
            padding: 14px 24px;
            font-size: 18px;
            font-weight: 600;
            display: flex;
            align-items: center;
            gap: 12px;
        }}
        .header .subtitle {{
            font-size: 13px;
            font-weight: 400;
            opacity: 0.85;
        }}
        .main-container {{
            display: flex;
            height: calc(100vh - 50px);
        }}
        .sidebar {{
            width: 300px;
            min-width: 300px;
            background: white;
            border-right: 1px solid #ddd;
            padding: 16px;
            overflow-y: auto;
            display: flex;
            flex-direction: column;
            gap: 12px;
        }}
        .sidebar h3 {{
            font-size: 11px;
            font-weight: 600;
            color: {COLOR_TELEMARK};
            margin-bottom: 3px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}
        .control-group {{
            display: flex;
            flex-direction: column;
            gap: 4px;
        }}
        .control-group label {{
            font-size: 12px;
            font-weight: 500;
            color: #555;
        }}
        .control-group select {{
            width: 100%;
            padding: 5px 8px;
            border: 1px solid #ccc;
            border-radius: 4px;
            font-size: 12px;
            background: white;
        }}
        .control-group input[type="range"] {{
            width: 100%;
            cursor: pointer;
        }}
        .checkbox-group {{
            display: flex;
            align-items: center;
            gap: 6px;
            font-size: 12px;
        }}
        .checkbox-group input {{
            width: 15px;
            height: 15px;
            cursor: pointer;
        }}
        .radio-group {{
            display: flex;
            gap: 2px;
        }}
        .radio-group label {{
            flex: 1;
            text-align: center;
            padding: 6px 4px;
            border: 1px solid #ccc;
            font-size: 11px;
            cursor: pointer;
            background: white;
            transition: all 0.15s;
        }}
        .radio-group label:first-child {{
            border-radius: 4px 0 0 4px;
        }}
        .radio-group label:last-child {{
            border-radius: 0 4px 4px 0;
        }}
        .radio-group input {{
            display: none;
        }}
        .radio-group label.active {{
            background: {COLOR_TELEMARK};
            color: white;
            border-color: {COLOR_TELEMARK};
        }}
        .legend {{
            display: flex;
            gap: 14px;
            font-size: 11px;
            align-items: center;
        }}
        .legend-item {{
            display: flex;
            align-items: center;
            gap: 4px;
        }}
        .legend-line {{
            width: 20px;
            height: 3px;
            border-radius: 2px;
        }}
        .stats-box {{
            background: #f8f9fa;
            border-radius: 6px;
            padding: 10px;
            font-size: 12px;
            line-height: 1.6;
        }}
        .stats-box .stat-value {{
            font-weight: 700;
            color: {COLOR_TELEMARK};
        }}
        .right-panel {{
            flex: 1;
            display: flex;
            flex-direction: column;
            overflow: hidden;
        }}
        .map-container {{
            flex: 1;
            position: relative;
            min-height: 250px;
        }}
        #map {{
            width: 100%;
            height: 100%;
        }}
        .bottom-panels {{
            display: flex;
            border-top: 1px solid #ddd;
            background: white;
        }}
        .sankey-container {{
            flex: 1;
            display: flex;
            height: 260px;
        }}
        .sankey-panel {{
            flex: 1;
            padding: 4px;
            display: flex;
            flex-direction: column;
        }}
        .sankey-panel h4 {{
            text-align: center;
            font-size: 12px;
            color: #555;
            padding: 2px 0;
            flex-shrink: 0;
        }}
        .sankey-chart {{
            flex: 1;
        }}
        .divider {{
            width: 1px;
            background: #ddd;
        }}
        .table-container {{
            width: 340px;
            min-width: 340px;
            border-left: 1px solid #ddd;
            overflow-y: auto;
            padding: 8px;
            height: 260px;
        }}
        .table-container h4 {{
            font-size: 12px;
            color: #555;
            margin-bottom: 6px;
            text-align: center;
        }}
        .top-table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 11px;
        }}
        .top-table th {{
            background: #f0f0f0;
            padding: 4px 6px;
            text-align: left;
            font-weight: 600;
            border-bottom: 1px solid #ddd;
            position: sticky;
            top: 0;
        }}
        .top-table td {{
            padding: 3px 6px;
            border-bottom: 1px solid #f0f0f0;
        }}
        .top-table tr:hover {{
            background: #f8f8f8;
        }}
        .top-table .count-til {{
            color: {COLOR_TILFLYTTING};
            font-weight: 600;
        }}
        .top-table .count-fra {{
            color: {COLOR_FRAFLYTTING};
            font-weight: 600;
        }}
        .flow-tooltip {{
            background: white !important;
            border: 1px solid #ccc !important;
            border-radius: 4px !important;
            padding: 6px 10px !important;
            font-family: 'Segoe UI', sans-serif !important;
            font-size: 12px !important;
            box-shadow: 0 2px 6px rgba(0,0,0,0.15) !important;
        }}
        .threshold-display {{
            font-size: 11px;
            color: #888;
            text-align: right;
        }}
        @media (max-width: 1100px) {{
            .table-container {{
                display: none;
            }}
        }}
        @media (max-width: 900px) {{
            .main-container {{
                flex-direction: column;
            }}
            .sidebar {{
                width: 100%;
                min-width: unset;
                flex-direction: row;
                flex-wrap: wrap;
                border-right: none;
                border-bottom: 1px solid #ddd;
                max-height: 200px;
            }}
        }}
    </style>
</head>
<body>
    <div class="header">
        <span>Flyttestrømmer – Telemark</span>
        <span class="subtitle" id="header-subtitle">Fylkesnivå</span>
    </div>
    <div class="main-container">
        <div class="sidebar">
            <div class="control-group">
                <h3>Nivå</h3>
                <div class="radio-group" id="level-toggle">
                    <label class="active" data-value="fylke"><input type="radio" name="level" value="fylke" checked><span>Fylke</span></label>
                    <label data-value="kommune"><input type="radio" name="level" value="kommune"><span>Kommune</span></label>
                </div>
            </div>

            <div class="control-group" id="kommune-group" style="display:none;">
                <h3>Velg kommune</h3>
                <select id="kommune-select"></select>
            </div>

            <div class="control-group" id="kommune-options" style="display:none;">
                <h3>Kommunefilter</h3>
                <label>Vis forbindelser med minst:</label>
                <input type="range" id="threshold-slider" min="1" max="100" value="5" step="1">
                <div class="threshold-display"><span id="threshold-value">5</span> personer</div>
                <div class="checkbox-group" style="margin-top:4px;">
                    <input type="checkbox" id="internal-checkbox">
                    <label for="internal-checkbox">Vis interne flyttinger</label>
                </div>
            </div>

            <div class="control-group">
                <h3>Retning</h3>
                <select id="direction-select">
                    <option value="both">Begge retninger</option>
                    <option value="tilflytting">Tilflytting (inn)</option>
                    <option value="fraflytting">Fraflytting (ut)</option>
                </select>
            </div>

            <div class="control-group">
                <h3>Forklaring</h3>
                <div class="legend">
                    <div class="legend-item">
                        <div class="legend-line" style="background: {COLOR_TILFLYTTING};"></div>
                        <span>Tilflytting</span>
                    </div>
                    <div class="legend-item">
                        <div class="legend-line" style="background: {COLOR_FRAFLYTTING};"></div>
                        <span>Fraflytting</span>
                    </div>
                </div>
            </div>

            <div class="control-group">
                <h3>Statistikk</h3>
                <div class="stats-box" id="stats-box">
                    Laster...
                </div>
            </div>
        </div>

        <div class="right-panel">
            <div class="map-container" id="map-container">
                <div id="map"></div>
            </div>
            <div class="bottom-panels">
                <div class="sankey-container">
                    <div class="sankey-panel">
                        <h4 id="sankey-til-title">Tilflytting (inn til Telemark)</h4>
                        <div id="sankey-tilflytting" class="sankey-chart"></div>
                    </div>
                    <div class="divider"></div>
                    <div class="sankey-panel">
                        <h4 id="sankey-fra-title">Fraflytting (ut fra Telemark)</h4>
                        <div id="sankey-fraflytting" class="sankey-chart"></div>
                    </div>
                </div>
                <div class="table-container" id="top-table-container">
                    <h4 id="table-title">Topp forbindelser</h4>
                    <table class="top-table" id="top-table">
                        <thead><tr><th>Fra</th><th>Til</th><th>Antall</th></tr></thead>
                        <tbody id="top-table-body"></tbody>
                    </table>
                </div>
            </div>
        </div>
    </div>

    <script>
    // ============================================================
    // Data (embedded from Python)
    // ============================================================
    const dataFra = {data_fra_json};
    const dataTil = {data_til_json};
    const dataFraFylke = {data_fra_fylke_json};
    const dataTilFylke = {data_til_fylke_json};
    const telemarkKommuner = {telemark_list_json};
    const fylkeCoords = {fylke_coords_json};
    const telemarkGeoJSON = {telemark_geojson_json};

    const COLOR_TILFLYTTING = "{COLOR_TILFLYTTING}";
    const COLOR_FRAFLYTTING = "{COLOR_FRAFLYTTING}";
    const COLOR_TELEMARK = "{COLOR_TELEMARK}";
    const TELEMARK_LAT = {FYLKE_COORDS['Telemark'][0]};
    const TELEMARK_LON = {FYLKE_COORDS['Telemark'][1]};

    // ============================================================
    // Initialize Leaflet map
    // ============================================================
    const map = L.map('map', {{
        center: [{MAP_CENTER[0]}, {MAP_CENTER[1]}],
        zoom: {MAP_ZOOM},
        zoomControl: true,
        preferCanvas: true,
    }});

    L.tileLayer('https://{{s}}.basemaps.cartocdn.com/light_all/{{z}}/{{x}}/{{y}}{{r}}.png', {{
        attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> &copy; <a href="https://carto.com/">CARTO</a>',
        maxZoom: 19,
    }}).addTo(map);

    // Border layers
    let kommuneBorderLayer = null;
    let fylkeBorderLayer = null;

    // Add Telemark kommune borders (always available, toggled by zoom/level)
    kommuneBorderLayer = L.geoJSON(telemarkGeoJSON, {{
        style: {{
            color: '#666',
            weight: 1,
            fillColor: 'transparent',
            fillOpacity: 0,
        }},
        onEachFeature: function(feature, layer) {{
            if (feature.properties && feature.properties.kommuneNavn) {{
                layer.bindTooltip(feature.properties.kommuneNavn, {{
                    permanent: false,
                    direction: 'center',
                    className: 'flow-tooltip',
                }});
            }}
        }}
    }});

    // Load county borders from Kartverket WFS
    fetch('https://wfs.geonorge.no/skwms1/wfs.administrative_enheter?service=WFS&version=2.0.0&request=GetFeature&typeNames=fylker_gjeldende&outputFormat=application/json&srsName=EPSG:4326')
        .then(r => r.json())
        .then(data => {{
            fylkeBorderLayer = L.geoJSON(data, {{
                style: {{
                    color: '#888',
                    weight: 1.5,
                    fillColor: 'transparent',
                    fillOpacity: 0,
                    dashArray: '4 3',
                }},
                onEachFeature: function(feature, layer) {{
                    const name = feature.properties.fylkesnavn || feature.properties.navn || '';
                    if (name) {{
                        layer.bindTooltip(name, {{
                            permanent: false,
                            direction: 'center',
                            className: 'flow-tooltip',
                        }});
                    }}
                }}
            }});
            // Show county borders by default (fylke level)
            if (currentLevel === 'fylke') fylkeBorderLayer.addTo(map);
        }})
        .catch(err => console.warn('Could not load county borders:', err));

    let flowLayer = L.layerGroup().addTo(map);
    let markerLayer = L.layerGroup().addTo(map);

    // ============================================================
    // Populate municipality dropdown
    // ============================================================
    const kommuneSelect = document.getElementById('kommune-select');
    telemarkKommuner.forEach(k => {{
        const opt = document.createElement('option');
        opt.value = k;
        opt.textContent = k;
        if (k === 'Skien') opt.selected = true;
        kommuneSelect.appendChild(opt);
    }});

    // ============================================================
    // Level toggle logic
    // ============================================================
    const levelLabels = document.querySelectorAll('#level-toggle label');
    let currentLevel = 'fylke';

    function updateBorderLayers() {{
        if (currentLevel === 'fylke') {{
            if (kommuneBorderLayer) kommuneBorderLayer.remove();
            if (fylkeBorderLayer) fylkeBorderLayer.addTo(map);
        }} else {{
            if (fylkeBorderLayer) fylkeBorderLayer.remove();
            if (kommuneBorderLayer) kommuneBorderLayer.addTo(map);
        }}
    }}

    levelLabels.forEach(lbl => {{
        lbl.addEventListener('click', () => {{
            levelLabels.forEach(l => l.classList.remove('active'));
            lbl.classList.add('active');
            currentLevel = lbl.dataset.value;

            const kommuneGroup = document.getElementById('kommune-group');
            const kommuneOpts = document.getElementById('kommune-options');
            const subtitle = document.getElementById('header-subtitle');
            if (currentLevel === 'kommune') {{
                kommuneGroup.style.display = '';
                kommuneOpts.style.display = '';
                subtitle.textContent = 'Kommunenivå – ' + kommuneSelect.value;
            }} else {{
                kommuneGroup.style.display = 'none';
                kommuneOpts.style.display = 'none';
                subtitle.textContent = 'Fylkesnivå';
            }}
            updateBorderLayers();
            updateAll();
        }});
    }});

    // ============================================================
    // Bézier curve helper
    // ============================================================
    function bezierCurve(from, to, numPoints) {{
        numPoints = numPoints || 30;
        const [lat1, lon1] = from;
        const [lat2, lon2] = to;
        const midLat = (lat1 + lat2) / 2;
        const midLon = (lon1 + lon2) / 2;
        const dx = lon2 - lon1;
        const dy = lat2 - lat1;
        const dist = Math.sqrt(dx * dx + dy * dy);
        if (dist === 0) return [[lat1, lon1]];
        const curvature = Math.min(dist * 0.15, 1.0);
        const ctrlLat = midLat + (-dx / dist) * curvature;
        const ctrlLon = midLon + (dy / dist) * curvature;

        const points = [];
        for (let i = 0; i <= numPoints; i++) {{
            const t = i / numPoints;
            const inv = 1 - t;
            points.push([
                inv * inv * lat1 + 2 * inv * t * ctrlLat + t * t * lat2,
                inv * inv * lon1 + 2 * inv * t * ctrlLon + t * t * lon2,
            ]);
        }}
        return points;
    }}

    // ============================================================
    // Draw flow lines on the map
    // ============================================================
    function drawFlows(flows, color) {{
        if (flows.length === 0) return;
        const maxCount = Math.max(...flows.map(f => f.count));

        flows.forEach(f => {{
            const weight = 1.5 + (f.count / maxCount) * 5;
            const opacity = 0.35 + (f.count / maxCount) * 0.45;
            const pts = bezierCurve([f.fromLat, f.fromLon], [f.toLat, f.toLon]);

            const line = L.polyline(pts, {{
                color: color,
                weight: weight,
                opacity: opacity,
                smoothFactor: 1,
                lineCap: 'round',
            }});
            line.bindTooltip(f.label, {{ sticky: true, className: 'flow-tooltip' }});
            flowLayer.addLayer(line);

            // Small circle at destination end as direction indicator
            const endPt = pts[pts.length - 1];
            const dot = L.circleMarker(endPt, {{
                radius: Math.max(3, weight * 0.6),
                fillColor: color,
                color: color,
                weight: 0,
                fillOpacity: opacity,
            }});
            dot.bindTooltip(f.label, {{ sticky: true, className: 'flow-tooltip' }});
            flowLayer.addLayer(dot);
        }});
    }}

    // ============================================================
    // Draw location markers
    // ============================================================
    function drawMarker(name, lat, lon, totalIn, totalOut) {{
        const marker = L.circleMarker([lat, lon], {{
            radius: 5,
            fillColor: COLOR_TELEMARK,
            color: 'white',
            weight: 1.5,
            fillOpacity: 0.9,
        }});
        marker.bindTooltip(
            `<strong>${{name}}</strong><br>Inn: ${{totalIn.toLocaleString('nb-NO')}}<br>Ut: ${{totalOut.toLocaleString('nb-NO')}}`,
            {{ direction: 'top', offset: [0, -6] }}
        );
        markerLayer.addLayer(marker);
    }}

    // ============================================================
    // Top table builder
    // ============================================================
    function buildTopTable(tilRows, fraRows, labelField) {{
        // tilRows/fraRows: arrays of {{ from, to, count }} sorted desc
        const tbody = document.getElementById('top-table-body');
        tbody.innerHTML = '';

        const maxRows = 15;
        // Interleave: show top tilflytting then top fraflytting
        const topTil = tilRows.slice(0, maxRows);
        const topFra = fraRows.slice(0, maxRows);

        if (topTil.length > 0) {{
            const headerRow = document.createElement('tr');
            headerRow.innerHTML = '<td colspan="3" style="background:#e8f0fe;font-weight:600;color:{COLOR_TILFLYTTING};padding:4px 6px;">▶ Tilflytting (inn)</td>';
            tbody.appendChild(headerRow);
            topTil.forEach(r => {{
                const tr = document.createElement('tr');
                tr.innerHTML = `<td>${{r.from}}</td><td>${{r.to}}</td><td class="count-til">${{r.count.toLocaleString('nb-NO')}}</td>`;
                tbody.appendChild(tr);
            }});
        }}
        if (topFra.length > 0) {{
            const headerRow = document.createElement('tr');
            headerRow.innerHTML = '<td colspan="3" style="background:#fce8e4;font-weight:600;color:{COLOR_FRAFLYTTING};padding:4px 6px;">▶ Fraflytting (ut)</td>';
            tbody.appendChild(headerRow);
            topFra.forEach(r => {{
                const tr = document.createElement('tr');
                tr.innerHTML = `<td>${{r.from}}</td><td>${{r.to}}</td><td class="count-fra">${{r.count.toLocaleString('nb-NO')}}</td>`;
                tbody.appendChild(tr);
            }});
        }}
    }}

    // ============================================================
    // County-level update
    // ============================================================
    function updateFylke(direction) {{
        flowLayer.clearLayers();
        markerLayer.clearLayers();

        const tilFlows = [];
        const fraFlows = [];

        if (direction === 'both' || direction === 'tilflytting') {{
            dataTilFylke.forEach(d => {{
                if (d.Fylke === 'Telemark') return;
                tilFlows.push({{
                    fromLat: d.lat, fromLon: d.lon,
                    toLat: TELEMARK_LAT, toLon: TELEMARK_LON,
                    count: d.Antall,
                    label: `<strong>${{d.Fylke}} → Telemark</strong><br>${{d.Antall.toLocaleString('nb-NO')}} personer`,
                }});
            }});
        }}

        if (direction === 'both' || direction === 'fraflytting') {{
            dataFraFylke.forEach(d => {{
                if (d.Fylke === 'Telemark') return;
                fraFlows.push({{
                    fromLat: TELEMARK_LAT, fromLon: TELEMARK_LON,
                    toLat: d.lat, toLon: d.lon,
                    count: d.Antall,
                    label: `<strong>Telemark → ${{d.Fylke}}</strong><br>${{d.Antall.toLocaleString('nb-NO')}} personer`,
                }});
            }});
        }}

        drawFlows(tilFlows, COLOR_TILFLYTTING);
        drawFlows(fraFlows, COLOR_FRAFLYTTING);

        // Draw Telemark marker
        const totalIn = dataTilFylke.reduce((s, d) => s + d.Antall, 0);
        const totalOut = dataFraFylke.reduce((s, d) => s + d.Antall, 0);
        drawMarker('Telemark', TELEMARK_LAT, TELEMARK_LON, totalIn, totalOut);

        // Draw other county markers
        const allFylker = new Set([...dataTilFylke.map(d => d.Fylke), ...dataFraFylke.map(d => d.Fylke)]);
        allFylker.forEach(f => {{
            if (f === 'Telemark') return;
            const coords = fylkeCoords[f];
            if (!coords) return;
            const inn = dataTilFylke.find(d => d.Fylke === f);
            const ut = dataFraFylke.find(d => d.Fylke === f);
            drawMarker(f, coords[0], coords[1], inn ? inn.Antall : 0, ut ? ut.Antall : 0);
        }});

        // Stats
        updateStatsFylke(totalIn, totalOut);

        // Sankey
        buildSankeyFylke('sankey-tilflytting', dataTilFylke, 'tilflytting');
        buildSankeyFylke('sankey-fraflytting', dataFraFylke, 'fraflytting');

        document.getElementById('sankey-til-title').textContent = 'Tilflytting (inn til Telemark)';
        document.getElementById('sankey-fra-title').textContent = 'Fraflytting (ut fra Telemark)';

        // Top table
        const tilTableRows = [...dataTilFylke].filter(d => d.Fylke !== 'Telemark').sort((a,b) => b.Antall - a.Antall)
            .map(d => ({{ from: d.Fylke, to: 'Telemark', count: d.Antall }}));
        const fraTableRows = [...dataFraFylke].filter(d => d.Fylke !== 'Telemark').sort((a,b) => b.Antall - a.Antall)
            .map(d => ({{ from: 'Telemark', to: d.Fylke, count: d.Antall }}));
        document.getElementById('table-title').textContent = 'Topp forbindelser – Fylke';
        buildTopTable(tilTableRows, fraTableRows);

        map.setView([62.0, 10.0], 5);
    }}

    // ============================================================
    // Municipality-level update
    // ============================================================
    function updateKommune(kommune, direction) {{
        flowLayer.clearLayers();
        markerLayer.clearLayers();

        document.getElementById('header-subtitle').textContent = 'Kommunenivå – ' + kommune;

        const threshold = parseInt(document.getElementById('threshold-slider').value);
        const showInternal = document.getElementById('internal-checkbox').checked;
        document.getElementById('threshold-value').textContent = threshold;

        // Filter data for this kommune
        let tilData = dataTil.filter(d => d["Til kommune"] === kommune);
        let fraData = dataFra.filter(d => d["Fra kommune"] === kommune);

        // Internal filter
        if (!showInternal) {{
            tilData = tilData.filter(d => !d.internal);
            fraData = fraData.filter(d => !d.internal);
        }}

        // Threshold filter
        tilData = tilData.filter(d => d["Antall"] >= threshold);
        fraData = fraData.filter(d => d["Antall"] >= threshold);

        const tilFlows = [];
        const fraFlows = [];

        if (direction === 'both' || direction === 'tilflytting') {{
            tilData.forEach(d => {{
                tilFlows.push({{
                    fromLat: d["Fra lat"], fromLon: d["Fra lon"],
                    toLat: d["Til lat"], toLon: d["Til lon"],
                    count: d["Antall"],
                    label: `<strong>${{d["Fra kommune"]}} → ${{d["Til kommune"]}}</strong><br>${{d["Antall"]}} personer`,
                }});
            }});
        }}

        if (direction === 'both' || direction === 'fraflytting') {{
            fraData.forEach(d => {{
                fraFlows.push({{
                    fromLat: d["Fra lat"], fromLon: d["Fra lon"],
                    toLat: d["Til lat"], toLon: d["Til lon"],
                    count: d["Antall"],
                    label: `<strong>${{d["Fra kommune"]}} → ${{d["Til kommune"]}}</strong><br>${{d["Antall"]}} personer`,
                }});
            }});
        }}

        drawFlows(tilFlows, COLOR_TILFLYTTING);
        drawFlows(fraFlows, COLOR_FRAFLYTTING);

        // Draw marker for selected kommune
        const allKomm = [...dataTil.filter(d => d["Til kommune"] === kommune), ...dataFra.filter(d => d["Fra kommune"] === kommune)];
        if (allKomm.length > 0) {{
            const sample = allKomm[0];
            const kLat = sample["Til kommune"] === kommune ? sample["Til lat"] : sample["Fra lat"];
            const kLon = sample["Til kommune"] === kommune ? sample["Til lon"] : sample["Fra lon"];
            const totalIn = tilData.reduce((s, d) => s + d["Antall"], 0);
            const totalOut = fraData.reduce((s, d) => s + d["Antall"], 0);
            drawMarker(kommune, kLat, kLon, totalIn, totalOut);

            // Stats
            const label = showInternal ? kommune : kommune + ' (ekskl. intern)';
            updateStatsKommune(label, totalIn, totalOut);
        }}

        // Sankey at kommune level (uses unfiltered internal setting)
        const sankeyShowInternal = showInternal;
        buildSankeyKommune('sankey-tilflytting', dataTil, 'tilflytting', kommune, sankeyShowInternal);
        buildSankeyKommune('sankey-fraflytting', dataFra, 'fraflytting', kommune, sankeyShowInternal);

        document.getElementById('sankey-til-title').textContent = `Tilflytting (inn til ${{kommune}})`;
        document.getElementById('sankey-fra-title').textContent = `Fraflytting (ut fra ${{kommune}})`;

        // Top table
        const tilSorted = [...tilData].sort((a,b) => b["Antall"] - a["Antall"]);
        const fraSorted = [...fraData].sort((a,b) => b["Antall"] - a["Antall"]);
        const tilTableRows = tilSorted.map(d => ({{ from: d["Fra kommune"], to: d["Til kommune"], count: d["Antall"] }}));
        const fraTableRows = fraSorted.map(d => ({{ from: d["Fra kommune"], to: d["Til kommune"], count: d["Antall"] }}));
        document.getElementById('table-title').textContent = `Topp forbindelser – ${{kommune}}`;
        buildTopTable(tilTableRows, fraTableRows);

        map.setView([59.4, 9.0], 7);
    }}

    // ============================================================
    // Stats updates
    // ============================================================
    function updateStatsFylke(totalIn, totalOut) {{
        const netto = totalIn - totalOut;
        const box = document.getElementById('stats-box');
        box.innerHTML = `
            <strong>Telemark (ekskl. intern flytting)</strong><br>
            Tilflytting: <span class="stat-value">${{totalIn.toLocaleString('nb-NO')}}</span><br>
            Fraflytting: <span class="stat-value">${{totalOut.toLocaleString('nb-NO')}}</span><br>
            Netto: <span class="stat-value" style="color:${{netto >= 0 ? '#2a9d2a' : '#d63333'}}">${{netto >= 0 ? '+' : ''}}${{netto.toLocaleString('nb-NO')}}</span>
        `;
    }}

    function updateStatsKommune(label, totalIn, totalOut) {{
        const netto = totalIn - totalOut;
        const box = document.getElementById('stats-box');
        box.innerHTML = `
            <strong>${{label}}</strong><br>
            Tilflytting: <span class="stat-value">${{totalIn.toLocaleString('nb-NO')}}</span><br>
            Fraflytting: <span class="stat-value">${{totalOut.toLocaleString('nb-NO')}}</span><br>
            Netto: <span class="stat-value" style="color:${{netto >= 0 ? '#2a9d2a' : '#d63333'}}">${{netto >= 0 ? '+' : ''}}${{netto.toLocaleString('nb-NO')}}</span>
        `;
    }}

    // ============================================================
    // Sankey diagram – County level
    // ============================================================
    function buildSankeyFylke(divId, data, direction) {{
        const filtered = data.filter(d => d.Fylke !== 'Telemark' && d.Antall > 0);
        if (filtered.length === 0) {{ Plotly.purge(divId); return; }}

        const sorted = [...filtered].sort((a, b) => b.Antall - a.Antall);

        let labels, sources, targets, values, linkColors;
        const target = 'Telemark';

        if (direction === 'tilflytting') {{
            labels = [...sorted.map(d => d.Fylke), target];
            const tIdx = labels.length - 1;
            sources = sorted.map((_, i) => i);
            targets = sorted.map(() => tIdx);
            values = sorted.map(d => d.Antall);
            linkColors = sorted.map(() => 'rgba(33, 102, 172, 0.35)');
        }} else {{
            labels = [target, ...sorted.map(d => d.Fylke)];
            sources = sorted.map(() => 0);
            targets = sorted.map((_, i) => i + 1);
            values = sorted.map(d => d.Antall);
            linkColors = sorted.map(() => 'rgba(214, 96, 77, 0.35)');
        }}

        const nodeColors = labels.map(l =>
            l === target ? COLOR_TELEMARK :
            direction === 'tilflytting' ? COLOR_TILFLYTTING : COLOR_FRAFLYTTING
        );

        Plotly.newPlot(divId, [{{
            type: 'sankey', orientation: 'h',
            node: {{ pad: 8, thickness: 20, label: labels, color: nodeColors,
                     hovertemplate: '%{{label}}: %{{value}} personer<extra></extra>' }},
            link: {{ source: sources, target: targets, value: values, color: linkColors,
                     hovertemplate: '%{{source.label}} → %{{target.label}}: %{{value}}<extra></extra>' }},
        }}], {{
            margin: {{ l: 5, r: 5, t: 5, b: 5 }},
            font: {{ family: 'Segoe UI, sans-serif', size: 11 }},
            paper_bgcolor: 'transparent',
        }}, {{ responsive: true, displayModeBar: false }});
    }}

    // ============================================================
    // Sankey diagram – Municipality level
    // ============================================================
    function buildSankeyKommune(divId, rawData, direction, kommune, showInternal) {{
        let filtered;
        if (direction === 'tilflytting') {{
            filtered = rawData.filter(d => d["Til kommune"] === kommune);
        }} else {{
            filtered = rawData.filter(d => d["Fra kommune"] === kommune);
        }}
        if (!showInternal) {{
            filtered = filtered.filter(d => !d.internal);
        }}
        if (filtered.length === 0) {{ Plotly.purge(divId); return; }}

        // Aggregate by county
        const agg = {{}};
        filtered.forEach(d => {{
            const key = direction === 'tilflytting' ? d["Fra fylke"] : d["Til fylke"];
            agg[key] = (agg[key] || 0) + d["Antall"];
        }});

        let entries = Object.entries(agg).filter(([k, v]) => v > 0);
        entries.sort((a, b) => b[1] - a[1]);

        let labels, sources, targets, values, linkColors;

        if (direction === 'tilflytting') {{
            labels = [...entries.map(([k]) => k), kommune];
            const tIdx = labels.length - 1;
            sources = entries.map((_, i) => i);
            targets = entries.map(() => tIdx);
            values = entries.map(([, v]) => v);
            linkColors = entries.map(() => 'rgba(33, 102, 172, 0.35)');
        }} else {{
            labels = [kommune, ...entries.map(([k]) => k)];
            sources = entries.map(() => 0);
            targets = entries.map((_, i) => i + 1);
            values = entries.map(([, v]) => v);
            linkColors = entries.map(() => 'rgba(214, 96, 77, 0.35)');
        }}

        const nodeColors = labels.map(l =>
            l === kommune ? COLOR_TELEMARK :
            telemarkKommuner.includes(l) ? COLOR_TELEMARK :
            direction === 'tilflytting' ? COLOR_TILFLYTTING : COLOR_FRAFLYTTING
        );

        Plotly.newPlot(divId, [{{
            type: 'sankey', orientation: 'h',
            node: {{ pad: 8, thickness: 20, label: labels, color: nodeColors,
                     hovertemplate: '%{{label}}: %{{value}} personer<extra></extra>' }},
            link: {{ source: sources, target: targets, value: values, color: linkColors,
                     hovertemplate: '%{{source.label}} → %{{target.label}}: %{{value}}<extra></extra>' }},
        }}], {{
            margin: {{ l: 5, r: 5, t: 5, b: 5 }},
            font: {{ family: 'Segoe UI, sans-serif', size: 11 }},
            paper_bgcolor: 'transparent',
        }}, {{ responsive: true, displayModeBar: false }});
    }}

    // ============================================================
    // Master update
    // ============================================================
    function updateAll() {{
        const direction = document.getElementById('direction-select').value;

        if (currentLevel === 'fylke') {{
            updateFylke(direction);
        }} else {{
            const kommune = kommuneSelect.value;
            updateKommune(kommune, direction);
        }}
    }}

    // ============================================================
    // Event listeners
    // ============================================================
    kommuneSelect.addEventListener('change', () => {{
        if (currentLevel === 'kommune') updateAll();
    }});
    document.getElementById('direction-select').addEventListener('change', updateAll);
    document.getElementById('threshold-slider').addEventListener('input', updateAll);
    document.getElementById('internal-checkbox').addEventListener('change', updateAll);

    // ============================================================
    // Initial render
    // ============================================================
    updateAll();
    setTimeout(() => {{ map.invalidateSize(); }}, 200);
    </script>
</body>
</html>
"""

# ============================================================
# Step 8: Save the HTML file
# ============================================================

output_path = os.path.join(script_dir, "flyttestroemmer.html")
with open(output_path, "w", encoding="utf-8") as f:
    f.write(full_html)

print(f"\nHTML saved to: {output_path}")
print(f"Open this file in a browser to see the interactive visualization.")
