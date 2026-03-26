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

# Telemark fylke color palette (for Sankey node coloring)
TELEMARK_PALETTE = [
    "#1C6C6C",  # Hav
    "#14828C",  # Fjord
    "#009BC2",  # Himmel
    "#1F9562",  # Gress
    "#2F7542",  # Gran
    "#A5983A",  # Korn
    "#7B7B7A",  # Stein
    "#727062",  # Berg
    "#8A6C3E",  # Strand
    "#BC7726",  # Siv
    "#996954",  # Bark
    "#B7173D",  # Nype
    "#5A2E61",  # Plomme
    "#414681",  # Blåveis
]

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
# Step 5: Prepare data as JSON for embedding in HTML
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
palette_json = json.dumps(TELEMARK_PALETTE, ensure_ascii=False)

# ============================================================
# Step 6: Generate the full interactive HTML
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
            font-size: 14px;
            background: #f5f5f5;
            color: #333;
        }}
        .header {{
            background: {COLOR_TELEMARK};
            color: white;
            padding: 12px 24px;
            font-size: 20px;
            font-weight: 600;
            display: flex;
            align-items: center;
            gap: 12px;
        }}
        .header .subtitle {{
            font-size: 14px;
            font-weight: 400;
            opacity: 0.85;
        }}
        .main-container {{
            display: flex;
            height: calc(100vh - 50px);
        }}
        /* LEFT SIDEBAR */
        .sidebar {{
            width: 280px;
            min-width: 280px;
            background: white;
            border-right: 1px solid #ddd;
            padding: 16px;
            overflow-y: auto;
            display: flex;
            flex-direction: column;
            gap: 12px;
        }}
        .sidebar h3 {{
            font-size: 12px;
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
            font-size: 13px;
            font-weight: 500;
            color: #555;
        }}
        .control-group select {{
            width: 100%;
            padding: 6px 8px;
            border: 1px solid #ccc;
            border-radius: 4px;
            font-size: 13px;
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
            font-size: 13px;
        }}
        .checkbox-group input {{
            width: 16px;
            height: 16px;
            cursor: pointer;
        }}
        .radio-group {{
            display: flex;
            gap: 2px;
        }}
        .radio-group label {{
            flex: 1;
            text-align: center;
            padding: 7px 4px;
            border: 1px solid #ccc;
            font-size: 13px;
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
            font-size: 12px;
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
            font-size: 13px;
            line-height: 1.7;
        }}
        .stats-box .stat-value {{
            font-weight: 700;
            color: {COLOR_TELEMARK};
        }}
        .threshold-display {{
            font-size: 12px;
            color: #888;
            text-align: right;
        }}
        /* CENTER MAP */
        .center-panel {{
            flex: 1;
            position: relative;
            min-width: 300px;
        }}
        #map {{
            width: 100%;
            height: 100%;
        }}
        /* RIGHT PANEL: table + sankeys stacked */
        .right-panel {{
            width: 380px;
            min-width: 380px;
            background: white;
            border-left: 1px solid #ddd;
            display: flex;
            flex-direction: column;
            overflow: hidden;
        }}
        .table-container {{
            overflow-y: auto;
            padding: 8px 10px;
            flex: 0 0 auto;
            max-height: 220px;
            border-bottom: 1px solid #eee;
        }}
        .table-container h4 {{
            font-size: 13px;
            color: #555;
            margin-bottom: 6px;
            text-align: center;
        }}
        .top-table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 12px;
        }}
        .top-table th {{
            background: #f0f0f0;
            padding: 5px 6px;
            text-align: left;
            font-weight: 600;
            border-bottom: 1px solid #ddd;
            position: sticky;
            top: 0;
        }}
        .top-table td {{
            padding: 4px 6px;
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
        .sankey-panel {{
            flex: 1;
            min-height: 0;
            display: flex;
            flex-direction: column;
            padding: 4px 6px;
            border-bottom: 1px solid #eee;
        }}
        .sankey-panel:last-child {{
            border-bottom: none;
        }}
        .sankey-panel h4 {{
            text-align: center;
            font-size: 13px;
            color: #555;
            padding: 2px 0;
            flex-shrink: 0;
        }}
        .sankey-chart {{
            flex: 1;
            min-height: 0;
        }}
        .flow-tooltip {{
            background: white !important;
            border: 1px solid #ccc !important;
            border-radius: 4px !important;
            padding: 6px 10px !important;
            font-family: 'Segoe UI', sans-serif !important;
            font-size: 13px !important;
            box-shadow: 0 2px 6px rgba(0,0,0,0.15) !important;
        }}
        .map-label {{
            background: none !important;
            border: none !important;
            box-shadow: none !important;
            font-size: 11px;
            font-weight: 600;
            color: #333;
            white-space: nowrap;
            text-shadow: 1px 1px 2px white, -1px -1px 2px white, 1px -1px 2px white, -1px 1px 2px white;
        }}
        @media (max-width: 1100px) {{
            .right-panel {{
                width: 320px;
                min-width: 320px;
            }}
        }}
        @media (max-width: 900px) {{
            .main-container {{
                flex-direction: column;
            }}
            .sidebar {{
                width: 100%;
                min-width: unset;
                border-right: none;
                border-bottom: 1px solid #ddd;
                max-height: 200px;
            }}
            .right-panel {{
                width: 100%;
                min-width: unset;
                height: 400px;
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
        <!-- LEFT SIDEBAR -->
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

            <div class="control-group" id="cluster-group" style="display:none;">
                <h3>Klyngeradius</h3>
                <label>Slå sammen nærliggende:</label>
                <input type="range" id="cluster-slider" min="0" max="100" value="30" step="5">
                <div class="threshold-display"><span id="cluster-value">30</span> %</div>
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

        <!-- CENTER MAP -->
        <div class="center-panel" id="map-container">
            <div id="map"></div>
        </div>

        <!-- RIGHT PANEL: table then sankeys -->
        <div class="right-panel">
            <div class="table-container" id="top-table-container">
                <h4 id="table-title">Topp forbindelser</h4>
                <table class="top-table" id="top-table">
                    <thead><tr><th>Fra</th><th>Til</th><th>Antall</th></tr></thead>
                    <tbody id="top-table-body"></tbody>
                </table>
            </div>
            <div class="sankey-panel">
                <h4 id="sankey-til-title">Tilflytting (inn til Telemark)</h4>
                <div id="sankey-tilflytting" class="sankey-chart"></div>
            </div>
            <div class="sankey-panel">
                <h4 id="sankey-fra-title">Fraflytting (ut fra Telemark)</h4>
                <div id="sankey-fraflytting" class="sankey-chart"></div>
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
    const PALETTE = {palette_json};

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

    let flowLayer = L.layerGroup().addTo(map);
    let markerLayer = L.layerGroup().addTo(map);
    let labelLayer = L.layerGroup().addTo(map);

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

    levelLabels.forEach(lbl => {{
        lbl.addEventListener('click', () => {{
            levelLabels.forEach(l => l.classList.remove('active'));
            lbl.classList.add('active');
            currentLevel = lbl.dataset.value;

            const kommuneGroup = document.getElementById('kommune-group');
            const kommuneOpts = document.getElementById('kommune-options');
            const clusterGroup = document.getElementById('cluster-group');
            const subtitle = document.getElementById('header-subtitle');
            if (currentLevel === 'kommune') {{
                kommuneGroup.style.display = '';
                kommuneOpts.style.display = '';
                clusterGroup.style.display = '';
                subtitle.textContent = 'Kommunenivå – ' + kommuneSelect.value;
            }} else {{
                kommuneGroup.style.display = 'none';
                kommuneOpts.style.display = 'none';
                clusterGroup.style.display = 'none';
                subtitle.textContent = 'Fylkesnivå';
            }}
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
    // Distance-based clustering for municipality flows
    // ============================================================
    function clusterFlows(flows, clusterPct) {{
        // clusterPct: 0 = no clustering, 100 = max clustering
        if (clusterPct === 0 || flows.length <= 1) return flows;

        // Base radius in degrees; scale with distance from Telemark
        const baseRadius = 0.1 + (clusterPct / 100) * 0.5;
        const distFactor = 0.02 + (clusterPct / 100) * 0.08;

        // Sort by count desc so largest is the cluster representative
        const sorted = [...flows].sort((a, b) => b.count - a.count);
        const used = new Array(sorted.length).fill(false);
        const clustered = [];

        for (let i = 0; i < sorted.length; i++) {{
            if (used[i]) continue;
            const anchor = sorted[i];

            // Distance from this point to Telemark
            const dLat = anchor.fromLat - TELEMARK_LAT;
            const dLon = anchor.fromLon - TELEMARK_LON;
            const distToTelemark = Math.sqrt(dLat * dLat + dLon * dLon);
            const radius = baseRadius + distFactor * distToTelemark;

            let totalCount = anchor.count;
            let wLat = anchor.fromLat * anchor.count;
            let wLon = anchor.fromLon * anchor.count;
            const names = [anchor.name || ''];
            used[i] = true;

            for (let j = i + 1; j < sorted.length; j++) {{
                if (used[j]) continue;
                const other = sorted[j];
                const dx = anchor.fromLat - other.fromLat;
                const dy = anchor.fromLon - other.fromLon;
                const d = Math.sqrt(dx * dx + dy * dy);
                if (d <= radius) {{
                    totalCount += other.count;
                    wLat += other.fromLat * other.count;
                    wLon += other.fromLon * other.count;
                    names.push(other.name || '');
                    used[j] = true;
                }}
            }}

            const clusterLabel = names.length > 1
                ? names[0] + ' m.fl. (' + names.length + ')'
                : names[0];

            clustered.push({{
                fromLat: wLat / totalCount,
                fromLon: wLon / totalCount,
                toLat: anchor.toLat,
                toLon: anchor.toLon,
                count: totalCount,
                name: names.length > 1 ? names[0] + ' m.fl.' : names[0],
                label: `<strong>${{clusterLabel}}</strong><br>${{totalCount.toLocaleString('nb-NO')}} personer`,
            }});
        }}
        return clustered;
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
    // Draw location markers with optional permanent label
    // ============================================================
    function drawMarker(name, lat, lon, totalIn, totalOut, showLabel) {{
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

        if (showLabel) {{
            const label = L.tooltip({{
                permanent: true,
                direction: 'right',
                offset: [8, 0],
                className: 'map-label',
            }});
            label.setContent(name);
            label.setLatLng([lat, lon]);
            labelLayer.addLayer(label);
        }}
    }}

    // ============================================================
    // Add data labels based on zoom level (for kommune view)
    // ============================================================
    let currentFlowLabels = []; // store {{ name, lat, lon, count }}
    function updateLabelsForZoom() {{
        labelLayer.clearLayers();
        if (currentLevel !== 'kommune' || currentFlowLabels.length === 0) return;
        const zoom = map.getZoom();
        // More labels as we zoom in: base 5 labels at zoom 7, up to all at zoom 12
        const maxLabels = Math.min(currentFlowLabels.length, Math.max(3, Math.floor(5 + (zoom - 7) * 4)));
        const sorted = [...currentFlowLabels].sort((a, b) => b.count - a.count);
        const toShow = sorted.slice(0, maxLabels);
        toShow.forEach(f => {{
            const label = L.tooltip({{
                permanent: true,
                direction: 'right',
                offset: [8, 0],
                className: 'map-label',
            }});
            label.setContent(f.name);
            label.setLatLng([f.lat, f.lon]);
            labelLayer.addLayer(label);
        }});
    }}
    map.on('zoomend', updateLabelsForZoom);

    // ============================================================
    // Top table builder
    // ============================================================
    function buildTopTable(tilRows, fraRows) {{
        const tbody = document.getElementById('top-table-body');
        tbody.innerHTML = '';
        const maxRows = 12;
        const topTil = tilRows.slice(0, maxRows);
        const topFra = fraRows.slice(0, maxRows);

        if (topTil.length > 0) {{
            const headerRow = document.createElement('tr');
            headerRow.innerHTML = '<td colspan="3" style="background:#e8f0fe;font-weight:600;color:{COLOR_TILFLYTTING};padding:5px 6px;">▶ Tilflytting (inn)</td>';
            tbody.appendChild(headerRow);
            topTil.forEach(r => {{
                const tr = document.createElement('tr');
                tr.innerHTML = `<td>${{r.from}}</td><td>${{r.to}}</td><td class="count-til">${{r.count.toLocaleString('nb-NO')}}</td>`;
                tbody.appendChild(tr);
            }});
        }}
        if (topFra.length > 0) {{
            const headerRow = document.createElement('tr');
            headerRow.innerHTML = '<td colspan="3" style="background:#fce8e4;font-weight:600;color:{COLOR_FRAFLYTTING};padding:5px 6px;">▶ Fraflytting (ut)</td>';
            tbody.appendChild(headerRow);
            topFra.forEach(r => {{
                const tr = document.createElement('tr');
                tr.innerHTML = `<td>${{r.from}}</td><td>${{r.to}}</td><td class="count-fra">${{r.count.toLocaleString('nb-NO')}}</td>`;
                tbody.appendChild(tr);
            }});
        }}
    }}

    // ============================================================
    // Assign Telemark palette colors to sorted entries
    // ============================================================
    function getPaletteColor(index) {{
        return PALETTE[index % PALETTE.length];
    }}

    function hexToRgba(hex, alpha) {{
        const r = parseInt(hex.slice(1, 3), 16);
        const g = parseInt(hex.slice(3, 5), 16);
        const b = parseInt(hex.slice(5, 7), 16);
        return `rgba(${{r}}, ${{g}}, ${{b}}, ${{alpha}})`;
    }}

    // ============================================================
    // County-level update
    // ============================================================
    function updateFylke(direction) {{
        flowLayer.clearLayers();
        markerLayer.clearLayers();
        labelLayer.clearLayers();
        currentFlowLabels = [];

        const tilFlows = [];
        const fraFlows = [];

        if (direction === 'both' || direction === 'tilflytting') {{
            dataTilFylke.forEach(d => {{
                if (d.Fylke === 'Telemark') return;
                tilFlows.push({{
                    fromLat: d.lat, fromLon: d.lon,
                    toLat: TELEMARK_LAT, toLon: TELEMARK_LON,
                    count: d.Antall,
                    name: d.Fylke,
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
                    name: d.Fylke,
                    label: `<strong>Telemark → ${{d.Fylke}}</strong><br>${{d.Antall.toLocaleString('nb-NO')}} personer`,
                }});
            }});
        }}

        drawFlows(tilFlows, COLOR_TILFLYTTING);
        drawFlows(fraFlows, COLOR_FRAFLYTTING);

        // Draw Telemark marker (always labeled)
        const totalIn = dataTilFylke.reduce((s, d) => s + d.Antall, 0);
        const totalOut = dataFraFylke.reduce((s, d) => s + d.Antall, 0);
        drawMarker('Telemark', TELEMARK_LAT, TELEMARK_LON, totalIn, totalOut, true);

        // Draw other county markers (all labeled in fylke view)
        const allFylker = new Set([...dataTilFylke.map(d => d.Fylke), ...dataFraFylke.map(d => d.Fylke)]);
        allFylker.forEach(f => {{
            if (f === 'Telemark') return;
            const coords = fylkeCoords[f];
            if (!coords) return;
            const inn = dataTilFylke.find(d => d.Fylke === f);
            const ut = dataFraFylke.find(d => d.Fylke === f);
            drawMarker(f, coords[0], coords[1], inn ? inn.Antall : 0, ut ? ut.Antall : 0, true);
        }});

        // Stats
        updateStats('Telemark (ekskl. intern flytting)', totalIn, totalOut);

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
        labelLayer.clearLayers();
        currentFlowLabels = [];

        document.getElementById('header-subtitle').textContent = 'Kommunenivå – ' + kommune;

        const threshold = parseInt(document.getElementById('threshold-slider').value);
        const showInternal = document.getElementById('internal-checkbox').checked;
        const clusterPct = parseInt(document.getElementById('cluster-slider').value);
        document.getElementById('threshold-value').textContent = threshold;
        document.getElementById('cluster-value').textContent = clusterPct;

        // Filter data for this kommune
        let tilData = dataTil.filter(d => d["Til kommune"] === kommune);
        let fraData = dataFra.filter(d => d["Fra kommune"] === kommune);

        if (!showInternal) {{
            tilData = tilData.filter(d => !d.internal);
            fraData = fraData.filter(d => !d.internal);
        }}

        tilData = tilData.filter(d => d["Antall"] >= threshold);
        fraData = fraData.filter(d => d["Antall"] >= threshold);

        // Build raw flow arrays with names
        let tilFlows = [];
        let fraFlows = [];

        if (direction === 'both' || direction === 'tilflytting') {{
            tilData.forEach(d => {{
                tilFlows.push({{
                    fromLat: d["Fra lat"], fromLon: d["Fra lon"],
                    toLat: d["Til lat"], toLon: d["Til lon"],
                    count: d["Antall"],
                    name: d["Fra kommune"],
                    label: `<strong>${{d["Fra kommune"]}} → ${{d["Til kommune"]}}</strong><br>${{d["Antall"]}} personer`,
                }});
            }});
        }}

        if (direction === 'both' || direction === 'fraflytting') {{
            fraData.forEach(d => {{
                fraFlows.push({{
                    fromLat: d["Til lat"], fromLon: d["Til lon"],
                    toLat: d["Fra lat"], toLon: d["Fra lon"],
                    count: d["Antall"],
                    name: d["Til kommune"],
                    label: `<strong>${{d["Fra kommune"]}} → ${{d["Til kommune"]}}</strong><br>${{d["Antall"]}} personer`,
                }});
            }});
        }}

        // Apply clustering
        tilFlows = clusterFlows(tilFlows, clusterPct);
        fraFlows = clusterFlows(fraFlows, clusterPct);

        drawFlows(tilFlows, COLOR_TILFLYTTING);
        drawFlows(fraFlows, COLOR_FRAFLYTTING);

        // Collect labels for zoom-responsive display
        tilFlows.forEach(f => currentFlowLabels.push({{ name: f.name, lat: f.fromLat, lon: f.fromLon, count: f.count }}));
        fraFlows.forEach(f => currentFlowLabels.push({{ name: f.name, lat: f.fromLat, lon: f.fromLon, count: f.count }}));

        // Draw marker for selected kommune
        const allKomm = [...dataTil.filter(d => d["Til kommune"] === kommune), ...dataFra.filter(d => d["Fra kommune"] === kommune)];
        if (allKomm.length > 0) {{
            const sample = allKomm[0];
            const kLat = sample["Til kommune"] === kommune ? sample["Til lat"] : sample["Fra lat"];
            const kLon = sample["Til kommune"] === kommune ? sample["Til lon"] : sample["Fra lon"];
            const totalIn = tilData.reduce((s, d) => s + d["Antall"], 0);
            const totalOut = fraData.reduce((s, d) => s + d["Antall"], 0);
            drawMarker(kommune, kLat, kLon, totalIn, totalOut, true);

            const label = showInternal ? kommune : kommune + ' (ekskl. intern)';
            updateStats(label, totalIn, totalOut);
        }}

        // Update zoom-based labels
        updateLabelsForZoom();

        // Sankey at kommune level
        buildSankeyKommune('sankey-tilflytting', dataTil, 'tilflytting', kommune, showInternal);
        buildSankeyKommune('sankey-fraflytting', dataFra, 'fraflytting', kommune, showInternal);

        document.getElementById('sankey-til-title').textContent = `Tilflytting (inn til ${{kommune}})`;
        document.getElementById('sankey-fra-title').textContent = `Fraflytting (ut fra ${{kommune}})`;

        // Top table (use unclustered data for accuracy)
        const tilSorted = [...tilData].sort((a,b) => b["Antall"] - a["Antall"]);
        const fraSorted = [...fraData].sort((a,b) => b["Antall"] - a["Antall"]);
        const tilTableRows = tilSorted.map(d => ({{ from: d["Fra kommune"], to: d["Til kommune"], count: d["Antall"] }}));
        const fraTableRows = fraSorted.map(d => ({{ from: d["Fra kommune"], to: d["Til kommune"], count: d["Antall"] }}));
        document.getElementById('table-title').textContent = `Topp forbindelser – ${{kommune}}`;
        buildTopTable(tilTableRows, fraTableRows);

        map.setView([59.4, 9.0], 7);
    }}

    // ============================================================
    // Stats update (shared)
    // ============================================================
    function updateStats(label, totalIn, totalOut) {{
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
    // Sankey diagram – County level (with palette colors)
    // ============================================================
    function buildSankeyFylke(divId, data, direction) {{
        const filtered = data.filter(d => d.Fylke !== 'Telemark' && d.Antall > 0);
        if (filtered.length === 0) {{ Plotly.purge(divId); return; }}

        const sorted = [...filtered].sort((a, b) => b.Antall - a.Antall);

        let labels, sources, targets, values, linkColors, nodeColors;
        const target = 'Telemark';

        if (direction === 'tilflytting') {{
            // Sources on left get palette colors, Telemark on right
            labels = [...sorted.map(d => d.Fylke), target];
            const tIdx = labels.length - 1;
            sources = sorted.map((_, i) => i);
            targets = sorted.map(() => tIdx);
            values = sorted.map(d => d.Antall);
            nodeColors = [...sorted.map((_, i) => getPaletteColor(i)), COLOR_TELEMARK];
            linkColors = sorted.map((_, i) => hexToRgba(getPaletteColor(i), 0.35));
        }} else {{
            // Telemark on left, destinations on right get palette colors
            labels = [target, ...sorted.map(d => d.Fylke)];
            sources = sorted.map(() => 0);
            targets = sorted.map((_, i) => i + 1);
            values = sorted.map(d => d.Antall);
            nodeColors = [COLOR_TELEMARK, ...sorted.map((_, i) => getPaletteColor(i))];
            linkColors = sorted.map((_, i) => hexToRgba(getPaletteColor(i), 0.35));
        }}

        Plotly.newPlot(divId, [{{
            type: 'sankey', orientation: 'h',
            node: {{ pad: 8, thickness: 20, label: labels, color: nodeColors,
                     hovertemplate: '%{{label}}: %{{value}} personer<extra></extra>' }},
            link: {{ source: sources, target: targets, value: values, color: linkColors,
                     hovertemplate: '%{{source.label}} → %{{target.label}}: %{{value}}<extra></extra>' }},
        }}], {{
            margin: {{ l: 5, r: 5, t: 5, b: 5 }},
            font: {{ family: 'Segoe UI, sans-serif', size: 12 }},
            paper_bgcolor: 'transparent',
        }}, {{ responsive: true, displayModeBar: false }});
    }}

    // ============================================================
    // Sankey diagram – Municipality level (with palette colors)
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

        let labels, sources, targets, values, linkColors, nodeColors;

        if (direction === 'tilflytting') {{
            labels = [...entries.map(([k]) => k), kommune];
            const tIdx = labels.length - 1;
            sources = entries.map((_, i) => i);
            targets = entries.map(() => tIdx);
            values = entries.map(([, v]) => v);
            nodeColors = [...entries.map((_, i) => getPaletteColor(i)), COLOR_TELEMARK];
            linkColors = entries.map((_, i) => hexToRgba(getPaletteColor(i), 0.35));
        }} else {{
            labels = [kommune, ...entries.map(([k]) => k)];
            sources = entries.map(() => 0);
            targets = entries.map((_, i) => i + 1);
            values = entries.map(([, v]) => v);
            nodeColors = [COLOR_TELEMARK, ...entries.map((_, i) => getPaletteColor(i))];
            linkColors = entries.map((_, i) => hexToRgba(getPaletteColor(i), 0.35));
        }}

        Plotly.newPlot(divId, [{{
            type: 'sankey', orientation: 'h',
            node: {{ pad: 8, thickness: 20, label: labels, color: nodeColors,
                     hovertemplate: '%{{label}}: %{{value}} personer<extra></extra>' }},
            link: {{ source: sources, target: targets, value: values, color: linkColors,
                     hovertemplate: '%{{source.label}} → %{{target.label}}: %{{value}}<extra></extra>' }},
        }}], {{
            margin: {{ l: 5, r: 5, t: 5, b: 5 }},
            font: {{ family: 'Segoe UI, sans-serif', size: 12 }},
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
    document.getElementById('cluster-slider').addEventListener('input', updateAll);

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
# Step 7: Save the HTML file
# ============================================================

output_path = os.path.join(script_dir, "flyttestroemmer.html")
with open(output_path, "w", encoding="utf-8") as f:
    f.write(full_html)

print(f"\nHTML saved to: {output_path}")
print(f"Open this file in a browser to see the interactive visualization.")
