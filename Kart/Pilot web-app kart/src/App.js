import React, { useState } from 'react';
import { MapContainer, TileLayer, GeoJSON, useMap } from 'react-leaflet';
import 'leaflet/dist/leaflet.css';
import './App.css';
import LayerControl from './components/LayerControl';
import Legend from './components/Legend';
import SearchControl from './components/SearchControl';
import GeodataLayer from './components/GeodataLayer';
import CompanyInfoBox from './components/CompanyInfoBox';
import { telemarkColors } from './utils/colors';

function MapController({ center, zoom }) {
  const map = useMap();
  React.useEffect(() => {
    map.setView(center, zoom);
  }, [center, zoom, map]);
  return null;
}

function App() {
  const [activeLayers, setActiveLayers] = useState({
    geodataBedrifter: true,
  });

  const [mapCenter, setMapCenter] = useState([59.5, 9.0]);
  const [mapZoom, setMapZoom] = useState(7);
  const [selectedFeature, setSelectedFeature] = useState(null);
  const [filterMinEmployees, setFilterMinEmployees] = useState(false);
  const [companyStats, setCompanyStats] = useState({ count: 0, totalEmployees: 0 });

  const layers = [
    {
      id: 'geodataBedrifter',
      name: 'Bedrifter (Geodata)',
      isMarkerLayer: true,
      legend: {
        title: 'Bedrifter i Telemark',
        items: [
          { color: telemarkColors.nype, label: 'Bedrift' },
        ]
      }
    }
  ];

  const handleLayerToggle = (layerId) => {
    setActiveLayers(prev => ({
      ...prev,
      [layerId]: !prev[layerId]
    }));
  };

  const handleFeatureClick = (feature, layer) => {
    setSelectedFeature(feature.properties);
    layer.bindPopup(`
      <div class="popup-content">
        <h3>${feature.properties.name || 'Område'}</h3>
        ${Object.entries(feature.properties)
          .filter(([key]) => key !== 'name')
          .map(([key, value]) => `<p><strong>${key}:</strong> ${value}</p>`)
          .join('')}
      </div>
    `).openPopup();
  };

  const onEachFeature = (feature, layer) => {
    layer.on({
      click: () => handleFeatureClick(feature, layer),
      mouseover: (e) => {
        e.target.setStyle({
          weight: 3,
          fillOpacity: 0.9
        });
      },
      mouseout: (e) => {
        e.target.setStyle({
          weight: 2,
          fillOpacity: 0.7
        });
      }
    });
  };

  const handleSearch = (location) => {
    setMapCenter([location.lat, location.lng]);
    setMapZoom(12);
  };

  const activeLayerObjects = layers.filter(layer => activeLayers[layer.id] && (layer.data || layer.isMarkerLayer));

  return (
    <div className="App">
      <header className="app-header">
        <h1>Telemark Pilot web-app kart</h1>
        <p>Interaktiv visualisering av geodata</p>
      </header>

      <div className="map-container">
        <MapContainer
          center={mapCenter}
          zoom={mapZoom}
          style={{ height: '100%', width: '100%' }}
          zoomControl={false}
        >
          <MapController center={mapCenter} zoom={mapZoom} />
          
          <TileLayer
            attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
            url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
          />

          
          <GeodataLayer 
            isActive={activeLayers.geodataBedrifter}
            filterTelemark={true}
            filterMinEmployees={filterMinEmployees}
            onStatsUpdate={setCompanyStats}
          />
        </MapContainer>

        <LayerControl
          layers={layers}
          activeLayers={activeLayers}
          onLayerToggle={handleLayerToggle}
        />

        <SearchControl onSearch={handleSearch} />

        {activeLayerObjects.length > 0 && (
          <Legend legends={activeLayerObjects.map(l => l.legend)} />
        )}

        <CompanyInfoBox
          companyCount={companyStats.count}
          totalEmployees={companyStats.totalEmployees}
          filterMinEmployees={filterMinEmployees}
          onFilterChange={setFilterMinEmployees}
          isVisible={activeLayers.geodataBedrifter}
        />

        {selectedFeature && (
          <div className="info-panel">
            <button 
              className="close-btn"
              onClick={() => setSelectedFeature(null)}
            >
              ×
            </button>
            <h3>Områdeinformasjon</h3>
            {Object.entries(selectedFeature).map(([key, value]) => (
              <div key={key} className="info-row">
                <span className="info-label">{key}:</span>
                <span className="info-value">{value}</span>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

export default App;
