import React, { useEffect, useState } from 'react';
import { Marker, Popup, useMap } from 'react-leaflet';
import L from 'leaflet';
import { fetchGeodataCompanies, filterCompaniesByTelemark } from '../utils/geodataApi';
import { telemarkColors } from '../utils/colors';

delete L.Icon.Default.prototype._getIconUrl;
L.Icon.Default.mergeOptions({
  iconRetinaUrl: require('leaflet/dist/images/marker-icon-2x.png'),
  iconUrl: require('leaflet/dist/images/marker-icon.png'),
  shadowUrl: require('leaflet/dist/images/marker-shadow.png'),
});

const createCompanyIcon = (color) => {
  return L.divIcon({
    className: 'custom-company-marker',
    html: `<div style="
      background-color: ${color};
      width: 10px;
      height: 10px;
      border-radius: 50%;
      border: 2px solid white;
      box-shadow: 0 0 4px rgba(0,0,0,0.4);
    "></div>`,
    iconSize: [10, 10],
    iconAnchor: [5, 5],
  });
};

function GeodataLayer({ isActive, filterTelemark = true, filterMinEmployees = false, onStatsUpdate }) {
  const [companies, setCompanies] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const map = useMap();

  useEffect(() => {
    if (!isActive) {
      setCompanies([]);
      return;
    }

    const loadCompanies = async () => {
      setLoading(true);
      setError(null);
      
      try {
        const bounds = map.getBounds();
        const data = await fetchGeodataCompanies(bounds, 1000);
        
        // Apply employee filter if enabled
        let filteredCompanies = data.features;
        if (filterMinEmployees) {
          filteredCompanies = data.features.filter(company => {
            const employees = company.properties.antallAnsatte || company.properties.firantansatt || 0;
            return employees >= 5;
          });
        }
        
        // Calculate statistics
        const totalEmployees = filteredCompanies.reduce((sum, company) => {
          const employees = company.properties.antallAnsatte || company.properties.firantansatt || 0;
          return sum + (typeof employees === 'number' ? employees : parseInt(employees) || 0);
        }, 0);
        
        setCompanies(filteredCompanies);
        
        // Update parent with statistics
        if (onStatsUpdate) {
          onStatsUpdate({
            count: filteredCompanies.length,
            totalEmployees: totalEmployees
          });
        }
        
        console.log(`Loaded ${filteredCompanies.length} Telemark companies from Geodata (${totalEmployees.toLocaleString('nb-NO')} ansatte)`);
      } catch (err) {
        console.error('Failed to load Geodata companies:', err);
        setError(err.message);
      } finally {
        setLoading(false);
      }
    };

    loadCompanies();

    const handleMoveEnd = () => {
      loadCompanies();
    };

    map.on('moveend', handleMoveEnd);

    return () => {
      map.off('moveend', handleMoveEnd);
    };
  }, [isActive, map, filterTelemark, filterMinEmployees, onStatsUpdate]);

  if (!isActive) return null;
  if (loading && companies.length === 0) {
    return null;
  }
  if (error) {
    console.error('Geodata layer error:', error);
    return null;
  }

  return (
    <>
      {companies.map((company, idx) => {
        const [lng, lat] = company.geometry.coordinates;
        const props = company.properties;
        
        return (
          <Marker
            key={`${props.orgnr}-${idx}`}
            position={[lat, lng]}
            icon={createCompanyIcon(telemarkColors.nype)}
          >
            <Popup>
              <div style={{ minWidth: '200px' }}>
                <h3 style={{ margin: '0 0 0.5rem 0', color: telemarkColors.vann }}>
                  {props.navn || 'Ukjent bedrift'}
                </h3>
                {props.orgnr && (
                  <p style={{ margin: '0.2rem 0', fontSize: '0.85rem' }}>
                    <strong>Org.nr:</strong> {props.orgnr}
                  </p>
                )}
                {props.adresse && (
                  <p style={{ margin: '0.2rem 0', fontSize: '0.85rem' }}>
                    <strong>Adresse:</strong> {props.adresse}
                  </p>
                )}
                {(props.postnr || props.poststed) && (
                  <p style={{ margin: '0.2rem 0', fontSize: '0.85rem' }}>
                    <strong>Poststed:</strong> {props.postnr} {props.poststed}
                  </p>
                )}
                {props.kommune && (
                  <p style={{ margin: '0.2rem 0', fontSize: '0.85rem' }}>
                    <strong>Kommune:</strong> {props.kommune}
                  </p>
                )}
                {props.naeringsnavn && (
                  <p style={{ margin: '0.2rem 0', fontSize: '0.85rem' }}>
                    <strong>Næring:</strong> {props.naeringsnavn}
                  </p>
                )}
                {props.antallAnsatte && (
                  <p style={{ margin: '0.2rem 0', fontSize: '0.85rem' }}>
                    <strong>Ansatte:</strong> {props.antallAnsatte}
                  </p>
                )}
                {props.organisasjonsform && (
                  <p style={{ margin: '0.2rem 0', fontSize: '0.85rem' }}>
                    <strong>Org.form:</strong> {props.organisasjonsform}
                  </p>
                )}
              </div>
            </Popup>
          </Marker>
        );
      })}
    </>
  );
}

export default GeodataLayer;
