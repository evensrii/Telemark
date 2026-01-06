const GEODATA_CONFIG = {
  url: "https://services.geodataonline.no/arcgis/rest/services/Geomap_UTM33_EUREF89/GeomapBedrifter/FeatureServer/0/query",
  token: process.env.REACT_APP_GEODATA_TOKEN
};

export const fetchGeodataCompanies = async (bounds = null, maxRecords = 1000) => {
  try {
    console.log('Geodata API Token:', GEODATA_CONFIG.token ? 'Token present' : 'Token missing');
    
    const params = new URLSearchParams({
      where: "fylfylkenavn='Telemark'",
      outFields: "*",
      returnGeometry: "true",
      f: "json",
      token: GEODATA_CONFIG.token,
      resultRecordCount: maxRecords.toString()
    });

    if (bounds) {
      const { _southWest, _northEast } = bounds;
      params.append('geometry', JSON.stringify({
        xmin: _southWest.lng,
        ymin: _southWest.lat,
        xmax: _northEast.lng,
        ymax: _northEast.lat,
        spatialReference: { wkid: 4326 }
      }));
      params.append('geometryType', 'esriGeometryEnvelope');
      params.append('inSR', '4326');
    }

    params.append('outSR', '4326');

    const url = `${GEODATA_CONFIG.url}?${params.toString()}`;
    console.log('Fetching from Geodata API...');
    console.log('Bounds:', bounds ? 'Using map bounds' : 'No bounds (all data)');
    
    const response = await fetch(url);
    
    if (!response.ok) {
      console.error('HTTP error response:', response.status, response.statusText);
      throw new Error(`HTTP error! status: ${response.status}`);
    }
    
    const data = await response.json();
    console.log('API Response:', data);
    
    if (data.error) {
      console.error('Geodata API error:', data.error);
      throw new Error(`Geodata API error: ${data.error.message}`);
    }

    const geojson = convertToGeoJSON(data);
    console.log('Converted to GeoJSON, features:', geojson.features.length);
    return geojson;
  } catch (error) {
    console.error('Error fetching Geodata companies:', error);
    throw error;
  }
};

const convertToGeoJSON = (esriData) => {
  if (!esriData.features || esriData.features.length === 0) {
    console.log('No features in API response');
    return {
      type: "FeatureCollection",
      features: []
    };
  }
  
  console.log('Converting', esriData.features.length, 'features from ESRI format');

  const features = esriData.features.map(feature => {
    const { attributes, geometry } = feature;
    
    if (!geometry || !geometry.x || !geometry.y) {
      return null;
    }

    return {
      type: "Feature",
      properties: {
        orgnr: attributes.firorgnr || attributes.ORGNR || attributes.orgnr,
        navn: attributes.firfirmanavn1 || attributes.NAVN || attributes.navn || attributes.name,
        adresse: attributes.firgateadresse || attributes.ADRESSE || attributes.adresse,
        postnr: attributes.firgatepnr || attributes.POSTNR || attributes.postnr,
        poststed: attributes.gatepoststed || attributes.POSTSTED || attributes.poststed,
        kommune: attributes.knrkommnavn || attributes.kommunenavn || attributes.KOMMUNE || attributes.kommune,
        naeringskode: attributes.nakkode1 || attributes.NAERINGSKODE || attributes.naeringskode,
        naeringsnavn: attributes.naktittel1 || attributes.NAERINGSNAVN || attributes.naeringsnavn,
        antallAnsatte: attributes.firantansatt || attributes.ANTALL_ANSATTE || attributes.antall_ansatte || attributes.antallAnsatte,
        organisasjonsform: attributes.orfkode || attributes.ORGANISASJONSFORM || attributes.organisasjonsform,
        status: attributes.stfstatusfirmaid || attributes.STATUS || attributes.status,
        ...attributes
      },
      geometry: {
        type: "Point",
        coordinates: [geometry.x, geometry.y]
      }
    };
  }).filter(f => f !== null);

  return {
    type: "FeatureCollection",
    features: features
  };
};

export const filterCompaniesByTelemark = (geojson) => {
  // Log first few fylke values to debug
  if (geojson.features.length > 0) {
    console.log('Sample fylke values:', geojson.features.slice(0, 5).map(f => 
      f.properties.fylkesnavn || f.properties.fylfylkenavn || 'NO FYLKE'
    ));
  }

  const filtered = {
    ...geojson,
    features: geojson.features.filter(feature => {
      const fylke = feature.properties.fylkesnavn || feature.properties.fylfylkenavn || '';
      const match = fylke === 'Telemark' || fylke.includes('Telemark');
      return match;
    })
  };
  
  console.log('Filtered to Telemark:', filtered.features.length, 'companies');
  return filtered;
};
