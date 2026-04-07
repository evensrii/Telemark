// Generic utility for fetching ArcGIS Online/Server feature layers

export const fetchArcGISFeatures = async (serviceUrl, where = "1=1", outFields = "*", maxRecords = 2000) => {
  try {
    const params = new URLSearchParams({
      where: where,
      outFields: outFields,
      returnGeometry: "true",
      f: "json",
      resultRecordCount: maxRecords.toString(),
      outSR: "4326" // WGS84 for Leaflet
    });

    const url = `${serviceUrl}/query?${params.toString()}`;
    console.log('Fetching from ArcGIS:', serviceUrl);
    
    const response = await fetch(url);
    
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }
    
    const data = await response.json();
    
    if (data.error) {
      console.error('ArcGIS API error:', data.error);
      throw new Error(`ArcGIS API error: ${data.error.message}`);
    }

    console.log('ArcGIS Response:', data.features?.length || 0, 'features');
    return convertEsriToGeoJSON(data);
  } catch (error) {
    console.error('Error fetching ArcGIS features:', error);
    throw error;
  }
};

// Simple Douglas-Peucker algorithm for polygon simplification
const simplifyCoordinates = (coords, tolerance = 0.001) => {
  if (coords.length <= 2) return coords;
  
  // Find point with maximum distance from line between first and last
  let maxDist = 0;
  let maxIndex = 0;
  const first = coords[0];
  const last = coords[coords.length - 1];
  
  for (let i = 1; i < coords.length - 1; i++) {
    const dist = perpendicularDistance(coords[i], first, last);
    if (dist > maxDist) {
      maxDist = dist;
      maxIndex = i;
    }
  }
  
  // If max distance is greater than tolerance, recursively simplify
  if (maxDist > tolerance) {
    const left = simplifyCoordinates(coords.slice(0, maxIndex + 1), tolerance);
    const right = simplifyCoordinates(coords.slice(maxIndex), tolerance);
    return left.slice(0, -1).concat(right);
  } else {
    return [first, last];
  }
};

const perpendicularDistance = (point, lineStart, lineEnd) => {
  const [x, y] = point;
  const [x1, y1] = lineStart;
  const [x2, y2] = lineEnd;
  
  const A = x - x1;
  const B = y - y1;
  const C = x2 - x1;
  const D = y2 - y1;
  
  const dot = A * C + B * D;
  const lenSq = C * C + D * D;
  const param = lenSq !== 0 ? dot / lenSq : -1;
  
  let xx, yy;
  
  if (param < 0) {
    xx = x1;
    yy = y1;
  } else if (param > 1) {
    xx = x2;
    yy = y2;
  } else {
    xx = x1 + param * C;
    yy = y1 + param * D;
  }
  
  const dx = x - xx;
  const dy = y - yy;
  return Math.sqrt(dx * dx + dy * dy);
};

const convertEsriToGeoJSON = (esriData, simplify = false) => {
  if (!esriData.features || esriData.features.length === 0) {
    console.log('No features in ArcGIS response');
    return {
      type: "FeatureCollection",
      features: []
    };
  }

  const features = esriData.features.map(feature => {
    const { attributes, geometry } = feature;
    
    if (!geometry) {
      return null;
    }

    // Handle different geometry types
    let geojsonGeometry;
    
    if (geometry.rings) {
      // Polygon - simplify if requested
      const rings = simplify 
        ? geometry.rings.map(ring => simplifyCoordinates(ring, 0.001))
        : geometry.rings;
      
      geojsonGeometry = {
        type: "Polygon",
        coordinates: rings
      };
    } else if (geometry.paths) {
      // Polyline
      geojsonGeometry = {
        type: "LineString",
        coordinates: geometry.paths[0]
      };
    } else if (geometry.x !== undefined && geometry.y !== undefined) {
      // Point
      geojsonGeometry = {
        type: "Point",
        coordinates: [geometry.x, geometry.y]
      };
    } else {
      return null;
    }

    return {
      type: "Feature",
      properties: attributes,
      geometry: geojsonGeometry
    };
  }).filter(f => f !== null);

  return {
    type: "FeatureCollection",
    features: features
  };
};

// PM10 specific function with pagination to fetch all features
export const fetchPM10Data = async () => {
  const serviceUrl = "https://services-eu1.arcgis.com/IT3fpqRm0QNhnnqL/arcgis/rest/services/Årsmiddelkonsentrasjon_av_PM10_i_perioden_2019___2023_WFL1/FeatureServer/0";
  console.log('fetchPM10Data called - fetching all features with pagination');
  
  try {
    // First, get the total count
    const countUrl = `${serviceUrl}/query?where=1=1&returnCountOnly=true&f=json`;
    const countResponse = await fetch(countUrl);
    const countData = await countResponse.json();
    const totalCount = countData.count;
    console.log(`Total PM10 features: ${totalCount}`);
    
    // Fetch in batches
    const batchSize = 2000;
    const allFeatures = [];
    
    for (let offset = 0; offset < totalCount; offset += batchSize) {
      console.log(`Fetching PM10 batch: ${offset} to ${offset + batchSize} of ${totalCount}`);
      
      const params = new URLSearchParams({
        where: "1=1",
        outFields: "*",
        returnGeometry: "true",
        f: "json",
        resultOffset: offset.toString(),
        resultRecordCount: batchSize.toString(),
        outSR: "4326"
      });
      
      const response = await fetch(`${serviceUrl}/query?${params.toString()}`);
      const data = await response.json();
      
      if (data.features) {
        allFeatures.push(...data.features);
      }
    }
    
    console.log(`PM10 data fetched: ${allFeatures.length} total features`);
    console.log('Simplifying geometries for better performance...');
    
    // Convert to GeoJSON with simplification enabled
    const geojson = convertEsriToGeoJSON({ features: allFeatures }, true);
    console.log('Geometries simplified');
    return geojson;
  } catch (error) {
    console.error('Error in fetchPM10Data:', error);
    throw error;
  }
};
