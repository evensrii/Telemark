// Test script to verify Geodata API connection
// Run this in browser console to test the API

const testGeodataAPI = async () => {
  const token = process.env.REACT_APP_GEODATA_TOKEN;
  console.log('Token from env:', token ? `${token.substring(0, 10)}...` : 'MISSING');
  
  const url = "https://services.geodataonline.no/arcgis/rest/services/Geomap_UTM33_EUREF89/GeomapBedrifter/FeatureServer/0/query";
  
  const params = new URLSearchParams({
    where: "1=1",
    outFields: "*",
    returnGeometry: "true",
    f: "json",
    token: token,
    resultRecordCount: "10"
  });
  
  const testUrl = `${url}?${params.toString()}`;
  console.log('Test URL (without token):', testUrl.replace(token, 'TOKEN_HIDDEN'));
  
  try {
    const response = await fetch(testUrl);
    console.log('Response status:', response.status);
    
    const data = await response.json();
    console.log('Response data:', data);
    
    if (data.error) {
      console.error('API Error:', data.error);
    } else if (data.features) {
      console.log('Success! Found', data.features.length, 'features');
      console.log('First feature:', data.features[0]);
    }
  } catch (error) {
    console.error('Fetch error:', error);
  }
};

export default testGeodataAPI;
