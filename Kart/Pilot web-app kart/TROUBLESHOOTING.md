# Troubleshooting: Bedrifter Layer Not Showing

## Problem
The "Bedrifter (Geodata)" layer is checked but no company markers appear on the map.

## Most Likely Cause
**Environment variable not loaded** - React requires a restart after creating or modifying `.env` files.

## Solution

### Step 1: Stop the Development Server
Press `Ctrl+C` in the terminal where `npm start` is running.

### Step 2: Verify .env File
Check that `c:\Users\eve1509\OneDrive - Telemark fylkeskommune\Github\Telemark\Kart\Pilot web-app kart\.env` contains:
```
REACT_APP_GEODATA_TOKEN=alm9T3FRLOXU7oEELqWZyp8uDqK8Tr_lPYeEssbtcFg.
```

### Step 3: Restart the Server
```bash
npm start
```

### Step 4: Check Browser Console
1. Open the map application
2. Press `F12` to open Developer Tools
3. Click on "Console" tab
4. Enable the "Bedrifter (Geodata)" layer
5. Look for these messages:
   - `Geodata API Token: Token present` ✅ (Good)
   - `Geodata API Token: Token missing` ❌ (Bad - restart didn't work)
   - `Fetching from Geodata API...`
   - `API Response: {...}`
   - `Converted to GeoJSON, features: X`
   - `Filtered to Telemark: X companies`

## Other Possible Issues

### Issue 2: API Token Invalid
**Symptoms:** Console shows error like "Invalid token" or 401/403 status
**Solution:** Verify the token is correct in `.env` file

### Issue 3: No Companies in View
**Symptoms:** Console shows "Filtered to Telemark: 0 companies"
**Solution:** 
- Zoom in to Skien, Porsgrunn, or other Telemark municipalities
- The filter might be too strict - check console for unfiltered count

### Issue 4: CORS Error
**Symptoms:** Console shows "CORS policy" error
**Solution:** This shouldn't happen with Geodata API, but if it does, contact Geodata support

## Debug Commands

Open browser console and run:
```javascript
// Check if token is loaded
console.log('Token:', process.env.REACT_APP_GEODATA_TOKEN ? 'Present' : 'Missing');

// Test API directly
fetch('https://services.geodataonline.no/arcgis/rest/services/Geomap_UTM33_EUREF89/GeomapBedrifter/FeatureServer/0/query?where=1=1&outFields=*&f=json&resultRecordCount=1&token=' + process.env.REACT_APP_GEODATA_TOKEN)
  .then(r => r.json())
  .then(d => console.log('API Test:', d));
```

## Expected Behavior

When working correctly, you should see:
1. Orange markers (🔴) appearing on the map in Telemark municipalities
2. Clicking a marker shows company information popup
3. Console shows successful API calls with feature counts
4. Markers update when you pan/zoom the map
