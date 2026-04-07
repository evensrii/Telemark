# Deploying to Episerver CMS

This guide explains how to integrate the Telemark Pilot web-app kart React application into your Episerver (Optimizely) CMS website.

## Overview

There are three main approaches to integrate this React app into Episerver:

### **Option 1: Build and Embed (Recommended)**
Build the React app as static files and embed them in an Episerver page or block.

### **Option 2: iFrame Integration**
Host the React app separately and embed it via iFrame in Episerver.

### **Option 3: React Component in Episerver**
Integrate React directly into Episerver's frontend build process.

---

## Option 1: Build and Embed (Recommended)

This is the simplest approach for most use cases.

### Step 1: Build the React Application

```bash
# Navigate to the project folder
cd "c:\Users\eve1509\OneDrive - Telemark fylkeskommune\Github\Telemark\Kart\Pilot web-app kart"

# Install dependencies (if not already done)
npm install

# Create production build
npm run build
```

This creates a `build` folder with optimized static files:
- `build/index.html`
- `build/static/js/*.js`
- `build/static/css/*.css`
- `build/static/media/*`

### Step 2: Upload to Episerver

**Option A: Upload to Episerver Media Library**
1. In Episerver CMS, go to **Assets** (Media Library)
2. Create a new folder: `kartlosning` or similar
3. Upload all files from the `build` folder maintaining the folder structure
4. Note the URL path to the uploaded files

**Option B: Deploy to Web Server**
1. Copy the entire `build` folder to your web server
2. Place it in a publicly accessible directory (e.g., `/wwwroot/kartlosning/`)
3. Ensure the folder is accessible via URL (e.g., `https://telemark.no/kartlosning/`)

### Step 3: Create Episerver Page or Block

**Create a Content Block:**

1. In Episerver, create a new **XHTML String** property or **Content Area** block
2. Add the following HTML code:

```html
<!-- Pilot web-app kart Container -->
<div id="kartlosning-root" style="width: 100%; height: 600px;"></div>

<!-- Load CSS -->
<link rel="stylesheet" href="/kartlosning/static/css/main.[hash].css">

<!-- Load JavaScript -->
<script src="/kartlosning/static/js/main.[hash].js"></script>

<!-- Initialize -->
<script>
  // The React app will automatically mount to #root or body
  // You may need to adjust based on your build output
</script>
```

**Note:** Replace `[hash]` with the actual hash from your build files (e.g., `main.abc123.js`).

### Step 4: Configure Environment Variables

Since the app uses environment variables (`.env`), you need to set them before building:

**Before running `npm run build`:**

1. Ensure `.env` file exists with your Geodata token:
```
REACT_APP_GEODATA_TOKEN=your-token-here
```

2. The build process will embed these values into the JavaScript bundle.

**Security Note:** The token will be visible in the built JavaScript. For production, consider:
- Using a server-side proxy to hide the token
- Implementing token refresh mechanism
- Restricting token permissions to read-only

---

## Option 2: iFrame Integration

Host the React app on a separate domain/subdomain and embed it in Episerver.

### Step 1: Deploy React App

Deploy the built app to:
- A subdomain: `https://kart.telemark.no`
- A subfolder: `https://telemark.no/kart/`
- A separate hosting service (Netlify, Vercel, Azure Static Web Apps)

### Step 2: Embed in Episerver

Add an iFrame to your Episerver page:

```html
<iframe 
  src="https://kart.telemark.no" 
  width="100%" 
  height="600px" 
  frameborder="0"
  title="Telemark Pilot web-app kart"
  style="border: none; min-height: 600px;">
</iframe>
```

**Pros:**
- Complete isolation from Episerver
- Easy to update independently
- No CMS conflicts

**Cons:**
- Requires separate hosting
- iFrame limitations (scrolling, responsiveness)

---

## Option 3: React in Episerver Frontend

Integrate React into Episerver's build process (advanced).

This requires:
1. Episerver project with modern frontend build (Webpack/Vite)
2. Installing React dependencies in Episerver project
3. Importing and rendering the map component

**Example (simplified):**

```javascript
// In Episerver's frontend JavaScript
import React from 'react';
import ReactDOM from 'react-dom';
import App from './kartlosning/App';

// Mount on specific pages
if (document.getElementById('kartlosning-container')) {
  ReactDOM.render(
    <App />,
    document.getElementById('kartlosning-container')
  );
}
```

This approach requires coordination with your Episerver development team.

---

## Recommended Deployment Steps

### For Quick Integration:

1. **Build the app:**
   ```bash
   npm run build
   ```

2. **Upload to Episerver:**
   - Upload `build` folder contents to `/wwwroot/pilot-web-app-kart/`

3. **Create Episerver block:**
   - Add HTML block with script references
   - Set container height (e.g., 600px)

4. **Add to page:**
   - Insert block into desired page
   - Publish and test

### For Production:

1. **Set up CI/CD pipeline:**
   - Automate builds when code changes
   - Deploy to production server automatically

2. **Configure CDN:**
   - Serve static assets via CDN for better performance
   - Enable caching for JS/CSS files

3. **Monitor performance:**
   - Track loading times
   - Monitor API usage (Geodata calls)

---

## Troubleshooting

### Map doesn't load
- Check browser console for errors
- Verify all static files are accessible
- Ensure Geodata token is valid

### Styling conflicts
- The app uses scoped CSS, but check for conflicts with Episerver styles
- Consider adding CSS namespace or iframe isolation

### CORS errors
- If hosting separately, ensure CORS headers are configured
- Geodata API should allow cross-origin requests

### Performance issues
- Consider lazy loading the map component
- Implement loading indicators
- Cache Geodata responses

---

## Environment Variables in Production

The app requires:
```
REACT_APP_GEODATA_TOKEN=your-token-here
```

**For production builds:**
1. Set environment variable before build
2. Or use build-time configuration
3. Or implement server-side proxy for API calls

---

## Support

For questions about:
- **React app:** Contact development team
- **Episerver integration:** Contact CMS team
- **Geodata API:** Check Geodata documentation

---

## Next Steps

1. Test the build locally: `npm run build && npx serve -s build`
2. Upload to test environment in Episerver
3. Verify functionality
4. Deploy to production
5. Monitor and optimize
