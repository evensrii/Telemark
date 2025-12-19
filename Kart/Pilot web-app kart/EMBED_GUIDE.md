# Embedding Telemark Pilot web-app kart (Everviz-Style)

This guide shows how to embed the map application in your Episerver website using the same method as Everviz charts.

## 🎯 Recommended Approach: Script Injection

This is the best solution for your situation because:
- Same access level as Everviz embeds
- No special CMS permissions needed
- Responsive and mobile-friendly
- Easy to update independently

---

## 📋 Step-by-Step Deployment

### **Step 1: Build the Application**

```bash
# Navigate to project folder
cd "c:\Users\eve1509\OneDrive - Telemark fylkeskommune\Github\Telemark\Kart\Pilot web-app kart"

# Ensure environment variables are set
# Check that .env file contains:
# REACT_APP_GEODATA_TOKEN=your-token-here

# Build production version
npm run build
```

This creates a `build` folder with all necessary files.

---

### **Step 2: Choose a Hosting Platform**

You need to host the built app somewhere. Here are your best options:

#### **Option A: Netlify (Recommended - Easiest)**

1. Go to https://www.netlify.com
2. Sign up with GitHub account (free)
3. Click "Add new site" → "Deploy manually"
4. Drag and drop the entire `build` folder
5. Netlify gives you a URL like: `https://telemark-kartlosning.netlify.app`
6. (Optional) Configure custom domain: `kart.telemark.no`

**Pros:**
- Free tier is generous
- Automatic HTTPS
- Fast CDN
- Easy updates (just drag new build folder)

#### **Option B: GitHub Pages (Free)**

```bash
# Install gh-pages
npm install -g gh-pages

# Deploy to GitHub Pages
npm run build
gh-pages -d build
```

Your app will be at: `https://evensrii.github.io/Telemark/`

**Pros:**
- Completely free
- Integrated with your GitHub repo
- Automatic updates via GitHub Actions

#### **Option C: Azure Static Web Apps**

If Telemark fylkeskommune has Azure subscription:

1. Go to Azure Portal
2. Create "Static Web App"
3. Connect to GitHub repository
4. Configure build settings
5. Get URL like: `https://telemark-kart.azurestaticapps.net`

**Pros:**
- Enterprise-grade
- Better security controls
- Integration with Azure services

---

### **Step 3: Get Your Embed Code**

Once hosted, you'll have a URL. Use one of these embed methods:

#### **Method 1: Simple iFrame (Easiest)**

```html
<div id="telemark-kartlosning" style="width: 100%; max-width: 1200px; margin: 0 auto;">
  <iframe 
    src="https://YOUR-HOSTING-URL.com" 
    style="width: 100%; height: 600px; border: none; display: block;"
    title="Telemark Pilot web-app kart - Bedrifter"
    allowfullscreen>
  </iframe>
</div>
```

**Replace `YOUR-HOSTING-URL.com` with your actual URL from Step 2.**

#### **Method 2: Script Injection (Like Everviz)**

First, you need to host the `embed.js` file alongside your app.

**Embed code for Episerver:**

```html
<div id="telemark-kartlosning-xyz"></div>
<script src="https://YOUR-HOSTING-URL.com/embed.js" defer></script>
```

This requires creating a custom `embed.js` script (see Advanced Setup below).

---

### **Step 4: Add to Episerver**

1. Log into Episerver CMS
2. Navigate to the page where you want the map
3. Add a new **HTML/Text block** or **XHTML String** property
4. Paste the embed code from Step 3
5. Save and publish

**Example in Episerver:**

```html
<div class="kartlosning-container">
  <h2>Bedrifter i Telemark</h2>
  <p>Utforsk bedrifter i Telemark fylke. Klikk på markørene for mer informasjon.</p>
  
  <div id="telemark-kartlosning">
    <iframe 
      src="https://telemark-kartlosning.netlify.app" 
      style="width: 100%; height: 600px; border: none; border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.1);"
      title="Telemark Pilot web-app kart - Bedrifter">
    </iframe>
  </div>
</div>
```

---

## 🎨 Styling Tips

### Make it responsive:

```html
<style>
  .kartlosning-container {
    max-width: 1200px;
    margin: 2rem auto;
    padding: 0 1rem;
  }
  
  .kartlosning-container iframe {
    width: 100%;
    height: 600px;
    border: none;
    border-radius: 8px;
    box-shadow: 0 2px 8px rgba(0,0,0,0.1);
  }
  
  @media (max-width: 768px) {
    .kartlosning-container iframe {
      height: 500px;
    }
  }
</style>
```

---

## 🔄 Updating the Map

When you make changes to the map:

1. **Build new version:**
   ```bash
   npm run build
   ```

2. **Deploy update:**
   - **Netlify:** Drag new `build` folder to Netlify dashboard
   - **GitHub Pages:** Run `gh-pages -d build`
   - **Azure:** Push to GitHub (auto-deploys)

3. **Clear cache (if needed):**
   - Users may need to refresh (Ctrl+F5)
   - Or add version parameter: `?v=2`

---

## 🔒 Security Considerations

### Geodata API Token

The token is embedded in the built JavaScript. To protect it:

1. **Use read-only token** (already done)
2. **Restrict token to specific domains** (if Geodata supports it)
3. **Monitor API usage** for abuse
4. **Rotate token periodically**

### CORS and Security Headers

If hosting on external domain, ensure:
- HTTPS is enabled (Netlify/GitHub Pages do this automatically)
- CORS headers allow embedding in Episerver
- Content Security Policy allows the iframe

---

## 📊 Recommended Setup for Telemark

Based on your needs, I recommend:

### **Quick Setup (Today):**
1. Build app: `npm run build`
2. Deploy to Netlify (5 minutes)
3. Get URL: `https://telemark-kartlosning.netlify.app`
4. Add to Episerver:
   ```html
   <div id="telemark-kartlosning">
     <iframe src="https://telemark-kartlosning.netlify.app" 
             style="width: 100%; height: 600px; border: none;">
     </iframe>
   </div>
   ```

### **Production Setup (Long-term):**
1. Coordinate with IT department
2. Set up Azure Static Web App
3. Configure custom domain: `kart.telemark.no`
4. Set up CI/CD pipeline for automatic updates
5. Implement monitoring and analytics

---

## 🆘 Troubleshooting

### Map doesn't load
- Check browser console (F12) for errors
- Verify hosting URL is accessible
- Check if Geodata token is valid

### iFrame shows scrollbars
- Add `scrolling="no"` to iframe
- Adjust height to fit content
- Use `overflow: hidden` in CSS

### Mobile display issues
- Ensure responsive height in CSS
- Test on different devices
- Consider min-height: 400px

### CORS errors
- Hosting platform should handle this
- Netlify/GitHub Pages work out of the box
- If self-hosting, configure CORS headers

---

## 📞 Next Steps

1. **Choose hosting platform** (Netlify recommended)
2. **Build and deploy** the app
3. **Test the embed code** in a test page
4. **Add to production** Episerver page
5. **Monitor and iterate**

---

## 💡 Example: Complete Episerver Block

```html
<!-- Bedriftskart Container -->
<div class="bedriftskart-section">
  <div class="container">
    <h2>Bedrifter i Telemark</h2>
    <p class="lead">
      Utforsk over 10 000 bedrifter i Telemark fylke. 
      Bruk filteret for å vise kun bedrifter med 5 eller flere ansatte.
    </p>
    
    <div class="map-embed">
      <iframe 
        src="https://telemark-kartlosning.netlify.app" 
        width="100%" 
        height="600" 
        frameborder="0"
        title="Interaktivt bedriftskart for Telemark"
        allowfullscreen>
      </iframe>
    </div>
    
    <p class="map-info">
      <small>
        Data fra Geodata Online. Oppdateres automatisk.
        <a href="https://github.com/evensrii/Telemark/tree/main/Kart/Pilot%20web-app%20kart">Kildekode</a>
      </small>
    </p>
  </div>
</div>

<style>
  .bedriftskart-section {
    padding: 3rem 0;
    background: #f8f9fa;
  }
  
  .map-embed {
    margin: 2rem 0;
    border-radius: 8px;
    overflow: hidden;
    box-shadow: 0 4px 12px rgba(0,0,0,0.1);
  }
  
  .map-embed iframe {
    display: block;
  }
  
  .map-info {
    text-align: center;
    color: #666;
    margin-top: 1rem;
  }
  
  @media (max-width: 768px) {
    .map-embed iframe {
      height: 500px;
    }
  }
</style>
```

---

## ✅ Checklist

- [ ] Build app with `npm run build`
- [ ] Choose hosting platform
- [ ] Deploy build folder
- [ ] Get hosting URL
- [ ] Test URL in browser
- [ ] Create embed code
- [ ] Add to Episerver test page
- [ ] Test on desktop and mobile
- [ ] Publish to production
- [ ] Monitor performance

---

**Need help?** Contact your IT department or the development team for assistance with hosting setup.
