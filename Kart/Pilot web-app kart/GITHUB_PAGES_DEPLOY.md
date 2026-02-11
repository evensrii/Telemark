# Deploying to GitHub Pages

Your React map app is now configured to deploy to GitHub Pages at:
**https://evensrii.github.io/Telemark/kart-bedrifter**

## 🚀 Quick Deploy

Run these commands in your terminal:

```bash
# Navigate to the project folder
cd "c:\Users\eve1509\OneDrive - Telemark fylkeskommune\Github\Telemark\Kart\Pilot web-app kart"

# Install gh-pages package (only needed once)
npm install --save-dev gh-pages

# Deploy to GitHub Pages (builds and publishes automatically)
npm run deploy
```

That's it! Your app will be live at: **https://evensrii.github.io/Telemark/kart-bedrifter**

---

## 📋 Step-by-Step Instructions

### Step 1: Install gh-pages Package

```bash
npm install --save-dev gh-pages
```

This installs the GitHub Pages deployment tool.

### Step 2: Deploy

```bash
npm run deploy
```

This command will:
1. Build your React app (`npm run build`)
2. Create a `gh-pages` branch in your repository
3. Push the built files to that branch
4. GitHub Pages automatically serves from the `gh-pages` branch

### Step 3: Wait 1-2 Minutes

GitHub Pages needs a moment to process the deployment. Then visit:
**https://evensrii.github.io/Telemark/kart-bedrifter**

---

## 🔄 Updating the App

Whenever you make changes:

```bash
# Make your code changes
# Then deploy again:
npm run deploy
```

The new version will be live in 1-2 minutes.

---

## 📝 Embed Code for Episerver

Once deployed, use this code in Episerver:

```html
<div id="telemark-kartlosning" style="max-width: 1200px; margin: 2rem auto;">
  <iframe 
    src="https://evensrii.github.io/Telemark/kart-bedrifter" 
    style="width: 100%; height: 600px; border: none; border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.1);"
    title="Telemark Pilot web-app kart - Bedrifter"
    allowfullscreen>
  </iframe>
</div>
```

### With Styling:

```html
<style>
  .kartlosning-embed {
    max-width: 1200px;
    margin: 2rem auto;
    padding: 0 1rem;
  }
  
  .kartlosning-embed iframe {
    width: 100%;
    height: 600px;
    border: none;
    border-radius: 8px;
    box-shadow: 0 2px 8px rgba(0,0,0,0.1);
  }
  
  @media (max-width: 768px) {
    .kartlosning-embed iframe {
      height: 500px;
    }
  }
</style>

<div class="kartlosning-embed">
  <h2>Bedrifter i Telemark</h2>
  <p>Utforsk bedrifter i Telemark fylke. Bruk filteret for å vise kun bedrifter med 5 eller flere ansatte.</p>
  
  <iframe 
    src="https://evensrii.github.io/Telemark/kart-bedrifter" 
    title="Telemark Pilot web-app kart - Bedrifter"
    allowfullscreen>
  </iframe>
</div>
```

---

## 🔧 Troubleshooting

### "gh-pages not found"
Run: `npm install --save-dev gh-pages`

### Deployment fails
- Ensure you're in the correct directory
- Check that you have Git configured
- Verify you have push access to the repository

### App shows blank page
- Check browser console (F12) for errors
- Verify the `homepage` field in `package.json` is correct
- Clear browser cache and try again

### Changes not showing
- Wait 2-3 minutes after deployment
- Hard refresh browser (Ctrl+Shift+R or Cmd+Shift+R)
- Check GitHub Pages settings to confirm it's enabled

---

## ✅ Verification Checklist

After running `npm run deploy`:

- [ ] Command completes without errors
- [ ] Visit https://evensrii.github.io/Telemark/kart-bedrifter
- [ ] Map loads and displays companies
- [ ] Info box shows company statistics
- [ ] Filter checkbox works
- [ ] Test on mobile device

---

## 🔐 Security Note

The Geodata API token is embedded in the built JavaScript files. This is acceptable because:
- The token is read-only
- It's already visible in your Python scripts
- Geodata API has rate limiting

For production, consider:
- Rotating the token periodically
- Monitoring API usage
- Implementing a server-side proxy if needed

---

## 📊 GitHub Pages Settings

Your current settings (from screenshot):
- ✅ GitHub Pages is enabled
- ✅ Site is live at https://evensrii.github.io/Telemark/
- ✅ Deploying from `main` branch
- ✅ HTTPS enforced

After running `npm run deploy`, a new `gh-pages` branch will be created automatically.

---

## 🎯 Next Steps

1. **Deploy now:**
   ```bash
   npm install --save-dev gh-pages
   npm run deploy
   ```

2. **Test the live site:**
   Visit https://evensrii.github.io/Telemark/kart-bedrifter

3. **Add to Episerver:**
   Copy the embed code above

4. **Share with colleagues:**
   The map is now publicly accessible!

---

## 💡 Tips

- **Automatic updates:** Just run `npm run deploy` after changes
- **Version control:** The `gh-pages` branch is managed automatically
- **Custom domain:** You can configure a custom domain in GitHub Pages settings
- **Analytics:** Consider adding Google Analytics to track usage

---

## 🆘 Need Help?

If deployment fails, check:
1. Git is installed and configured
2. You have push access to the repository
3. The `.env` file exists with your Geodata token
4. All dependencies are installed (`npm install`)

Common error solutions:
- **Permission denied:** Check GitHub authentication
- **Build fails:** Run `npm run build` separately to see errors
- **404 error:** Wait 2-3 minutes for GitHub Pages to update
