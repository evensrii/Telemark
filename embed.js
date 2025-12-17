(function() {
  'use strict';
  
  // Configuration
  const KARTLOSNING_URL = 'https://YOUR-HOSTING-URL.com'; // Replace with your actual hosting URL
  const CONTAINER_ID = 'telemark-kartlosning-embed';
  
  // Find the script tag that loaded this file
  const currentScript = document.currentScript || (function() {
    const scripts = document.getElementsByTagName('script');
    return scripts[scripts.length - 1];
  })();
  
  // Get the container element (parent of the script)
  const container = currentScript.parentElement;
  
  // Set container ID if not already set
  if (!container.id) {
    container.id = CONTAINER_ID;
  }
  
  // Create iframe for the map
  const iframe = document.createElement('iframe');
  iframe.src = KARTLOSNING_URL;
  iframe.style.width = '100%';
  iframe.style.height = '600px';
  iframe.style.border = 'none';
  iframe.style.display = 'block';
  iframe.title = 'Telemark Kartløsning - Bedrifter';
  iframe.setAttribute('allowfullscreen', '');
  
  // Make responsive
  iframe.style.minHeight = '400px';
  
  // Add loading indicator
  const loader = document.createElement('div');
  loader.style.cssText = 'text-align: center; padding: 40px; color: #005260; font-family: sans-serif;';
  loader.innerHTML = '<p>Laster kart...</p>';
  container.appendChild(loader);
  
  // Replace loader with iframe when loaded
  iframe.onload = function() {
    container.removeChild(loader);
  };
  
  container.appendChild(iframe);
  
  // Handle window resize for responsive behavior
  function adjustHeight() {
    const width = container.offsetWidth;
    if (width < 768) {
      iframe.style.height = '500px';
    } else {
      iframe.style.height = '600px';
    }
  }
  
  window.addEventListener('resize', adjustHeight);
  adjustHeight();
  
})();
