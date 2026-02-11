# Veiledning: Pilot web-app kart med React, Leaflet og ArcGIS/Geodata

Dette dokumentet beskriver prosessen og de tekniske løsningene for å lage interaktive kartløsninger basert på React og Leaflet, med data fra ArcGIS/Geodata, deployet til GitHub Pages og innbygget i Episerver.

---

## 📋 Innholdsfortegnelse

1. [Oversikt over prosessen](#oversikt-over-prosessen)
2. [Tekniske forutsetninger](#tekniske-forutsetninger)
3. [Prosjektstruktur](#prosjektstruktur)
4. [API-integrasjon](#api-integrasjon)
5. [Testing og utvikling](#testing-og-utvikling)
6. [Deployment til GitHub Pages](#deployment-til-github-pages)
7. [Innbygging i Episerver](#innbygging-i-episerver)
8. [Oppdatering og vedlikehold](#oppdatering-og-vedlikehold)

---

## Oversikt over prosessen

Prosessen for å lage en kartapplikasjon består av følgende hovedsteg:

1. **Oppsett**: Opprett React-prosjekt og installer nødvendige pakker (Leaflet, React-Leaflet, gh-pages)
2. **Utvikling**: Bygg kartkomponenter og integrer API-er for datainnhenting
3. **Testing**: Test lokalt under utvikling
4. **Bygging**: Bygg produksjonsversjon med optimaliserte filer
5. **Deployment**: Deploy til GitHub Pages
6. **Innbygging**: Bygg inn i Episerver via "div id=..."-metoden.

---

## Tekniske forutsetninger

### Verktøy og tilganger
- Node.js og npm installert
- Git konfigurert med GitHub-tilgang
- Geodata/ArcGIS API-token
- Tilgang til Episerver CMS

### Miljøvariabler
API-tokens må lagres sikkert i `.env`-fil:
- Alle React-miljøvariabler må starte med `REACT_APP_`
- `.env`-filen må legges til i `.gitignore` for å unngå at tokens lastes opp til GitHub
- Opprett `.env.example` som mal for andre utviklere

---

## Prosjektstruktur

### Mappeorganisering
Prosjektet organiseres i følgende struktur:

**Components** (`src/components/`):
- Kartlag-komponenter (f.eks. GeodataLayer) for å hente og vise data
- Kontrollpaneler (LayerControl, Legend, SearchControl)
- Informasjonsbokser for statistikk og filtrering

**Utilities** (`src/utils/`):
- API-hjelpefunksjoner for Geodata/ArcGIS
- Styling-funksjoner for kartlag
- Fargepaletter (Telemark fylkeskommune-farger)

**Hovedfiler**:
- `App.js`: Hovedkomponent med kart, state management og kontroller
- `index.js`: Inngangspunkt for React-appen

### Viktige tekniske komponenter
- **MapContainer**: Leaflet-kart med sentrum og zoom-nivå
- **TileLayer**: Bakgrunnskart (f.eks. OpenStreetMap)
- **Marker/Polygon-komponenter**: For å vise geografiske data
- **State management**: Håndtere aktive lag, filtre og statistikk

---

## API-integrasjon

### ArcGIS REST API
GeodataOnline og ArcGIS Online tilbyr REST API-er for å hente geografiske data:

**Viktige parametere**:
- `where`: SQL-lignende filtrering (f.eks. `fylkesnavn='Telemark'`)
- `outFields`: Hvilke felt som skal returneres (`*` for alle)
- `returnGeometry`: Om geometri skal inkluderes (true/false)
- `token`: API-token for autentisering
- `outSR`: Koordinatsystem (4326 = WGS84 for Leaflet)
- `resultRecordCount`: Maks antall resultater per spørring

### Datakonvertering
ArcGIS returnerer data i ESRI JSON-format som må konverteres til GeoJSON:
- ESRI JSON bruker `rings` for polygoner, `paths` for linjer, `x/y` for punkter
- GeoJSON bruker `coordinates`-array med [longitude, latitude]
- Konverteringsfunksjon håndterer alle geometrityper

### Server-side filtrering
Filtrer data på API-nivå for bedre ytelse:
- Bruk `where`-parameter for å begrense resultater
- Eksempel: `fylkesnavn='Telemark' AND antallAnsatte >= 5`
- Reduserer datamengde og forbedrer lastetid

### Paginering for store datasett
For datasett med >2000 features:
- Bruk `resultOffset` og `resultRecordCount` for å hente i batches
- Kombiner alle batches til ett GeoJSON-objekt
- Implementer geometri-simplifikasjon (Douglas-Peucker) for bedre ytelse

---

## Testing og utvikling

### Lokal testing
Under utvikling kjøres `npm start` for å starte utviklingsserver på `localhost:3000`.

**Testpunkter**:
- Verifiser at kart lastes med riktig sentrum og zoom
- Sjekk at data hentes fra API (se Network-fane i utviklerverktøy)
- Test at markører/polygoner vises korrekt
- Verifiser interaktivitet (klikk, hover, popup)
- Test responsivt design på ulike skjermstørrelser

### Produksjonsbygg
Før deployment bygges produksjonsversjon med `npm run build`.

**Konfigurasjon i package.json**:
- `homepage`: Full URL til hvor appen skal være tilgjengelig
- `predeploy`: Bygger automatisk før deployment
- `deploy`: Deployer til GitHub Pages med `-e [subfolder]` for undermapper

**Build-output**:
- Optimaliserte JavaScript- og CSS-filer
- Minifisert kode for raskere lasting
- Source maps for debugging (valgfritt)

---

## Deployment til GitHub Pages

### Prosess
Deployment gjøres med kommandoen `npm run deploy` som:
1. Bygger produksjonsversjon automatisk
2. Oppretter/oppdaterer `gh-pages` branch i repository
3. Pusher bygget til GitHub
4. Gjør appen tilgjengelig på GitHub Pages etter 1-2 minutter

### GitHub Pages-innstillinger
I repository settings → Pages:
- Velg `gh-pages` som source branch (opprettes automatisk ved første deploy)
- Appen blir tilgjengelig på URL spesifisert i `homepage`-feltet

### Undermapper
For å ha flere kartapplikasjoner på samme GitHub Pages-site:
- Bruk `-e [subfolder]` i deploy-script
- Eksempel: `-e kart-bedrifter` gir URL `/Telemark/kart-bedrifter`
- Hver kartapplikasjon får sin egen undermappe

### Oppdateringer
Ved endringer i koden:
- Kjør `npm run deploy` på nytt
- Nye versjonen er live etter 1-2 minutter
- Brukere kan trenge hard refresh (Ctrl+Shift+R) for å se endringer

---

## Innbygging i Episerver

### Iframe-metode
Kartapplikasjonen bygges inn i Episerver via "div id=..." Dette fungerer utmerket i en egen blokk i Episerver!

**Grunnleggende embed-kode**:
```html
<div id="kartlosning-container">
  <iframe 
    src="https://evensrii.github.io/Telemark/[subfolder]" 
    style="width: 100%; height: 600px; border: none;"
    title="Pilot web-app kart"
    allowfullscreen>
  </iframe>
</div>
```

### Implementering i Episerver
1. Opprett ny HTML/Tekst-blokk i Episerver
2. Lim inn embed-koden med riktig URL
3. Tilpass styling etter behov (høyde, bredde, border-radius, etc.)
4. Lagre og publiser

### Fordeler med "div id=..."-metode
- **Isolasjon**: Kartapplikasjonen påvirkes ikke av Episerver-styling
- **Enkel oppdatering**: Endringer i kartapplikasjonen reflekteres automatisk
- **Responsivt**: Fungerer på desktop og mobil
- **Ingen CMS-tilpasninger**: Krever ikke spesielle Episerver-tilganger

### Styling
CSS kan legges til for å tilpasse utseende:
- Maks bredde og sentrering
- Border-radius for avrundede hjørner
- Box-shadow for dybdeeffekt
- Media queries for responsivt design

---

## Oppdatering og vedlikehold

### Oppdateringsprosess
1. Gjør endringer i koden
2. Test lokalt med `npm start`
3. Deploy med `npm run deploy`
4. Verifiser på live-URL etter 1-2 minutter

### Versjonskontroll
- Commit endringer til Git med beskrivende meldinger
- Push til GitHub for backup og samarbeid
- Oppdater versjonsnummer i `package.json` ved større endringer

### Vedlikehold
- Oppdater npm-pakker jevnlig for sikkerhet og nye funksjoner
- Sjekk at API-tokens er gyldige
- Overvåk ytelse og optimaliser ved behov
- Test på ulike nettlesere og enheter

---

## Vanlige utfordringer og løsninger

### Kart vises ikke
- Sjekk at Leaflet CSS er importert
- Verifiser kartsentrum og zoom-nivå
- Kontroller at TileLayer er inkludert

### API-data lastes ikke
- Verifiser at token er satt i `.env`
- Sjekk API-URL og parametere
- Test API-kall direkte i nettleser
- Sjekk nettverksfane i utviklerverktøy

### 404-feil på GitHub Pages
- Verifiser `homepage`-URL i `package.json`
- Sjekk at GitHub Pages bruker `gh-pages` branch
- Vent 5 minutter etter deployment

### Treg ytelse
- Implementer server-side filtrering
- Bruk paginering for store datasett
- Implementer geometri-simplifikasjon (Douglas-Peucker)
- Vurder clustering for mange markører

---

## Beste praksis

### Sikkerhet
- Lagre API-tokens i `.env`-fil (ikke commit til Git)
- Bruk read-only tokens
- Roter tokens jevnlig

### Ytelse
- Filtrer data på server-side når mulig
- Optimaliser geometri for store datasett
- Bruk lazy loading for data
- Minimer antall API-kall

### Kode-kvalitet
- Bruk meningsfulle variabel- og funksjonnavn
- Kommenter kompleks logikk
- Følg React best practices
- Oppdater dokumentasjon ved endringer

---

## Nyttige ressurser

- **React**: https://react.dev/
- **Leaflet**: https://leafletjs.com/
- **React-Leaflet**: https://react-leaflet.js.org/
- **ArcGIS REST API**: https://developers.arcgis.com/rest/
- **Geodata dokumentasjon**: https://dokumentasjon.geodataonline.no/
- **GitHub Pages**: https://pages.github.com/

---

**Sist oppdatert:** 17. desember 2024  
**Versjon:** 1.0  
**Forfatter:** Telemark fylkeskommune - Utviklingsteam
