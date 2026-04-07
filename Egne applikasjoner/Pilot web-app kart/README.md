# Telemark Pilot web-app kart

En interaktiv kartapplikasjon for visualisering av geodata bygget med React og Leaflet.

## Funksjoner

- **Interaktivt kart** med zoom, pan og klikk-funksjonalitet
- **Flere kartlag** som kan slås av og på:
  - Svekket is (basert på NVE data)
  - Befolkningstetthet
  - Arealbruk
  - Bedrifter (live data fra Geodata Online)
- **Tegnforklaring** som viser fargekodering for aktive lag
- **Søkefunksjon** for å navigere til spesifikke steder i Telemark
- **Informasjonspanel** som viser detaljer når du klikker på områder
- **Responsiv design** som fungerer på desktop og mobil

## Teknologi

- **React 18** - UI-rammeverk
- **Leaflet** - Kartbibliotek
- **React-Leaflet** - React-integrasjon for Leaflet
- **OpenStreetMap** - Basiskart

## Installasjon

### Forutsetninger

- Node.js (versjon 14 eller nyere)
- npm eller yarn

### Steg 1: Installer avhengigheter

```bash
cd "Kart\Pilot web-app kart"
npm install
```

### Steg 2: Konfigurer miljøvariabler

Kopier `.env.example` til `.env` og legg inn ditt Geodata API-token:

```bash
cp .env.example .env
```

Rediger `.env` og erstatt `your-geodata-token-here` med ditt faktiske token fra Geodata Online.

**VIKTIG:** `.env`-filen er lagt til i `.gitignore` og vil ikke bli lastet opp til GitHub. Dette beskytter API-tokenet ditt.

### Steg 3: Start utviklingsserver

```bash
npm start
```

Applikasjonen åpnes automatisk i nettleseren på `http://localhost:3000`

## Bruk

### Navigering
- **Zoom**: Bruk musehjulet eller +/- knappene på kartet
- **Pan**: Klikk og dra kartet
- **Søk**: Skriv inn et stedsnavn i søkefeltet øverst

### Kartlag
- Klikk på "Kartlag" i venstre hjørne for å vise/skjule lag
- Huk av for lagene du vil se på kartet
- Flere lag kan være aktive samtidig

### Tegnforklaring
- Tegnforklaringen vises nederst til høyre
- Den oppdateres automatisk basert på aktive lag
- Klikk på headeren for å minimere/maksimere

### Områdeinformasjon
- Klikk på et område på kartet for å se detaljer
- Informasjon vises i en popup og i et sidepanel
- Klikk X for å lukke sidepanelet

## Prosjektstruktur

```
Pilot web-app kart/
├── public/
│   └── index.html          # HTML-mal
├── src/
│   ├── components/         # React-komponenter
│   │   ├── LayerControl.js # Kontroll for kartlag
│   │   ├── Legend.js       # Tegnforklaring
│   │   └── SearchControl.js # Søkefunksjonalitet
│   ├── data/
│   │   └── sampleData.js   # Eksempeldata (GeoJSON)
│   ├── utils/
│   │   └── mapStyles.js    # Styling-funksjoner for kartlag
│   ├── App.js              # Hovedkomponent
│   ├── App.css             # Hovedstyling
│   └── index.js            # Entry point
├── package.json            # Avhengigheter
└── README.md              # Denne filen
```

## Tilpasse data

### Legge til nye kartlag

1. **Opprett GeoJSON-data** i `src/data/sampleData.js`:
```javascript
export const myNewData = {
  type: "FeatureCollection",
  features: [...]
};
```

2. **Opprett styling-funksjon** i `src/utils/mapStyles.js`:
```javascript
export const getMyNewStyle = (feature) => {
  return {
    fillColor: '#color',
    weight: 2,
    opacity: 1,
    color: 'white',
    fillOpacity: 0.7
  };
};
```

3. **Legg til laget** i `App.js` i `layers`-arrayet:
```javascript
{
  id: 'myNewLayer',
  name: 'Mitt nye lag',
  data: myNewData,
  style: getMyNewStyle,
  legend: {
    title: 'Tittel',
    items: [...]
  }
}
```

### Bruke eksterne datakilder

#### Geodata Online API

Applikasjonen inkluderer integrasjon med Geodata Online for bedriftsdata:

```javascript
// src/utils/geodataApi.js
const GEODATA_CONFIG = {
  url: "https://services.geodataonline.no/arcgis/rest/services/Geomap_UTM33_EUREF89/GeomapBedrifter/FeatureServer/0/query",
  token: process.env.REACT_APP_GEODATA_TOKEN
};
```

**Sikkerhet:** API-tokenet lagres i `.env`-filen som er ekskludert fra Git via `.gitignore`.

Bedriftslaget:
- Henter data dynamisk basert på kartutsnitt
- Filtrerer automatisk på Telemark-kommuner
- Viser bedriftsinformasjon i popup ved klikk
- Oppdateres når kartet flyttes

#### WMS-tjenester

For å bruke WMS-tjenester eller eksterne GeoJSON-filer:

```javascript
import { WMSTileLayer } from 'react-leaflet';

// I MapContainer:
<WMSTileLayer
  url="https://example.com/wms"
  layers="layer_name"
  format="image/png"
  transparent={true}
/>
```

## Bygging for produksjon

```bash
npm run build
```

Dette oppretter en optimalisert produksjonsversjon i `build/`-mappen.

## Deployment

Applikasjonen kan deployes til:
- **Netlify**: Dra og slipp `build`-mappen
- **GitHub Pages**: Bruk `gh-pages` pakke
- **Azure Static Web Apps**
- **Vercel**

## Videre utvikling

### Forslag til forbedringer:
- [ ] Integrasjon med NVE sine faktiske WMS-tjenester
- [ ] Eksport av kartutsnitt som bilde
- [ ] Tegne-verktøy for å markere områder
- [ ] Tidslinje for å vise historiske data
- [ ] Sammenligning av flere lag side-ved-side
- [ ] Utskriftsfunksjon
- [ ] Lagre favoritt-visninger
- [ ] Dele kart-visninger via URL

## Lisens

Dette prosjektet er utviklet for Telemark fylkeskommune.

## Kontakt

For spørsmål eller support, kontakt IT-avdelingen.
