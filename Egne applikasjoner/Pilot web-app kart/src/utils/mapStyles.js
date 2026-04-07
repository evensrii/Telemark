import { telemarkColors } from './colors';

export const getColorForValue = (value, ranges) => {
  for (let range of ranges) {
    if (value >= range.min && value < range.max) {
      return range.color;
    }
  }
  return ranges[ranges.length - 1].color;
};

// PM10 air quality styling based on concentration (µg/m³)
// Color scale from ArcGIS layer definition
export const getPM10Style = (feature) => {
  const pm10 = feature.properties.PM10;
  let color;

  if (pm10 < 6.39) {
    color = '#fff5f0'; // Very low
  } else if (pm10 < 8.86) {
    color = '#fee0d2'; // Low
  } else if (pm10 < 11.34) {
    color = '#fcbba1'; // Moderate-low
  } else if (pm10 < 13.81) {
    color = '#fc9272'; // Moderate
  } else if (pm10 < 16.28) {
    color = '#fb6a4a'; // Moderate-high
  } else if (pm10 < 18.75) {
    color = '#ef3b2c'; // High
  } else if (pm10 < 21.23) {
    color = '#cb181d'; // Very high
  } else if (pm10 < 23.70) {
    color = '#a50f15'; // Extremely high
  } else {
    color = '#67000d'; // Hazardous
  }

  return {
    fillColor: color,
    weight: 0,
    opacity: 0,
    color: 'transparent',
    fillOpacity: 0.7
  };
};

export const getIceWeakeningStyle = (feature) => {
  const thickness = feature.properties.thickness;
  let color;

  if (thickness < 100) {
    color = telemarkColors.vann;
  } else if (thickness < 500) {
    color = telemarkColors.fjord;
  } else if (thickness < 1000) {
    color = telemarkColors.himmel;
  } else if (thickness < 1500) {
    color = telemarkColors.gress;
  } else if (thickness < 2000) {
    color = telemarkColors.korn;
  } else if (thickness < 3000) {
    color = telemarkColors.nype;
  } else if (thickness < 4000) {
    color = telemarkColors.plomme;
  } else {
    color = telemarkColors.gran;
  }

  return {
    fillColor: color,
    weight: 2,
    opacity: 1,
    color: 'white',
    fillOpacity: 0.7
  };
};

export const getPopulationStyle = (feature) => {
  const density = feature.properties.population;
  let color;

  if (density < 50) {
    color = telemarkColors.strand;
  } else if (density < 100) {
    color = telemarkColors.siv;
  } else if (density < 200) {
    color = telemarkColors.himmel;
  } else if (density < 500) {
    color = telemarkColors.fjord;
  } else if (density < 1000) {
    color = telemarkColors.hav;
  } else {
    color = telemarkColors.vann;
  }

  return {
    fillColor: color,
    weight: 2,
    opacity: 1,
    color: 'white',
    fillOpacity: 0.7
  };
};

export const getLandUseStyle = (feature) => {
  const type = feature.properties.type;
  const colorMap = {
    'Skog': telemarkColors.gran,
    'Jordbruk': telemarkColors.gress,
    'Bebygd': telemarkColors.korn,
    'Vann': telemarkColors.fjord,
    'Annet': telemarkColors.bark
  };

  return {
    fillColor: colorMap[type] || telemarkColors.bark,
    weight: 2,
    opacity: 1,
    color: 'white',
    fillOpacity: 0.7
  };
};
