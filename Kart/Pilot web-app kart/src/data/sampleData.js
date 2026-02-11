export const iceWeakeningData = {
  type: "FeatureCollection",
  features: [
    {
      type: "Feature",
      properties: {
        name: "Nordsjø område",
        thickness: 150,
        category: "100-500",
        risk: "Moderat"
      },
      geometry: {
        type: "Polygon",
        coordinates: [[
          [8.5, 60.5],
          [9.5, 60.5],
          [9.5, 61.0],
          [8.5, 61.0],
          [8.5, 60.5]
        ]]
      }
    },
    {
      type: "Feature",
      properties: {
        name: "Telemark kyst",
        thickness: 750,
        category: "500-1000",
        risk: "Middels"
      },
      geometry: {
        type: "Polygon",
        coordinates: [[
          [8.8, 59.0],
          [9.8, 59.0],
          [9.8, 59.5],
          [8.8, 59.5],
          [8.8, 59.0]
        ]]
      }
    },
    {
      type: "Feature",
      properties: {
        name: "Skagerrak sør",
        thickness: 1250,
        category: "1000-1500",
        risk: "Høy"
      },
      geometry: {
        type: "Polygon",
        coordinates: [[
          [9.0, 58.5],
          [10.0, 58.5],
          [10.0, 59.0],
          [9.0, 59.0],
          [9.0, 58.5]
        ]]
      }
    },
    {
      type: "Feature",
      properties: {
        name: "Indre fjordområde",
        thickness: 1750,
        category: "1500-2000",
        risk: "Høy"
      },
      geometry: {
        type: "Polygon",
        coordinates: [[
          [8.0, 59.5],
          [8.8, 59.5],
          [8.8, 60.0],
          [8.0, 60.0],
          [8.0, 59.5]
        ]]
      }
    },
    {
      type: "Feature",
      properties: {
        name: "Nordøst område",
        thickness: 2500,
        category: "2000-3000",
        risk: "Svært høy"
      },
      geometry: {
        type: "Polygon",
        coordinates: [[
          [9.5, 60.0],
          [10.5, 60.0],
          [10.5, 60.5],
          [9.5, 60.5],
          [9.5, 60.0]
        ]]
      }
    },
    {
      type: "Feature",
      properties: {
        name: "Fjellområde vest",
        thickness: 3500,
        category: "3000-4000",
        risk: "Ekstrem"
      },
      geometry: {
        type: "Polygon",
        coordinates: [[
          [7.5, 59.8],
          [8.3, 59.8],
          [8.3, 60.3],
          [7.5, 60.3],
          [7.5, 59.8]
        ]]
      }
    },
    {
      type: "Feature",
      properties: {
        name: "Høyfjell nord",
        thickness: 4500,
        category: ">4000",
        risk: "Kritisk"
      },
      geometry: {
        type: "Polygon",
        coordinates: [[
          [8.0, 60.5],
          [9.0, 60.5],
          [9.0, 61.0],
          [8.0, 61.0],
          [8.0, 60.5]
        ]]
      }
    }
  ]
};

export const populationData = {
  type: "FeatureCollection",
  features: [
    {
      type: "Feature",
      properties: {
        name: "Skien sentrum",
        population: 1200,
        density: "500-1000",
        inhabitants: 35000
      },
      geometry: {
        type: "Polygon",
        coordinates: [[
          [9.58, 59.19],
          [9.64, 59.19],
          [9.64, 59.23],
          [9.58, 59.23],
          [9.58, 59.19]
        ]]
      }
    },
    {
      type: "Feature",
      properties: {
        name: "Porsgrunn sentrum",
        population: 850,
        density: "500-1000",
        inhabitants: 28000
      },
      geometry: {
        type: "Polygon",
        coordinates: [[
          [9.63, 59.12],
          [9.69, 59.12],
          [9.69, 59.16],
          [9.63, 59.16],
          [9.63, 59.12]
        ]]
      }
    },
    {
      type: "Feature",
      properties: {
        name: "Notodden",
        population: 320,
        density: "200-500",
        inhabitants: 12500
      },
      geometry: {
        type: "Polygon",
        coordinates: [[
          [9.23, 59.54],
          [9.29, 59.54],
          [9.29, 59.58],
          [9.23, 59.58],
          [9.23, 59.54]
        ]]
      }
    },
    {
      type: "Feature",
      properties: {
        name: "Kragerø",
        population: 180,
        density: "100-200",
        inhabitants: 10500
      },
      geometry: {
        type: "Polygon",
        coordinates: [[
          [9.39, 58.85],
          [9.45, 58.85],
          [9.45, 58.89],
          [9.39, 58.89],
          [9.39, 58.85]
        ]]
      }
    },
    {
      type: "Feature",
      properties: {
        name: "Landlige områder",
        population: 35,
        density: "<50",
        inhabitants: 2500
      },
      geometry: {
        type: "Polygon",
        coordinates: [[
          [8.5, 59.3],
          [9.0, 59.3],
          [9.0, 59.7],
          [8.5, 59.7],
          [8.5, 59.3]
        ]]
      }
    }
  ]
};

export const landUseData = {
  type: "FeatureCollection",
  features: [
    {
      type: "Feature",
      properties: {
        name: "Skogområde nord",
        type: "Skog",
        area_km2: 450,
        coverage: "85%"
      },
      geometry: {
        type: "Polygon",
        coordinates: [[
          [8.3, 59.6],
          [9.2, 59.6],
          [9.2, 60.1],
          [8.3, 60.1],
          [8.3, 59.6]
        ]]
      }
    },
    {
      type: "Feature",
      properties: {
        name: "Jordbruksland",
        type: "Jordbruk",
        area_km2: 180,
        coverage: "65%"
      },
      geometry: {
        type: "Polygon",
        coordinates: [[
          [9.3, 59.1],
          [9.9, 59.1],
          [9.9, 59.4],
          [9.3, 59.4],
          [9.3, 59.1]
        ]]
      }
    },
    {
      type: "Feature",
      properties: {
        name: "Bebygd område Skien",
        type: "Bebygd",
        area_km2: 45,
        coverage: "90%"
      },
      geometry: {
        type: "Polygon",
        coordinates: [[
          [9.57, 59.18],
          [9.65, 59.18],
          [9.65, 59.24],
          [9.57, 59.24],
          [9.57, 59.18]
        ]]
      }
    },
    {
      type: "Feature",
      properties: {
        name: "Norsjø",
        type: "Vann",
        area_km2: 56,
        coverage: "100%"
      },
      geometry: {
        type: "Polygon",
        coordinates: [[
          [9.1, 59.4],
          [9.3, 59.4],
          [9.3, 59.5],
          [9.1, 59.5],
          [9.1, 59.4]
        ]]
      }
    },
    {
      type: "Feature",
      properties: {
        name: "Fjellområde",
        type: "Annet",
        area_km2: 320,
        coverage: "75%"
      },
      geometry: {
        type: "Polygon",
        coordinates: [[
          [8.0, 59.7],
          [8.5, 59.7],
          [8.5, 60.2],
          [8.0, 60.2],
          [8.0, 59.7]
        ]]
      }
    }
  ]
};
