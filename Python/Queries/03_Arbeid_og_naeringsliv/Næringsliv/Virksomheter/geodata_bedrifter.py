from arcgis.gis import GIS
from arcgis.features import FeatureLayer

# Connect to ArcGIS Online (anonymous)
gis = GIS()

# Base URL of the Feature Service
feature_service_url = "https://services.geodataonline.no/arcgis/rest/services/Geomap_UTM33_EUREF89/GeomapBedrifter/FeatureServer/3"

# Access the first layer (layer index 0, update if needed)
feature_layer = FeatureLayer(feature_service_url)

# Example: Query all features
# query_result = feature_layer.query(where="1=1", out_fields="*", return_geometry=True)

# Example: Query where company name contains "AS"
query_result = feature_layer.query(where="fylfylkenavn LIKE 'Telemark'", out_fields="*", return_geometry=True)
df = query_result.sdf
print(df)

