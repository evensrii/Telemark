from arcgis.gis import GIS
from arcgis.features import FeatureLayer

# Replace with your ArcGIS Online or Enterprise portal URL
portal_url = "https://telemarkfylke.maps.arcgis.com"
username = "even.sannes.riiser_telemarkfylke"
password = "Fh*m%DYLa&Y2gMDu"

#api_key = "AAPTxy8BH1VEsoebNVZXo8HurHctbX12jupR4wsoK-qCHtIsFtgqvwpD6EZOkudO1rp6H6rX-uf1q0UQhsLR5WhakuveDMTtl376BxpvbjQMDHX8wx9WdqufHw7-B_irSFPhKk6tjJCdGs_KAOdHMU-dmeLI8JxjmkdoeYplq4TH_de_ReYYGyPaE0i-z03fISSsrnWZPS_MFW6tVVhXll4A3xkAiNP1uoMC-NFaz9FFw0ZoWdoxT-P_CcnmXBwpqHpXAT1_TixLR8NP"

geodata_token = "Eqem47Ubui27pO-oUpMYRjkKviG3OUthEimCh0gFEGAZmOV5-sByn5PIcx0jQfcSwWkE2qGqJn690iuyptt5hm18QFEgIj3Qgi_mevbufPY."

#{"token": "Eqem47Ubui27pO-oUpMYRjkKviG3OUthEimCh0gFEGAZmOV5-sByn5PIcx0jQfcSwWkE2qGqJn690iuyptt5hm18QFEgIj3Qgi_mevbufPY.", "expires": 1770216576908}

# Authenticate
gis = GIS(portal_url, username, password)

# Base URL of the Feature Service
feature_service_url = "https://services.geodataonline.no/arcgis/rest/services/Geomap_UTM33_EUREF89/GeomapBedrifter/FeatureServer/3"

# Access the first layer
feature_layer = FeatureLayer(feature_service_url)

# Example: Query all features
query_result = feature_layer.query(where="1=1", out_fields="*", return_geometry=True)

# Example: Query where company name contains "AS"
#query_result = feature_layer.query(where="fylfylkenavn LIKE 'Telemark'", out_fields="*", return_geometry=True, token=geodata_token)
df = query_result.sdf
print(df)

