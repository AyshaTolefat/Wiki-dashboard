import geopandas as gpd
from pathlib import Path

BASE = Path(__file__).resolve().parents[1]
DATA = BASE / "data"

geojson_path = DATA / "ne_50m_admin_0_countries.geojson"
out_path = DATA / "country_centroids_iso3.csv"

gdf = gpd.read_file(geojson_path)

# Natural Earth fields can be ISO_A3 or ADM0_A3; prefer ISO_A3 if present
iso_col = "ISO_A3" if "ISO_A3" in gdf.columns else "ADM0_A3"
gdf = gdf[gdf[iso_col].notna()].copy()
gdf = gdf[gdf[iso_col] != "-99"].copy()

# centroid in EPSG:4326 isn't perfect, but good enough for rotation
gdf["centroid"] = gdf.geometry.centroid
gdf["lon"] = gdf["centroid"].x
gdf["lat"] = gdf["centroid"].y

gdf[[iso_col, "lat", "lon"]].rename(columns={iso_col: "iso3"}).to_csv(out_path, index=False)
print("Wrote:", out_path)
