"""
download_dataset.py - Descarga las tiles del dataset Google Open Buildings
para la Región Metropolitana de Santiago y las filtra por las 5 zonas definidas.

Versión optimizada para memoria.
"""
import os
import io
import gzip
import requests
import pandas as pd
import sys

# -------------------------------------------------------------------------
# Bounding boxes de las 5 zonas según la especificación de la tarea
# -------------------------------------------------------------------------
ZONES = {
    "Z1": {"name": "Providencia", "lat_min": -33.445, "lat_max": -33.420, "lon_min": -70.640, "lon_max": -70.600},
    "Z2": {"name": "Las Condes",  "lat_min": -33.420, "lat_max": -33.390, "lon_min": -70.600, "lon_max": -70.550},
    "Z3": {"name": "Maipu",       "lat_min": -33.530, "lat_max": -33.490, "lon_min": -70.790, "lon_max": -70.740},
    "Z4": {"name": "Santiago Centro", "lat_min": -33.460, "lat_max": -33.430, "lon_min": -70.670, "lon_max": -70.630},
    "Z5": {"name": "Pudahuel",    "lat_min": -33.470, "lat_max": -33.430, "lon_min": -70.810, "lon_max": -70.760},
}

TILE_URLS = [
    "https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/967_buildings.csv.gz",
]

OUTPUT_PATH = "data/santiago_buildings.parquet"
OUTPUT_PATH_CSV = "data/santiago_buildings.csv"


def download_and_filter():
    os.makedirs("data", exist_ok=True)

    all_dfs = []

    # Calcular el bounding box global de todas las zonas
    lat_min_global = min(z["lat_min"] for z in ZONES.values())
    lat_max_global = max(z["lat_max"] for z in ZONES.values())
    lon_min_global = min(z["lon_min"] for z in ZONES.values())
    lon_max_global = max(z["lon_max"] for z in ZONES.values())

    print(f"Bounding box global: lat=[{lat_min_global}, {lat_max_global}], lon=[{lon_min_global}, {lon_max_global}]")

    for url in TILE_URLS:
        tile_name = url.split("/")[-1]
        print(f"Descargando tile: {tile_name}...")
        try:
            resp = requests.get(url, timeout=300, stream=True)
            resp.raise_for_status()

            # Usamos chunks para no cargar el archivo de 1GB en memoria
            # Descomprimimos al vuelo
            with gzip.open(resp.raw, "rt") as f:
                # Leemos en pedazos de 100k filas
                chunk_iter = pd.read_csv(f, chunksize=100000)
                
                for i, chunk in enumerate(chunk_iter):
                    # Extraer lat/lon si no están
                    if "latitude" not in chunk.columns and "geometry" in chunk.columns:
                        coords = chunk["geometry"].str.extract(r"POINT \(([^ ]+) ([^ )]+)\)")
                        chunk["longitude"] = coords[0].astype(float)
                        chunk["latitude"] = coords[1].astype(float)
                    
                    # Filtrar al bounding box global
                    mask = (
                        (chunk["latitude"] >= lat_min_global) & (chunk["latitude"] <= lat_max_global) &
                        (chunk["longitude"] >= lon_min_global) & (chunk["longitude"] <= lon_max_global)
                    )
                    filtered = chunk[mask][["latitude", "longitude", "area_in_meters", "confidence"]].copy()
                    filtered = filtered.dropna()
                    
                    if not filtered.empty:
                        all_dfs.append(filtered)
                    
                    if i % 10 == 0:
                        print(f"  Procesados {i*100000} filas...")

        except Exception as e:
            print(f"  ERROR descargando/procesando {tile_name}: {e}")

    if not all_dfs:
        print("ERROR: No se pudieron obtener datos reales.")
        sys.exit(1)

    final_df = pd.concat(all_dfs, ignore_index=True)

    # Agregar zona_id
    def assign_zone(row):
        for zone_id, z in ZONES.items():
            if (z["lat_min"] <= row["latitude"] <= z["lat_max"] and
                    z["lon_min"] <= row["longitude"] <= z["lon_max"]):
                return zone_id
        return None

    print("Asignando zonas...")
    final_df["zone_id"] = final_df.apply(assign_zone, axis=1)
    final_df = final_df.dropna(subset=["zone_id"])

    print(f"Dataset final: {len(final_df)} edificios.")
    final_df.to_parquet(OUTPUT_PATH, index=False)
    final_df.to_csv(OUTPUT_PATH_CSV, index=False)
    print(f"Dataset guardado en {OUTPUT_PATH}")


if __name__ == "__main__":
    download_and_filter()