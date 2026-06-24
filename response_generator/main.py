"""
main.py - Generador de Respuestas (Response Generator)
Servicio FastAPI que carga el dataset en memoria y responde consultas Q1-Q5.
"""
import os
import time
import numpy as np
import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional

# -------------------------------------------------------------------------
# Configuración
# -------------------------------------------------------------------------
PROCESSING_DELAY_MS = int(os.getenv("PROCESSING_DELAY_MS", "100"))
DATASET_PATH = "/data/santiago_buildings.parquet"
DATASET_PATH_CSV = "/data/santiago_buildings.csv"

# Áreas de los bounding boxes en km² para el cálculo de densidad
ZONE_AREA_KM2 = {
    "Z1": (33.445 - 33.420) * (70.640 - 70.600) * 111 * 111 * np.cos(np.radians(33.43)),
    "Z2": (33.420 - 33.390) * (70.600 - 70.550) * 111 * 111 * np.cos(np.radians(33.405)),
    "Z3": (33.530 - 33.490) * (70.790 - 70.740) * 111 * 111 * np.cos(np.radians(33.51)),
    "Z4": (33.460 - 33.430) * (70.670 - 70.630) * 111 * 111 * np.cos(np.radians(33.445)),
    "Z5": (33.470 - 33.430) * (70.810 - 70.760) * 111 * 111 * np.cos(np.radians(33.45)),
}

app = FastAPI(title="Response Generator", version="1.0")

# -------------------------------------------------------------------------
# Carga del dataset en memoria al iniciar
# -------------------------------------------------------------------------
data: dict[str, pd.DataFrame] = {}


def load_dataset():
    global data
    path = DATASET_PATH if os.path.exists(DATASET_PATH) else DATASET_PATH_CSV
    if not os.path.exists(path):
        raise RuntimeError(f"Dataset no encontrado en {path}. Ejecuta download_dataset.py primero.")

    print(f"Cargando dataset desde {path}...")
    if path.endswith(".parquet"):
        df = pd.read_parquet(path)
    else:
        df = pd.read_csv(path)

    for zone_id in df["zone_id"].unique():
        zone_df = df[df["zone_id"] == zone_id].copy()
        data[zone_id] = zone_df
        print(f"  {zone_id}: {len(zone_df)} edificios cargados en memoria")

    print("Dataset cargado exitosamente.")


@app.on_event("startup")
def startup():
    load_dataset()


# -------------------------------------------------------------------------
# Modelos de Request/Response
# -------------------------------------------------------------------------
class QueryRequest(BaseModel):
    zone_id: str
    query_type: str
    confidence_min: Optional[float] = 0.0
    bins: Optional[int] = 5
    zone_id_b: Optional[str] = None  # Para Q4


class QueryResponse(BaseModel):
    result: dict
    processing_time_ms: float


# -------------------------------------------------------------------------
# Queries Q1-Q5
# -------------------------------------------------------------------------
def q1_count(zone_id: str, confidence_min: float = 0.0) -> dict:
    df = data[zone_id]
    count = int((df["confidence"] >= confidence_min).sum())
    return {"count": count, "zone_id": zone_id, "confidence_min": confidence_min}


def q2_area(zone_id: str, confidence_min: float = 0.0) -> dict:
    df = data[zone_id]
    filtered = df[df["confidence"] >= confidence_min]["area_in_meters"]
    if filtered.empty:
        return {"avg_area": 0.0, "total_area": 0.0, "n": 0}
    return {
        "avg_area": float(filtered.mean()),
        "total_area": float(filtered.sum()),
        "n": int(len(filtered)),
    }


def q3_density(zone_id: str, confidence_min: float = 0.0) -> dict:
    count = q1_count(zone_id, confidence_min)["count"]
    area_km2 = ZONE_AREA_KM2.get(zone_id, 1.0)
    return {"density_per_km2": round(count / area_km2, 4), "zone_id": zone_id, "area_km2": round(area_km2, 4)}


def q4_compare(zone_a: str, zone_b: str, confidence_min: float = 0.0) -> dict:
    da = q3_density(zone_a, confidence_min)["density_per_km2"]
    db = q3_density(zone_b, confidence_min)["density_per_km2"]
    return {
        "zone_a": zone_a, "density_a": da,
        "zone_b": zone_b, "density_b": db,
        "winner": zone_a if da > db else zone_b,
    }


def q5_confidence_dist(zone_id: str, bins: int = 5) -> dict:
    df = data[zone_id]
    scores = df["confidence"].values
    counts, edges = np.histogram(scores, bins=bins, range=(0, 1))
    buckets = [
        {"bucket": i, "min": round(float(edges[i]), 4), "max": round(float(edges[i + 1]), 4), "counts": int(counts[i])}
        for i in range(bins)
    ]
    return {"buckets": buckets, "zone_id": zone_id}


# -------------------------------------------------------------------------
# Endpoints
# -------------------------------------------------------------------------
@app.get("/health")
def health():
    return {"status": "ok", "zones_loaded": list(data.keys())}


@app.post("/query", response_model=QueryResponse)
def handle_query(req: QueryRequest):
    if req.zone_id not in data:
        raise HTTPException(status_code=404, detail=f"Zona '{req.zone_id}' no encontrada")

    start = time.time()

    # Simular tiempo de procesamiento real
    time.sleep(PROCESSING_DELAY_MS / 1000.0)

    qt = req.query_type
    if qt == "Q1":
        result = q1_count(req.zone_id, req.confidence_min)
    elif qt == "Q2":
        result = q2_area(req.zone_id, req.confidence_min)
    elif qt == "Q3":
        result = q3_density(req.zone_id, req.confidence_min)
    elif qt == "Q4":
        if not req.zone_id_b:
            raise HTTPException(status_code=400, detail="Q4 requiere zone_id_b")
        result = q4_compare(req.zone_id, req.zone_id_b, req.confidence_min)
    elif qt == "Q5":
        result = q5_confidence_dist(req.zone_id, req.bins)
    else:
        raise HTTPException(status_code=400, detail=f"Tipo de consulta '{qt}' no reconocido")

    elapsed_ms = (time.time() - start) * 1000
    return QueryResponse(result=result, processing_time_ms=round(elapsed_ms, 3))
