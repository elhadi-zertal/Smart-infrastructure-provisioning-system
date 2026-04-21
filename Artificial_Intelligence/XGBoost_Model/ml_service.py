from __future__ import annotations
import asyncio, json, logging, os, shutil, time
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional
import joblib, numpy as np, pandas as pd, xgboost as xgb
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from sklearn.metrics import mean_absolute_error
from contextlib import asynccontextmanager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ml_service")

MODELS_DIR     = Path(os.getenv("MODELS_DIR", "./models"))
N_NEW_TREES    = int(os.getenv("ML_NEW_TREES", "20"))
MIN_BATCH_ROWS = int(os.getenv("ML_MIN_BATCH_ROWS", "100"))

FEATURE_COLS = ["up", "scrape_duration_seconds", "cpu_busy_pct", "ram_usage_pct", "io_util_pct", "http_5xx_rate", "net_drop_rate", "power_watts", "is_worker", "is_master", "is_monitor", "vlan_enc"]
TARGET_WEIGHTS = ["w_cpu", "w_ram", "w_io", "w_energy"]
TARGET_THRESH  = ["thresh_cpu", "thresh_ram", "thresh_http"]
ALL_TARGETS    = TARGET_WEIGHTS + TARGET_THRESH

_boosters: dict[str, xgb.Booster] = {}
_vlan_encoder = None
_update_lock = asyncio.Lock()
_last_update_ts: Optional[float] = None
_tree_counts: dict[str, int] = {}
_current_config: dict = {}

def _load_models():
    global _boosters, _vlan_encoder, _tree_counts
    for target in ALL_TARGETS:
        b = xgb.Booster()
        b.load_model(str(MODELS_DIR / f"{target}.ubj"))
        _boosters[target] = b
        _tree_counts[target] = b.num_boosted_rounds()
    enc_path = MODELS_DIR / "vlan_encoder.joblib"
    if enc_path.exists(): _vlan_encoder = joblib.load(enc_path)

def _build_features(snapshots: list[dict]) -> pd.DataFrame:
    rows = []
    for s in snapshots:
        vlan_enc = int(_vlan_encoder.transform([s["vlan"]])[0]) if _vlan_encoder and s["vlan"] in _vlan_encoder.classes_ else 0
        rows.append({
            "up": float(s["up"]), "scrape_duration_seconds": float(s["scrape_duration"]),
            "cpu_busy_pct": float(s["cpu_pct"]), "ram_usage_pct": float(s["ram_pct"]),
            "io_util_pct": float(s["io_pct"]), "http_5xx_rate": float(s["http_5xx_rate"]),
            "net_drop_rate": float(s["net_drop_rate"]), "power_watts": float(s["power_watts"]),
            "is_worker": int("worker" in s["vm_name"].lower()), "is_master": int("master" in s["vm_name"].lower()),
            "is_monitor": int("monitor" in s["vm_name"].lower()), "vlan_enc": vlan_enc,
        })
    return pd.DataFrame(rows, columns=FEATURE_COLS)

def _predict_cluster_config(X: pd.DataFrame) -> dict:
    d = xgb.DMatrix(X, feature_names=FEATURE_COLS)
    preds = {target: _boosters[target].predict(d) for target in ALL_TARGETS}

    up_mask = X["up"].values == 1.0
    if up_mask.sum() == 0: up_mask = np.ones(len(X), dtype=bool)

    w_raw = {t: float(preds[t][up_mask].mean()) for t in TARGET_WEIGHTS}
    w_sum = sum(max(0.0, v) for v in w_raw.values())
    weights = {t: round(max(0.0, w_raw[t]) / w_sum, 6) if w_sum > 0 else 0.25 for t in TARGET_WEIGHTS}

    cpu_crit = round(float(np.clip(preds["thresh_cpu"][up_mask].mean(), 55.0, 95.0)), 2)
    ram_crit = round(float(np.clip(preds["thresh_ram"][up_mask].mean(), 60.0, 95.0)), 2)
    http_crit = round(float(np.clip(preds["thresh_http"][up_mask].mean(), 0.3, 5.0)), 3)

    return {**weights, "thresh_cpu_crit": cpu_crit, "thresh_cpu_warn": max(40.0, cpu_crit - 15.0),
            "thresh_ram_crit": ram_crit, "thresh_ram_warn": max(50.0, ram_crit - 13.0),
            "thresh_http_crit": http_crit, "thresh_http_warn": max(0.1, http_crit - 0.9)}

def _incremental_update_sync(X: pd.DataFrame, snapshots: list[dict]) -> dict:
    global _tree_counts, _last_update_ts
    d_batch = xgb.DMatrix(X, feature_names=FEATURE_COLS)
    soft_labels = {t: _boosters[target].predict(d_batch) for target in ALL_TARGETS}

    report = {}
    for target in ALL_TARGETS:
        # BUG-13 Fix: Model collapse prevention. We bypass self-training unless real labels exist.
        logger.warning(f"[{target}] Incremental learning bypassed to prevent model collapse on soft labels. Awaiting ground-truth framework.")
        mae_stable = mean_absolute_error(soft_labels[target], soft_labels[target])

        report[target] = {
            "mae_before": 0.0, "mae_after": 0.0,
            "trees_total": _tree_counts[target], "saved": False, "status": "disabled_no_ground_truth"
        }

        # BUG-14 Fix: Prune old backups (if active)
        for f in MODELS_DIR.glob(f"{target}.ubj.bak.*"):
            if time.time() - f.stat().st_mtime > 86400 * 7:
                f.unlink()

    _last_update_ts = time.time()
    return report

class VMSnapshot(BaseModel):
    instance: str; vm_name: str; vlan: str; up: float; scrape_duration: float
    cpu_pct: float; ram_pct: float; io_pct: float; http_5xx_rate: float
    net_drop_rate: float; power_watts: float

class SyncRequest(BaseModel):
    snapshots: List[VMSnapshot]
    sent_at: str

# BUG-12 Fix: Lifespan
@asynccontextmanager
async def lifespan(app: FastAPI):
    _load_models()
    logger.info("ML service ready.")
    yield

app = FastAPI(title="Cluster ML Service", lifespan=lifespan)

@app.post("/sync")
async def sync(request: SyncRequest):
    if not _boosters: raise HTTPException(503, "Models not loaded")
    snapshots_dicts = [s.model_dump() for s in request.snapshots]
    X = _build_features(snapshots_dicts)
    if len(X) < MIN_BATCH_ROWS: raise HTTPException(422, f"Need >= {MIN_BATCH_ROWS} rows")

    config_dict = _predict_cluster_config(X)
    async with _update_lock:
        update_report = await asyncio.to_thread(_incremental_update_sync, X, snapshots_dicts)

    global _current_config
    _current_config = config_dict
    return {"status": "ok", "config": config_dict, "update_report": update_report, "n_samples": len(X)}
