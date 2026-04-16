"""
ML Service — Adaptive Cluster Configuration
============================================
Standalone FastAPI microservice (port 8001).

Receives 5-minute metric batches from main.py, performs incremental
XGBoost training, and returns cluster-level fitness weights + thresholds
for the DE-WOA algorithm.

Endpoints
---------
POST /sync          Receive batch → incremental update → return config
GET  /config        Return current config without updating models
GET  /health        Liveness + model status
GET  /model-info    Per-model tree count and last-update timestamp

Usage
-----
    uvicorn ml_service:app --host 0.0.0.0 --port 8001
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

import joblib
import numpy as np
import pandas as pd
import xgboost as xgb
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from sklearn.metrics import mean_absolute_error

# ─────────────────────────────────────────────
#  Logging
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
)
logger = logging.getLogger("ml_service")

# ─────────────────────────────────────────────
#  Config from environment
# ─────────────────────────────────────────────
MODELS_DIR       = Path(os.getenv("MODELS_DIR",       "./models"))
N_NEW_TREES      = int(os.getenv("ML_NEW_TREES",       "20"))
MIN_BATCH_ROWS   = int(os.getenv("ML_MIN_BATCH_ROWS",  "10"))

# Feature columns — MUST match what the notebook trained on
FEATURE_COLS = [
    "up",
    "scrape_duration_seconds",
    "cpu_busy_pct",
    "ram_usage_pct",
    "io_util_pct",
    "http_5xx_rate",
    "net_drop_rate",
    "power_watts",
    "is_worker",
    "is_master",
    "is_monitor",
    "vlan_enc",
]

TARGET_WEIGHTS = ["w_cpu", "w_ram", "w_io", "w_energy"]
TARGET_THRESH  = ["thresh_cpu", "thresh_ram", "thresh_http"]
ALL_TARGETS    = TARGET_WEIGHTS + TARGET_THRESH

# ─────────────────────────────────────────────
#  Global state
# ─────────────────────────────────────────────
_boosters:      dict[str, xgb.Booster] = {}
_vlan_encoder   = None          # LabelEncoder loaded from models/
_update_lock    = asyncio.Lock() # Only one incremental update at a time
_last_update_ts: Optional[float] = None
_tree_counts:   dict[str, int]  = {}

# Current cluster config — updated after every /sync
_current_config: dict = {
    "w_cpu"       : 0.35,
    "w_ram"       : 0.35,
    "w_io"        : 0.15,
    "w_energy"    : 0.15,
    "thresh_cpu"  : 80.0,
    "thresh_ram"  : 85.0,
    "thresh_http" : 2.0,
}


# ─────────────────────────────────────────────
#  Model loading
# ─────────────────────────────────────────────
def _load_models() -> None:
    """Load all .ubj boosters and the VLAN encoder from MODELS_DIR."""
    global _boosters, _vlan_encoder, _tree_counts

    missing = [t for t in ALL_TARGETS if not (MODELS_DIR / f"{t}.ubj").exists()]
    if missing:
        raise FileNotFoundError(
            f"Missing model files in '{MODELS_DIR}': {missing}. "
            "Run the training notebook first."
        )

    for target in ALL_TARGETS:
        b = xgb.Booster()
        b.load_model(str(MODELS_DIR / f"{target}.ubj"))
        _boosters[target]    = b
        _tree_counts[target] = b.num_boosted_rounds()

    enc_path = MODELS_DIR / "vlan_encoder.joblib"
    if enc_path.exists():
        _vlan_encoder = joblib.load(enc_path)
        logger.info(f"VLAN encoder loaded — classes: {_vlan_encoder.classes_.tolist()}")
    else:
        logger.warning("vlan_encoder.joblib not found — vlan_enc will default to 0")

    logger.info(
        f"Loaded {len(_boosters)} models. "
        f"Tree counts: { {t: _tree_counts[t] for t in ALL_TARGETS} }"
    )


# ─────────────────────────────────────────────
#  Feature engineering
#  (main.py already sends computed rates,
#   so this is mostly mapping + role flags)
# ─────────────────────────────────────────────
def _build_features(snapshots: list[dict]) -> pd.DataFrame:
    """
    Convert a list of VMSnapshot dicts into a feature DataFrame
    ready for XGBoost inference and incremental training.
    """
    rows = []
    for s in snapshots:
        vm_name  = s["vm_name"].lower()
        vlan_raw = s["vlan"]

        # VLAN ordinal encoding
        if _vlan_encoder is not None:
            known = set(_vlan_encoder.classes_)
            safe  = vlan_raw if vlan_raw in known else _vlan_encoder.classes_[0]
            vlan_enc = int(_vlan_encoder.transform([safe])[0])
        else:
            vlan_enc = 0

        rows.append({
            "up"                      : float(s["up"]),
            "scrape_duration_seconds" : float(s["scrape_duration"]),
            "cpu_busy_pct"            : float(np.clip(s["cpu_pct"],  0, 100)),
            "ram_usage_pct"           : float(np.clip(s["ram_pct"],  0, 100)),
            "io_util_pct"             : float(np.clip(s["io_pct"],   0, 100)),
            "http_5xx_rate"           : float(np.clip(s["http_5xx_rate"], 0, 1)),
            "net_drop_rate"           : float(np.clip(s["net_drop_rate"], 0, 1)),
            "power_watts"             : float(s["power_watts"]),
            "is_worker"               : int("worker"  in vm_name),
            "is_master"               : int("master"  in vm_name),
            "is_monitor"              : int(any(r in vm_name for r in ["monitor", "influx", "snmp"])),
            "vlan_enc"                : vlan_enc,
        })

    return pd.DataFrame(rows, columns=FEATURE_COLS)


# ─────────────────────────────────────────────
#  Prediction → cluster-level config
# ─────────────────────────────────────────────
def _predict_cluster_config(X: pd.DataFrame) -> dict:
    """
    Run inference on every row in X, then aggregate to one cluster config.

    Weights   → mean across all UP VMs, then re-normalised to sum to 1.
    Thresholds → mean across k8s-worker VMs only (they are the scaling targets).
                 Falls back to all VMs if no workers present.
    """
    d = xgb.DMatrix(X, feature_names=FEATURE_COLS)

    # Per-row predictions: shape (n_rows,) per target
    preds = {target: _boosters[target].predict(d) for target in ALL_TARGETS}

    # ── Weights: average over UP VMs ──────────────────────────────────────────
    up_mask = X["up"].values == 1.0
    if up_mask.sum() == 0:
        up_mask = np.ones(len(X), dtype=bool)  # fallback: use all rows

    w_raw = {t: float(preds[t][up_mask].mean()) for t in TARGET_WEIGHTS}

    # Clip negatives then normalise to sum to 1.0
    w_sum = sum(max(0.0, v) for v in w_raw.values())
    if w_sum > 0:
        weights = {t: round(max(0.0, w_raw[t]) / w_sum, 6) for t in TARGET_WEIGHTS}
    else:
        weights = {t: 0.25 for t in TARGET_WEIGHTS}

    # ── Thresholds: average over worker VMs ───────────────────────────────────
    worker_mask = X["is_worker"].values == 1
    thresh_mask = worker_mask & up_mask if worker_mask.sum() > 0 else up_mask

    thresholds = {
        "thresh_cpu"  : round(float(np.clip(preds["thresh_cpu" ][thresh_mask].mean(), 50.0, 95.0)), 2),
        "thresh_ram"  : round(float(np.clip(preds["thresh_ram" ][thresh_mask].mean(), 55.0, 95.0)), 2),
        "thresh_http" : round(float(np.clip(preds["thresh_http"][thresh_mask].mean(),  0.1,  5.0)), 3),
    }

    return {**weights, **thresholds}


# ─────────────────────────────────────────────
#  Incremental update
# ─────────────────────────────────────────────
def _incremental_update_sync(X: pd.DataFrame, snapshots: list[dict]) -> dict:
    """
    Runs in a thread (called via asyncio.to_thread).
    Appends N_NEW_TREES boosting rounds to each model using the new batch.
    Since we don't have ground-truth targets in live data, we use the model's
    own current predictions as soft labels — a form of self-supervised
    continual adaptation that prevents catastrophic forgetting.
    """
    global _tree_counts, _last_update_ts

    # Soft labels: current model predictions on this batch
    d_batch = xgb.DMatrix(X, feature_names=FEATURE_COLS)
    soft_labels = {t: _boosters[t].predict(d_batch) for t in ALL_TARGETS}

    params = {
        "max_depth"        : 6,
        "learning_rate"    : 0.03,   # lower LR for incremental (more conservative)
        "subsample"        : 0.8,
        "colsample_bytree" : 0.8,
        "min_child_weight" : 3,
        "tree_method"      : "hist",
        "objective"        : "reg:squarederror",
        "eval_metric"      : "mae",
        "seed"             : 42,
    }

    report = {}
    for target in ALL_TARGETS:
        # MAE before
        preds_before = _boosters[target].predict(d_batch)
        mae_before   = mean_absolute_error(soft_labels[target], preds_before)

        # Append trees on top of existing booster
        d_labeled = xgb.DMatrix(X, label=soft_labels[target], feature_names=FEATURE_COLS)
        updated = xgb.train(
            params,
            d_labeled,
            num_boost_round = N_NEW_TREES,
            xgb_model       = _boosters[target],   # ← warm-start: appends trees
            verbose_eval    = False,
        )

        preds_after = updated.predict(d_batch)
        mae_after   = mean_absolute_error(soft_labels[target], preds_after)

        # Overwrite in-memory booster and persist to disk
        _boosters[target]    = updated
        _tree_counts[target] = updated.num_boosted_rounds()
        updated.save_model(str(MODELS_DIR / f"{target}.ubj"))

        report[target] = {
            "mae_before"  : round(float(mae_before), 6),
            "mae_after"   : round(float(mae_after),  6),
            "trees_total" : _tree_counts[target],
        }

    _last_update_ts = time.time()
    logger.info(
        f"Incremental update complete (+{N_NEW_TREES} trees). "
        f"Total trees now: { {t: _tree_counts[t] for t in ALL_TARGETS} }"
    )
    return report


# ─────────────────────────────────────────────
#  Pydantic schemas
# ─────────────────────────────────────────────
class VMSnapshot(BaseModel):
    """One VM's metrics snapshot — sent by main.py every 10 s per VM."""
    instance        : str   = Field(..., example="10.10.10.10:9100")
    vm_name         : str   = Field(..., example="k8s-worker-6")
    vlan            : str   = Field(..., example="vlan-1-app")
    up              : float = Field(..., ge=0.0, le=1.0)
    scrape_duration : float = Field(0.1,  ge=0.0)
    cpu_pct         : float = Field(...,  ge=0.0, le=100.0, description="CPU usage 0–100")
    ram_pct         : float = Field(...,  ge=0.0, le=100.0, description="RAM usage 0–100")
    io_pct          : float = Field(0.0,  ge=0.0, le=100.0)
    http_5xx_rate   : float = Field(0.0,  ge=0.0, le=1.0,  description="5xx / total requests")
    net_drop_rate   : float = Field(0.0,  ge=0.0, le=1.0,  description="dropped / total packets")
    power_watts     : float = Field(50.0, ge=0.0)


class SyncRequest(BaseModel):
    """Payload sent by main.py every 5 minutes."""
    snapshots : List[VMSnapshot] = Field(..., min_length=1)
    sent_at   : str              = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )


class ClusterConfig(BaseModel):
    """Predicted cluster-level config returned to main.py."""
    w_cpu       : float
    w_ram       : float
    w_io        : float
    w_energy    : float
    thresh_cpu  : float = Field(..., description="CPU alert threshold (%)")
    thresh_ram  : float = Field(..., description="RAM alert threshold (%)")
    thresh_http : float = Field(..., description="HTTP 5xx alert threshold (rate)")


class SyncResponse(BaseModel):
    status       : str
    config       : ClusterConfig
    update_report: dict
    n_samples    : int
    updated_at   : str


# ─────────────────────────────────────────────
#  FastAPI app
# ─────────────────────────────────────────────
app = FastAPI(
    title       = "Cluster ML Service",
    description = "XGBoost adaptive weights & thresholds for DE-WOA cluster manager.",
    version     = "1.0.0",
)


@app.on_event("startup")
def startup():
    _load_models()
    logger.info("ML service ready.")


# ── POST /sync ────────────────────────────────────────────────────────────────
@app.post("/sync", response_model=SyncResponse)
async def sync(request: SyncRequest):
    """
    Main endpoint called by main.py every 5 minutes.

    Steps
    -----
    1. Build feature matrix from the received snapshots
    2. Predict cluster-level config (weights + thresholds)
    3. Run incremental update in a background thread
    4. Return new config to main.py
    """
    if not _boosters:
        raise HTTPException(503, "Models not loaded yet — retry in a few seconds.")

    snapshots_dicts = [s.model_dump() for s in request.snapshots]
    X = _build_features(snapshots_dicts)

    if len(X) < MIN_BATCH_ROWS:
        raise HTTPException(
            422,
            f"Batch too small: {len(X)} rows received, need ≥ {MIN_BATCH_ROWS}. "
            "Returning last known config.",
        )

    # Predict BEFORE the update so main.py gets stable predictions
    config_dict = _predict_cluster_config(X)

    # Incremental update runs in a thread — doesn't block the event loop
    async with _update_lock:
        update_report = await asyncio.to_thread(_incremental_update_sync, X, snapshots_dicts)

    # Update global cache
    global _current_config
    _current_config = config_dict

    logger.info(
        f"/sync — {len(X)} samples | "
        f"w=({config_dict['w_cpu']:.3f}, {config_dict['w_ram']:.3f}, "
        f"{config_dict['w_io']:.3f}, {config_dict['w_energy']:.3f}) | "
        f"thresh=({config_dict['thresh_cpu']}, {config_dict['thresh_ram']}, "
        f"{config_dict['thresh_http']})"
    )

    return SyncResponse(
        status        = "ok",
        config        = ClusterConfig(**config_dict),
        update_report = update_report,
        n_samples     = len(X),
        updated_at    = datetime.now(timezone.utc).isoformat(),
    )


# ── GET /config ───────────────────────────────────────────────────────────────
@app.get("/config", response_model=ClusterConfig)
def get_config():
    """Return the last computed cluster config without triggering a model update."""
    return ClusterConfig(**_current_config)


# ── GET /health ───────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    return {
        "status"         : "ok",
        "models_loaded"  : len(_boosters),
        "models_ready"   : len(_boosters) == len(ALL_TARGETS),
        "last_update"    : (
            datetime.fromtimestamp(_last_update_ts, tz=timezone.utc).isoformat()
            if _last_update_ts else None
        ),
        "seconds_since_update": (
            round(time.time() - _last_update_ts, 1) if _last_update_ts else None
        ),
    }


# ── GET /model-info ───────────────────────────────────────────────────────────
@app.get("/model-info")
def model_info():
    return {
        "targets"     : ALL_TARGETS,
        "feature_cols": FEATURE_COLS,
        "tree_counts" : _tree_counts,
        "models_dir"  : str(MODELS_DIR),
        "n_new_trees_per_sync": N_NEW_TREES,
    }
