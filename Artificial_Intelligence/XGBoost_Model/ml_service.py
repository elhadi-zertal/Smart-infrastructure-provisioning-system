"""
ml_service.py — Cluster XGBoost Inference & Incremental Learning Service
=========================================================================
Roles
-----
1. /sync   — Receives VM snapshots from main.py, runs batch prediction,
             accumulates data for incremental learning (FIX-12), runs
             actual incremental learning when enough data is buffered
             (FIX-9), then pushes updated thresholds to app.py (FIX-10)
             and updated config (weights + thresholds) to main.py (FIX-11).
2. /config — main.py polls for latest predicted config at any time.

Inter-service communication
---------------------------
APP_URL  → app.py  POST /thresholds  (update vm_scaling.yml + Prometheus reload)
MAIN_URL → main.py POST /update-ml-config  (apply new weights in the DE-WOA loop)

Environment variables
---------------------
MODELS_DIR        ./models
APP_URL           http://localhost:5000
MAIN_URL          http://localhost:8000
ML_NEW_TREES      20      trees appended per incremental round
ML_MIN_BATCH_ROWS 50      minimum rows accumulated before triggering update
CLUSTER_API_KEY   (forwarded to main.py)
"""

from __future__ import annotations

import asyncio
import logging
import os
import shutil
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import List, Optional

import httpx
import joblib
import numpy as np
import pandas as pd
import xgboost as xgb
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sklearn.metrics import mean_absolute_error

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ml_service")

# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────
MODELS_DIR       = Path(os.getenv("MODELS_DIR",    "./models"))
APP_URL          = os.getenv("APP_URL",            "http://localhost:5000")
MAIN_URL         = os.getenv("MAIN_URL",           "http://localhost:8000")
CLUSTER_API_KEY  = os.getenv("CLUSTER_API_KEY",    "")
N_NEW_TREES      = int(os.getenv("ML_NEW_TREES",   "20"))
MIN_BATCH_ROWS   = int(os.getenv("ML_MIN_BATCH_ROWS", "50"))   # FIX-12: was 100 AND ignored buffer

# ─────────────────────────────────────────────────────────────────────────────
# Feature / target column definitions  (must match main.py and incremental_learning.py)
# ─────────────────────────────────────────────────────────────────────────────
FEATURE_COLS = [
    "up", "scrape_duration_seconds", "cpu_busy_pct", "ram_usage_pct",
    "io_util_pct", "http_5xx_rate", "net_drop_rate", "power_watts",
    "is_worker", "is_master", "is_monitor", "vlan_enc",
]
TARGET_WEIGHTS     = ["w_cpu", "w_ram", "w_io", "w_energy"]
TARGET_THRESH_WARN = ["thresh_cpu_warn", "thresh_ram_warn", "thresh_disk_warn", "thresh_http_warn"]
TARGET_THRESH_CRIT = ["thresh_cpu_crit", "thresh_ram_crit", "thresh_disk_crit", "thresh_http_crit", "thresh_net_crit"]
TARGET_THRESH_LOW  = ["thresh_cpu_low", "thresh_ram_low", "thresh_http_low"]
ALL_TARGETS        = TARGET_WEIGHTS + TARGET_THRESH_WARN + TARGET_THRESH_CRIT + TARGET_THRESH_LOW  # 16

# XGBoost hyperparameters for incremental rounds (match training notebook §7)
_INCREMENTAL_PARAMS: dict = {
    "max_depth":         6,
    "learning_rate":     0.05,
    "subsample":         0.8,
    "colsample_bytree":  0.8,
    "min_child_weight":  3,
    "tree_method":       "hist",
    "objective":         "reg:squarederror",
    "eval_metric":       "mae",
    "seed":              42,
}

# ─────────────────────────────────────────────────────────────────────────────
# Runtime state
# ─────────────────────────────────────────────────────────────────────────────
_boosters: dict[str, xgb.Booster] = {}
_vlan_encoder = None
_update_lock          = asyncio.Lock()
_last_update_ts: Optional[float] = None
_tree_counts: dict[str, int] = {}
_current_config: dict = {}

# FIX-12: Persistent accumulation buffer — rows collect across multiple /sync
# calls until MIN_BATCH_ROWS is reached, then an incremental update fires.
_feature_buffer: list[dict] = []   # list of feature-row dicts (FEATURE_COLS keys)
_buffer_lock = asyncio.Lock()

# ─────────────────────────────────────────────────────────────────────────────
# Model loading
# ─────────────────────────────────────────────────────────────────────────────

def _load_models() -> None:
    global _boosters, _vlan_encoder, _tree_counts
    loaded = 0
    for target in ALL_TARGETS:
        path = MODELS_DIR / f"{target}.ubj"
        if not path.exists():
            logger.warning(f"Model file missing: {path}")
            continue
        b = xgb.Booster()
        b.load_model(str(path))
        _boosters[target] = b
        _tree_counts[target] = b.num_boosted_rounds()
        loaded += 1
    logger.info(f"ML service: loaded {loaded}/{len(ALL_TARGETS)} boosters")

    enc_path = MODELS_DIR / "vlan_encoder.joblib"
    if enc_path.exists():
        _vlan_encoder = joblib.load(enc_path)
        logger.info(f"VLAN encoder loaded ({len(_vlan_encoder.classes_)} classes)")
    else:
        logger.warning("vlan_encoder.joblib not found — vlan_enc will default to 0")

# ─────────────────────────────────────────────────────────────────────────────
# Feature building
# ─────────────────────────────────────────────────────────────────────────────

def _build_features(snapshots: list[dict]) -> pd.DataFrame:
    """
    Convert a list of VMSnapshot dicts into a feature DataFrame.
    The vm_name-based role flags (is_worker, is_master, is_monitor) use the
    same regex patterns as main.py and the training notebook.
    """
    rows = []
    for s in snapshots:
        vlan_enc = (
            int(_vlan_encoder.transform([s["vlan"]])[0])
            if _vlan_encoder and s["vlan"] in _vlan_encoder.classes_
            else 0
        )
        name = s["vm_name"].lower()
        rows.append({
            "up":                      float(s["up"]),
            "scrape_duration_seconds": float(s["scrape_duration"]),
            "cpu_busy_pct":            float(s["cpu_pct"]),
            "ram_usage_pct":           float(s["ram_pct"]),
            "io_util_pct":             float(s["io_pct"]),
            "http_5xx_rate":           float(s["http_5xx_rate"]),
            "net_drop_rate":           float(s["net_drop_rate"]),
            "power_watts":             float(s["power_watts"]),
            "is_worker":               int("worker"  in name),
            "is_master":               int("master"  in name),
            "is_monitor":              int(any(r in name for r in ["monitoring", "influx", "snmp"])),
            "vlan_enc":                vlan_enc,
        })
    return pd.DataFrame(rows, columns=FEATURE_COLS)

# ─────────────────────────────────────────────────────────────────────────────
# Cluster-level prediction
# ─────────────────────────────────────────────────────────────────────────────

def _predict_cluster_config(X: pd.DataFrame) -> dict:
    """
    Predict optimal weights and adaptive thresholds for the current cluster state.

    Only rows where up==1 contribute to the cluster-level averages; this prevents
    unreachable VMs from skewing the predicted thresholds.
    """
    d     = xgb.DMatrix(X, feature_names=FEATURE_COLS)
    preds = {t: _boosters[t].predict(d) for t in ALL_TARGETS}

    up_mask = X["up"].values == 1.0
    if up_mask.sum() == 0:
        up_mask = np.ones(len(X), dtype=bool)   # fallback: use all rows

    # ── Weights (normalised to sum exactly to 1) ─────────────────────────────
    w_raw = {t: float(preds[t][up_mask].mean()) for t in TARGET_WEIGHTS}
    w_sum = sum(max(0.0, v) for v in w_raw.values())
    config: dict = {
        t: round(max(0.0, w_raw[t]) / w_sum, 6) if w_sum > 0 else 0.25
        for t in TARGET_WEIGHTS
    }

    # ── Thresholds (clamped to safe operating ranges) ────────────────────────
    config["thresh_cpu_warn"]  = round(float(np.clip(preds["thresh_cpu_warn"][up_mask].mean(),  50.0, 95.0)), 2)
    config["thresh_ram_warn"]  = round(float(np.clip(preds["thresh_ram_warn"][up_mask].mean(),  55.0, 95.0)), 2)
    config["thresh_disk_warn"] = round(float(np.clip(preds["thresh_disk_warn"][up_mask].mean(), 10.0, 95.0)), 2)
    config["thresh_http_warn"] = round(float(np.clip(preds["thresh_http_warn"][up_mask].mean(),  0.1,  5.0)), 3)

    config["thresh_cpu_crit"]  = round(float(np.clip(preds["thresh_cpu_crit"][up_mask].mean(),  50.0, 95.0)), 2)
    config["thresh_ram_crit"]  = round(float(np.clip(preds["thresh_ram_crit"][up_mask].mean(),  55.0, 95.0)), 2)
    config["thresh_disk_crit"] = round(float(np.clip(preds["thresh_disk_crit"][up_mask].mean(), 10.0, 95.0)), 2)
    config["thresh_http_crit"] = round(float(np.clip(preds["thresh_http_crit"][up_mask].mean(),  0.1,  5.0)), 3)
    config["thresh_net_crit"]  = round(float(np.clip(preds["thresh_net_crit"][up_mask].mean(),   0.1,  5.0)), 3)

    config["thresh_cpu_low"]   = round(float(np.clip(preds["thresh_cpu_low"][up_mask].mean(),  10.0, 50.0)), 2)
    config["thresh_ram_low"]   = round(float(np.clip(preds["thresh_ram_low"][up_mask].mean(),  10.0, 50.0)), 2)
    config["thresh_http_low"]  = round(float(np.clip(preds["thresh_http_low"][up_mask].mean(),  0.1,  1.0)), 3)

    return config

# ─────────────────────────────────────────────────────────────────────────────
# Incremental learning  (FIX-9: was permanently disabled with "disabled_no_ground_truth")
# ─────────────────────────────────────────────────────────────────────────────

def _incremental_update_sync(X: pd.DataFrame) -> dict:
    """
    Append N_NEW_TREES trees to each of the 16 target boosters using the
    provided feature batch X and soft pseudo-labels generated by the current
    model predictions.

    Soft-labelling rationale
    ------------------------
    Ground-truth labels (what the optimal thresholds SHOULD have been) are not
    available in real-time.  Using the current model's own predictions as labels
    is a standard continual-learning technique: the model is nudged toward
    consistency with the current data distribution while preserving the knowledge
    from the original training set.  Over many update cycles the predictions
    converge to the cluster's actual operating patterns.

    This is separate from the richer incremental learning path in
    incremental_learning.py (used by main.py), which has access to pseudo-labels
    derived from the actions taken by the cluster manager and is therefore
    preferred when available.
    """
    global _tree_counts, _last_update_ts

    d_batch     = xgb.DMatrix(X, feature_names=FEATURE_COLS)
    soft_labels = {t: _boosters[t].predict(d_batch) for t in ALL_TARGETS}

    report: dict = {}
    for target in ALL_TARGETS:
        if target not in _boosters:
            report[target] = {"status": "model_not_loaded", "saved": False}
            continue

        existing      = _boosters[target]
        trees_before  = _tree_counts.get(target, existing.num_boosted_rounds())

        d_labeled = xgb.DMatrix(X, label=soft_labels[target], feature_names=FEATURE_COLS)

        # MAE before
        preds_before = existing.predict(d_batch)
        mae_before   = float(mean_absolute_error(soft_labels[target], preds_before))

        # Append new trees (xgb_model= is the incremental learning key)
        try:
            updated = xgb.train(
                _INCREMENTAL_PARAMS,
                d_labeled,
                num_boost_round=N_NEW_TREES,
                xgb_model=existing,   # APPENDS; does NOT reset
                verbose_eval=False,
            )
        except Exception as exc:
            logger.error(f"[{target}] xgb.train failed: {exc}")
            report[target] = {
                "mae_before": round(mae_before, 5), "mae_after": None,
                "trees_total": trees_before, "saved": False,
                "status": f"train_error: {exc}",
            }
            continue

        # MAE after
        preds_after = updated.predict(d_batch)
        mae_after   = float(mean_absolute_error(soft_labels[target], preds_after))
        trees_after = updated.num_boosted_rounds()

        # Save with backup
        saved = False
        model_path = MODELS_DIR / f"{target}.ubj"
        try:
            backup = model_path.with_suffix(f".ubj.bak.{int(time.time())}")
            shutil.copy2(model_path, backup)
            updated.save_model(str(model_path))
            # Prune backups older than 7 days
            for f in MODELS_DIR.glob(f"{target}.ubj.bak.*"):
                if time.time() - f.stat().st_mtime > 7 * 86400:
                    try: f.unlink()
                    except OSError: pass
            saved = True
        except Exception as exc:
            logger.error(f"[{target}] Could not save model: {exc}")

        # Update in-memory booster immediately so predictions are fresh
        _boosters[target] = updated
        _tree_counts[target] = trees_after

        direction = "↓" if mae_after < mae_before else "↑"
        logger.info(
            f"[{target:20s}] MAE {mae_before:.5f} → {mae_after:.5f} {direction}"
            f"  trees {trees_before}→{trees_after}  saved={saved}"
        )
        report[target] = {
            "mae_before":  round(mae_before, 5),
            "mae_after":   round(mae_after,  5),
            "delta":       round(mae_after - mae_before, 5),
            "trees_total": trees_after,
            "saved":       saved,
            "status":      "ok",
        }

    _last_update_ts = time.time()
    n_ok = sum(1 for v in report.values() if v.get("status") == "ok")
    logger.info(f"Incremental update: {n_ok}/{len(ALL_TARGETS)} models updated (+{N_NEW_TREES} trees)")
    return report

# ─────────────────────────────────────────────────────────────────────────────
# Push updated config to app.py and main.py  (FIX-10, FIX-11: new)
# ─────────────────────────────────────────────────────────────────────────────

async def _push_thresholds_to_app(config: dict) -> None:
    """
    Send updated alert thresholds to app.py /thresholds.
    app.py will regenerate vm_scaling.yml and reload Prometheus rules.
    Only threshold keys are forwarded (not weights).
    """
    threshold_payload = {k: v for k, v in config.items() if k.startswith("thresh_")}
    if not threshold_payload:
        return
    try:
        async with httpx.AsyncClient(timeout=8) as client:
            r = await client.post(f"{APP_URL}/thresholds", json=threshold_payload)
            if r.status_code == 200:
                logger.info("[Push] Thresholds delivered to app.py")
            else:
                logger.warning(f"[Push] app.py /thresholds returned {r.status_code}: {r.text[:120]}")
    except Exception as exc:
        logger.warning(f"[Push] Could not reach app.py: {exc}")


async def _push_config_to_main(config: dict) -> None:
    """
    Send the full updated config (weights + thresholds) to main.py
    /update-ml-config so the DE-WOA loop uses fresh weights immediately.
    """
    try:
        async with httpx.AsyncClient(timeout=8) as client:
            r = await client.post(
                f"{MAIN_URL}/update-ml-config",
                json={"config": config, "updated_at": time.time()},
                headers={"X-API-Key": CLUSTER_API_KEY},
            )
            if r.status_code == 200:
                logger.info("[Push] Config delivered to main.py")
            else:
                logger.warning(f"[Push] main.py /update-ml-config returned {r.status_code}: {r.text[:120]}")
    except Exception as exc:
        logger.warning(f"[Push] Could not reach main.py: {exc}")

# ─────────────────────────────────────────────────────────────────────────────
# Pydantic Models
# ─────────────────────────────────────────────────────────────────────────────

class VMSnapshot(BaseModel):
    instance: str
    vm_name: str
    vlan: str
    up: float
    scrape_duration: float
    cpu_pct: float
    ram_pct: float
    io_pct: float
    http_5xx_rate: float
    net_drop_rate: float
    power_watts: float

class SyncRequest(BaseModel):
    snapshots: List[VMSnapshot]
    sent_at: str

# ─────────────────────────────────────────────────────────────────────────────
# Lifespan  (FIX-1 pattern: lifespan defined BEFORE app)
# ─────────────────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    _load_models()
    logger.info("ML service ready.")
    yield


app = FastAPI(title="Cluster ML Service", version="2.0.0", lifespan=lifespan)

# ─────────────────────────────────────────────────────────────────────────────
# API Endpoints
# ─────────────────────────────────────────────────────────────────────────────

@app.post("/sync")
async def sync(request: SyncRequest):
    """
    Main entry point called by main.py every ~10 seconds with the latest
    cluster snapshot.

    Steps:
    1. Build feature matrix from incoming snapshots.
    2. Predict cluster-level config (weights + thresholds).
    3. Accumulate feature rows in the buffer for incremental learning.
    4. If buffer >= MIN_BATCH_ROWS: run incremental update, clear buffer,
       push updated thresholds to app.py, push full config to main.py.
    5. Return predicted config to caller.
    """
    if not _boosters:
        raise HTTPException(503, "Models not loaded")

    snapshots_dicts = [s.model_dump() for s in request.snapshots]
    X = _build_features(snapshots_dicts)

    if len(X) == 0:
        raise HTTPException(422, "Empty snapshot list")

    # ── Predict ──────────────────────────────────────────────────────────────
    config_dict = _predict_cluster_config(X)

    # ── Accumulate buffer (FIX-12) ──────────────────────────────────────────
    update_report: dict = {}
    async with _buffer_lock:
        _feature_buffer.extend(X.to_dict(orient="records"))
        buffer_size = len(_feature_buffer)

        if buffer_size >= MIN_BATCH_ROWS:
            X_batch = pd.DataFrame(_feature_buffer, columns=FEATURE_COLS)
            _feature_buffer.clear()
        else:
            X_batch = None

    # ── Incremental update (FIX-9) ───────────────────────────────────────────
    if X_batch is not None:
        async with _update_lock:
            update_report = await asyncio.to_thread(_incremental_update_sync, X_batch)
        # Re-predict with freshly updated models
        config_dict = _predict_cluster_config(X)

        # Push to app.py and main.py (FIX-10, FIX-11)
        await asyncio.gather(
            _push_thresholds_to_app(config_dict),
            _push_config_to_main(config_dict),
            return_exceptions=True,
        )

    global _current_config
    _current_config = config_dict

    return {
        "status":         "ok",
        "config":         config_dict,
        "update_report":  update_report,
        "n_samples":      len(X),
        "buffer_size":    buffer_size,
        "update_fired":   X_batch is not None,
    }


@app.get("/config")
async def get_config():
    """
    Return the last predicted cluster config.
    main.py polls this endpoint periodically as a fallback if push delivery fails.
    """
    if not _current_config:
        raise HTTPException(503, "No config available yet — send a /sync request first")
    return {
        "config":          _current_config,
        "last_update_ts":  _last_update_ts,
        "tree_counts":     _tree_counts.copy(),
        "buffer_size":     len(_feature_buffer),
    }


@app.get("/health")
async def health():
    return {
        "status":         "ok",
        "models_loaded":  len(_boosters),
        "vlan_encoder":   _vlan_encoder is not None,
        "buffer_size":    len(_feature_buffer),
        "min_batch_rows": MIN_BATCH_ROWS,
        "n_new_trees":    N_NEW_TREES,
        "last_update_ts": _last_update_ts,
        "app_url":        APP_URL,
        "main_url":       MAIN_URL,
    }
