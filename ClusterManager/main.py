"""
PFE Cluster Manager
===================
Receives Prometheus Alertmanager webhooks and manages:
  - Proxmox QEMU VMs
  - Proxmox LXC containers
  - Kubernetes Deployments

Alert label schema expected from Prometheus alerts.yml:

    For Proxmox VM:
        target_type : "vm"
        vmid        : "152"
        severity    : "warning" | "critical"
        type        : "cpu" | "memory" | "disk_io" | "network" | "http_5xx"

    For Proxmox LXC:
        target_type : "lxc"
        vmid        : "201"
        severity    : "warning" | "critical"
        type        : "cpu" | "memory" | "disk_io" | "network" | "http_5xx"

    For Kubernetes Deployment:
        target_type : "k8s"
        namespace   : "default"
        deployment  : "my-app"
        severity    : "warning" | "critical"
        type        : "cpu" | "memory" | "network" | "http_5xx"

Scaling logic:
    WARNING  → add to warning_queue (DE-WOA provisioning algorithm handles it
               on the next 30s tick)
    CRITICAL →
        CPU      → vertical   : +1 vCPU  (Proxmox) | +200m CPU limit (k8s)
        MEMORY   → vertical   : +20% RAM (Proxmox) | +20% memory limit (k8s)
        DISK_IO  → vertical   : +15G     (Proxmox only)
        NETWORK  → horizontal : clone VM/LXC | +1 replica (k8s)
        HTTP_5XX → horizontal : clone VM/LXC | +1 replica (k8s)

Fitness function:
    fitness = sqrt(w1*(ΔC*)² + w2*(ΔR*)² + w3*(ΔIO*)² + w4*(ΔE*)²)

    where ΔX  = resource needed by VM  − resource available on node
    and   ΔX* = (ΔX − mean_ΔX) / σ_ΔX   (z-score normalisation)

DE-WOA Hybrid Algorithm:
    A population of n complete VM→node mappings is evolved over T iterations.
    Each iteration has two phases:

    WOA phase  — operates on the first half of the population (X_woa).
                 Each mapping either encircles X* (exploitation),
                 performs a random search toward X_rand (exploration),
                 or executes a spiral bubble-net attack around X*.
                 The branch is chosen stochastically, and the tendency
                 shifts from exploration to exploitation as t → T.

    DE phase   — operates on the second half (X_de), disjoint from X_woa.
                 For each mapping Xj, a donor Vj is created by perturbing
                 X* with the difference between two random mappings drawn
                 from outside X_de.  A binomial crossover (CR=0.5) produces
                 a trial Uj; selection keeps whichever of Uj / Xj is better.

    After both phases, X* is updated to the global best across the full
    population.  The final X* after T iterations is the output.
"""

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from enum import Enum
from dotenv import load_dotenv
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from proxmoxer import ProxmoxAPI
from contextlib import asynccontextmanager
from typing import Optional
from kubernetes import client, config as kube_config
from kubernetes.client.rest import ApiException
from concurrent.futures import ThreadPoolExecutor, as_completed

import asyncio
import json
import math
import os
import logging
import time
import random
import urllib.parse
import urllib.request
from collections import deque
from datetime import datetime, timezone
import httpx          # pip install httpx
import yaml           # pip install pyyaml
from pathlib import Path

# ─────────────────────────────────────────────
#  Logging
# ─────────────────────────────────────────────
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_error_seen:    dict[str, float] = {}
ERROR_COOLDOWN = 60  # seconds


def _throttled_warning(key: str, message: str) -> None:
    now = time.monotonic()
    if now - _error_seen.get(key, 0) >= ERROR_COOLDOWN:
        logger.warning(message)
        _error_seen[key] = now


# ─────────────────────────────────────────────
#  Environment (.env file)
# ─────────────────────────────────────────────
load_dotenv()

PROXMOX_HOST       = os.getenv("PROXMOX_HOST")
PROXMOX_USER       = os.getenv("PROXMOX_USER")
PROXMOX_TOKEN_NAME = os.getenv("PROXMOX_TOKEN_NAME")
PROXMOX_TOKEN_UUID = os.getenv("PROXMOX_TOKEN_UUID")

PROXMOX_TIMEOUT = int(os.getenv("PROXMOX_TIMEOUT", "4"))

# Energy consumption constants (watts)
ENERGY_PER_CPU_CORE  = int(os.getenv("ENERGY_PER_CPU_CORE",  "50"))
ENERGY_PER_GB_MEMORY = int(os.getenv("ENERGY_PER_GB_MEMORY", "10"))

# ── DE-WOA algorithm parameters ──────────────────────────────────────────────
#
#  N  : population size — number of complete VM→node mappings evolved in parallel.
#       Larger N explores more of the solution space but costs more compute.
#       Rule of thumb: N ≥ 2 * number_of_VMs_in_queue.
#
#  T  : number of iterations (generations).
#       Each iteration runs one WOA pass + one DE pass over the full population.
#       More iterations → better solutions, but longer runtime.
#       The scheduler tick is 30 s, so keep N*T manageable (e.g. 10*20 = 200 evals).
#
#  F  : DE scaling factor ∈ (0, 2].
#       Controls how much the difference vector (Xr1 − Xr2) perturbs X*.
#       F = 0.5 is the classic default; higher F → more aggressive mutation.
#
#  CR : DE crossover rate ∈ [0, 1].
#       Probability that each VM assignment in the trial vector Uj comes from
#       the donor Vj rather than the original Xj.
#       CR = 0.5 gives equal weight to donor and original (as specified).
#
#  B  : WOA spiral shape constant.
#       Controls how tightly the logarithmic spiral winds around X*.
#       B = 1 is the standard WOA default.
#
#  WOA_FRACTION : fraction of the population allocated to the WOA phase.
#       The remaining (1 − WOA_FRACTION) is used for the DE phase.
#       0.5 splits the population evenly between the two algorithms.

DE_WOA_N            = int(os.getenv("DE_WOA_N",            "10"))
DE_WOA_T            = int(os.getenv("DE_WOA_T",            "20"))
DE_WOA_F            = float(os.getenv("DE_WOA_F",          "0.5"))
DE_WOA_CR           = float(os.getenv("DE_WOA_CR",         "0.5"))
DE_WOA_B            = float(os.getenv("DE_WOA_B",          "1.0"))
DE_WOA_WOA_FRACTION = float(os.getenv("DE_WOA_WOA_FRACTION","0.5"))

# ── ML Service integration ────────────────────────────────────────────────────
ML_SERVICE_URL      = os.getenv("ML_SERVICE_URL",      "http://localhost:8001")
ML_SYNC_INTERVAL    = int(os.getenv("ML_SYNC_INTERVAL", "300"))   # seconds (5 min)
ML_REQUEST_TIMEOUT  = int(os.getenv("ML_REQUEST_TIMEOUT", "30"))  # seconds
ML_ENABLED          = os.getenv("ML_ENABLED", "true").lower() == "true"

# Prometheus alert rule file — rewritten by the ML sync job
ALERTS_YML_PATH       = Path(os.getenv("ALERTS_YML_PATH",  "./alerts.yml"))
PROMETHEUS_URL_RELOAD = os.getenv("PROMETHEUS_URL_RELOAD", "http://localhost:9090")

# Fitness weights — must sum to exactly 1.0 (validated at startup)
W_CPU = float(os.getenv("W_CPU", "0.35"))
W_RAM = float(os.getenv("W_RAM", "0.35"))
W_IO  = float(os.getenv("W_IO",  "0.15"))
W_E   = float(os.getenv("W_E",   "0.15"))

# Prometheus
PROMETHEUS_URL        = os.getenv("PROMETHEUS_URL", "")
PROMETHEUS_VMID_LABEL = os.getenv("PROMETHEUS_VMID_LABEL", "vmid")
VM_CPU_QUERY          = os.getenv("VM_CPU_QUERY",    "")
VM_RAM_QUERY          = os.getenv("VM_RAM_QUERY",    "")
VM_IO_QUERY           = os.getenv("VM_IO_QUERY",     "")
VM_ENERGY_QUERY       = os.getenv("VM_ENERGY_QUERY", "")

NODE_IO_CAPACITY_BPS = float(os.getenv("NODE_IO_CAPACITY_BPS", str(500 * 1024 * 1024)))

HISTORY_WINDOW      = int(os.getenv("HISTORY_WINDOW",      "500"))
MIN_HISTORY_SAMPLES = int(os.getenv("MIN_HISTORY_SAMPLES", "30"))

_delta_history: dict[str, deque[float]] = {
    metric: deque(maxlen=HISTORY_WINDOW)
    for metric in ("cpu", "ram", "io", "energy")
}

# Rolling 5-minute buffer of VM metric snapshots for the ML service.
# poll_cluster() (runs every 10 s) appends one entry per VM per cycle.
# ml_sync_job() (runs every 5 min) drains this buffer and POSTs it.
_ml_snapshot_buffer: deque[dict] = deque(maxlen=500)   # ~500 = 5 min × 10 VMs

# Last ML-predicted config (updated after every successful /sync)
_ml_config: dict = {
    "w_cpu"            : W_CPU,
    "w_ram"            : W_RAM,
    "w_io"             : W_IO,
    "w_energy"         : W_E,
    "thresh_cpu_warn"  : 64.0,
    "thresh_cpu_crit"  : 80.0,
    "thresh_ram_warn"  : 72.0,
    "thresh_ram_crit"  : 85.0,
    "thresh_http_warn" : 1.1,
    "thresh_http_crit" : 2.0,
}

# ─────────────────────────────────────────────
#  Proxmox client
# ─────────────────────────────────────────────
px = ProxmoxAPI(
    PROXMOX_HOST,
    user=PROXMOX_USER,
    token_name=PROXMOX_TOKEN_NAME,
    token_value=PROXMOX_TOKEN_UUID,
    verify_ssl=False,
    timeout=PROXMOX_TIMEOUT,
)

# ─────────────────────────────────────────────
#  Kubernetes client
# ─────────────────────────────────────────────
try:
    kube_config.load_kube_config()
    k8s_apps = client.AppsV1Api()
    k8s_core = client.CoreV1Api()
    logger.info("Kubernetes client connected via kubeconfig.")
except Exception as _kube_exc:
    k8s_apps = None
    k8s_core = None
    logger.warning(f"Kubernetes client not available: {_kube_exc}")

# ─────────────────────────────────────────────
#  Prometheus — actual per-VM metrics fetch
# ─────────────────────────────────────────────
_PROMETHEUS_METRICS: list[tuple[str, str]] = [
    ("cpu_actual",    VM_CPU_QUERY),
    ("ram_actual",    VM_RAM_QUERY),
    ("io_actual",     VM_IO_QUERY),
    ("energy_actual", VM_ENERGY_QUERY),
]


def _prometheus_instant_query(query: str) -> dict[str, float]:
    encoded = urllib.parse.quote(query)
    url     = f"{PROMETHEUS_URL}/api/v1/query?query={encoded}"
    try:
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=PROXMOX_TIMEOUT) as resp:
            data = json.loads(resp.read().decode())

        if data.get("status") != "success":
            _throttled_warning(
                f"prometheus:query:{query[:40]}",
                f"Prometheus non-success for query '{query}': {data.get('status')}",
            )
            return {}

        result: dict[str, float] = {}
        for item in data.get("data", {}).get("result", []):
            vmid = item["metric"].get(PROMETHEUS_VMID_LABEL)
            if vmid is None:
                continue
            try:
                result[str(vmid)] = float(item["value"][1])
            except (IndexError, ValueError):
                pass
        return result

    except Exception as e:
        _throttled_warning(
            f"prometheus:query:{query[:40]}",
            f"Could not reach Prometheus for query '{query}': {e}",
        )
        return {}


def fetch_vm_metrics_from_prometheus() -> dict[str, dict[str, float]]:
    if not PROMETHEUS_URL:
        return {}

    active = [(field, q) for field, q in _PROMETHEUS_METRICS if q]
    if not active:
        return {}

    collected: dict[str, dict[str, float]] = {}
    with ThreadPoolExecutor(max_workers=len(active)) as pool:
        futures = {pool.submit(_prometheus_instant_query, q): field for field, q in active}
        for future in as_completed(futures):
            field = futures[future]
            collected[field] = future.result()

    return collected


# ─────────────────────────────────────────────
#  Enums
# ─────────────────────────────────────────────
class AlertSeverity(Enum):
    WARNING  = "warning"
    CRITICAL = "critical"


class AlertType(Enum):
    CPU      = "cpu"
    MEMORY   = "memory"
    DISK_IO  = "disk_io"
    NETWORK  = "network"
    HTTP_5XX = "http_5xx"


class TargetType(Enum):
    VM  = "vm"
    LXC = "lxc"
    K8S = "k8s"


# ─────────────────────────────────────────────
#  Pydantic models
# ─────────────────────────────────────────────
class AlertLabels(BaseModel):
    alertname:   str = ""
    severity:    str = ""
    type:        str = ""
    target_type: str = "vm"
    vmid:        str = ""
    instance:    str = ""
    namespace:   str = "default"
    deployment:  str = ""


class AlertAnnotations(BaseModel):
    summary:     str = ""
    description: str = ""
    value:       str = "0"


class PrometheusAlert(BaseModel):
    status:      str
    labels:      AlertLabels
    annotations: AlertAnnotations


class AlertmanagerWebhook(BaseModel):
    version: str
    status:  str
    alerts:  list[PrometheusAlert]


class VMResources(BaseModel):
    cores:   Optional[int] = None
    memory:  Optional[int] = None
    disk:    Optional[str] = None
    disk_id: Optional[str] = "scsi0"

class WeightsUpdate(BaseModel):
    w_cpu: float
    w_ram: float
    w_io: float
    w_e: float
    source: str = "xgboost"
    bottleneck: str = "unknown"
    confidence: float = 0.0


# ─────────────────────────────────────────────
#  In-memory cluster state
# ─────────────────────────────────────────────
cluster_state: dict = {
    "nodes":       {},
    "vms":         {},
    "lxc":         {},
    "deployments": {},
}

_state_lock  = asyncio.Lock()
warning_queue: list[dict] = []

_current_weights_meta = {
    "source": "static_env",
    "bottleneck": "None",
    "confidence": 1.0,
    "last_updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
}


# ─────────────────────────────────────────────
#  Proxmox — per-node fetch
# ─────────────────────────────────────────────
_DOWN_ERRORS = ("failed to get address info", "hostname lookup", "Name or service not known")


def _fetch_node(node: dict) -> tuple[str, dict, dict, dict]:
    node_name = node["node"]
    node_info: dict = node.copy()
    vms:  dict = {}
    lxcs: dict = {}

    try:
        status      = px.nodes(node_name).status.get()
        cpu_cores   = status.get("cpuinfo", {}).get("cpus", 1)
        maxmem_gb   = status.get("memory", {}).get("total", 0) / (1024 ** 3)
        e_capacity  = cpu_cores * ENERGY_PER_CPU_CORE + maxmem_gb * ENERGY_PER_GB_MEMORY
        cpu_used    = status.get("cpu", 0) * cpu_cores
        mem_used_gb = status.get("memory", {}).get("used", 0) / (1024 ** 3)
        e_used      = cpu_used * ENERGY_PER_CPU_CORE + mem_used_gb * ENERGY_PER_GB_MEMORY
        node_info = {
            "node":            node_name,
            "status":          node.get("status"),
            "cpu":             status.get("cpu", 0),
            "maxcpu":          cpu_cores,
            "mem":             status.get("memory", {}).get("used", 0),
            "maxmem":          status.get("memory", {}).get("total", 0),
            "uptime":          status.get("uptime"),
            "energy_used":     e_used,
            "energy_capacity": e_capacity,
        }
    except Exception as e:
        err = str(e)
        key = f"{node_name}:stats"
        if any(s in err for s in _DOWN_ERRORS):
            _throttled_warning(key, f"Node {node_name} is down")
        else:
            _throttled_warning(key, f"Could not fetch stats for node {node_name}: {e}")

    try:
        for vm in px.nodes(node_name).qemu.get():
            vm["node"]          = node_name
            cores               = vm.get("cores", 1)
            maxmem_gb           = vm.get("maxmem", 0) / (1024 ** 3)
            vm["energy_needed"] = cores * ENERGY_PER_CPU_CORE + maxmem_gb * ENERGY_PER_GB_MEMORY
            vms[vm["vmid"]]     = vm
    except Exception as e:
        err = str(e)
        key = f"{node_name}:vms"
        if any(s in err for s in _DOWN_ERRORS):
            _throttled_warning(key, f"Node {node_name} is down")
        else:
            _throttled_warning(key, f"Could not fetch VMs for node {node_name}: {e}")

    try:
        for ct in px.nodes(node_name).lxc.get():
            ct["node"]          = node_name
            cores               = ct.get("cores", 1)
            maxmem_gb           = ct.get("maxmem", 0) / (1024 ** 3)
            ct["energy_needed"] = cores * ENERGY_PER_CPU_CORE + maxmem_gb * ENERGY_PER_GB_MEMORY
            lxcs[ct["vmid"]]    = ct
    except Exception as e:
        err = str(e)
        key = f"{node_name}:lxc"
        if any(s in err for s in _DOWN_ERRORS):
            _throttled_warning(key, f"Node {node_name} is down")
        else:
            _throttled_warning(key, f"Could not fetch LXC for node {node_name}: {e}")

    return node_name, node_info, vms, lxcs


def fetch_proxmox_state() -> dict:
    state: dict = {"nodes": {}, "vms": {}, "lxc": {}}
    try:
        nodes = px.nodes.get()
    except Exception as e:
        logger.error(f"Could not list Proxmox nodes: {e}")
        return state

    with ThreadPoolExecutor(max_workers=max(len(nodes), 1)) as pool:
        futures = {pool.submit(_fetch_node, node): node["node"] for node in nodes}
        for future in as_completed(futures):
            node_name, node_info, vms, lxcs = future.result()
            state["nodes"][node_name] = node_info
            state["vms"].update(vms)
            state["lxc"].update(lxcs)

    return state


def fetch_kubernetes_state() -> dict:
    deployments: dict = {}
    if not k8s_apps:
        return deployments

    try:
        result = k8s_apps.list_deployment_for_all_namespaces()
        for dep in result.items:
            key       = f"{dep.metadata.namespace}/{dep.metadata.name}"
            container = dep.spec.template.spec.containers[0]
            limits    = container.resources.limits   if container.resources and container.resources.limits   else {}
            requests  = container.resources.requests if container.resources and container.resources.requests else {}
            deployments[key] = {
                "name":               dep.metadata.name,
                "namespace":          dep.metadata.namespace,
                "replicas":           dep.spec.replicas,
                "ready_replicas":     dep.status.ready_replicas     or 0,
                "available_replicas": dep.status.available_replicas or 0,
                "cpu_limit":          limits.get("cpu"),
                "memory_limit":       limits.get("memory"),
                "cpu_request":        requests.get("cpu"),
                "memory_request":     requests.get("memory"),
            }
    except ApiException as e:
        logger.warning(f"Could not fetch Kubernetes deployments: {e}")

    return deployments


def diff_proxmox_states(old: dict, new: dict) -> dict:
    ov, nv = set(old["vms"].keys()),   set(new["vms"].keys())
    on, nn = set(old["nodes"].keys()), set(new["nodes"].keys())
    ol, nl = set(old["lxc"].keys()),   set(new["lxc"].keys())
    return {
        "added_vms":     nv - ov,
        "removed_vms":   ov - nv,
        "changed_vms":  {v for v in ov & nv if old["vms"][v].get("status")   != new["vms"][v].get("status")},
        "added_nodes":   nn - on,
        "removed_nodes": on - nn,
        "changed_nodes":{n for n in on & nn if old["nodes"][n].get("status") != new["nodes"][n].get("status")},
        "added_lxc":     nl - ol,
        "removed_lxc":   ol - nl,
        "changed_lxc":  {v for v in ol & nl if old["lxc"][v].get("status")   != new["lxc"][v].get("status")},
    }


def _collect_ml_snapshot(vm: dict, instance_id: str) -> None:
    """
    Builds one metric snapshot from a VM's current state and appends
    it to _ml_snapshot_buffer. Called from inside poll_cluster() for
    every live VM.
    """
    # Proxmox reports cpu as a ratio 0-1, mem/maxmem in bytes
    cpu_pct = float(vm.get("cpu",    0.0)) * 100.0
    max_mem = float(vm.get("maxmem", 1))
    ram_pct = (1.0 - float(vm.get("mem", 0)) / max(max_mem, 1)) * 100.0

    # Disk IO: Proxmox gives diskread/diskwrite in bytes/s — normalise to %
    disk_bps = float(vm.get("diskread", 0)) + float(vm.get("diskwrite", 0))
    io_pct   = min(disk_bps / NODE_IO_CAPACITY_BPS * 100.0, 100.0)

    # Power: from Prometheus scaph query if available, else estimate from CPU
    power_microwatts = vm.get("energy_actual", 0)
    if not power_microwatts:
        power_watts = 40.0 + (80.0 * (cpu_pct / 100.0))
    else:
        power_watts = power_microwatts / 1_000_000

    http_5xx_rate = float(vm.get("http_5xx_rate", 0.0))
    net_drop_rate = float(vm.get("net_drop_rate", 0.0))

    # Derive VLAN from the instance IP prefix
    ip = str(instance_id).split(":")[0]
    if ip.startswith("10.10.10."): vlan = "vlan-1-app"
    elif ip.startswith("10.20.20."): vlan = "vlan-2-app"
    elif ip.startswith("10.30.30."): vlan = "vlan-monitoring"
    else: vlan = "wan"

    _ml_snapshot_buffer.append({
        "instance"        : str(instance_id),
        "vm_name"         : vm.get("name", str(vm.get("vmid", instance_id))),
        "vlan"            : vlan,
        "up"              : 1.0 if vm.get("status") == "running" else 0.0,
        "scrape_duration" : float(vm.get("scrape_duration", 0.1)),
        "cpu_pct"         : round(cpu_pct,  3),
        "ram_pct"         : round(ram_pct,  3),
        "io_pct"          : round(io_pct,   3),
        "http_5xx_rate"   : round(http_5xx_rate, 5),
        "net_drop_rate"   : round(net_drop_rate,  5),
        "power_watts"     : round(power_watts,    3),
    })


def _rewrite_alerts_yml(
    thresh_cpu_warn: float, thresh_cpu_crit: float,
    thresh_ram_warn: float, thresh_ram_crit: float,
    thresh_http_warn: float, thresh_http_crit: float
) -> None:
    """
    Overwrite ALERTS_YML_PATH with Prometheus alert rules that use the
    ML-predicted thresholds, then signal Prometheus to reload its config.
    """
    rules = {
        "groups": [
            {
                "name": "adaptive_cluster_alerts",
                "rules": [
                    {
                        "alert": "HighCPU_Warning",
                        "expr": (
                            f"100 - (avg by(instance) "
                            f"(rate(node_cpu_seconds_total{{mode='idle'}}[2m])) * 100) "
                            f"> {thresh_cpu_warn:.1f}"
                        ),
                        "for": "2m",
                        "labels"     : {"severity": "warning", "type": "cpu"},
                        "annotations": {
                            "summary"    : "CPU usage above warning threshold",
                            "description": (
                                f"Instance {{{{ $labels.instance }}}} CPU > "
                                f"{thresh_cpu_warn:.1f}% (ML threshold)."
                            ),
                        },
                    },
                    {
                        "alert": "HighCPU_Critical",
                        "expr": (
                            f"100 - (avg by(instance) "
                            f"(rate(node_cpu_seconds_total{{mode='idle'}}[2m])) * 100) "
                            f"> {thresh_cpu_crit:.1f}"
                        ),
                        "for": "1m",
                        "labels"     : {"severity": "critical", "type": "cpu"},
                        "annotations": {
                            "summary"    : "CPU usage above critical threshold",
                            "description": (
                                f"Instance {{{{ $labels.instance }}}} CPU > "
                                f"{thresh_cpu_crit:.1f}% (ML threshold)."
                            ),
                        },
                    },
                    {
                        "alert": "HighMemory_Warning",
                        "expr": (
                            f"(1 - node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes) "
                            f"* 100 > {thresh_ram_warn:.1f}"
                        ),
                        "for": "2m",
                        "labels"     : {"severity": "warning", "type": "memory"},
                        "annotations": {
                            "summary": "Memory usage above warning threshold",
                            "description": (
                                f"Instance {{{{ $labels.instance }}}} RAM > "
                                f"{thresh_ram_warn:.1f}% (ML threshold)."
                            ),
                        },
                    },
                    {
                        "alert": "HighMemory_Critical",
                        "expr": (
                            f"(1 - node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes) "
                            f"* 100 > {thresh_ram_crit:.1f}"
                        ),
                        "for": "1m",
                        "labels"     : {"severity": "critical", "type": "memory"},
                        "annotations": {
                            "summary": "Memory usage above critical threshold",
                            "description": (
                                f"Instance {{{{ $labels.instance }}}} RAM > "
                                f"{thresh_ram_crit:.1f}% (ML threshold)."
                            ),
                        },
                    },
                    {
                        "alert": "High5xxRate_Warning",
                        "expr": (
                            f"rate(http_requests_total{{status=~'5..'}}[5m]) / "
                            f"rate(http_requests_total[5m]) > {thresh_http_warn / 100.0:.4f}"
                        ),
                        "for": "2m",
                        "labels"     : {"severity": "warning", "type": "http_5xx"},
                        "annotations": {
                            "summary": "HTTP 5xx rate above warning threshold",
                        },
                    },
                    {
                        "alert": "High5xxRate_Critical",
                        "expr": (
                            f"rate(http_requests_total{{status=~'5..'}}[5m]) / "
                            f"rate(http_requests_total[5m]) > {thresh_http_crit / 100.0:.4f}"
                        ),
                        "for": "1m",
                        "labels"     : {"severity": "critical", "type": "http_5xx"},
                        "annotations": {
                            "summary": "HTTP 5xx rate above critical threshold",
                        },
                    },
                    {
                        "alert": "NodeDown",
                        "expr": "up == 0",
                        "for": "1m",
                        "labels"     : {"severity": "critical", "type": "network"},
                        "annotations": {
                            "summary"    : "Node unreachable",
                            "description": "Instance {{{{ $labels.instance }}}} is down.",
                        },
                    },
                ],
            }
        ]
    }

    ALERTS_YML_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(ALERTS_YML_PATH, "w") as f:
        yaml.dump(rules, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

    logger.info(
        f"alerts.yml rewritten → thresh_cpu_crit={thresh_cpu_crit}, "
        f"thresh_ram_crit={thresh_ram_crit}, thresh_http_crit={thresh_http_crit}"
    )

    try:
        req = urllib.request.Request(
            f"{PROMETHEUS_URL_RELOAD}/-/reload",
            method="POST",
            headers={"Content-Length": "0"},
        )
        with urllib.request.urlopen(req, timeout=5):
            pass
        logger.info("Prometheus reloaded successfully.")
    except Exception as exc:
        _throttled_warning("prometheus:reload", f"Prometheus reload failed: {exc}")


async def ml_sync_job() -> None:
    """
    Scheduled every 5 minutes by APScheduler.

    1. Drain the snapshot buffer (collect last 5-min of metrics)
    2. POST to ml_service /sync
    3. On success:
       a. Update global DE-WOA weights (W_CPU, W_RAM, W_IO, W_E)
       b. Update internal threshold variables
       c. Rewrite alerts.yml + reload Prometheus
    """
    global W_CPU, W_RAM, W_IO, W_E, _ml_config

    if not ML_ENABLED:
        return

    # 1. Drain buffer
    snapshots = list(_ml_snapshot_buffer)
    _ml_snapshot_buffer.clear()

    if len(snapshots) == 0:
        logger.warning("ml_sync_job: buffer empty, skipping this cycle.")
        return

    logger.info(f"ml_sync_job: sending {len(snapshots)} snapshots to ML service...")

    # 2. POST to ML service
    payload = {
        "snapshots": snapshots,
        "sent_at"  : __import__("datetime").datetime.utcnow().isoformat() + "Z",
    }

    try:
        async with httpx.AsyncClient(timeout=ML_REQUEST_TIMEOUT) as client:
            response = await client.post(f"{ML_SERVICE_URL}/sync", json=payload)
            response.raise_for_status()
            data = response.json()

    except httpx.TimeoutException:
        logger.error(f"ml_sync_job: request timed out after {ML_REQUEST_TIMEOUT}s.")
        return
    except httpx.HTTPStatusError as exc:
        logger.error(f"ml_sync_job: ML service returned {exc.response.status_code}: {exc.response.text}")
        return
    except Exception as exc:
        logger.error(f"ml_sync_job: unexpected error — {exc}")
        return

    # 3. Apply new config
    cfg = data.get("config", {})

    new_w_cpu = float(cfg.get("w_cpu",    W_CPU))
    new_w_ram = float(cfg.get("w_ram",    W_RAM))
    new_w_io  = float(cfg.get("w_io",     W_IO ))
    new_w_e   = float(cfg.get("w_energy", W_E  ))

    w_sum = new_w_cpu + new_w_ram + new_w_io + new_w_e
    if abs(w_sum - 1.0) > 0.01:
        logger.warning(f"ml_sync_job: weights sum to {w_sum:.4f}, normalising.")
        new_w_cpu /= w_sum
        new_w_ram /= w_sum
        new_w_io  /= w_sum
        new_w_e   /= w_sum

    W_CPU = new_w_cpu
    W_RAM = new_w_ram
    W_IO  = new_w_io
    W_E   = new_w_e

    _ml_config = cfg

    # Extract thresholds safely (falling back to single keys if ML hasn't split them yet)
    t_cpu_c  = float(cfg.get("thresh_cpu_crit",  cfg.get("thresh_cpu",  80.0)))
    t_cpu_w  = float(cfg.get("thresh_cpu_warn",  t_cpu_c * 0.8))

    t_ram_c  = float(cfg.get("thresh_ram_crit",  cfg.get("thresh_ram",  85.0)))
    t_ram_w  = float(cfg.get("thresh_ram_warn",  t_ram_c * 0.85))

    t_http_c = float(cfg.get("thresh_http_crit", cfg.get("thresh_http", 2.0)))
    t_http_w = float(cfg.get("thresh_http_warn", t_http_c * 0.5))

    await asyncio.to_thread(
        _rewrite_alerts_yml,
        t_cpu_w, t_cpu_c,
        t_ram_w, t_ram_c,
        t_http_w, t_http_c
    )

    logger.info(
        f"ml_sync_job: applied new config — "
        f"w=({W_CPU:.3f}, {W_RAM:.3f}, {W_IO:.3f}, {W_E:.3f}) | "
        f"thresh_cpu_crit={t_cpu_c:.1f} "
        f"thresh_ram_crit={t_ram_c:.1f} "
        f"thresh_http_crit={t_http_c:.1f}"
    )


async def poll_cluster() -> None:
    global cluster_state
    try:
        proxmox, k8s_deps, prom_metrics = await asyncio.gather(
            asyncio.to_thread(fetch_proxmox_state),
            asyncio.to_thread(fetch_kubernetes_state),
            asyncio.to_thread(fetch_vm_metrics_from_prometheus),
        )

        node_energy_sum: dict[str, float] = {}
        node_io_sum:     dict[str, float] = {}

        for kind in ("vms", "lxc"):
            for vmid, vm in proxmox[kind].items():
                vmid_str = str(vmid)
                node     = vm.get("node")

                for field, per_vm in prom_metrics.items():
                    val = per_vm.get(vmid_str)
                    if val is not None:
                        vm[field] = val

                if node:
                    energy = vm.get("energy_actual")
                    if energy is not None:
                        node_energy_sum[node] = node_energy_sum.get(node, 0.0) + energy
                    io = vm.get("io_actual")
                    if io is not None:
                        node_io_sum[node] = node_io_sum.get(node, 0.0) + io

                _collect_ml_snapshot(vm, f"{vm.get('ip', vmid)}:9100")

        for node_name, watts in node_energy_sum.items():
            if node_name in proxmox["nodes"]:
                proxmox["nodes"][node_name]["energy_used"] = watts

        for node_name, bps in node_io_sum.items():
            if node_name in proxmox["nodes"]:
                proxmox["nodes"][node_name]["io_used"] = bps

        _update_delta_history(proxmox)

        diff = diff_proxmox_states(cluster_state, proxmox)
        if diff["added_vms"]:     logger.info(f"New VMs:       {diff['added_vms']}")
        if diff["removed_vms"]:   logger.info(f"Removed VMs:   {diff['removed_vms']}")
        if diff["added_lxc"]:     logger.info(f"New LXCs:      {diff['added_lxc']}")
        if diff["removed_lxc"]:   logger.info(f"Removed LXCs:  {diff['removed_lxc']}")
        if diff["added_nodes"]:   logger.info(f"New nodes:     {diff['added_nodes']}")
        if diff["removed_nodes"]: logger.info(f"Removed nodes: {diff['removed_nodes']}")
        for vmid in diff["changed_vms"]:
            logger.info(f"VM  {vmid}: {cluster_state['vms'][vmid]['status']} -> {proxmox['vms'][vmid]['status']}")
        for vmid in diff["changed_lxc"]:
            logger.info(f"LXC {vmid}: {cluster_state['lxc'][vmid]['status']} -> {proxmox['lxc'][vmid]['status']}")

        old_deps = set(cluster_state["deployments"].keys())
        new_deps = set(k8s_deps.keys())
        if new_deps - old_deps: logger.info(f"New deployments:     {new_deps - old_deps}")
        if old_deps - new_deps: logger.info(f"Removed deployments: {old_deps - new_deps}")
        for key in old_deps & new_deps:
            old_r = cluster_state["deployments"][key].get("replicas")
            new_r = k8s_deps[key].get("replicas")
            if old_r != new_r:
                logger.info(f"Deployment {key}: replicas {old_r} -> {new_r}")

        async with _state_lock:
            cluster_state = {**proxmox, "deployments": k8s_deps}

        if any(diff.values()) or old_deps != new_deps:
            logger.info("Cluster state updated.")
        else:
            logger.debug("No changes detected.")

    except Exception as e:
        logger.error(f"Polling error: {e}")


# ─────────────────────────────────────────────
#  Fitness function
# ─────────────────────────────────────────────
def _update_delta_history(state: dict) -> None:
    nodes = state["nodes"]
    for kind in ("vms", "lxc"):
        for vm in state[kind].values():
            for node_deltas in _compute_raw_deltas(vm, nodes).values():
                for metric, value in node_deltas.items():
                    _delta_history[metric].append(value)

    logger.debug(
        "Delta history sizes — "
        + ", ".join(f"{m}: {len(_delta_history[m])}" for m in _delta_history)
    )


def _compute_raw_deltas(vm: dict, nodes: dict) -> dict[str, dict[str, float]]:
    vm_cpu = vm.get(
        "cpu_actual",
        vm.get("cores", 1) * vm.get("cpu", 1.0),
    )
    vm_ram = vm.get(
        "ram_actual",
        vm.get("maxmem", 0) * vm.get("mem", 1.0) if vm.get("mem") else vm.get("maxmem", 0),
    )
    vm_io     = vm.get("io_actual",     0.0)
    vm_energy = vm.get("energy_actual", vm.get("energy_needed", 0.0))

    deltas: dict[str, dict[str, float]] = {}
    for name, info in nodes.items():
        if not info or info.get("status") != "online":
            continue

        cpu_avail = info["maxcpu"] * (1.0 - info.get("cpu", 0.0))
        ram_avail = info["maxmem"] - info.get("mem", 0)
        io_avail  = NODE_IO_CAPACITY_BPS - info.get("io_used", 0.0)
        e_avail   = info["energy_capacity"] - info.get("energy_used", 0.0)

        deltas[name] = {
            "cpu":    vm_cpu    - cpu_avail,
            "ram":    vm_ram    - ram_avail,
            "io":     vm_io     - io_avail,
            "energy": vm_energy - e_avail,
        }
    return deltas


def _zscore_normalize(deltas: dict[str, dict[str, float]]) -> dict[str, dict[str, float]]:
    if len(deltas) <= 1:
        return {n: {"cpu": 0.0, "ram": 0.0, "io": 0.0, "energy": 0.0} for n in deltas}

    use_history = all(
        len(_delta_history[m]) >= MIN_HISTORY_SAMPLES
        for m in ("cpu", "ram", "io", "energy")
    )

    normalized: dict[str, dict[str, float]] = {n: {} for n in deltas}

    for metric in ("cpu", "ram", "io", "energy"):
        if use_history:
            hist = _delta_history[metric]
            mean = sum(hist) / len(hist)
            var  = sum((v - mean) ** 2 for v in hist) / len(hist)
        else:
            values = [deltas[n][metric] for n in deltas]
            mean   = sum(values) / len(values)
            var    = sum((v - mean) ** 2 for v in values) / len(values)

        std = math.sqrt(var) if var > 0 else 1.0
        for n in deltas:
            normalized[n][metric] = (deltas[n][metric] - mean) / std

    if not use_history:
        logger.debug(
            f"_zscore_normalize: using cross-sectional σ "
            f"(history {min(len(_delta_history[m]) for m in _delta_history)}"
            f"/{MIN_HISTORY_SAMPLES} samples)"
        )

    return normalized


def calculate_fitness(vm: dict, node_name: str, nodes: dict) -> float:
    """
    Weighted Euclidean fitness for placing `vm` on `node_name`.
    Returns float('inf') if the node cannot physically host the VM.
    """
    deltas = _compute_raw_deltas(vm, nodes)
    raw    = deltas.get(node_name)
    if raw is None:
        return float("inf")

    if raw["cpu"] > 0 or raw["ram"] > 0 or raw["energy"] > 0:
        return float("inf")

    norm = _zscore_normalize(deltas)[node_name]
    return math.sqrt(
        W_CPU * norm["cpu"]    ** 2 +
        W_RAM * norm["ram"]    ** 2 +
        W_IO  * norm["io"]     ** 2 +
        W_E   * norm["energy"] ** 2
    )


# ─────────────────────────────────────────────
#  DE-WOA — Discrete mapping helpers
#
#  A "mapping" in this context is a list of node names, one per VM,
#  indexed by position in working_ids:
#
#      mapping[k] = node_name assigned to working_ids[k]
#
#  All WOA and DE operations are defined in terms of these lists.
# ─────────────────────────────────────────────

def _mapping_fitness(mapping: list[str], vm_ids: list[int], vms_data: dict, nodes: dict) -> float:
    """
    Total fitness of a complete mapping (sum of individual VM→node fitnesses).
    Returns float('inf') if any single assignment is infeasible.
    """
    total = 0.0
    for vmid, node_name in zip(vm_ids, mapping):
        f = calculate_fitness(vms_data[vmid], node_name, nodes)
        if f == float("inf"):
            return float("inf")
        total += f
    return total


def _random_mapping(vm_ids: list[int], nodes: list[str]) -> list[str]:
    """
    Create one random mapping: assign each VM a randomly chosen node
    (with replacement — multiple VMs may share a node in horizontal scaling).
    """
    return [random.choice(nodes) for _ in vm_ids]


def _hamming_distance(m1: list[str], m2: list[str]) -> int:
    """Number of positions where two mappings differ."""
    return sum(1 for a, b in zip(m1, m2) if a != b)


def _move_toward(
    xi:       list[str],
    target:   list[str],
    strength: float,
    nodes:    list[str],
) -> list[str]:
    """
    WOA encircling / random-search step — move xi toward target.

    strength = |A| * D  (scaled Hamming distance, clamped to [0, m]).

    For each of the floor(strength) positions where xi differs from target,
    we adopt target's assignment.  This directly reduces the Hamming distance
    between xi and target by `strength` steps.

    If strength ≥ m (all positions), xi becomes a copy of target.
    """
    result       = xi.copy()
    diff_pos     = [k for k in range(len(xi)) if xi[k] != target[k]]
    n_copy       = min(len(diff_pos), max(0, int(strength)))

    if diff_pos and n_copy > 0:
        chosen = random.sample(diff_pos, n_copy)
        for k in chosen:
            result[k] = target[k]

    return result


def _spiral_update(
    xi:   list[str],
    best: list[str],
    b:    float,
) -> list[str]:
    """
    WOA bubble-net spiral step.

    In continuous WOA:
        xi(t+1) = D * e^(b*l) * cos(2*π*l) + X*(t)
        where D = |X* - xi|  and  l ~ Uniform(-1, 1)

    For discrete mappings we derive a position-adoption probability from
    the spiral factor and apply it per VM slot:

        l              ~ Uniform(-1, 1)
        spiral_factor  = e^(b*l) * cos(2*π*l)    ∈ [-e^b, e^b]
        p_adopt        = (spiral_factor + e^b) / (2 * e^b)   ∈ [0, 1]

    For each position k:
        if random() < p_adopt  → xi[k] = best[k]   (pull toward X*)
        else                   → xi[k] unchanged    (stay, preserving diversity)

    When l = 0  → spiral_factor = 1 → p_adopt = (1 + e^b)/(2*e^b) ≈ 0.68 for b=1
    When l = ±1 → cos(±2π) = 1 → maximum pull or push depending on e^(b*l)
    """
    l             = random.uniform(-1.0, 1.0)
    spiral_factor = math.exp(b * l) * math.cos(2 * math.pi * l)
    e_b           = math.exp(b)
    p_adopt       = (spiral_factor + e_b) / (2.0 * e_b)   # normalised to [0, 1]
    p_adopt       = max(0.0, min(1.0, p_adopt))

    result = xi.copy()
    for k in range(len(xi)):
        if random.random() < p_adopt:
            result[k] = best[k]
    return result


def _de_mutation(
    x_best: list[str],
    xr1:    list[str],
    xr2:    list[str],
    F:      float,
) -> list[str]:
    """
    DE mutation:  Vj = X* + F * (Xr1 − Xr2)

    In discrete mapping space:
      • (Xr1 − Xr2) is the set of positions where Xr1 and Xr2 differ.
      • For each such position, with probability F we apply Xr1's value
        as a perturbation on top of X*; otherwise we keep X*'s assignment.
      • Positions where Xr1 == Xr2 always inherit from X* (no perturbation).

    This produces a donor vector Vj that is rooted in the best known
    solution X* but perturbed in the directions where random individuals
    in the population disagree, scaled by F.
    """
    donor = x_best.copy()
    for k in range(len(x_best)):
        if xr1[k] != xr2[k] and random.random() < F:
            donor[k] = xr1[k]
    return donor


def _de_crossover(xj: list[str], vj: list[str], CR: float) -> list[str]:
    """
    DE binomial crossover:  Uj[k] = Vj[k] if rand() ≤ CR else Xj[k]

    Each VM assignment slot independently adopts the donor (Vj) with
    probability CR, or keeps the original (Xj) with probability 1−CR.
    CR = 0.5 gives equal weighting to the two parents.

    At least one slot always comes from Vj (standard DE guarantee),
    chosen at a random index if no slot was selected by the coin flips.
    """
    trial      = xj.copy()
    guaranteed = random.randrange(len(xj))   # ensure ≥ 1 donor gene
    for k in range(len(xj)):
        if k == guaranteed or random.random() <= CR:
            trial[k] = vj[k]
    return trial


# ─────────────────────────────────────────────
#  Hybrid DE-WOA Provisioning Algorithm
#
#  Population-based metaheuristic that finds the best VM→node mapping
#  by evolving N complete mappings over T iterations.
#
#  Each iteration:
#    WOA phase — encircle X* (exploit) or search randomly (explore)
#                or spiral around X* (bubble-net), chosen stochastically.
#                The balance shifts from exploration to exploitation as t→T.
#    DE phase  — mutate with X* + F*(Xr1−Xr2), crossover at CR=0.5,
#                greedy selection.  Operates on a disjoint sub-population.
#    Both phases share and update the same global best X*.
#
#  Returns best_mapping: vmid (int) → node_name (str).
# ─────────────────────────────────────────────
def hybrid_de_woa_provisioning(
    vm_ids:      list[int],
    target_type: TargetType,
    nodes:       dict,
    N:           int   = DE_WOA_N,
    T:           int   = DE_WOA_T,
    F:           float = DE_WOA_F,
    CR:          float = DE_WOA_CR,
    b:           float = DE_WOA_B,
    woa_frac:    float = DE_WOA_WOA_FRACTION,
) -> dict[int, str]:
    """
    Parameters
    ----------
    vm_ids      : list of VM ids to provision (from the warning queue)
    target_type : VM or LXC (selects the right cluster_state sub-dict)
    nodes       : current snapshot of cluster_state["nodes"]
    N           : population size (number of parallel mappings)
    T           : number of WOA+DE iterations
    F           : DE scaling factor for mutation
    CR          : DE crossover rate (0.5 → equal donor / original mix)
    b           : WOA spiral shape constant
    woa_frac    : fraction of population used for WOA (rest used for DE)

    Returns
    -------
    dict  vmid → node_name   (empty dict if no feasible mapping found)
    """
    state_key    = "vms" if target_type == TargetType.VM else "lxc"
    vms_data     = cluster_state[state_key]

    valid_vm_ids = [v for v in vm_ids if v in vms_data]
    online_nodes = [n for n, info in nodes.items() if info and info.get("status") == "online"]

    if not valid_vm_ids or not online_nodes:
        logger.warning("DE-WOA: no valid VMs or online nodes.")
        return {}

    m           = len(valid_vm_ids)   # number of VMs to map
    working_ids = valid_vm_ids        # all queued VMs handled in one pass

    # Ensure population is large enough for both WOA and DE sub-sets,
    # and that we have at least 2 individuals for Xr1, Xr2 selection.
    N = max(N, 6)

    # ── Step 1: Initialise population of N random mappings ───────────────────
    population: list[list[str]] = [
        _random_mapping(working_ids, online_nodes) for _ in range(N)
    ]

    # ── Find initial X* ──────────────────────────────────────────────────────
    fitnesses: list[float] = [
        _mapping_fitness(mp, working_ids, vms_data, nodes)
        for mp in population
    ]

    best_idx  = min(range(N), key=lambda i: fitnesses[i])
    x_star    = population[best_idx].copy()
    f_star    = fitnesses[best_idx]

    # Split population into two disjoint sub-sets:
    #   X_woa indices → WOA phase
    #   X_de  indices → DE phase
    # The split is fixed for the lifetime of the run (indices, not content).
    n_woa = max(2, int(N * woa_frac))
    n_de  = N - n_woa

    # Shuffle indices once so the split is random, not always first-half/second-half
    all_indices = list(range(N))
    random.shuffle(all_indices)
    woa_indices = all_indices[:n_woa]
    de_indices  = all_indices[n_woa:]

    logger.debug(
        f"DE-WOA init: N={N}, T={T}, m={m}, "
        f"woa={n_woa}, de={n_de}, best_f={f_star:.4f}"
    )

    # ── Main loop ─────────────────────────────────────────────────────────────
    for t in range(T):

        # WOA adaptive parameter a decreases linearly 2 → 0 over T iterations.
        # When a is large → |A| is likely large → exploration.
        # When a is small → |A| is likely small → exploitation around X*.
        a = 2.0 * (1.0 - t / T)

        # ── WOA Phase ────────────────────────────────────────────────────────
        for i in woa_indices:
            xi = population[i]

            r1, r2 = random.random(), random.random()
            A      = 2.0 * a * r1 - a    # ∈ [-2a, 2a]; |A| > 1 when a large
            C      = 2.0 * r2             # ∈ [0, 2]
            p      = random.random()

            if p < 0.5:
                if abs(A) < 1.0:
                    # ── Encircling prey (exploitation) ────────────────────
                    # D = |C * X* - Xi|  (scaled Hamming distance to X*)
                    # Xi(t+1) = X* - A * D
                    # In discrete space: copy |A| * D positions from X* into Xi.
                    D        = _hamming_distance(xi, x_star) * C
                    strength = abs(A) * D
                    population[i] = _move_toward(xi, x_star, strength, online_nodes)

                else:
                    # ── Random search (exploration) ───────────────────────
                    # Pick a random individual from the WOA sub-population
                    # (excluding self) as the random "prey" position X_rand.
                    other_woa = [j for j in woa_indices if j != i]
                    if other_woa:
                        rand_idx  = random.choice(other_woa)
                        x_rand    = population[rand_idx]
                        D         = _hamming_distance(xi, x_rand) * C
                        strength  = abs(A) * D
                        population[i] = _move_toward(xi, x_rand, strength, online_nodes)
                    else:
                        # Fallback: move toward X* if no other WOA individual
                        D        = _hamming_distance(xi, x_star) * C
                        strength = abs(A) * D
                        population[i] = _move_toward(xi, x_star, strength, online_nodes)

            else:
                # ── Bubble-net spiral attack ──────────────────────────────
                # xi(t+1) = D * e^(b*l) * cos(2*π*l) + X*(t)
                population[i] = _spiral_update(xi, x_star, b)

            # Update fitness for this individual
            fitnesses[i] = _mapping_fitness(population[i], working_ids, vms_data, nodes)

        # Update X* after WOA phase
        woa_best_idx = min(woa_indices, key=lambda i: fitnesses[i])
        if fitnesses[woa_best_idx] < f_star:
            f_star  = fitnesses[woa_best_idx]
            x_star  = population[woa_best_idx].copy()
            logger.debug(f"DE-WOA t={t}: WOA improved X* → f={f_star:.4f}")

        # ── DE Phase ─────────────────────────────────────────────────────────
        # Xr1 and Xr2 are drawn from individuals NOT in X_de (i.e., from X_woa
        # or any other pool outside X_de) so that mutation diversity comes from
        # a region of the population that DE itself has not touched this iteration.
        donor_pool = [j for j in all_indices if j not in de_indices]

        for j in de_indices:
            xj = population[j]

            # ── Mutation: Vj = X* + F * (Xr1 - Xr2) ─────────────────────
            # Xr1, Xr2 must be distinct and not xj itself
            eligible = [k for k in donor_pool if k != j]
            if len(eligible) < 2:
                # Not enough donors — skip DE for this individual this iteration
                continue

            r1_idx, r2_idx = random.sample(eligible, 2)
            xr1 = population[r1_idx]
            xr2 = population[r2_idx]

            vj = _de_mutation(x_star, xr1, xr2, F)

            # ── Crossover: Uj[k] = Vj[k] if rand≤CR else Xj[k] ─────────
            uj = _de_crossover(xj, vj, CR)

            # ── Selection: keep whichever of Uj / Xj is better ──────────
            f_uj = _mapping_fitness(uj, working_ids, vms_data, nodes)
            f_xj = fitnesses[j]

            if f_uj < f_xj:
                population[j] = uj
                fitnesses[j]  = f_uj

        # Update X* after DE phase (DE may have found a better solution)
        de_best_idx = min(de_indices, key=lambda i: fitnesses[i])
        if fitnesses[de_best_idx] < f_star:
            f_star = fitnesses[de_best_idx]
            x_star = population[de_best_idx].copy()
            logger.debug(f"DE-WOA t={t}: DE improved X* → f={f_star:.4f}")

    # ── Final result ─────────────────────────────────────────────────────────
    if f_star == float("inf"):
        logger.warning("DE-WOA: no feasible mapping found — cluster may be at capacity.")
        return {}

    best_mapping = {vmid: node for vmid, node in zip(working_ids, x_star)}
    logger.info(
        f"DE-WOA complete: {m} VM(s) mapped in {T} iterations, "
        f"best total fitness={f_star:.4f}, mapping={best_mapping}"
    )
    return best_mapping


# ─────────────────────────────────────────────
#  Resource calculator for Proxmox vertical scaling
# ─────────────────────────────────────────────
def compute_proxmox_vertical_resources(alert_type: AlertType, target: dict) -> VMResources:
    match alert_type:
        case AlertType.CPU:
            return VMResources(cores=target.get("cores", 1) + 1)
        case AlertType.MEMORY:
            current_mb = target.get("maxmem", 0) // (1024 * 1024)
            return VMResources(memory=int(current_mb * 1.20))
        case AlertType.DISK_IO:
            return VMResources(disk="+15G", disk_id="scsi0")
        case _:
            return VMResources()


# ─────────────────────────────────────────────
#  Proxmox Scaling — Vertical
# ─────────────────────────────────────────────
def proxmox_scale_vertically(vmid: int, target_type: TargetType, resources: VMResources) -> None:
    state_key = "vms" if target_type == TargetType.VM else "lxc"
    target    = cluster_state[state_key].get(vmid)
    if not target:
        raise HTTPException(404, f"{target_type.value} {vmid} not found")

    node      = target["node"]
    node_info = cluster_state["nodes"].get(node)
    if not node_info:
        raise HTTPException(500, f"Node {node} not found in cluster state")

    old_energy = target.get("energy_needed", 0)
    new_cores  = resources.cores  if resources.cores  is not None else target.get("cores", 1)
    new_mem_gb = (resources.memory / 1024) if resources.memory is not None \
                 else target.get("maxmem", 0) / (1024 ** 3)
    new_energy = new_cores * ENERGY_PER_CPU_CORE + new_mem_gb * ENERGY_PER_GB_MEMORY

    if node_info["energy_used"] - old_energy + new_energy > node_info["energy_capacity"]:
        logger.warning(f"Insufficient energy on {node} for vertical scale of {target_type.value} {vmid}")
        raise HTTPException(500, f"Insufficient energy capacity on node {node}")

    try:
        config: dict = {}
        if resources.cores  is not None: config["cores"]  = resources.cores
        if resources.memory is not None: config["memory"] = resources.memory

        if config:
            if target_type == TargetType.VM:
                px.nodes(node).qemu(vmid).config.put(**config)
            else:
                px.nodes(node).lxc(vmid).config.put(**config)
            logger.info(f"{target_type.value.upper()} {vmid} vertical scale applied: {config}")

        if resources.disk is not None:
            if target_type == TargetType.VM:
                disk_id = resources.disk_id or "scsi0"
                px.nodes(node).qemu(vmid).resize.put(disk=disk_id, size=resources.disk)
            else:
                px.nodes(node).lxc(vmid).resize.put(volume="rootfs", size=resources.disk)
            logger.info(f"{target_type.value.upper()} {vmid} disk resized: {resources.disk}")

    except Exception as e:
        logger.error(f"Failed to vertically scale {target_type.value} {vmid}: {e}")
        raise HTTPException(500, str(e))


# ─────────────────────────────────────────────
#  Proxmox Scaling — Horizontal
# ─────────────────────────────────────────────
def proxmox_scale_horizontally(vmid: int, target_type: TargetType) -> None:
    state_key = "vms" if target_type == TargetType.VM else "lxc"
    target    = cluster_state[state_key].get(vmid)
    if not target:
        raise HTTPException(404, f"{target_type.value} {vmid} not found")

    node      = target["node"]
    node_info = cluster_state["nodes"].get(node)
    if not node_info:
        raise HTTPException(500, f"Node {node} not found in cluster state")

    extra_energy = target.get("energy_needed", 0)
    if node_info["energy_used"] + extra_energy > node_info["energy_capacity"]:
        logger.warning(f"Insufficient energy on {node} for horizontal scale of {target_type.value} {vmid}")
        raise HTTPException(500, f"Insufficient energy capacity on node {node}")

    all_ids  = list(cluster_state["vms"].keys()) + list(cluster_state["lxc"].keys())
    new_vmid = max(all_ids, default=100) + 1

    try:
        if target_type == TargetType.VM:
            px.nodes(node).qemu(vmid).clone.post(
                newid=new_vmid,
                name=f"{target.get('name', 'vm')}-clone",
                full=1,
            )
            px.nodes(node).qemu(new_vmid).status.start.post()
        else:
            px.nodes(node).lxc(vmid).clone.post(
                newid=new_vmid,
                hostname=f"{target.get('name', 'lxc')}-clone",
            )
            px.nodes(node).lxc(new_vmid).status.start.post()

        logger.info(f"{target_type.value.upper()} {vmid} cloned -> {new_vmid} (started)")

    except Exception as e:
        logger.error(f"Failed to horizontally scale {target_type.value} {vmid}: {e}")
        raise HTTPException(500, str(e))


# ─────────────────────────────────────────────
#  Kubernetes helpers
# ─────────────────────────────────────────────
def _parse_cpu_millicores(cpu_str: str) -> int:
    cpu_str = cpu_str.strip()
    if cpu_str.endswith("m"):
        return int(cpu_str[:-1])
    return int(float(cpu_str) * 1000)


def k8s_scale_vertically(namespace: str, deployment: str, alert_type: AlertType) -> None:
    if not k8s_apps:
        raise HTTPException(503, "Kubernetes client not available")

    key      = f"{namespace}/{deployment}"
    dep_info = cluster_state["deployments"].get(key)
    if not dep_info:
        raise HTTPException(404, f"Deployment {key} not found")

    try:
        dep       = k8s_apps.read_namespaced_deployment(name=deployment, namespace=namespace)
        container = dep.spec.template.spec.containers[0]

        if container.resources          is None: container.resources          = client.V1ResourceRequirements()
        if container.resources.limits   is None: container.resources.limits   = {}
        if container.resources.requests is None: container.resources.requests = {}

        match alert_type:
            case AlertType.CPU:
                current_str = container.resources.limits.get("cpu", "500m")
                current_m   = _parse_cpu_millicores(current_str)
                new_m       = current_m + 200
                container.resources.limits["cpu"]   = f"{new_m}m"
                container.resources.requests["cpu"] = f"{new_m}m"
                logger.info(f"Deployment {key}: CPU {current_m}m -> {new_m}m")

            case AlertType.MEMORY:
                current_str = container.resources.limits.get("memory", "512Mi")
                current_mi  = int(current_str.replace("Mi", ""))
                new_mi      = int(current_mi * 1.20)
                container.resources.limits["memory"]   = f"{new_mi}Mi"
                container.resources.requests["memory"] = f"{new_mi}Mi"
                logger.info(f"Deployment {key}: memory {current_mi}Mi -> {new_mi}Mi")

            case _:
                logger.warning(f"Vertical k8s scaling not applicable for {alert_type.name}")
                return

        k8s_apps.patch_namespaced_deployment(name=deployment, namespace=namespace, body=dep)

    except ApiException as e:
        logger.error(f"Failed to vertically scale deployment {key}: {e}")
        raise HTTPException(500, str(e))


def k8s_scale_horizontally(namespace: str, deployment: str) -> None:
    if not k8s_apps:
        raise HTTPException(503, "Kubernetes client not available")

    key      = f"{namespace}/{deployment}"
    dep_info = cluster_state["deployments"].get(key)
    if not dep_info:
        raise HTTPException(404, f"Deployment {key} not found")

    try:
        new_replicas = dep_info.get("replicas", 1) + 1
        k8s_apps.patch_namespaced_deployment_scale(
            name=deployment,
            namespace=namespace,
            body={"spec": {"replicas": new_replicas}},
        )
        logger.info(f"Deployment {key}: replicas -> {new_replicas}")

    except ApiException as e:
        logger.error(f"Failed to horizontally scale deployment {key}: {e}")
        raise HTTPException(500, str(e))


# ─────────────────────────────────────────────
#  Warning queue helper
# ─────────────────────────────────────────────
def _add_to_warning_queue(
    identifier:  str,
    target_type: str,
    alert_type:  AlertType,
    value:       float,
    summary:     str,
    target:      dict,
) -> None:
    """
    Upsert into the warning queue — one entry per (identifier, target_type).
    Multiple alert types for the same VM are stored in the same entry's
    `alerts` dict and handled together by the DE-WOA processor.
    """
    existing = next(
        (e for e in warning_queue
         if e["identifier"] == identifier and e["target_type"] == target_type),
        None,
    )

    if existing is None:
        warning_queue.append({
            "identifier":  identifier,
            "target_type": target_type,
            "alerts": {
                alert_type.value: {"value": value, "summary": summary},
            },
            "target": target,
        })
        logger.info(f"{target_type.upper()} {identifier} queued ({alert_type.name}, value={value})")
    else:
        if alert_type.value not in existing["alerts"]:
            logger.info(
                f"{target_type.upper()} {identifier} already queued — "
                f"adding {alert_type.name} (value={value})"
            )
        else:
            logger.debug(
                f"{target_type.upper()} {identifier} — refreshing "
                f"{alert_type.name} value {existing['alerts'][alert_type.value]['value']} -> {value}"
            )
        existing["alerts"][alert_type.value] = {"value": value, "summary": summary}
        existing["target"] = target


def _combine_proxmox_resources(alert_types: list[AlertType], target: dict) -> VMResources:
    combined = VMResources()
    for alert_type in alert_types:
        r = compute_proxmox_vertical_resources(alert_type, target)
        if r.cores  is not None: combined.cores  = r.cores
        if r.memory is not None: combined.memory = r.memory
        if r.disk   is not None:
            combined.disk    = r.disk
            combined.disk_id = r.disk_id
    return combined


# ─────────────────────────────────────────────
#  Warning queue processor (scheduled every 30 s)
# ─────────────────────────────────────────────
async def process_warning_queue() -> None:
    if not warning_queue:
        return

    logger.info(f"Processing {len(warning_queue)} warning(s) via DE-WOA...")

    proxmox_items = [e for e in list(warning_queue) if e["target_type"] in ("vm", "lxc")]
    k8s_items     = [e for e in list(warning_queue) if e["target_type"] == "k8s"]

    # ── Kubernetes warnings ──────────────────────────────────────────────────
    for item in k8s_items:
        try:
            ns, dep   = item["identifier"].split("/", 1)
            all_types = [AlertType(t) for t in item["alerts"]]
            needs_retry = False

            for alert_type in all_types:
                try:
                    match alert_type:
                        case AlertType.CPU | AlertType.MEMORY:
                            k8s_scale_vertically(ns, dep, alert_type)
                        case AlertType.NETWORK | AlertType.HTTP_5XX:
                            k8s_scale_horizontally(ns, dep)
                        case _:
                            logger.warning(f"No auto-action for k8s {alert_type.name} warning.")
                            needs_retry = True
                            continue
                except Exception as e:
                    logger.error(f"Warning queue: k8s {alert_type.name} failed for {item['identifier']}: {e}")
                    needs_retry = True

            if not needs_retry and item in warning_queue:
                warning_queue.remove(item)

        except Exception as e:
            logger.error(f"Warning queue: k8s provisioning failed for {item['identifier']}: {e}")

    # ── Proxmox warnings ─────────────────────────────────────────────────────
    for ttype_str, ttype_enum in (("vm", TargetType.VM), ("lxc", TargetType.LXC)):
        group = [e for e in proxmox_items if e["target_type"] == ttype_str]
        if not group:
            continue

        vm_ids: list[int] = []
        for item in group:
            try:
                vm_ids.append(int(item["identifier"]))
            except ValueError:
                logger.warning(f"Invalid vmid in warning queue: {item['identifier']}")

        if not vm_ids:
            continue

        # Run the full DE-WOA in a thread to keep the event loop free
        best_mapping: dict[int, str] = await asyncio.to_thread(
            hybrid_de_woa_provisioning,
            vm_ids,
            ttype_enum,
            cluster_state["nodes"],
        )

        state_key = "vms" if ttype_str == "vm" else "lxc"

        for item in group:
            try:
                vmid         = int(item["identifier"])
                target       = cluster_state[state_key].get(vmid)
                if not target:
                    continue

                all_types    = [AlertType(t) for t in item["alerts"]]
                best_node    = best_mapping.get(vmid)
                current_node = target.get("node")

                if best_node and best_node != current_node:
                    logger.info(
                        f"DE-WOA recommends migrating {ttype_str.upper()} {vmid}: "
                        f"{current_node} → {best_node}  (manual action required)"
                    )

                vertical_types   = [t for t in all_types if t in (AlertType.CPU, AlertType.MEMORY, AlertType.DISK_IO)]
                horizontal_types = [t for t in all_types if t in (AlertType.NETWORK, AlertType.HTTP_5XX)]
                needs_retry      = False

                if vertical_types:
                    type_names = ", ".join(t.name for t in vertical_types)
                    try:
                        resources = _combine_proxmox_resources(vertical_types, target)
                        proxmox_scale_vertically(vmid, ttype_enum, resources)
                        logger.info(
                            f"{ttype_str.upper()} {vmid}: combined vertical scale "
                            f"[{type_names}] applied in one call"
                        )
                    except Exception as e:
                        logger.error(
                            f"Warning queue: combined vertical scale failed for "
                            f"{ttype_str} {vmid} [{type_names}]: {e}"
                        )
                        needs_retry = True

                if horizontal_types:
                    try:
                        proxmox_scale_horizontally(vmid, ttype_enum)
                    except Exception as e:
                        logger.error(f"Warning queue: horizontal scale failed for {ttype_str} {vmid}: {e}")
                        needs_retry = True

                if not needs_retry and item in warning_queue:
                    warning_queue.remove(item)

            except Exception as e:
                logger.error(f"Warning queue: Proxmox provisioning failed for {item['identifier']}: {e}")


# ─────────────────────────────────────────────
#  Core alert handler
# ─────────────────────────────────────────────
def handle_alert(alert: PrometheusAlert) -> None:
    try:
        severity = AlertSeverity(alert.labels.severity.lower())
    except ValueError:
        logger.warning(f"Unknown severity '{alert.labels.severity}' — skipping.")
        return

    try:
        alert_type = AlertType(alert.labels.type.lower())
    except ValueError:
        logger.warning(f"Unknown alert type '{alert.labels.type}' — skipping.")
        return

    try:
        target_type = TargetType(alert.labels.target_type.lower())
    except ValueError:
        logger.warning(f"Unknown target_type '{alert.labels.target_type}' — skipping.")
        return

    value = float(alert.annotations.value or 0)

    if target_type == TargetType.K8S:
        ns  = alert.labels.namespace
        dep = alert.labels.deployment
        if not ns or not dep:
            logger.warning("K8s alert missing 'namespace' or 'deployment' label — skipping.")
            return

        key    = f"{ns}/{dep}"
        target = cluster_state["deployments"].get(key)
        if not target:
            logger.warning(f"Deployment {key} not found in cluster state — skipping.")
            return

        logger.info(f"Alert — K8S {key} | {alert_type.name} | {severity.name} | value={value}")

        if severity == AlertSeverity.WARNING:
            _add_to_warning_queue(key, target_type.value, alert_type, value, alert.annotations.summary, target)
        else:
            match alert_type:
                case AlertType.CPU | AlertType.MEMORY:
                    k8s_scale_vertically(ns, dep, alert_type)
                case AlertType.NETWORK | AlertType.HTTP_5XX:
                    k8s_scale_horizontally(ns, dep)
                case AlertType.DISK_IO:
                    logger.warning(f"DISK_IO k8s scaling not automated — queuing {key}.")
                    _add_to_warning_queue(key, target_type.value, alert_type, value, alert.annotations.summary, target)

    else:
        if not alert.labels.vmid:
            logger.warning("Proxmox alert missing 'vmid' label — skipping.")
            return

        vmid      = int(alert.labels.vmid)
        state_key = "vms" if target_type == TargetType.VM else "lxc"
        target    = cluster_state[state_key].get(vmid)
        if not target:
            logger.warning(f"{target_type.value} {vmid} not found in cluster state — skipping.")
            return

        logger.info(f"Alert — {target_type.value.upper()} {vmid} | {alert_type.name} | {severity.name} | value={value}")

        if severity == AlertSeverity.WARNING:
            _add_to_warning_queue(str(vmid), target_type.value, alert_type, value, alert.annotations.summary, target)
        else:
            match alert_type:
                case AlertType.CPU | AlertType.MEMORY | AlertType.DISK_IO:
                    resources = compute_proxmox_vertical_resources(alert_type, target)
                    proxmox_scale_vertically(vmid, target_type, resources)
                case AlertType.NETWORK | AlertType.HTTP_5XX:
                    proxmox_scale_horizontally(vmid, target_type)


# ─────────────────────────────────────────────
#  Lifespan
# ─────────────────────────────────────────────
scheduler = AsyncIOScheduler()


@asynccontextmanager
async def lifespan(app: FastAPI):
    global cluster_state

    weight_sum = W_CPU + W_RAM + W_IO + W_E
    if abs(weight_sum - 1.0) >= 1e-6:
        raise RuntimeError(
            f"Fitness weights must sum to 1.0 (W_CPU={W_CPU}, W_RAM={W_RAM}, "
            f"W_IO={W_IO}, W_E={W_E} → sum={weight_sum:.6f})"
        )

    if not (0.0 < DE_WOA_WOA_FRACTION < 1.0):
        raise RuntimeError(
            f"DE_WOA_WOA_FRACTION must be in (0, 1), got {DE_WOA_WOA_FRACTION}"
        )

    logger.info(
        f"DE-WOA config: N={DE_WOA_N}, T={DE_WOA_T}, F={DE_WOA_F}, "
        f"CR={DE_WOA_CR}, b={DE_WOA_B}, woa_frac={DE_WOA_WOA_FRACTION}"
    )

    logger.info("Loading initial cluster state...")
    proxmox, k8s_deps = await asyncio.gather(
        asyncio.to_thread(fetch_proxmox_state),
        asyncio.to_thread(fetch_kubernetes_state),
    )
    async with _state_lock:
        cluster_state = {**proxmox, "deployments": k8s_deps}
    logger.info(
        f"Initial state: {len(cluster_state['nodes'])} node(s), "
        f"{len(cluster_state['vms'])} VM(s), "
        f"{len(cluster_state['lxc'])} LXC(s), "
        f"{len(cluster_state['deployments'])} deployment(s)."
    )

    scheduler.add_job(poll_cluster,          "interval", seconds=10, max_instances=1)
    scheduler.add_job(process_warning_queue, "interval", seconds=30, max_instances=1)
    scheduler.add_job(ml_sync_job,           "interval", seconds=ML_SYNC_INTERVAL, max_instances=1)
    scheduler.start()
    yield
    scheduler.shutdown()


# ─────────────────────────────────────────────
#  FastAPI app
# ─────────────────────────────────────────────
app = FastAPI(
    lifespan=lifespan,
    title="PFE Cluster Manager",
    description=(
        "Proxmox + Kubernetes cluster manager. "
        "Receives Prometheus Alertmanager webhooks and scales resources automatically "
        "using a Hybrid DE-WOA population-based provisioning algorithm."
    ),
)


# ── Alertmanager webhook ──────────────────────────────────────────────────────
@app.post("/alertmanager/webhook")
def alertmanager_webhook(payload: AlertmanagerWebhook):
    handled = resolved = skipped = 0
    for alert in payload.alerts:
        if alert.status == "resolved":
            resolved += 1
        elif alert.status == "firing":
            handle_alert(alert)
            handled += 1
        else:
            skipped += 1
    return {"status": "ok", "handled": handled, "resolved": resolved, "skipped": skipped}


# ── Health check ──────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    return {
        "status":        "ok",
        "nodes":         len(cluster_state["nodes"]),
        "vms":           len(cluster_state["vms"]),
        "lxc":           len(cluster_state["lxc"]),
        "deployments":   len(cluster_state["deployments"]),
        "warning_queue": len(warning_queue),
        "delta_history": {m: len(_delta_history[m]) for m in _delta_history},
        "using_historical_sigma": all(
            len(_delta_history[m]) >= MIN_HISTORY_SAMPLES for m in _delta_history
        ),
        "de_woa_config": {
            "N": DE_WOA_N, "T": DE_WOA_T, "F": DE_WOA_F,
            "CR": DE_WOA_CR, "b": DE_WOA_B, "woa_fraction": DE_WOA_WOA_FRACTION,
        },
    }


# ── Delta history diagnostics ─────────────────────────────────────────────────
@app.get("/delta-history/stats")
def delta_history_stats():
    stats = {}
    for metric, hist in _delta_history.items():
        n = len(hist)
        if n == 0:
            stats[metric] = {"samples": 0}
            continue
        mean = sum(hist) / n
        std  = math.sqrt(sum((v - mean) ** 2 for v in hist) / n)
        stats[metric] = {
            "samples":            n,
            "window":             HISTORY_WINDOW,
            "min_samples_needed": MIN_HISTORY_SAMPLES,
            "ready":              n >= MIN_HISTORY_SAMPLES,
            "mean":               round(mean, 4),
            "std":                round(std,  4),
            "min":                round(min(hist), 4),
            "max":                round(max(hist), 4),
        }
    return stats


# ── Proxmox cluster state ─────────────────────────────────────────────────────
@app.get("/nodes")
def get_nodes():
    return cluster_state["nodes"]


@app.get("/nodes/{node_name}")
def get_node(node_name: str):
    n = cluster_state["nodes"].get(node_name)
    if not n:
        raise HTTPException(404, f"Node {node_name} not found")
    return n


@app.get("/vms")
def get_vms():
    return cluster_state["vms"]


@app.get("/vms/{vmid}")
def get_vm(vmid: int):
    vm = cluster_state["vms"].get(vmid)
    if not vm:
        raise HTTPException(404, f"VM {vmid} not found")
    return vm


@app.get("/lxc")
def get_lxc():
    return cluster_state["lxc"]


@app.get("/lxc/{vmid}")
def get_lxc_container(vmid: int):
    ct = cluster_state["lxc"].get(vmid)
    if not ct:
        raise HTTPException(404, f"LXC {vmid} not found")
    return ct


# ── Kubernetes state ──────────────────────────────────────────────────────────
@app.get("/deployments")
def get_deployments():
    return cluster_state["deployments"]


@app.get("/deployments/{namespace}/{deployment}")
def get_deployment(namespace: str, deployment: str):
    key = f"{namespace}/{deployment}"
    dep = cluster_state["deployments"].get(key)
    if not dep:
        raise HTTPException(404, f"Deployment {key} not found")
    return dep


# ── Warning queue ─────────────────────────────────────────────────────────────
@app.get("/warning-queue")
def get_warning_queue():
    return warning_queue


@app.delete("/warning-queue/{identifier:path}")
def remove_from_warning_queue(
    identifier:  str,
    type:        Optional[str] = Query(default=None, description="Alert type to remove e.g. 'cpu'. Omit to remove entire entry."),
    target_type: Optional[str] = Query(default=None, description="Target type filter e.g. 'vm'"),
):
    global warning_queue
    removed   = 0
    new_queue = []

    for e in warning_queue:
        id_match = e["identifier"] == identifier
        tt_match = target_type is None or e["target_type"] == target_type

        if id_match and tt_match:
            if type is None:
                removed += 1
            else:
                if type in e["alerts"]:
                    del e["alerts"][type]
                    removed += 1
                if e["alerts"]:
                    new_queue.append(e)
        else:
            new_queue.append(e)

    warning_queue = new_queue
    return {"status": "ok", "removed": removed}


# ── Manual scaling ────────────────────────────────────────────────────────────
@app.put("/vms/{vmid}/resources")
def update_vm_resources(vmid: int, resources: VMResources):
    proxmox_scale_vertically(vmid, TargetType.VM, resources)
    return {"status": "ok", "vmid": vmid}


@app.put("/lxc/{vmid}/resources")
def update_lxc_resources(vmid: int, resources: VMResources):
    proxmox_scale_vertically(vmid, TargetType.LXC, resources)
    return {"status": "ok", "vmid": vmid}


@app.post("/vms/{vmid}/clone")
def clone_vm(vmid: int):
    proxmox_scale_horizontally(vmid, TargetType.VM)
    return {"status": "ok", "vmid": vmid}


@app.post("/lxc/{vmid}/clone")
def clone_lxc(vmid: int):
    proxmox_scale_horizontally(vmid, TargetType.LXC)
    return {"status": "ok", "vmid": vmid}


@app.patch("/deployments/{namespace}/{deployment}/replicas")
def scale_deployment_replicas(namespace: str, deployment: str, replicas: int):
    if not k8s_apps:
        raise HTTPException(503, "Kubernetes client not available")
    try:
        k8s_apps.patch_namespaced_deployment_scale(
            name=deployment,
            namespace=namespace,
            body={"spec": {"replicas": replicas}},
        )
        return {"status": "ok", "deployment": f"{namespace}/{deployment}", "replicas": replicas}
    except ApiException as e:
        raise HTTPException(500, str(e))


# ─────────────────────────────────────────────
#  AI Layer & Data Engineering Endpoints (Phase 3)
# ─────────────────────────────────────────────

@app.get("/cluster-snapshot")
def get_cluster_snapshot():
    """
    Called by the XGBoost service every 10s to build the Feature Vector (X).
    Combines the current cluster state with the rolling delta-history stats.
    """
    stats = delta_history_stats()
    return {
        "nodes": cluster_state["nodes"],
        "vms": cluster_state["vms"],
        "lxc": cluster_state["lxc"],
        "deployments": cluster_state["deployments"],
        "delta_history_stats": stats
    }


@app.post("/weights")
def update_weights(payload: WeightsUpdate):
    """
    Receives dynamic fitness function weights from the XGBoost ML Service.
    """
    global W_CPU, W_RAM, W_IO, W_E, _current_weights_meta
    
    # Validate sum is 1.0 (with small floating point tolerance)
    total = payload.w_cpu + payload.w_ram + payload.w_io + payload.w_e
    if abs(total - 1.0) > 0.001:
        raise HTTPException(status_code=400, detail="weights do not sum to 1.0")

    # Update global weights for the DE-WOA fitness function
    W_CPU, W_RAM, W_IO, W_E = payload.w_cpu, payload.w_ram, payload.w_io, payload.w_e
    
    # Save metadata
    _current_weights_meta = {
        "source": payload.source,
        "bottleneck": payload.bottleneck,
        "confidence": payload.confidence,
        "last_updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    }
    
    return {"status": "ok", "applied_at": _current_weights_meta["last_updated"]}


@app.get("/weights")
def get_weights():
    """
    Called by the AutoGen agent to explain to the administrator 
    why specific resources are currently prioritized.
    """
    return {
        "w_cpu": W_CPU,
        "w_ram": W_RAM,
        "w_io": W_IO,
        "w_e": W_E,
        **_current_weights_meta
    }


@app.get("/ml-config")
def get_ml_config():
    """Returns the last ML-predicted cluster config and current DE-WOA weights."""
    return {
        "de_woa_weights": {
            "W_CPU": W_CPU,
            "W_RAM": W_RAM,
            "W_IO" : W_IO,
            "W_E"  : W_E,
        },
        "ml_predicted_config"  : _ml_config,
        "alerts_yml_path"      : str(ALERTS_YML_PATH),
        "ml_service_url"       : ML_SERVICE_URL,
        "ml_sync_interval_sec" : ML_SYNC_INTERVAL,
        "buffer_size"          : len(_ml_snapshot_buffer),
    }