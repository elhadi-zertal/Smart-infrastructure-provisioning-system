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

Scaling logic (from notes.txt):
    WARNING  → add to warning_queue (DE-WOA provisioning algorithm handles it
               on the next 30s tick)
    CRITICAL →
        CPU      → vertical   : +1 vCPU  (Proxmox) | +200m CPU limit (k8s)
        MEMORY   → vertical   : +20% RAM (Proxmox) | +20% memory limit (k8s)
        DISK_IO  → vertical   : +15G     (Proxmox only)
        NETWORK  → horizontal : clone VM/LXC | +1 replica (k8s)
        HTTP_5XX → horizontal : clone VM/LXC | +1 replica (k8s)

Fitness function (from details.tex):
    fitness = sqrt(w1*(ΔC*)² + w2*(ΔR*)² + w3*(ΔIO*)² + w4*(ΔE*)²)

    where ΔX  = resource needed by VM  − resource available on node
    and   ΔX* = (ΔX − mean_ΔX) / σ_ΔX   (z-score normalisation across nodes)
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

# ─────────────────────────────────────────────
#  Logging
# ─────────────────────────────────────────────
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Suppress duplicate node-error spam — same (node, error_type) logged at most
# once per ERROR_COOLDOWN seconds.
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

PROXMOX_HOST       = os.getenv("PROXMOX_HOST")        # e.g. "172.25.5.90"
PROXMOX_USER       = os.getenv("PROXMOX_USER")        # e.g. "root@pam"
PROXMOX_TOKEN_NAME = os.getenv("PROXMOX_TOKEN_NAME")  # e.g. "my-token"
PROXMOX_TOKEN_UUID = os.getenv("PROXMOX_TOKEN_UUID")  # e.g. "ba95b5cc-..."

# Keep well below the scheduler interval (10 s) to avoid pile-ups.
PROXMOX_TIMEOUT = int(os.getenv("PROXMOX_TIMEOUT", "4"))

# Energy consumption constants (watts)
ENERGY_PER_CPU_CORE  = int(os.getenv("ENERGY_PER_CPU_CORE",  "50"))
ENERGY_PER_GB_MEMORY = int(os.getenv("ENERGY_PER_GB_MEMORY", "10"))

# DE-WOA algorithm parameters
DE_WOA_K1 = int(os.getenv("DE_WOA_K1", "10"))  # remapping attempts per sample
DE_WOA_K2 = int(os.getenv("DE_WOA_K2", "5"))   # number of independent samples

# Fitness weights — must sum to exactly 1.0 (validated at startup)
W_CPU = float(os.getenv("W_CPU", "0.35"))
W_RAM = float(os.getenv("W_RAM", "0.35"))
W_IO  = float(os.getenv("W_IO",  "0.15"))
W_E   = float(os.getenv("W_E",   "0.15"))

# Prometheus — for reading actual per-VM energy consumption
# Set PROMETHEUS_URL to enable; leave empty to fall back to the static estimate.
# The queries must return a vector with a "vmid" label on every result.
#   Example (Kepler):  sum by (vm_id) (kepler_vm_core_joules_total)
#   Example (custom):  vm_energy_watts{job="proxmox"}
PROMETHEUS_URL      = os.getenv("PROMETHEUS_URL", "")         # e.g. "http://prometheus:9090"
VM_ENERGY_QUERY     = os.getenv("VM_ENERGY_QUERY",  "vm_energy_watts")
LXC_ENERGY_QUERY    = os.getenv("LXC_ENERGY_QUERY", "lxc_energy_watts")
PROMETHEUS_VMID_LABEL = os.getenv("PROMETHEUS_VMID_LABEL", "vmid")  # label that carries the vmid

# Rolling history of observed ΔX values — used to compute a stable σ
# for z-score normalisation instead of the cross-sectional σ across nodes.
# Populated every poll cycle; fitness falls back to cross-sectional σ until
# MIN_HISTORY_SAMPLES observations have been collected.
HISTORY_WINDOW       = int(os.getenv("HISTORY_WINDOW",       "500"))
MIN_HISTORY_SAMPLES  = int(os.getenv("MIN_HISTORY_SAMPLES",  "30"))

_delta_history: dict[str, deque[float]] = {
    metric: deque(maxlen=HISTORY_WINDOW)
    for metric in ("cpu", "ram", "io", "energy")
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
#  Reads ~/.kube/config automatically (same file kubectl uses).
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
#  Prometheus — actual per-VM energy fetch
# ─────────────────────────────────────────────
def fetch_vm_energy_from_prometheus() -> dict[str, float]:
    """
    Queries the Prometheus HTTP API for actual per-VM / per-LXC energy
    consumption in watts.

    Returns {vmid_str: watts}  e.g. {"152": 87.4, "201": 43.1}

    Runs in a thread (called via asyncio.to_thread from poll_cluster).
    Returns an empty dict — and logs a throttled warning — if Prometheus
    is unreachable or misconfigured, so the rest of the poll continues.
    """
    if not PROMETHEUS_URL:
        return {}

    results: dict[str, float] = {}

    for query in (VM_ENERGY_QUERY, LXC_ENERGY_QUERY):
        encoded = urllib.parse.quote(query)
        url     = f"{PROMETHEUS_URL}/api/v1/query?query={encoded}"
        try:
            req = urllib.request.Request(url, headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=PROXMOX_TIMEOUT) as resp:
                data = json.loads(resp.read().decode())

            if data.get("status") != "success":
                _throttled_warning(
                    f"prometheus:energy:{query}",
                    f"Prometheus returned non-success status for query '{query}': {data.get('status')}",
                )
                continue

            for item in data.get("data", {}).get("result", []):
                vmid = item["metric"].get(PROMETHEUS_VMID_LABEL)
                if vmid is None:
                    continue
                try:
                    # Prometheus instant-vector value: [timestamp, "value_string"]
                    results[str(vmid)] = float(item["value"][1])
                except (IndexError, ValueError):
                    pass

        except Exception as e:
            _throttled_warning(
                f"prometheus:energy:{query}",
                f"Could not fetch energy from Prometheus (query='{query}'): {e}",
            )

    return results
class AlertSeverity(Enum):
    WARNING  = "warning"
    CRITICAL = "critical"


class AlertType(Enum):
    CPU      = "cpu"
    MEMORY   = "memory"
    DISK_IO  = "disk_io"   # Proxmox only (k8s uses persistent volumes separately)
    NETWORK  = "network"
    HTTP_5XX = "http_5xx"  # requires blackbox_exporter, not node_exporter


class TargetType(Enum):
    VM  = "vm"   # Proxmox QEMU VM       → cluster_state["vms"]
    LXC = "lxc"  # Proxmox LXC container → cluster_state["lxc"]
    K8S = "k8s"  # Kubernetes Deployment → cluster_state["deployments"]


# ─────────────────────────────────────────────
#  Prometheus Alertmanager webhook models
# ─────────────────────────────────────────────
class AlertLabels(BaseModel):
    alertname:   str = ""
    severity:    str = ""        # "warning" | "critical"
    type:        str = ""        # "cpu" | "memory" | "disk_io" | "network" | "http_5xx"
    target_type: str = "vm"      # "vm" | "lxc" | "k8s"
    # Proxmox fields
    vmid:        str = ""        # Proxmox VM or LXC ID e.g. "152"
    instance:    str = ""        # node_exporter address e.g. "192.168.1.10:9100"
    # Kubernetes fields
    namespace:   str = "default"
    deployment:  str = ""


class AlertAnnotations(BaseModel):
    summary:     str = ""
    description: str = ""
    value:       str = "0"  # current metric value at time of alert


class PrometheusAlert(BaseModel):
    status:      str             # "firing" | "resolved"
    labels:      AlertLabels
    annotations: AlertAnnotations


class AlertmanagerWebhook(BaseModel):
    """Root payload sent by Alertmanager (may batch multiple alerts)."""
    version: str
    status:  str
    alerts:  list[PrometheusAlert]


# ─────────────────────────────────────────────
#  Proxmox VM/LXC resource request model
# ─────────────────────────────────────────────
class VMResources(BaseModel):
    cores:   Optional[int] = None  # number of vCPUs
    memory:  Optional[int] = None  # RAM in MB
    disk:    Optional[str] = None  # e.g. "+15G" (can only grow, never shrink)
    disk_id: Optional[str] = "scsi0"  # relevant for VMs; LXC always uses "rootfs"


# ─────────────────────────────────────────────
#  In-memory cluster state
#  Refreshed every 10 s by poll_cluster().
# ─────────────────────────────────────────────
cluster_state: dict = {
    "nodes":       {},  # node_name        → Proxmox node info + CPU/RAM/energy stats
    "vms":         {},  # vmid (int)        → Proxmox VM info + which node
    "lxc":         {},  # vmid (int)        → Proxmox LXC info + which node
    "deployments": {},  # "namespace/name"  → Kubernetes deployment info
}

# Asyncio lock — ensures poll_cluster() and process_warning_queue() never
# write to cluster_state concurrently.
_state_lock = asyncio.Lock()

# Items flagged with WARNING, waiting for the DE-WOA provisioning algorithm.
warning_queue: list[dict] = []


# ─────────────────────────────────────────────
#  Proxmox — per-node fetch (runs in a thread pool)
# ─────────────────────────────────────────────
_DOWN_ERRORS = ("failed to get address info", "hostname lookup", "Name or service not known")


def _fetch_node(node: dict) -> tuple[str, dict, dict, dict]:
    """
    Fetches stats, VMs, and LXC containers for a single Proxmox node.
    Runs concurrently inside a ThreadPoolExecutor.

    Returns (node_name, node_info, vms_dict, lxcs_dict).
    """
    node_name = node["node"]
    node_info: dict = node.copy()
    vms:  dict = {}
    lxcs: dict = {}

    # ── Node-level stats ─────────────────────
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

    # ── QEMU VMs ─────────────────────────────
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

    # ── LXC containers ───────────────────────
    try:
        for ct in px.nodes(node_name).lxc.get():
            ct["node"] = node_name
            cores      = ct.get("cores", 1)
            # Proxmox LXC API returns maxmem in bytes, same as QEMU — not MB.
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


# ─────────────────────────────────────────────
#  Cluster polling — concurrent across nodes
# ─────────────────────────────────────────────
def fetch_proxmox_state() -> dict:
    """
    Fetches the full Proxmox cluster state.
    All nodes are queried IN PARALLEL so that a slow/unreachable node does
    not block the others.  Total wall-clock time ≤ PROXMOX_TIMEOUT.
    """
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
    """Fetches all Deployments from all namespaces.
    Key format: "namespace/name"  e.g. "default/my-app"
    """
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
    """Detect added, removed, and status-changed VMs, LXCs, and nodes."""
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


async def poll_cluster() -> None:
    """
    Called every 10 s by the scheduler.  Refreshes the full cluster_state.

    Blocking I/O is offloaded to threads via asyncio.to_thread() so the event
    loop is never frozen, and FastAPI request handlers stay responsive.

    Extra steps vs. a plain poll:
      1. Fetch actual per-VM energy from Prometheus (if configured).
      2. Enrich each VM/LXC entry with `energy_actual` (watts) and update
         node `energy_used` to the sum of real VM watts on that node.
      3. Record all (vm, node) ΔX observations into _delta_history so that
         _zscore_normalize can use a stable historical σ.
    """
    global cluster_state
    try:
        proxmox, k8s_deps, energy_data = await asyncio.gather(
            asyncio.to_thread(fetch_proxmox_state),
            asyncio.to_thread(fetch_kubernetes_state),
            asyncio.to_thread(fetch_vm_energy_from_prometheus),
        )

        # ── Enrich VMs / LXCs with actual energy ─────────────────────────────
        # Also recompute each node's energy_used as the sum of measured VM watts
        # on that node — more accurate than the CPU/RAM estimate when real data
        # is available.
        node_actual_energy: dict[str, float] = {}  # node_name → total watts

        for kind in ("vms", "lxc"):
            for vmid, vm in proxmox[kind].items():
                actual = energy_data.get(str(vmid))
                if actual is not None:
                    vm["energy_actual"] = actual
                    node = vm.get("node")
                    if node:
                        node_actual_energy[node] = node_actual_energy.get(node, 0.0) + actual

        # Replace estimated energy_used with real summed watts where we have data
        for node_name, watts in node_actual_energy.items():
            if node_name in proxmox["nodes"]:
                proxmox["nodes"][node_name]["energy_used"] = watts

        # ── Update delta history (background — never blocks scaling) ──────────
        _update_delta_history(proxmox)

        # ── Log Proxmox diffs ─────────────────
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

        # ── Log Kubernetes diffs ──────────────
        old_deps = set(cluster_state["deployments"].keys())
        new_deps = set(k8s_deps.keys())
        if new_deps - old_deps: logger.info(f"New deployments:     {new_deps - old_deps}")
        if old_deps - new_deps: logger.info(f"Removed deployments: {old_deps - new_deps}")
        for key in old_deps & new_deps:
            old_r, new_r = cluster_state["deployments"][key].get("replicas"), k8s_deps[key].get("replicas")
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
#  Fitness Function  (details.tex §1 & §2)
#
#  fitness = sqrt( w1*(ΔC*)² + w2*(ΔR*)² + w3*(ΔIO*)² + w4*(ΔE*)² )
#
#  ΔX  = resource needed by the VM  −  resource available on the node
#  ΔX* = (ΔX − mean_ΔX) / σ_ΔX
#
#  mean and σ are computed from _delta_history (all (vm, node) ΔX pairs
#  observed over the last HISTORY_WINDOW poll cycles).  When the history
#  has fewer than MIN_HISTORY_SAMPLES points we fall back to the
#  cross-sectional σ across the current candidate nodes.
#
#  Returns float('inf') when the node cannot physically host the VM.
# ─────────────────────────────────────────────
def _update_delta_history(state: dict) -> None:
    """
    Record every (vm, node) ΔX pair from the current poll snapshot into
    the rolling _delta_history buffers.

    Called once per poll cycle (every 10 s) so the buffers accumulate a
    genuine time-series distribution of ΔX values rather than just the
    cross-sectional spread across nodes at a single instant.
    Each deque is bounded to HISTORY_WINDOW entries (oldest dropped first).
    """
    nodes = state["nodes"]
    for kind in ("vms", "lxc"):
        for vm in state[kind].values():
            for node_deltas in _compute_raw_deltas(vm, nodes).values():
                for metric, value in node_deltas.items():
                    _delta_history[metric].append(value)

    logger.debug(
        f"Delta history sizes — "
        + ", ".join(f"{m}: {len(_delta_history[m])}" for m in _delta_history)
    )


def _compute_raw_deltas(vm: dict, nodes: dict) -> dict[str, dict[str, float]]:
    """
    For every online candidate node compute raw ΔX values:
        ΔX = resource_needed_by_vm − resource_available_on_node

    Negative ΔX → node has a surplus (feasible).
    Positive ΔX → node is overloaded (infeasible, rejected later).

    Energy source priority:
        1. vm["energy_actual"]  — real watts from Prometheus (preferred)
        2. vm["energy_needed"]  — static estimate (fallback)
    """
    vm_cores  = vm.get("cores", 1)
    vm_mem_gb = vm.get("maxmem", 0) / (1024 ** 3)
    # Prefer measured watts; fall back to static estimate
    vm_energy = vm.get("energy_actual", vm.get("energy_needed", 0))

    deltas: dict[str, dict[str, float]] = {}
    for name, info in nodes.items():
        if not info or info.get("status") != "online":
            continue
        cpu_avail = info["maxcpu"] * (1.0 - info.get("cpu", 0))
        ram_avail = (info["maxmem"] - info.get("mem", 0)) / (1024 ** 3)
        e_avail   = info["energy_capacity"] - info.get("energy_used", 0)

        deltas[name] = {
            "cpu":    vm_cores  - cpu_avail,
            "ram":    vm_mem_gb - ram_avail,
            "io":     0.0,        # disk/network IO not tracked at node level yet
            "energy": vm_energy - e_avail,
        }
    return deltas


def _zscore_normalize(deltas: dict[str, dict[str, float]]) -> dict[str, dict[str, float]]:
    """
    Per-metric z-score normalisation (details.tex §1: Xi* = (Xi − X̄) / σ).

    σ source — chosen automatically:
      • Historical σ  (preferred): computed from _delta_history once it holds
        ≥ MIN_HISTORY_SAMPLES observations.  Captures the true distribution of
        ΔX values seen across many (vm, node, time) combinations, so a single
        new observation is normalised consistently with past behaviour.
      • Cross-sectional σ (fallback): computed across the current candidate
        nodes only — used during warm-up before enough history exists.

    If only one candidate node exists, normalisation is undefined and we
    return zeros (any single node is trivially the best fit).
    If σ = 0 for a metric, all nodes are equal on that dimension — keep zeros.
    """
    if len(deltas) <= 1:
        return {n: {"cpu": 0.0, "ram": 0.0, "io": 0.0, "energy": 0.0} for n in deltas}

    use_history = all(
        len(_delta_history[m]) >= MIN_HISTORY_SAMPLES
        for m in ("cpu", "ram", "io", "energy")
    )

    normalized: dict[str, dict[str, float]] = {n: {} for n in deltas}

    for metric in ("cpu", "ram", "io", "energy"):
        if use_history:
            hist   = _delta_history[metric]
            mean   = sum(hist) / len(hist)
            var    = sum((v - mean) ** 2 for v in hist) / len(hist)
        else:
            # Cross-sectional fallback
            values = [deltas[n][metric] for n in deltas]
            mean   = sum(values) / len(values)
            var    = sum((v - mean) ** 2 for v in values) / len(values)

        std = math.sqrt(var) if var > 0 else 1.0
        for n in deltas:
            normalized[n][metric] = (deltas[n][metric] - mean) / std

    if not use_history:
        logger.debug(
            f"_zscore_normalize: using cross-sectional σ "
            f"(history has {min(len(_delta_history[m]) for m in _delta_history)}"
            f"/{MIN_HISTORY_SAMPLES} min samples)"
        )

    return normalized


def calculate_fitness(vm: dict, node_name: str, nodes: dict) -> float:
    """
    Weighted Euclidean fitness for placing `vm` on `node_name`
    (details.tex §2).

    Returns float('inf') if the node cannot host the VM (any ΔX > 0,
    i.e., the VM's demand exceeds what the node can supply).
    """
    deltas = _compute_raw_deltas(vm, nodes)
    raw    = deltas.get(node_name)
    if raw is None:
        return float("inf")

    # Reject infeasible placements before normalisation
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
#  Hybrid DE-WOA Provisioning Algorithm
#
#  Maps a batch of VMs (identified by vmid) to the best available nodes.
#
#  Algorithm (notes.txt):
#    1. Randomly sample m PMs from n   (m = min(|VMs|, |nodes|))
#    2. Map m VMs to the m sampled PMs (random permutation)
#    3. Calculate total fitness
#    4. Remap k1 times, keep the best mapping for this sample
#    5. Try a fresh sample of m PMs
#    6–7. Repeat k2 times
#    8. Return the mapping with the best overall fitness across all k2 samples
# ─────────────────────────────────────────────
def hybrid_de_woa_provisioning(
    vm_ids:      list[int],
    target_type: TargetType,
    nodes:       dict,
    k1:          int = DE_WOA_K1,
    k2:          int = DE_WOA_K2,
) -> dict[int, str]:
    """
    Returns best_mapping: vmid (int) → node_name (str).
    Returns an empty dict if no feasible mapping exists.
    """
    state_key    = "vms" if target_type == TargetType.VM else "lxc"
    vms_data     = cluster_state[state_key]

    valid_vm_ids = [v for v in vm_ids if v in vms_data]
    online_nodes = [n for n, info in nodes.items() if info and info.get("status") == "online"]

    if not valid_vm_ids or not online_nodes:
        logger.warning("DE-WOA: no valid VMs or online nodes available.")
        return {}

    # m must not exceed the number of available online nodes
    m             = min(len(valid_vm_ids), len(online_nodes))
    working_ids   = valid_vm_ids[:m]   # process the rest in a subsequent tick

    best_overall_mapping: dict[int, str] = {}
    best_overall_fitness: float          = float("inf")

    for _ in range(k2):
        # Step 1: sample m distinct PMs
        sample_nodes = random.sample(online_nodes, m)

        best_mapping: dict[int, str] = {}
        best_fitness: float          = float("inf")

        for _ in range(k1):
            # Step 2: random 1-to-1 assignment of VMs to sampled PMs
            shuffled = random.sample(sample_nodes, m)
            mapping  = dict(zip(working_ids, shuffled))

            # Step 3: total fitness = sum of individual fitnesses
            total = sum(
                calculate_fitness(vms_data[vmid], node_name, nodes)
                for vmid, node_name in mapping.items()
            )

            # Step 4: keep best over k1 remappings
            if total < best_fitness:
                best_fitness = total
                best_mapping = mapping

        # Step 8: keep best over k2 samples
        if best_fitness < best_overall_fitness:
            best_overall_fitness = best_fitness
            best_overall_mapping = best_mapping

    if best_overall_fitness == float("inf"):
        logger.warning("DE-WOA: no feasible mapping found — cluster may be at capacity.")

    return best_overall_mapping


# ─────────────────────────────────────────────
#  Resource calculator for Proxmox vertical scaling
# ─────────────────────────────────────────────
def compute_proxmox_vertical_resources(
    alert_type: AlertType,
    target:     dict,
) -> VMResources:
    """
    Returns the new resource values to apply via Proxmox config API:
        CPU     → +1 vCPU
        MEMORY  → +20% of current RAM  (value in MB for the Proxmox API)
        DISK_IO → +15 G
    Both VMs and LXCs store maxmem in bytes from the Proxmox API.
    """
    match alert_type:
        case AlertType.CPU:
            # "cores" is the correct Proxmox field — not "cpus"
            return VMResources(cores=target.get("cores", 1) + 1)

        case AlertType.MEMORY:
            # maxmem is in bytes for both QEMU VMs and LXC containers
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
    """
    Increases CPU / RAM / Disk on a Proxmox VM or LXC in-place.
    Triggered by CRITICAL: CPU | MEMORY | DISK_IO
    """
    state_key = "vms" if target_type == TargetType.VM else "lxc"
    target    = cluster_state[state_key].get(vmid)
    if not target:
        raise HTTPException(404, f"{target_type.value} {vmid} not found")

    node      = target["node"]
    node_info = cluster_state["nodes"].get(node)
    if not node_info:
        raise HTTPException(500, f"Node {node} not found in cluster state")

    # Energy feasibility check
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
                # LXC uses "rootfs" as the primary volume — disk_id is ignored
                px.nodes(node).lxc(vmid).resize.put(volume="rootfs", size=resources.disk)
            logger.info(f"{target_type.value.upper()} {vmid} disk resized: {resources.disk}")

    except Exception as e:
        logger.error(f"Failed to vertically scale {target_type.value} {vmid}: {e}")
        raise HTTPException(500, str(e))


# ─────────────────────────────────────────────
#  Proxmox Scaling — Horizontal
# ─────────────────────────────────────────────
def proxmox_scale_horizontally(vmid: int, target_type: TargetType) -> None:
    """
    Clones a Proxmox VM or LXC and starts the clone on the same node.
    Triggered by CRITICAL: NETWORK | HTTP_5XX
    """
    state_key = "vms" if target_type == TargetType.VM else "lxc"
    target    = cluster_state[state_key].get(vmid)
    if not target:
        raise HTTPException(404, f"{target_type.value} {vmid} not found")

    node      = target["node"]
    node_info = cluster_state["nodes"].get(node)
    if not node_info:
        raise HTTPException(500, f"Node {node} not found in cluster state")

    # Energy feasibility check
    extra_energy = target.get("energy_needed", 0)
    if node_info["energy_used"] + extra_energy > node_info["energy_capacity"]:
        logger.warning(f"Insufficient energy on {node} for horizontal scale of {target_type.value} {vmid}")
        raise HTTPException(500, f"Insufficient energy capacity on node {node}")

    # Pick a new vmid that does not collide with any existing VM or LXC
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
    """
    Parse a Kubernetes CPU string to millicores.
    Handles all valid formats: "500m", "1", "1.5", "0.25".
    """
    cpu_str = cpu_str.strip()
    if cpu_str.endswith("m"):
        return int(cpu_str[:-1])
    # Whole or decimal cores (e.g. "1", "1.5") → convert to millicores
    return int(float(cpu_str) * 1000)


# ─────────────────────────────────────────────
#  Kubernetes Scaling — Vertical
# ─────────────────────────────────────────────
def k8s_scale_vertically(namespace: str, deployment: str, alert_type: AlertType) -> None:
    """
    Increases CPU or memory limits on a Kubernetes Deployment.
    Triggered by CRITICAL: CPU | MEMORY

    CPU    → +200 m  (e.g. "500m" → "700m", "1" → "1200m")
    MEMORY → +20 %   (e.g. "512Mi" → "614Mi")
    """
    if not k8s_apps:
        raise HTTPException(503, "Kubernetes client not available")

    key      = f"{namespace}/{deployment}"
    dep_info = cluster_state["deployments"].get(key)
    if not dep_info:
        raise HTTPException(404, f"Deployment {key} not found")

    try:
        dep       = k8s_apps.read_namespaced_deployment(name=deployment, namespace=namespace)
        container = dep.spec.template.spec.containers[0]

        if container.resources            is None: container.resources            = client.V1ResourceRequirements()
        if container.resources.limits     is None: container.resources.limits     = {}
        if container.resources.requests   is None: container.resources.requests   = {}

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


# ─────────────────────────────────────────────
#  Kubernetes Scaling — Horizontal
# ─────────────────────────────────────────────
def k8s_scale_horizontally(namespace: str, deployment: str) -> None:
    """
    Adds +1 replica to a Kubernetes Deployment.
    Triggered by CRITICAL: NETWORK | HTTP_5XX
    """
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

    Each entry holds an `alerts` dict keyed by AlertType value so that a VM
    with both a CPU warning and a memory warning is represented as a single
    queue entry with two alert types, rather than two separate entries.
    When a new alert type arrives for an already-queued VM it is simply added
    to (or updated in) the existing entry's `alerts` dict.

    Queue entry shape:
    {
        "identifier":  "152",          # vmid or "namespace/name"
        "target_type": "vm",
        "alerts": {
            "cpu":    {"value": 96.0, "summary": "CPU high on VM 152"},
            "memory": {"value": 75.0, "summary": "RAM high on VM 152"},
        },
        "target": { ... },             # snapshot of the resource at alert time
    }
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
        # VM is already in the queue — add or refresh this alert type
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
        # Refresh the target snapshot so the provisioner uses up-to-date resource figures
        existing["target"] = target


# ─────────────────────────────────────────────
#  Combined resource builder for Proxmox warnings
# ─────────────────────────────────────────────
def _combine_proxmox_resources(alert_types: list[AlertType], target: dict) -> VMResources:
    """
    Merges the resource deltas for multiple alert types into a single
    VMResources object so they can be applied in ONE Proxmox API call.

    Example: CPU + MEMORY warnings on VM 152
        CPU    → cores  = current + 1
        MEMORY → memory = current * 1.20
        Result → VMResources(cores=N+1, memory=M*1.20)   ← single PUT
    """
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
#  Warning queue processor  (scheduled every 30 s)
#
#  Runs the Hybrid DE-WOA algorithm on every Proxmox item in the queue
#  to find the optimal node placement, then applies pre-emptive scaling.
#  Kubernetes items are resolved directly (no physical node placement needed).
# ─────────────────────────────────────────────
async def process_warning_queue() -> None:
    if not warning_queue:
        return

    logger.info(f"Processing {len(warning_queue)} warning(s) via DE-WOA...")

    proxmox_items = [e for e in list(warning_queue) if e["target_type"] in ("vm", "lxc")]
    k8s_items     = [e for e in list(warning_queue) if e["target_type"] == "k8s"]

    # ── Kubernetes warnings ──────────────────────────────────────────────────
    # Each entry may carry multiple alert types — handle all of them.
    for item in k8s_items:
        try:
            ns, dep     = item["identifier"].split("/", 1)
            all_types   = [AlertType(t) for t in item["alerts"]]
            needs_retry = False

            for alert_type in all_types:
                try:
                    match alert_type:
                        case AlertType.CPU | AlertType.MEMORY:
                            k8s_scale_vertically(ns, dep, alert_type)
                        case AlertType.NETWORK | AlertType.HTTP_5XX:
                            k8s_scale_horizontally(ns, dep)
                        case _:
                            logger.warning(f"No auto-action for k8s {alert_type.name} warning — left in queue.")
                            needs_retry = True
                            continue
                except Exception as e:
                    logger.error(f"Warning queue: k8s {alert_type.name} failed for {item['identifier']}: {e}")
                    needs_retry = True

            if not needs_retry and item in warning_queue:
                warning_queue.remove(item)

        except Exception as e:
            logger.error(f"Warning queue: k8s provisioning failed for {item['identifier']}: {e}")

    # ── Proxmox warnings — grouped by target_type ────────────────────────────
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

        # Run DE-WOA in a thread so the event loop stays free
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
                        f"DE-WOA: migrate {ttype_str.upper()} {vmid} "
                        f"{current_node} → {best_node}  (manual action required)"
                    )

                # Separate vertical and horizontal alert types
                vertical_types   = [t for t in all_types if t in (AlertType.CPU, AlertType.MEMORY, AlertType.DISK_IO)]
                horizontal_types = [t for t in all_types if t in (AlertType.NETWORK, AlertType.HTTP_5XX)]

                needs_retry = False

                # ── Vertical: combine ALL types into a SINGLE Proxmox API call ──
                if vertical_types:
                    type_names = ", ".join(t.name for t in vertical_types)
                    try:
                        resources = _combine_proxmox_resources(vertical_types, target)
                        proxmox_scale_vertically(vmid, ttype_enum, resources)
                        logger.info(
                            f"{ttype_str.upper()} {vmid}: applied combined vertical scale "
                            f"for [{type_names}] in one call"
                        )
                    except Exception as e:
                        logger.error(
                            f"Warning queue: combined vertical scale failed for "
                            f"{ttype_str} {vmid} [{type_names}]: {e}"
                        )
                        needs_retry = True

                # ── Horizontal: one clone covers all horizontal triggers ───────
                if horizontal_types:
                    try:
                        proxmox_scale_horizontally(vmid, ttype_enum)
                    except Exception as e:
                        logger.error(
                            f"Warning queue: horizontal scale failed for {ttype_str} {vmid}: {e}"
                        )
                        needs_retry = True

                if not needs_retry and item in warning_queue:
                    warning_queue.remove(item)

            except Exception as e:
                logger.error(f"Warning queue: Proxmox provisioning failed for {item['identifier']}: {e}")


# ─────────────────────────────────────────────
#  Core alert handler
# ─────────────────────────────────────────────
def handle_alert(alert: PrometheusAlert) -> None:
    """
    Parses Prometheus alert labels and dispatches the correct scaling action.

    WARNING  → enqueue for DE-WOA provisioning algorithm
    CRITICAL → act immediately
    """
    # ── Parse labels ─────────────────────────
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

    # ── Kubernetes Deployment ─────────────────────────────────────────────────
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
        else:  # CRITICAL
            match alert_type:
                case AlertType.CPU | AlertType.MEMORY:
                    k8s_scale_vertically(ns, dep, alert_type)
                case AlertType.NETWORK | AlertType.HTTP_5XX:
                    k8s_scale_horizontally(ns, dep)
                case AlertType.DISK_IO:
                    # k8s disk scaling requires a manual PV/PVC resize — queue it
                    logger.warning(f"DISK_IO k8s scaling not automated — queuing {key}.")
                    _add_to_warning_queue(key, target_type.value, alert_type, value, alert.annotations.summary, target)

    # ── Proxmox VM / LXC ─────────────────────────────────────────────────────
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
        else:  # CRITICAL
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

    # Validate fitness weights at startup — fail fast rather than silently
    # computing nonsense fitness values for the entire run.
    weight_sum = W_CPU + W_RAM + W_IO + W_E
    if abs(weight_sum - 1.0) >= 1e-6:
        raise RuntimeError(
            f"Fitness weights must sum to 1.0 (W_CPU={W_CPU}, W_RAM={W_RAM}, "
            f"W_IO={W_IO}, W_E={W_E} → sum={weight_sum:.6f})"
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

    # max_instances=1: skip a tick if the previous one is still running
    scheduler.add_job(poll_cluster,          "interval", seconds=10, max_instances=1)
    scheduler.add_job(process_warning_queue, "interval", seconds=30, max_instances=1)
    scheduler.start()
    yield
    scheduler.shutdown()


# ─────────────────────────────────────────────
#  App
# ─────────────────────────────────────────────
app = FastAPI(
    lifespan=lifespan,
    title="PFE Cluster Manager",
    description=(
        "Proxmox + Kubernetes cluster manager. "
        "Receives Prometheus Alertmanager webhooks and scales resources automatically."
    ),
)


# ─────────────────────────────────────────────
#  Route — Alertmanager webhook
#
#  Configure alertmanager.yml on your Prometheus server:
#
#      receivers:
#        - name: 'pfe-api'
#          webhook_configs:
#            - url: 'http://<api-host>:8000/alertmanager/webhook'
#              send_resolved: false
#      route:
#        receiver: 'pfe-api'
# ─────────────────────────────────────────────
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


# ─────────────────────────────────────────────
#  Route — Health check
# ─────────────────────────────────────────────
@app.get("/health")
def health():
    return {
        "status":        "ok",
        "nodes":         len(cluster_state["nodes"]),
        "vms":           len(cluster_state["vms"]),
        "lxc":           len(cluster_state["lxc"]),
        "deployments":   len(cluster_state["deployments"]),
        "warning_queue": len(warning_queue),
        "delta_history": {
            m: len(_delta_history[m]) for m in _delta_history
        },
        "using_historical_sigma": all(
            len(_delta_history[m]) >= MIN_HISTORY_SAMPLES for m in _delta_history
        ),
    }


# ─────────────────────────────────────────────
#  Route — Delta history diagnostics
# ─────────────────────────────────────────────
@app.get("/delta-history/stats")
def delta_history_stats():
    """
    Returns mean, σ, min, max, and sample count for each ΔX metric in the
    rolling history.  Useful for tuning HISTORY_WINDOW and MIN_HISTORY_SAMPLES
    and for verifying that the fitness function is normalising correctly.
    """
    stats = {}
    for metric, hist in _delta_history.items():
        n = len(hist)
        if n == 0:
            stats[metric] = {"samples": 0}
            continue
        mean = sum(hist) / n
        std  = math.sqrt(sum((v - mean) ** 2 for v in hist) / n)
        stats[metric] = {
            "samples":           n,
            "window":            HISTORY_WINDOW,
            "min_samples_needed": MIN_HISTORY_SAMPLES,
            "ready":             n >= MIN_HISTORY_SAMPLES,
            "mean":              round(mean, 4),
            "std":               round(std,  4),
            "min":               round(min(hist), 4),
            "max":               round(max(hist), 4),
        }
    return stats


# ─────────────────────────────────────────────
#  Routes — Proxmox cluster state
# ─────────────────────────────────────────────
@app.get("/nodes")
def get_nodes():
    """All Proxmox physical machines with CPU / RAM / energy stats."""
    return cluster_state["nodes"]


@app.get("/nodes/{node_name}")
def get_node(node_name: str):
    n = cluster_state["nodes"].get(node_name)
    if not n:
        raise HTTPException(404, f"Node {node_name} not found")
    return n


@app.get("/vms")
def get_vms():
    """All Proxmox QEMU VMs across the whole cluster."""
    return cluster_state["vms"]


@app.get("/vms/{vmid}")
def get_vm(vmid: int):
    vm = cluster_state["vms"].get(vmid)
    if not vm:
        raise HTTPException(404, f"VM {vmid} not found")
    return vm


@app.get("/lxc")
def get_lxc():
    """All Proxmox LXC containers across the whole cluster."""
    return cluster_state["lxc"]


@app.get("/lxc/{vmid}")
def get_lxc_container(vmid: int):
    ct = cluster_state["lxc"].get(vmid)
    if not ct:
        raise HTTPException(404, f"LXC {vmid} not found")
    return ct


# ─────────────────────────────────────────────
#  Routes — Kubernetes state
# ─────────────────────────────────────────────
@app.get("/deployments")
def get_deployments():
    """All Kubernetes Deployments across all namespaces."""
    return cluster_state["deployments"]


@app.get("/deployments/{namespace}/{deployment}")
def get_deployment(namespace: str, deployment: str):
    key = f"{namespace}/{deployment}"
    dep = cluster_state["deployments"].get(key)
    if not dep:
        raise HTTPException(404, f"Deployment {key} not found")
    return dep


# ─────────────────────────────────────────────
#  Routes — Warning queue
# ─────────────────────────────────────────────
@app.get("/warning-queue")
def get_warning_queue():
    """All targets (VMs, LXCs, Deployments) with active warnings waiting for provisioning."""
    return warning_queue


@app.delete("/warning-queue/{identifier:path}")
def remove_from_warning_queue(
    identifier:  str,
    type:        Optional[str] = Query(default=None, description="Remove only this alert type e.g. 'cpu'. Omit to remove the entire entry."),
    target_type: Optional[str] = Query(default=None, description="Target type filter e.g. 'vm'"),
):
    """
    Remove an entry (or a single alert type within an entry) from the warning queue.

    For Proxmox: identifier = vmid e.g. "152"
    For k8s:     identifier = "namespace/deployment" e.g. "default/my-app"

    - Omit `type` → remove the entire entry for that identifier.
    - Pass `type=cpu` → remove only the CPU alert; other alert types on the
      same VM stay queued. If that was the last type the entry is removed too.
    """
    global warning_queue
    removed = 0

    new_queue = []
    for e in warning_queue:
        id_match = e["identifier"] == identifier
        tt_match = target_type is None or e["target_type"] == target_type

        if id_match and tt_match:
            if type is None:
                # Drop the whole entry
                removed += 1
            else:
                # Drop only the requested alert type
                if type in e["alerts"]:
                    del e["alerts"][type]
                    removed += 1
                if e["alerts"]:
                    # Entry still has other alert types — keep it
                    new_queue.append(e)
                # else: entry is now empty, drop it silently
        else:
            new_queue.append(e)

    warning_queue = new_queue
    return {"status": "ok", "removed": removed}


# ─────────────────────────────────────────────
#  Routes — Manual scaling (for testing / override)
# ─────────────────────────────────────────────
@app.put("/vms/{vmid}/resources")
def update_vm_resources(vmid: int, resources: VMResources):
    """Manually trigger vertical scaling on a Proxmox VM."""
    proxmox_scale_vertically(vmid, TargetType.VM, resources)
    return {"status": "ok", "vmid": vmid}


@app.put("/lxc/{vmid}/resources")
def update_lxc_resources(vmid: int, resources: VMResources):
    """Manually trigger vertical scaling on a Proxmox LXC."""
    proxmox_scale_vertically(vmid, TargetType.LXC, resources)
    return {"status": "ok", "vmid": vmid}


@app.post("/vms/{vmid}/clone")
def clone_vm(vmid: int):
    """Manually trigger horizontal scaling (clone) on a Proxmox VM."""
    proxmox_scale_horizontally(vmid, TargetType.VM)
    return {"status": "ok", "vmid": vmid}


@app.post("/lxc/{vmid}/clone")
def clone_lxc(vmid: int):
    """Manually trigger horizontal scaling (clone) on a Proxmox LXC."""
    proxmox_scale_horizontally(vmid, TargetType.LXC)
    return {"status": "ok", "vmid": vmid}


@app.patch("/deployments/{namespace}/{deployment}/replicas")
def scale_deployment_replicas(namespace: str, deployment: str, replicas: int):
    """Manually set the replica count of a Kubernetes Deployment."""
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