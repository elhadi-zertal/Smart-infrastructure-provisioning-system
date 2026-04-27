import asyncio
import copy
import json
import math
import os
import logging
import time
import random
from collections import deque
from datetime import datetime, timezone
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel
from enum import Enum
from dotenv import load_dotenv
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from proxmoxer import ProxmoxAPI
from kubernetes import client, config as kube_config
import httpx

# ─────────────────────────────────────────────
#  Logging & Environment
# ─────────────────────────────────────────────
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_error_seen: dict[str, float] = {}
ERROR_COOLDOWN = 60

def _throttled_warning(key: str, message: str) -> None:
    now = time.monotonic()
    if now - _error_seen.get(key, 0) >= ERROR_COOLDOWN:
        logger.warning(message)
        _error_seen[key] = now

load_dotenv()

CLUSTER_API_KEY    = os.getenv("CLUSTER_API_KEY", "dev-secure-key-123")
PROXMOX_HOST       = os.getenv("PROXMOX_HOST")
PROXMOX_USER       = os.getenv("PROXMOX_USER")
PROXMOX_TOKEN_NAME = os.getenv("PROXMOX_TOKEN_NAME")
PROXMOX_TOKEN_UUID = os.getenv("PROXMOX_TOKEN_UUID")
PROXMOX_TIMEOUT    = int(os.getenv("PROXMOX_TIMEOUT", "4"))
ENERGY_PER_CPU_CORE  = int(os.getenv("ENERGY_PER_CPU_CORE",  "50"))
ENERGY_PER_GB_MEMORY = int(os.getenv("ENERGY_PER_GB_MEMORY", "10"))

DE_WOA_N            = int(os.getenv("DE_WOA_N",            "10"))
DE_WOA_T            = int(os.getenv("DE_WOA_T",            "20"))
DE_WOA_F            = float(os.getenv("DE_WOA_F",          "0.5"))
DE_WOA_CR           = float(os.getenv("DE_WOA_CR",         "0.5"))
DE_WOA_B            = float(os.getenv("DE_WOA_B",          "1.0"))
DE_WOA_WOA_FRACTION = float(os.getenv("DE_WOA_WOA_FRACTION","0.5"))

PROMETHEUS_URL        = os.getenv("PROMETHEUS_URL", "")
PROMETHEUS_VMID_LABEL = os.getenv("PROMETHEUS_VMID_LABEL", "vmid")

NODE_IO_CAPACITY_BPS = float(os.getenv("NODE_IO_CAPACITY_BPS", str(500 * 1024 * 1024)))
HISTORY_WINDOW      = int(os.getenv("HISTORY_WINDOW",      "500"))
MIN_HISTORY_SAMPLES = int(os.getenv("MIN_HISTORY_SAMPLES", "30"))

# Scale Down Constraints
SCALEDOWN_SUSTAINED_POLLS = int(os.getenv("SCALEDOWN_SUSTAINED_POLLS", "1"))
SCALEDOWN_INTERVAL_S      = int(os.getenv("SCALEDOWN_INTERVAL_S",      "120"))
MIN_VM_CORES              = int(os.getenv("MIN_VM_CORES",              "1"))
MIN_VM_MEMORY_MB          = int(os.getenv("MIN_VM_MEMORY_MB",          "512"))
APPROVAL_TTL_SECONDS      = int(os.getenv("APPROVAL_TTL_SECONDS",      "3600"))

SCALEDOWN_CPU_LOW_PCT   = float(os.getenv("SCALEDOWN_CPU_LOW_PCT",   "30.0"))
SCALEDOWN_RAM_LOW_PCT   = float(os.getenv("SCALEDOWN_RAM_LOW_PCT",   "30.0"))
SCALEDOWN_HTTP_LOW_PCT  = float(os.getenv("SCALEDOWN_HTTP_LOW_PCT",  "0.5"))

# Orchestrator variables linked to ML Sync
ML_SERVICE_URL = os.getenv("ML_SERVICE_URL", "http://localhost:8001")

W_CPU = float(os.getenv("W_CPU", "0.35"))
W_RAM = float(os.getenv("W_RAM", "0.35"))
W_IO  = float(os.getenv("W_IO",  "0.15"))
W_E   = float(os.getenv("W_E",   "0.15"))
_weights_lock = asyncio.Lock()

_ml_config: dict = {
    "w_cpu": W_CPU, "w_ram": W_RAM, "w_io": W_IO, "w_energy": W_E,
    "thresh_cpu_warn": 80.0, "thresh_cpu_crit": 95.0, "thresh_cpu_low": SCALEDOWN_CPU_LOW_PCT,
    "thresh_ram_warn": 60.0, "thresh_ram_crit": 80.0, "thresh_ram_low": SCALEDOWN_RAM_LOW_PCT,
    "thresh_http_warn": 1.0, "thresh_http_crit": 5.0, "thresh_http_low": SCALEDOWN_HTTP_LOW_PCT,
    "thresh_disk_warn": 80.0, "thresh_disk_crit": 90.0, "thresh_net_crit": 2.0
}

_ml_metrics_buffer: list[dict] = []

# ─────────────────────────────────────────────
#  State Trackers
# ─────────────────────────────────────────────
cluster_state: dict = {"nodes": {}, "vms": {}, "lxc": {}, "deployments": {}}
_state_lock  = asyncio.Lock()
warning_queue: list[dict] = []

_delta_history: dict[str, deque[float]] = {metric: deque(maxlen=HISTORY_WINDOW) for metric in ("cpu", "ram", "io", "energy")}
_scaledown_tracker: dict[int, dict] = {}
_clone_registry: dict[int, int] = {}
approval_queue: list[dict] = []
_approval_lock = asyncio.Lock()

action_log: deque[dict] = deque(maxlen=100)
_current_cycle_actions: dict[int, dict] = {}

def verify_api_key(x_api_key: str = Header(...)):
    if x_api_key != CLUSTER_API_KEY: raise HTTPException(status_code=403, detail="Invalid API Key")

px = ProxmoxAPI(PROXMOX_HOST, user=PROXMOX_USER, token_name=PROXMOX_TOKEN_NAME, token_value=PROXMOX_TOKEN_UUID, verify_ssl=False, timeout=PROXMOX_TIMEOUT)
k8s_apps = None
k8s_core = None

# ─────────────────────────────────────────────
#  Pydantic Models
# ─────────────────────────────────────────────
class AlertSeverity(Enum): WARNING = "warning"; CRITICAL = "critical"
class AlertType(Enum): CPU = "cpu"; MEMORY = "memory"; DISK_IO = "disk_io"; NETWORK = "network"; HTTP_5XX = "http_5xx"
class TargetType(Enum): VM = "vm"; LXC = "lxc"; K8S = "k8s"

class AlertLabels(BaseModel): alertname: str=""; severity: str=""; type: str=""; target_type: str="vm"; vmid: str=""; instance: str=""; namespace: str="default"; deployment: str=""
class AlertAnnotations(BaseModel): summary: str=""; description: str=""; value: str="0"
class PrometheusAlert(BaseModel): status: str; labels: AlertLabels; annotations: AlertAnnotations
class AlertmanagerWebhook(BaseModel): version: str; status: str; alerts: list[PrometheusAlert]

# ─────────────────────────────────────────────
#  Polling & Telemetry
# ─────────────────────────────────────────────
def _fetch_node(node: dict) -> tuple:
    node_name = node["node"]
    node_info = node.copy()
    vms, lxcs = {}, {}
    try:
        status = px.nodes(node_name).status.get()
        cpu_cores = status.get("cpuinfo", {}).get("cpus", 1)
        maxmem_gb = status.get("memory", {}).get("total", 0) / (1024 ** 3)
        node_info.update({
            "node": node_name, "status": node.get("status"),
            "cpu": status.get("cpu", 0), "maxcpu": cpu_cores,
            "mem": status.get("memory", {}).get("used", 0), "maxmem": status.get("memory", {}).get("total", 0),
            "energy_used": (status.get("cpu", 0) * cpu_cores * ENERGY_PER_CPU_CORE) + ((status.get("memory", {}).get("used", 0) / (1024**3)) * ENERGY_PER_GB_MEMORY),
            "energy_capacity": cpu_cores * ENERGY_PER_CPU_CORE + maxmem_gb * ENERGY_PER_GB_MEMORY,
        })
    except Exception: pass

    try:
        for vm in px.nodes(node_name).qemu.get():
            vm["node"] = node_name
            vm["energy_needed"] = vm.get("cores", 1) * ENERGY_PER_CPU_CORE + (vm.get("maxmem", 0)/(1024**3)) * ENERGY_PER_GB_MEMORY
            vms[vm["vmid"]] = vm
    except Exception: pass

    try:
        for ct in px.nodes(node_name).lxc.get():
            ct["node"] = node_name
            ct["energy_needed"] = ct.get("cores", 1) * ENERGY_PER_CPU_CORE + (ct.get("maxmem", 0)/(1024**3)) * ENERGY_PER_GB_MEMORY
            lxcs[ct["vmid"]] = ct
    except Exception: pass
    return node_name, node_info, vms, lxcs

def fetch_proxmox_state() -> dict:
    state = {"nodes": {}, "vms": {}, "lxc": {}}
    try: nodes = px.nodes.get()
    except Exception: return state
    # Max Workers shouldn't exceed ThreadPool constraints (max 32 typically)
    with ThreadPoolExecutor(max_workers=min(max(len(nodes), 1), 32)) as pool:
        futures = {pool.submit(_fetch_node, n): n["node"] for n in nodes}
        for f in as_completed(futures):
            nn, ninfo, vms, lxcs = f.result()
            state["nodes"][nn] = ninfo
            state["vms"].update(vms)
            state["lxc"].update(lxcs)
    return state

def fetch_kubernetes_state() -> dict:
    deps = {}
    if not k8s_apps: return deps
    try:
        for dep in k8s_apps.list_deployment_for_all_namespaces().items:
            key = f"{dep.metadata.namespace}/{dep.metadata.name}"
            c = dep.spec.template.spec.containers[0]
            limits = c.resources.limits if c.resources and c.resources.limits else {}
            deps[key] = {
                "name": dep.metadata.name, "namespace": dep.metadata.namespace,
                "replicas": dep.spec.replicas, "ready_replicas": dep.status.ready_replicas or 0,
                "cpu_limit": limits.get("cpu"), "memory_limit": limits.get("memory")
            }
    except Exception: pass
    return deps

def fetch_vm_metrics_from_prometheus() -> dict[str, dict[str, float]]:
    # Mocking standard prometheus behaviour here; implement actual queries based on your env
    return {}

def _record_ml_snapshot(vm: dict, vmid: int, instance_id: str) -> None:
    cpu_pct  = float(vm.get("cpu", 0.0)) * 100.0
    ram_pct  = (float(vm.get("mem", 0)) / max(float(vm.get("maxmem", 1)), 1)) * 100.0
    disk_bps = float(vm.get("diskread", 0)) + float(vm.get("diskwrite", 0))
    io_pct   = min(disk_bps / NODE_IO_CAPACITY_BPS * 100.0, 100.0)
    power_w  = vm.get("power_watts", 40.0 + (80.0 * (cpu_pct / 100.0)))

    vm_name = vm.get("name", str(vmid))

    _ml_metrics_buffer.append({
        "instance": instance_id,
        "vm_name": vm_name,
        "vlan": str(vm.get("vlan", "1")),
        "up": 1.0 if vm.get("status") == "running" else 0.0,
        "scrape_duration": float(vm.get("scrape_duration", 0.1)),
        "cpu_pct": cpu_pct,
        "ram_pct": ram_pct,
        "io_pct": io_pct,
        "http_5xx_rate": float(vm.get("http_5xx_rate", 0)),
        "net_drop_rate": float(vm.get("net_drop_rate", 0)),
        "power_watts": power_w
    })

async def poll_cluster() -> None:
    global cluster_state
    try:
        proxmox, k8s_deps, prom_metrics = await asyncio.gather(
            asyncio.to_thread(fetch_proxmox_state),
            asyncio.to_thread(fetch_kubernetes_state),
            asyncio.to_thread(fetch_vm_metrics_from_prometheus),
        )

        for kind in ("vms", "lxc"):
            for vmid, vm in proxmox[kind].items():
                vmid_str = str(vmid)
                for field, per_vm in prom_metrics.items():
                    if vmid_str in per_vm: vm[field] = per_vm[vmid_str]

                # Create snapshot buffer for the Standalone ML Service
                _record_ml_snapshot(vm, vmid, f"{vm.get('ip', vmid)}:9100")

        _current_cycle_actions.clear()
        _update_delta_history(proxmox)
        async with _state_lock:
            cluster_state = {**proxmox, "deployments": k8s_deps}

    except Exception as e:
        logger.error(f"Polling error: {e}")

def _update_delta_history(state: dict) -> None:
    nodes = state["nodes"]
    for kind in ("vms", "lxc"):
        for vm in state[kind].values():
            for node_deltas in _compute_raw_deltas(vm, nodes).values():
                for metric, value in node_deltas.items():
                    _delta_history[metric].append(value)

def _compute_raw_deltas(vm: dict, nodes: dict) -> dict:
    vm_cpu = vm.get("cpu_actual", vm.get("cores", 1) * vm.get("cpu", 1.0))
    vm_ram = vm.get("ram_actual", vm.get("maxmem", 0) * vm.get("mem", 1.0) if vm.get("mem") else vm.get("maxmem", 0))
    vm_io, vm_energy = vm.get("io_actual", 0.0), vm.get("energy_actual", vm.get("energy_needed", 0.0))

    deltas = {}
    for name, info in nodes.items():
        if not info or info.get("status") != "online": continue
        cpu_avail = info["maxcpu"] * (1.0 - info.get("cpu", 0.0))
        ram_avail = info["maxmem"] - info.get("mem", 0)
        io_avail  = NODE_IO_CAPACITY_BPS - info.get("io_used", 0.0)
        e_avail   = info["energy_capacity"] - info.get("energy_used", 0.0)
        deltas[name] = {"cpu": vm_cpu - cpu_avail, "ram": vm_ram - ram_avail, "io": vm_io - io_avail, "energy": vm_energy - e_avail}
    return deltas

def _zscore_normalize(deltas: dict) -> dict:
    if len(deltas) <= 1: return {n: {"cpu": 0.0, "ram": 0.0, "io": 0.0, "energy": 0.0} for n in deltas}
    normalized = {n: {} for n in deltas}
    use_hist = all(len(_delta_history[m]) >= MIN_HISTORY_SAMPLES for m in ("cpu", "ram", "io", "energy"))

    for metric in ("cpu", "ram", "io", "energy"):
        if use_hist:
            hist = _delta_history[metric]
            mean = sum(hist) / len(hist)
            var  = sum((v - mean) ** 2 for v in hist) / len(hist)
        else:
            values = [deltas[n][metric] for n in deltas]
            mean   = sum(values) / len(values)
            var    = sum((v - mean) ** 2 for v in values) / len(values)

        std = math.sqrt(var) if var > 1e-6 else 1.0
        for n in deltas:
            normalized[n][metric] = (deltas[n][metric] - mean) / std
    return normalized

def calculate_fitness(vm: dict, node_name: str, nodes: dict, weights: tuple) -> float:
    w_c, w_r, w_i, w_e = weights
    deltas = _compute_raw_deltas(vm, nodes)
    raw = deltas.get(node_name)
    if not raw or raw["cpu"] > 0 or raw["ram"] > 0 or raw["energy"] > 0: return float("inf")
    norm = _zscore_normalize(deltas)[node_name]
    return math.sqrt(w_c * norm["cpu"]**2 + w_r * norm["ram"]**2 + w_i * norm["io"]**2 + w_e * norm["energy"]**2)

def _mapping_fitness(mapping: list[str], vm_ids: list[int], vms_data: dict, nodes: dict, weights: tuple) -> float:
    total = 0.0
    for vmid, node_name in zip(vm_ids, mapping):
        f = calculate_fitness(vms_data[vmid], node_name, nodes, weights)
        if f == float("inf"): return float("inf")
        total += f
    return total

def hybrid_de_woa_provisioning(vm_ids: list[int], target_type: TargetType, nodes: dict, weights: tuple) -> dict[int, str]:
    state_key = "vms" if target_type == TargetType.VM else "lxc"
    state = cluster_state
    vms_data = state[state_key]

    valid_vm_ids = [v for v in vm_ids if v in vms_data]
    online_nodes = [n for n, info in nodes.items() if info and info.get("status") == "online"]
    if not valid_vm_ids or not online_nodes: return {}

    N, T, F, CR, b, woa_frac = max(DE_WOA_N, 6), DE_WOA_T, DE_WOA_F, DE_WOA_CR, DE_WOA_B, DE_WOA_WOA_FRACTION
    population = [[random.choice(online_nodes) for _ in valid_vm_ids] for _ in range(N)]
    fitnesses = [_mapping_fitness(mp, valid_vm_ids, vms_data, nodes, weights) for mp in population]

    best_idx = min(range(N), key=lambda i: fitnesses[i])
    x_star, f_star = population[best_idx].copy(), fitnesses[best_idx]

    all_indices = list(range(N))
    random.shuffle(all_indices)
    n_woa = max(2, int(N * woa_frac))
    woa_indices, de_indices = all_indices[:n_woa], all_indices[n_woa:]

    for t in range(T):
        a = 2.0 * (1.0 - t / T)
        for i in woa_indices:
            xi = population[i]
            r1, r2, p = random.random(), random.random(), random.random()
            A, C = 2.0 * a * r1 - a, 2.0 * r2
            if p < 0.5:
                target_map = x_star if abs(A) < 1.0 else population[random.choice([j for j in woa_indices if j != i] or [best_idx])]
                D = sum(1 for k in range(len(xi)) if xi[k] != target_map[k]) * C
                strength = abs(A) * D
                diff_pos = [k for k in range(len(xi)) if xi[k] != target_map[k]]
                n_copy = min(len(diff_pos), max(0, int(strength)))
                for k in random.sample(diff_pos, n_copy) if n_copy > 0 else []: xi[k] = target_map[k]
            else:
                l = random.uniform(-1.0, 1.0)
                p_adopt = max(0.0, min(1.0, (math.exp(b * l) * math.cos(2 * math.pi * l) + math.exp(b)) / (2.0 * math.exp(b))))
                for k in range(len(xi)):
                    if random.random() < p_adopt: xi[k] = x_star[k]
            fitnesses[i] = _mapping_fitness(xi, valid_vm_ids, vms_data, nodes, weights)

        woa_best_idx = min(woa_indices, key=lambda i: fitnesses[i])
        if fitnesses[woa_best_idx] < f_star:
            f_star, x_star = fitnesses[woa_best_idx], population[woa_best_idx].copy()

        donor_pool = [j for j in all_indices if j not in de_indices]
        for j in de_indices:
            eligible = [k for k in donor_pool if k != j]
            if len(eligible) < 2: continue
            xr1, xr2 = population[eligible[0]], population[eligible[1]]
            donor = [xr1[k] if xr1[k] != xr2[k] and random.random() < F else x_star[k] for k in range(len(x_star))]
            trial = [donor[k] if random.random() <= CR else population[j][k] for k in range(len(x_star))]

            f_trial = _mapping_fitness(trial, valid_vm_ids, vms_data, nodes, weights)
            if f_trial < fitnesses[j]:
                population[j], fitnesses[j] = trial, f_trial

        de_best_idx = min(de_indices, key=lambda i: fitnesses[i])
        if fitnesses[de_best_idx] < f_star:
            f_star, x_star = fitnesses[de_best_idx], population[de_best_idx].copy()

    if f_star == float("inf"): return {}
    return {vmid: node for vmid, node in zip(valid_vm_ids, x_star)}

# ─────────────────────────────────────────────
#  Action Logging
# ─────────────────────────────────────────────
def _log_action(action: str, vmid: int, params: dict, outcome: str, reason: str, triggered_by: str = "scaledown") -> None:
    action_log.appendleft({
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "action": action,
        "vmid": vmid,
        "params": params,
        "outcome": outcome,
        "reason": reason,
        "triggered_by": triggered_by,
    })
    _current_cycle_actions.setdefault(vmid, {})[action] = 1

def _node_has_headroom(node_info: dict, vm: dict, extra_cores: int = 1, extra_mem_frac: float = 0.20) -> bool:
    if not node_info or node_info.get("status") != "online": return False
    node_cpu_free = node_info.get("maxcpu", 0) * (1.0 - node_info.get("cpu", 1.0))
    if node_cpu_free < extra_cores: return False
    vm_maxmem    = float(vm.get("maxmem", 0))
    node_ram_free = node_info.get("maxmem", 0) - node_info.get("mem", 0)
    if node_ram_free < vm_maxmem * extra_mem_frac: return False
    additional_energy = extra_cores * ENERGY_PER_CPU_CORE + (vm_maxmem * extra_mem_frac / (1024**3)) * ENERGY_PER_GB_MEMORY
    node_energy_free  = node_info.get("energy_capacity", 0) - node_info.get("energy_used", 0)
    if node_energy_free < additional_energy: return False
    return True

def _add_to_warning_queue(identifier: str, target_type: str, alert_type: AlertType, value: float, summary: str, target: dict):
    target_cp = copy.deepcopy(target)
    for e in warning_queue:
        if e["identifier"] == identifier and e["target_type"] == target_type:
            e["alerts"][alert_type.value] = {"value": value, "summary": summary}
            e["target"] = target_cp
            return
    warning_queue.append({"identifier": identifier, "target_type": target_type, "alerts": {alert_type.value: {"value": value, "summary": summary}}, "target": target_cp})

async def process_warning_queue() -> None:
    if not warning_queue: return
    proxmox_items = [e for e in list(warning_queue) if e["target_type"] in ("vm", "lxc")]
    k8s_items     = [e for e in list(warning_queue) if e["target_type"] == "k8s"]

    state = cluster_state
    for item in k8s_items:
        ns, dep = item["identifier"].split("/", 1)
        needs_retry = False
        for t in item["alerts"]:
            at = AlertType(t)
            if at in (AlertType.CPU, AlertType.MEMORY):
                try:
                    dep_obj = state["deployments"].get(f"{ns}/{dep}")
                    if dep_obj and k8s_apps:
                        container = k8s_apps.read_namespaced_deployment(dep, ns).spec.template.spec.containers[0]
                        limits = container.resources.limits or {}
                        if at == AlertType.CPU:
                            cur = float(limits.get("cpu", "0.5").rstrip("m")) / (1000 if "m" in limits.get("cpu", "") else 1)
                            limits["cpu"] = f"{int(cur * 1.25 * 1000)}m"
                        elif at == AlertType.MEMORY:
                            cur_bytes = int(limits.get("memory", "512Mi").rstrip("Mi")) * 1024 * 1024
                            limits["memory"] = f"{int(cur_bytes * 1.20 / (1024*1024))}Mi"
                        patch = {"spec": {"template": {"spec": {"containers": [{"name": container.name, "resources": {"limits": limits}}]}}}}
                        k8s_apps.patch_namespaced_deployment(dep, ns, patch)
                except Exception as e: needs_retry = True
            elif at in (AlertType.NETWORK, AlertType.HTTP_5XX):
                try:
                    dep_obj = state["deployments"].get(f"{ns}/{dep}")
                    if dep_obj and k8s_apps:
                        new_replicas = dep_obj.get("replicas", 1) + 1
                        k8s_apps.patch_namespaced_deployment(dep, ns, {"spec": {"replicas": new_replicas}})
                except Exception as e: needs_retry = True
            elif at == AlertType.DISK_IO: continue
            else: needs_retry = True
        if not needs_retry and item in warning_queue: warning_queue.remove(item)

    async with _state_lock: nodes_snapshot = copy.deepcopy(state["nodes"])
    async with _weights_lock: weights_snapshot = (W_CPU, W_RAM, W_IO, W_E)

    for ttype_str, ttype_enum in (("vm", TargetType.VM), ("lxc", TargetType.LXC)):
        group = [e for e in proxmox_items if e["target_type"] == ttype_str]
        if not group: continue
        vm_ids = [int(item["identifier"]) for item in group if item["identifier"].isdigit()]

        best_mapping = await asyncio.to_thread(hybrid_de_woa_provisioning, vm_ids, ttype_enum, nodes_snapshot, weights_snapshot)
        state_key = "vms" if ttype_str == "vm" else "lxc"

        for item in group:
            vmid       = int(item["identifier"])
            target     = state[state_key].get(vmid)
            if not target: continue

            current_node = target.get("node")
            best_node    = best_mapping.get(vmid)
            node_info    = nodes_snapshot.get(current_node, {})

            needs_migration = False
            if best_node and best_node != current_node:
                needs_migration, reason = True, "DE-WOA recommends better placement"
            elif not _node_has_headroom(node_info, target):
                needs_migration, reason = True, "current node lacks headroom for scale-up"

            if needs_migration and best_node and best_node != current_node:
                try:
                    if ttype_enum == TargetType.VM:
                        await asyncio.to_thread(px.nodes(current_node).qemu(vmid).migrate.post, target=best_node, online=1)
                        _log_action("migrate", vmid, {"from": current_node, "to": best_node}, "success", reason, "scaleup")
                        await asyncio.sleep(5)
                        current_node = best_node
                except Exception as e:
                    _log_action("migrate", vmid, {"from": current_node, "to": best_node}, "error", str(e), "scaleup")

            for alert_key, alert_data in item["alerts"].items():
                at = AlertType(alert_key)

                if at == AlertType.CPU:
                    new_cores = int(target.get("cores", 1)) + 1
                    try:
                        if ttype_enum == TargetType.VM: await asyncio.to_thread(px.nodes(current_node).qemu(vmid).config.put, cores=new_cores)
                        else: await asyncio.to_thread(px.nodes(current_node).lxc(vmid).config.put, cores=new_cores)
                        _log_action("scaleup_cores", vmid, {"cores": new_cores}, "success", "CPU threshold exceeded", "scaleup")
                    except Exception as e: _log_action("scaleup_cores", vmid, {"cores": new_cores}, "error", str(e), "scaleup")

                elif at == AlertType.MEMORY:
                    new_mem_mb = int(int(target.get("maxmem", 512 * 1024 * 1024)) * 1.20 / (1024 * 1024))
                    try:
                        if ttype_enum == TargetType.VM: await asyncio.to_thread(px.nodes(current_node).qemu(vmid).config.put, memory=new_mem_mb)
                        else: await asyncio.to_thread(px.nodes(current_node).lxc(vmid).config.put, memory=new_mem_mb)
                        _log_action("scaleup_memory", vmid, {"memory_mb": new_mem_mb}, "success", "RAM threshold exceeded", "scaleup")
                    except Exception as e: _log_action("scaleup_memory", vmid, {"memory_mb": new_mem_mb}, "error", str(e), "scaleup")

                elif at == AlertType.DISK_IO:
                    try:
                        if ttype_enum == TargetType.VM: await asyncio.to_thread(px.nodes(current_node).qemu(vmid).resize.put, disk="scsi0", size="+15G")
                        else: await asyncio.to_thread(px.nodes(current_node).lxc(vmid).resize.put, volume="rootfs", size="+15G")
                        _log_action("scaleup_disk", vmid, {"disk": "+15G"}, "success", "IO free capacity < 20%", "scaleup")
                    except Exception as e: _log_action("scaleup_disk", vmid, {"disk": "+15G"}, "error", str(e), "scaleup")

                elif at in (AlertType.NETWORK, AlertType.HTTP_5XX):
                    try:
                        new_vmid = int(px.cluster.nextid.get())
                        target_clone_node = best_node or current_node
                        if ttype_enum == TargetType.VM:
                            await asyncio.to_thread(px.nodes(current_node).qemu(vmid).clone.post, newid=new_vmid, target=target_clone_node, full=1)
                            await asyncio.sleep(3)
                            await asyncio.to_thread(px.nodes(target_clone_node).qemu(new_vmid).status.start.post)
                        else:
                            await asyncio.to_thread(px.nodes(current_node).lxc(vmid).clone.post, newid=new_vmid, hostname=f"clone-{vmid}-{new_vmid}")
                            await asyncio.sleep(3)
                            await asyncio.to_thread(px.nodes(target_clone_node).lxc(new_vmid).status.start.post)
                        _clone_registry[new_vmid] = vmid
                        _log_action("scaleout_clone", vmid, {"new_vmid": new_vmid, "node": target_clone_node}, "success", f"Horizontal scale out triggered", "scaleup")
                    except Exception as e: _log_action("scaleout_clone", vmid, {}, "error", str(e), "scaleup")

            if item in warning_queue: warning_queue.remove(item)

def handle_alert(alert: PrometheusAlert) -> None:
    try: severity, alert_type, target_type = AlertSeverity(alert.labels.severity.lower()), AlertType(alert.labels.type.lower()), TargetType(alert.labels.target_type.lower())
    except ValueError: return
    state, value = cluster_state, float(alert.annotations.value or 0)
    if target_type == TargetType.K8S:
        key = f"{alert.labels.namespace}/{alert.labels.deployment}"
        if target := state["deployments"].get(key): _add_to_warning_queue(key, target_type.value, alert_type, value, alert.annotations.summary, target)
    else:
        vmid = int(alert.labels.vmid)
        if target := state["vms" if target_type == TargetType.VM else "lxc"].get(vmid): _add_to_warning_queue(str(vmid), target_type.value, alert_type, value, alert.annotations.summary, target)

# ─────────────────────────────────────────────
#  SCALE-DOWN ENGINE (Using dynamic ML Thresholds)
# ─────────────────────────────────────────────
async def check_scaledown() -> None:
    now, now_ts = datetime.now(timezone.utc), datetime.now(timezone.utc).timestamp()
    async with _state_lock:
        vms_snapshot, lxc_snapshot = copy.deepcopy(cluster_state["vms"]), copy.deepcopy(cluster_state["lxc"])

    async with _approval_lock:
        expired = [a for a in approval_queue if a.get("expires_at", 0) < now_ts]
        for entry in expired:
            approval_queue.remove(entry)
            _log_action("approval_expired", entry.get("vmid", 0), {}, "expired", f"Action {entry['id']} exceeded TTL", "system")

    # Dynamic Thresholds provided by ML Sync Loop
    t_cpu_low  = _ml_config.get("thresh_cpu_low", SCALEDOWN_CPU_LOW_PCT)
    t_ram_low  = _ml_config.get("thresh_ram_low", SCALEDOWN_RAM_LOW_PCT)
    t_http_low = _ml_config.get("thresh_http_low", SCALEDOWN_HTTP_LOW_PCT)

    for ttype_str, snapshot in (("vm", vms_snapshot), ("lxc", lxc_snapshot)):
        for vmid, vm in snapshot.items():
            if vm.get("status") != "running":
                _scaledown_tracker.pop(vmid, None)
                continue

            cpu_pct  = float(vm.get("cpu", 0.0)) * 100.0
            ram_pct  = (float(vm.get("mem", 0)) / max(float(vm.get("maxmem", 1)), 1) * 100.0)
            http_5xx = float(vm.get("http_5xx_rate", 0.0)) * 100.0

            tracker = _scaledown_tracker.setdefault(vmid, {"cpu_low": 0, "ram_low": 0, "http_low": 0})
            tracker["cpu_low"]  = tracker["cpu_low"]  + 1 if cpu_pct  < t_cpu_low  else 0
            tracker["ram_low"]  = tracker["ram_low"]  + 1 if ram_pct  < t_ram_low  else 0
            tracker["http_low"] = tracker["http_low"] + 1 if http_5xx < t_http_low else 0

            sustained_cpu  = tracker["cpu_low"]  >= SCALEDOWN_SUSTAINED_POLLS
            sustained_ram  = tracker["ram_low"]  >= SCALEDOWN_SUSTAINED_POLLS
            sustained_http = tracker["http_low"] >= SCALEDOWN_SUSTAINED_POLLS
            is_clone, node = vmid in _clone_registry, vm.get("node", "")

            # ── HIGH RISK: Delete Clone ──
            if is_clone and sustained_cpu and sustained_ram and sustained_http:
                if not any(a.get("vmid") == vmid and a.get("type") == "delete_vm" for a in approval_queue):
                    action_id = f"delete_{vmid}_{int(now_ts)}"
                    async with _approval_lock:
                        approval_queue.append({
                            "id": action_id, "type": "delete_vm", "target_type": ttype_str, "vmid": vmid, "node": node,
                            "parent_vmid": _clone_registry[vmid], "reason": "Sustained low load for clone",
                            "risk": "high", "queued_at": now.isoformat(), "expires_at": now_ts + APPROVAL_TTL_SECONDS,
                        })
                tracker["cpu_low"] = tracker["ram_low"] = tracker["http_low"] = 0
                _current_cycle_actions.setdefault(vmid, {})["delete_vm"] = 1
                continue

            # ── MEDIUM RISK: Vertical Scale-down ──
            current_cores = int(vm.get("cores", 1))
            if sustained_cpu and current_cores > MIN_VM_CORES:
                new_cores = max(MIN_VM_CORES, current_cores - 1)
                try:
                    if ttype_str == "vm": await asyncio.to_thread(px.nodes(node).qemu(vmid).config.put, cores=new_cores)
                    else: await asyncio.to_thread(px.nodes(node).lxc(vmid).config.put, cores=new_cores)
                    _log_action("scaledown_cores", vmid, {"cores": new_cores}, "success", "CPU low sustained")
                    tracker["cpu_low"] = 0
                except Exception as e: _log_action("scaledown_cores", vmid, {"cores": new_cores}, "error", str(e))

            current_mem_mb = int(vm.get("maxmem", 0)) // (1024 * 1024)
            if sustained_ram and current_mem_mb > MIN_VM_MEMORY_MB:
                new_mem_mb = max(MIN_VM_MEMORY_MB, int(current_mem_mb * 0.80))
                try:
                    if ttype_str == "vm": await asyncio.to_thread(px.nodes(node).qemu(vmid).config.put, memory=new_mem_mb)
                    else: await asyncio.to_thread(px.nodes(node).lxc(vmid).config.put, memory=new_mem_mb)
                    _log_action("scaledown_memory", vmid, {"memory_mb": new_mem_mb}, "success", "RAM low sustained")
                    tracker["ram_low"] = 0
                except Exception as e: _log_action("scaledown_memory", vmid, {"memory_mb": new_mem_mb}, "error", str(e))

async def _execute_approved_action(entry: dict) -> str:
    action_type, vmid, node, ttype_str = entry.get("type"), entry.get("vmid"), entry.get("node"), entry.get("target_type", "vm")
    if action_type == "delete_vm":
        try:
            if ttype_str == "vm":
                try:
                    await asyncio.to_thread(px.nodes(node).qemu(vmid).status.stop.post)
                    await asyncio.sleep(5)
                except Exception: pass
                await asyncio.to_thread(px.nodes(node).qemu(vmid).delete)
            else:
                try:
                    await asyncio.to_thread(px.nodes(node).lxc(vmid).status.stop.post)
                    await asyncio.sleep(3)
                except Exception: pass
                await asyncio.to_thread(px.nodes(node).lxc(vmid).delete)
            _clone_registry.pop(vmid, None)
            _scaledown_tracker.pop(vmid, None)
            _log_action("delete_vm", vmid, {"node": node}, "success", "Admin approved deletion", "admin_approval")
            return "success"
        except Exception as e:
            _log_action("delete_vm", vmid, {"node": node}, "error", str(e), "admin_approval")
            return f"error: {e}"
    return f"unknown action type: {action_type}"

# ─────────────────────────────────────────────────────────────────────────────
#  Standalone ML Service Sync Loop
# ─────────────────────────────────────────────────────────────────────────────
async def ml_incremental_update_job():
    logger.info("Syncing metrics with ML service...")
    if not _ml_metrics_buffer:
        return

    snapshots = copy.deepcopy(_ml_metrics_buffer)
    _ml_metrics_buffer.clear()

    try:
        async with httpx.AsyncClient() as client:
            payload = {
                "snapshots": snapshots,
                "sent_at": datetime.now(timezone.utc).isoformat()
            }
            resp = await client.post(f"{ML_SERVICE_URL}/sync", json=payload, timeout=30.0)

            if resp.status_code == 422:
                logger.warning(f"ML service requests more snapshots batch size: {resp.text}")
                # Re-queue for next sync if batch was too small for ML Service Requirements
                _ml_metrics_buffer.extend(snapshots)
                return

            resp.raise_for_status()
            data = resp.json()

            config = data.get("config", {})
            if config:
                global _ml_config, W_CPU, W_RAM, W_IO, W_E
                async with _weights_lock:
                    _ml_config.update(config)
                    W_CPU = config.get("w_cpu", W_CPU)
                    W_RAM = config.get("w_ram", W_RAM)
                    W_IO  = config.get("w_io", W_IO)
                    W_E   = config.get("w_energy", W_E)
                logger.info(f"ML config dynamically updated via service: weights=({W_CPU:.2f}, {W_RAM:.2f}, {W_IO:.2f}, {W_E:.2f})")
    except Exception as exc:
        logger.error(f"ML sync failed: {exc}")

scheduler = AsyncIOScheduler()
@asynccontextmanager
async def lifespan(app: FastAPI):
    global cluster_state, k8s_apps, k8s_core, W_CPU, W_RAM, W_IO, W_E
    try:
        kube_config.load_kube_config()
        k8s_apps = client.AppsV1Api()
        k8s_core = client.CoreV1Api()
        logger.info("Kubernetes client connected via kubeconfig.")
    except Exception as e: logger.warning(f"Kubernetes client unavailable: {e}")

    w_sum = W_CPU + W_RAM + W_IO + W_E
    if abs(w_sum - 1.0) >= 0.01: raise RuntimeError(f"Weights sum to {w_sum:.4f}")
    elif abs(w_sum - 1.0) > 1e-6: W_CPU, W_RAM, W_IO, W_E = W_CPU/w_sum, W_RAM/w_sum, W_IO/w_sum, W_E/w_sum

    proxmox, k8s_deps = await asyncio.gather(asyncio.to_thread(fetch_proxmox_state), asyncio.to_thread(fetch_kubernetes_state))
    cluster_state = {**proxmox, "deployments": k8s_deps}

    scheduler.add_job(poll_cluster,             "interval", seconds=10,                   id="poll_cluster")
    scheduler.add_job(process_warning_queue,    "interval", seconds=30,                   id="process_warning_queue")
    scheduler.add_job(check_scaledown,          "interval", seconds=SCALEDOWN_INTERVAL_S, id="check_scaledown")
    scheduler.add_job(ml_incremental_update_job,"interval", minutes=5,                    id="ml_incremental_update")
    scheduler.start()
    yield
    scheduler.shutdown()

app = FastAPI(lifespan=lifespan, title="PFE Cluster Manager")

# ─────────────────────────────────────────────
#  API Endpoints
# ─────────────────────────────────────────────
@app.get("/config")
async def get_config():
    """Return the currently operating configuration, including dynamic ML parameters."""
    return {"status": "ok", "config": _ml_config, "w_cpu": W_CPU, "w_ram": W_RAM, "w_io": W_IO, "w_energy": W_E}

@app.post("/webhook")
async def prometheus_webhook(webhook: AlertmanagerWebhook):
    """Receive alerts directly from Prometheus AlertManager"""
    for alert in webhook.alerts:
        handle_alert(alert)
    return {"status": "ok"}

@app.get("/approve/{action_id}")
async def approve_action(action_id: str):
    """Planner/Critic high risk approval mechanism execution endpoint."""
    async with _approval_lock:
        entry = next((a for a in approval_queue if a["id"] == action_id), None)
        if not entry:
            raise HTTPException(404, "Action not found or expired")
        approval_queue.remove(entry)

    result = await _execute_approved_action(entry)
    return {"status": "executed", "result": result}

@app.get("/reject/{action_id}")
async def reject_action(action_id: str):
    """Planner/Critic high risk approval mechanism rejection endpoint."""
    async with _approval_lock:
        entry = next((a for a in approval_queue if a["id"] == action_id), None)
        if not entry:
            raise HTTPException(404, "Action not found or expired")
        approval_queue.remove(entry)
    _log_action(entry["type"], entry["vmid"], {}, "rejected", "Admin rejected action", "admin_approval")
    return {"status": "rejected"}
