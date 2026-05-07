"""
main.py — PFE Cluster Manager
==============================
Fully patched:
 - Action labels correctly propagated via Pydantic.
 - Scales horizontally for K8S CPU/RAM appropriately.
 - Handles Network/TCP Overflow VM Cloning.
"""
import asyncio, copy, json, math, os, re, shlex, logging, time, random, urllib.parse, urllib.request, csv
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from contextlib import asynccontextmanager
from typing import Optional

try:
    import paramiko
    HAS_PARAMIKO = True
except ImportError:
    HAS_PARAMIKO = False

from fastapi import FastAPI, HTTPException, Header, Depends
from pydantic import BaseModel
from enum import Enum
from dotenv import load_dotenv
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from proxmoxer import ProxmoxAPI
from kubernetes import client, config as kube_config
from kubernetes.client.rest import ApiException
from concurrent.futures import ThreadPoolExecutor, as_completed
import httpx, joblib, pandas as pd, xgboost as xgb, numpy as np

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

CLUSTER_API_KEY    = os.getenv("CLUSTER_API_KEY", "26108a4e-1735-406d-9ab8-5141ac8596f5")
PROXMOX_HOST       = os.getenv("PROXMOX_HOST")
PROXMOX_USER       = os.getenv("PROXMOX_USER")
PROXMOX_TOKEN_NAME = os.getenv("PROXMOX_TOKEN_NAME")
PROXMOX_TOKEN_UUID = os.getenv("PROXMOX_TOKEN_UUID")

if not all([PROXMOX_HOST, PROXMOX_USER, PROXMOX_TOKEN_NAME, PROXMOX_TOKEN_UUID]):
    logger.error("CRITICAL: Missing Proxmox credentials in .env")
    exit(1)

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
VM_CPU_QUERY          = os.getenv("VM_CPU_QUERY",    "")
VM_RAM_QUERY          = os.getenv("VM_RAM_QUERY",    "")
VM_IO_QUERY           = os.getenv("VM_IO_QUERY",     "")
VM_ENERGY_QUERY       = os.getenv("VM_ENERGY_QUERY", "")
VM_HTTP_5XX_QUERY     = os.getenv("VM_HTTP_5XX_QUERY", "")
VM_NET_DROP_QUERY     = os.getenv("VM_NET_DROP_QUERY", "")

NODE_IO_CAPACITY_BPS = float(os.getenv("NODE_IO_CAPACITY_BPS", str(500 * 1024 * 1024)))
HISTORY_WINDOW      = int(os.getenv("HISTORY_WINDOW",      "500"))
MIN_HISTORY_SAMPLES = int(os.getenv("MIN_HISTORY_SAMPLES", "30"))

SCALEDOWN_SUSTAINED_POLLS = int(os.getenv("SCALEDOWN_SUSTAINED_POLLS", "1"))
SCALEDOWN_INTERVAL_S    = int(os.getenv("SCALEDOWN_INTERVAL_S",      "120"))
MIN_VM_CORES            = int(os.getenv("MIN_VM_CORES",              "1"))
MIN_VM_MEMORY_MB        = int(os.getenv("MIN_VM_MEMORY_MB",          "512"))
APPROVAL_TTL_SECONDS    = int(os.getenv("APPROVAL_TTL_SECONDS",      "3600"))
SCALEDOWN_CPU_LOW_PCT   = float(os.getenv("SCALEDOWN_CPU_LOW_PCT",  "30.0"))
SCALEDOWN_RAM_LOW_PCT   = float(os.getenv("SCALEDOWN_RAM_LOW_PCT",  "30.0"))
SCALEDOWN_HTTP_LOW_PCT  = float(os.getenv("SCALEDOWN_HTTP_LOW_PCT",  "0.5"))

ML_URL   = os.getenv("ML_URL",   "http://10.30.30.100:8001")
APP_URL  = os.getenv("APP_URL",  "http://localhost:5000")

CLONE_SSH_USER      = os.getenv("CLONE_SSH_USER",      "root")
CLONE_SSH_KEY_PATH  = os.getenv("CLONE_SSH_KEY_PATH",  "")
CLONE_SSH_PASSWORD  = os.getenv("CLONE_SSH_PASSWORD",  "")
CLONE_SSH_PORT      = int(os.getenv("CLONE_SSH_PORT",  "22"))
CLONE_BOOT_TIMEOUT  = int(os.getenv("CLONE_BOOT_TIMEOUT", "180"))
CLONE_K8S_ROLE      = os.getenv("CLONE_K8S_ROLE",       "worker")
CLONE_REPLICATE_PODS = os.getenv("CLONE_REPLICATE_PODS", "true").lower() == "true"

K8S_MASTER_IP       = os.getenv("K8S_MASTER_IP",       "")
K8S_MASTER_SSH_USER = os.getenv("K8S_MASTER_SSH_USER", "root")
K8S_MASTER_SSH_KEY  = os.getenv("K8S_MASTER_SSH_KEY",  "")
K8S_MASTER_SSH_PASS = os.getenv("K8S_MASTER_SSH_PASS", "")

MODELS_DIR  = os.getenv("MODELS_DIR",  "./models")
DATA_BUFFER = os.getenv("DATA_BUFFER", "./data/live_buffer")
N_NEW_TREES = int(os.getenv("ML_NEW_TREES", "20"))
os.makedirs(DATA_BUFFER, exist_ok=True)

FEATURE_COLS =[
    "up", "scrape_duration_seconds", "cpu_busy_pct", "ram_usage_pct",
    "io_util_pct", "http_5xx_rate", "net_drop_rate", "power_watts",
    "is_worker", "is_master", "is_monitor", "vlan_enc",
]
TARGET_WEIGHTS     =["w_cpu", "w_ram", "w_io", "w_energy"]
TARGET_THRESH_WARN =["thresh_cpu_warn", "thresh_ram_warn", "thresh_disk_warn", "thresh_http_warn"]
TARGET_THRESH_CRIT =["thresh_cpu_crit", "thresh_ram_crit", "thresh_disk_crit", "thresh_http_crit", "thresh_net_crit"]
TARGET_THRESH_LOW  =["thresh_cpu_low", "thresh_ram_low", "thresh_http_low"]
ALL_TARGETS        = TARGET_WEIGHTS + TARGET_THRESH_WARN + TARGET_THRESH_CRIT + TARGET_THRESH_LOW

_ml_boosters: dict[str, xgb.Booster] = {}
_vlan_encoder = None
_ml_metrics_buffer: list[dict] =[]
_ml_logs_buffer: list[dict] =[]

W_CPU = float(os.getenv("W_CPU", "0.35"))
W_RAM = float(os.getenv("W_RAM", "0.35"))
W_IO  = float(os.getenv("W_IO",  "0.15"))
W_E   = float(os.getenv("W_E",   "0.15"))
_weights_lock = asyncio.Lock()

_ml_config: dict = {
    "w_cpu": W_CPU, "w_ram": W_RAM, "w_io": W_IO, "w_energy": W_E,
    "thresh_cpu_warn": 80.0, "thresh_cpu_crit": 95.0,  "thresh_cpu_low": 30.0,
    "thresh_ram_warn": 60.0, "thresh_ram_crit": 80.0,  "thresh_ram_low": 30.0,
    "thresh_http_warn": 1.0, "thresh_http_crit": 5.0,  "thresh_http_low": 0.5,
    "thresh_disk_warn": 20.0, "thresh_disk_crit": 10.0,
    "thresh_net_crit": 2.0,
}
_ml_config_lock = asyncio.Lock()

cluster_state: dict = {"nodes": {}, "vms": {}, "lxc": {}, "deployments": {}}
_state_lock  = asyncio.Lock()
warning_queue: list[dict] =[]

_delta_history: dict[str, deque[float]] = {m: deque(maxlen=HISTORY_WINDOW) for m in ("cpu", "ram", "io", "energy")}
_scaledown_tracker: dict[int, dict] = {}
_clone_registry: dict[int, int] = {}
approval_queue: list[dict] =[]
_approval_lock = asyncio.Lock()

action_log: deque[dict] = deque(maxlen=100)
_current_cycle_actions: dict[int, dict] = {}

def verify_api_key(x_api_key: str = Header(...)):
    if x_api_key != CLUSTER_API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API Key")

px = ProxmoxAPI(
    PROXMOX_HOST, user=PROXMOX_USER,
    token_name=PROXMOX_TOKEN_NAME, token_value=PROXMOX_TOKEN_UUID,
    verify_ssl=False, timeout=PROXMOX_TIMEOUT,
)
k8s_apps = None
k8s_core = None

class AlertSeverity(Enum): WARNING = "warning"; CRITICAL = "critical"
class AlertType(Enum): CPU = "cpu"; MEMORY = "memory"; DISK_IO = "disk_io"; NETWORK = "network"; HTTP_5XX = "http_5xx"
class TargetType(Enum): VM = "vm"; LXC = "lxc"; K8S = "k8s"

class AlertLabels(BaseModel):
    alertname: str = ""; severity: str = ""; type: str = ""
    target_type: str = "vm"; vmid: str = ""; instance: str = ""
    namespace: str = "default"; deployment: str = ""
    upstream: str = ""
    action: str = ""
    model_config = {"extra": "allow"}

class AlertAnnotations(BaseModel):
    summary: str = ""; description: str = ""; value: str = "0"

class PrometheusAlert(BaseModel):
    status: str; labels: AlertLabels; annotations: AlertAnnotations

class AlertmanagerWebhook(BaseModel):
    version: str; status: str; alerts: list[PrometheusAlert]

class VMResources(BaseModel):
    cores: Optional[int] = None; memory: Optional[int] = None
    disk: Optional[str] = None; disk_id: Optional[str] = "scsi0"

class MigrateRequest(BaseModel):
    target_node: str

class MigrateFromNodeRequest(BaseModel):
    node: str
    reason: str = ""

class MLConfigUpdate(BaseModel):
    config: dict
    updated_at: Optional[float] = None

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
            "mem": status.get("memory", {}).get("used", 0),
            "maxmem": status.get("memory", {}).get("total", 0),
            "energy_used": (status.get("cpu", 0) * cpu_cores * ENERGY_PER_CPU_CORE) +
                           (status.get("memory", {}).get("used", 0) / (1024**3)) * ENERGY_PER_GB_MEMORY,
            "energy_capacity": cpu_cores * ENERGY_PER_CPU_CORE + maxmem_gb * ENERGY_PER_GB_MEMORY,
        })
    except Exception:
        pass

    try:
        for vm in px.nodes(node_name).qemu.get():
            vm["node"] = node_name
            vm["energy_needed"] = vm.get("cores", 1) * ENERGY_PER_CPU_CORE + (vm.get("maxmem", 0) / (1024**3)) * ENERGY_PER_GB_MEMORY
            vms[vm["vmid"]] = vm
    except Exception:
        pass

    try:
        for ct in px.nodes(node_name).lxc.get():
            ct["node"] = node_name
            ct["energy_needed"] = ct.get("cores", 1) * ENERGY_PER_CPU_CORE + (ct.get("maxmem", 0) / (1024**3)) * ENERGY_PER_GB_MEMORY
            lxcs[ct["vmid"]] = ct
    except Exception:
        pass

    return node_name, node_info, vms, lxcs

def fetch_proxmox_state() -> dict:
    state = {"nodes": {}, "vms": {}, "lxc": {}}
    try:
        nodes = px.nodes.get()
    except Exception:
        return state
    with ThreadPoolExecutor(max_workers=max(len(nodes), 1)) as pool:
        futures = {pool.submit(_fetch_node, n): n["node"] for n in nodes}
        for f in as_completed(futures):
            nn, ninfo, vms, lxcs = f.result()
            state["nodes"][nn] = ninfo
            state["vms"].update(vms)
            state["lxc"].update(lxcs)
    return state

def fetch_kubernetes_state() -> dict:
    deps = {}
    if not k8s_apps:
        return deps
    try:
        for dep in k8s_apps.list_deployment_for_all_namespaces().items:
            key = f"{dep.metadata.namespace}/{dep.metadata.name}"
            c = dep.spec.template.spec.containers[0]
            limits = c.resources.limits if c.resources and c.resources.limits else {}
            deps[key] = {
                "name": dep.metadata.name, "namespace": dep.metadata.namespace,
                "replicas": dep.spec.replicas,
                "ready_replicas": dep.status.ready_replicas or 0,
                "cpu_limit": limits.get("cpu"), "memory_limit": limits.get("memory"),
            }
    except Exception:
        pass
    return deps

def fetch_vm_metrics_from_prometheus() -> dict[str, dict[str, float]]:
    if not PROMETHEUS_URL: return {}
    named_queries =[("cpu_busy_pct", VM_CPU_QUERY), ("ram_usage_pct", VM_RAM_QUERY), ("io_util_pct", VM_IO_QUERY),
                     ("power_watts", VM_ENERGY_QUERY), ("http_5xx_rate", VM_HTTP_5XX_QUERY), ("net_drop_rate", VM_NET_DROP_QUERY)]
    result: dict[str, dict[str, float]] = {}
    base = PROMETHEUS_URL.rstrip("/")
    for field, query in named_queries:
        if not query: continue
        try:
            url = f"{base}/api/v1/query?query={urllib.parse.quote(query)}"
            with urllib.request.urlopen(url, timeout=3) as resp:
                payload = json.loads(resp.read())
            if payload.get("status") != "success": continue
            per_vm: dict[str, float] = {}
            for series in payload["data"]["result"]:
                vmid = series["metric"].get(PROMETHEUS_VMID_LABEL, "")
                if vmid: per_vm[str(vmid)] = float(series["value"][1])
            if per_vm: result[field] = per_vm
        except Exception as exc:
            _throttled_warning(f"prom_{field}", f"Prometheus query failed for {field}: {exc}")
    return result

def _record_ml_snapshot(vm: dict, vmid: int, instance_id: str, time_str: str) -> None:
    cpu_pct  = float(vm.get("cpu", 0.0)) * 100.0
    ram_pct  = (float(vm.get("mem", 0)) / max(float(vm.get("maxmem", 1)), 1)) * 100.0
    disk_bps = float(vm.get("diskread", 0)) + float(vm.get("diskwrite", 0))
    io_pct   = min(disk_bps / NODE_IO_CAPACITY_BPS * 100.0, 100.0)
    power_w  = (vm.get("energy_actual") / 1e6) if vm.get("energy_actual") else 40.0 + 80.0 * (cpu_pct / 100.0)
    vm_name = vm.get("name", str(vmid))

    _ml_metrics_buffer.append({
        "_time": time_str, "instance": instance_id, "vm_name": vm_name,
        "up": 1.0 if vm.get("status") == "running" else 0.0,
        "scrape_duration_seconds": float(vm.get("scrape_duration", 0.1)),
        "cpu_busy_pct": cpu_pct, "ram_usage_pct": ram_pct, "io_util_pct": io_pct,
        "http_5xx_rate": float(vm.get("http_5xx_rate", 0)), "net_drop_rate": float(vm.get("net_drop_rate", 0)),
        "power_watts": power_w, "is_worker": int("worker" in vm_name.lower()), "is_master": int("master" in vm_name.lower()),
        "is_monitor": int(any(r in vm_name.lower() for r in["monitoring", "influx", "snmp"])),
        "vlan_enc": int(_vlan_encoder.transform([vm.get("vlan", "")])[0]) if _vlan_encoder is not None and vm.get("vlan", "") in _vlan_encoder.classes_ else 0,
    })

    total_stress = max(1.0, cpu_pct + ram_pct + io_pct + (power_w / 280.0 * 100.0))
    acts = _current_cycle_actions.get(vmid, {})
    async_ml_cfg = _ml_config

    _ml_logs_buffer.append({
        "_time": time_str, "instance": instance_id, "vm_name": vm_name,
        "w_cpu": cpu_pct / total_stress, "w_ram": ram_pct / total_stress, "w_io": io_pct / total_stress, "w_energy": (power_w / 280.0 * 100.0) / total_stress,
        "thresh_cpu_warn": async_ml_cfg.get("thresh_cpu_warn", 80.0), "thresh_ram_warn": async_ml_cfg.get("thresh_ram_warn", 60.0), "thresh_disk_warn": async_ml_cfg.get("thresh_disk_warn", 20.0), "thresh_http_warn": async_ml_cfg.get("thresh_http_warn", 1.0),
        "thresh_cpu_crit": async_ml_cfg.get("thresh_cpu_crit", 95.0), "thresh_ram_crit": async_ml_cfg.get("thresh_ram_crit", 80.0), "thresh_disk_crit": async_ml_cfg.get("thresh_disk_crit", 10.0), "thresh_http_crit": async_ml_cfg.get("thresh_http_crit", 5.0), "thresh_net_crit": async_ml_cfg.get("thresh_net_crit", 2.0),
        "thresh_cpu_low": async_ml_cfg.get("thresh_cpu_low", SCALEDOWN_CPU_LOW_PCT), "thresh_ram_low": async_ml_cfg.get("thresh_ram_low", SCALEDOWN_RAM_LOW_PCT), "thresh_http_low": async_ml_cfg.get("thresh_http_low", SCALEDOWN_HTTP_LOW_PCT),
        "action_vertical_cpu": acts.get("scaleup_cores", 0), "action_vertical_ram": acts.get("scaleup_memory", 0), "action_vertical_disk": acts.get("scaleup_disk", 0),
        "action_horizontal": acts.get("scaleout_clone", 0), "action_scaledown_cpu": acts.get("scaledown_cores", 0), "action_scaledown_ram": acts.get("scaledown_memory", 0), "action_delete_vm": acts.get("delete_vm", 0),
    })

async def poll_cluster() -> None:
    global cluster_state
    try:
        proxmox, k8s_deps, prom_metrics = await asyncio.gather(
            asyncio.to_thread(fetch_proxmox_state), asyncio.to_thread(fetch_kubernetes_state), asyncio.to_thread(fetch_vm_metrics_from_prometheus),
        )
        time_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        for kind in ("vms", "lxc"):
            for vmid, vm in proxmox[kind].items():
                vmid_str = str(vmid)
                for field, per_vm in prom_metrics.items():
                    if vmid_str in per_vm: vm[field] = per_vm[vmid_str]
                _record_ml_snapshot(vm, vmid, f"{vm.get('ip', vmid)}:9100", time_str)
        _current_cycle_actions.clear(); _update_delta_history(proxmox)
        async with _state_lock: cluster_state = {**proxmox, "deployments": k8s_deps}
        await _push_snapshots_to_ml_service(proxmox)
    except Exception as e:
        logger.error(f"Polling error: {e}")

async def _push_snapshots_to_ml_service(proxmox: dict) -> None:
    if not ML_URL: return
    snapshots =[]
    for kind in ("vms", "lxc"):
        for vmid, vm in proxmox[kind].items():
            if vm.get("status") != "running": continue
            cpu_pct = float(vm.get("cpu", 0.0)) * 100.0
            ram_pct = (float(vm.get("mem", 0)) / max(float(vm.get("maxmem", 1)), 1)) * 100.0
            disk_bps = float(vm.get("diskread", 0)) + float(vm.get("diskwrite", 0))
            io_pct  = min(disk_bps / NODE_IO_CAPACITY_BPS * 100.0, 100.0)
            power_w = (vm.get("energy_actual") / 1e6) if vm.get("energy_actual") else 40.0 + 80.0 * (cpu_pct / 100.0)
            snapshots.append({
                "instance": f"{vm.get('ip', vmid)}:9100", "vm_name": vm.get("name", str(vmid)), "vlan": vm.get("vlan", ""),
                "up": 1.0 if vm.get("status") == "running" else 0.0, "scrape_duration": float(vm.get("scrape_duration", 0.1)),
                "cpu_pct": cpu_pct, "ram_pct": ram_pct, "io_pct": io_pct, "http_5xx_rate": float(vm.get("http_5xx_rate", 0.0)),
                "net_drop_rate": float(vm.get("net_drop_rate", 0.0)), "power_watts": power_w,
            })
    if not snapshots: return
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.post(f"{ML_URL}/sync", json={"snapshots": snapshots, "sent_at": datetime.now(timezone.utc).isoformat()})
        if r.status_code == 200 and "config" in r.json(): await _apply_ml_config(r.json()["config"])
    except Exception as exc: _throttled_warning("ml_sync", f"ml_service /sync failed: {exc}")

async def _apply_ml_config(config: dict) -> None:
    global W_CPU, W_RAM, W_IO, W_E
    async with _ml_config_lock:
        w_cpu, w_ram, w_io, w_e = config.get("w_cpu", _ml_config["w_cpu"]), config.get("w_ram", _ml_config["w_ram"]), config.get("w_io", _ml_config["w_io"]), config.get("w_energy", _ml_config["w_energy"])
        w_sum = w_cpu + w_ram + w_io + w_e
        if w_sum > 0: W_CPU, W_RAM, W_IO, W_E = w_cpu/w_sum, w_ram/w_sum, w_io/w_sum, w_e/w_sum
        _ml_config.update({k: v for k, v in config.items() if v is not None})
        _ml_config["w_cpu"], _ml_config["w_ram"], _ml_config["w_io"], _ml_config["w_energy"] = W_CPU, W_RAM, W_IO, W_E

async def ml_config_fallback_poll() -> None:
    if not ML_URL: return
    try:
        async with httpx.AsyncClient(timeout=8) as client:
            r = await client.get(f"{ML_URL}/config")
        if r.status_code == 200 and "config" in r.json(): await _apply_ml_config(r.json()["config"])
    except Exception as exc: _throttled_warning("ml_poll", f"ml_service /config poll failed: {exc}")

async def _register_vm_with_sd(ip: str, hostname: str, vmid: Optional[int] = None) -> None:
    if not APP_URL: return
    payload: dict = {"ip": ip, "hostname": hostname, "type": "vm", "env": "prod"}
    if vmid is not None: payload["vmid"] = str(vmid)
    try:
        async with httpx.AsyncClient(timeout=8) as client: r = await client.post(f"{APP_URL}/hosts", json=payload)
    except Exception as exc: logger.warning(f"[SD] Could not register {ip}: {exc}")

async def _deregister_vm_from_sd(ip: str) -> None:
    if not APP_URL: return
    try:
        async with httpx.AsyncClient(timeout=8) as client: r = await client.delete(f"{APP_URL}/hosts/{ip}")
    except Exception as exc: logger.warning(f"[SD] Could not deregister {ip}: {exc}")

def _ssh_connect(host: str, user: str, key_path: str, password: str, port: int = 22, timeout: int = 20) -> "paramiko.SSHClient":
    if not HAS_PARAMIKO: raise RuntimeError("paramiko not installed")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    kwargs: dict = dict(hostname=host, username=user, port=port, timeout=timeout, banner_timeout=30, auth_timeout=30)
    if key_path and Path(key_path).exists(): kwargs["key_filename"], kwargs["look_for_keys"], kwargs["allow_agent"] = key_path, False, False
    elif password: kwargs["password"], kwargs["look_for_keys"], kwargs["allow_agent"] = password, False, False
    else: kwargs["look_for_keys"], kwargs["allow_agent"] = True, True
    ssh.connect(**kwargs)
    return ssh

def _ssh_run(ssh: "paramiko.SSHClient", command: str, timeout: int = 60) -> tuple[int, str, str]:
    _, stdout, stderr = ssh.exec_command(command, timeout=timeout)
    return (stdout.channel.recv_exit_status(), stdout.read().decode(errors="replace"), stderr.read().decode(errors="replace"))

async def _wait_for_vm_ip(vmid: int, node: str, timeout: int = 180) -> Optional[str]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            ifaces = await asyncio.to_thread(px.nodes(node).qemu(vmid).agent("network-get-interfaces").get)
            for iface in ifaces.get("result",[]):
                if iface.get("name") == "lo": continue
                for addr in iface.get("ip-addresses",[]):
                    if addr.get("ip-address-type") == "ipv4" and not addr["ip-address"].startswith("127.") and not addr["ip-address"].startswith("169.254."): return addr["ip-address"]
        except Exception: pass
        try:
            ifaces = await asyncio.to_thread(px.nodes(node).lxc(vmid).interfaces.get)
            for iface in ifaces:
                if iface.get("name") == "lo": continue
                inet = iface.get("inet", "")
                if inet and "/" in inet:
                    ip = inet.split("/")[0]
                    if not ip.startswith("127.") and not ip.startswith("169.254."): return ip
        except Exception: pass
        await asyncio.sleep(10)
    return None

async def _wait_for_ssh(host: str, port: int = 22, timeout: int = 120) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=5)
            writer.close(); await writer.wait_closed(); return True
        except Exception: await asyncio.sleep(5)
    return False

def _fix_machine_id(ssh: "paramiko.SSHClient") -> None:
    _ssh_run(ssh, "rm -f /etc/machine-id /var/lib/dbus/machine-id")
    _ssh_run(ssh, "systemd-machine-id-setup")
    _ssh_run(ssh, "ln -sf /etc/machine-id /var/lib/dbus/machine-id 2>/dev/null || true")

def _fix_hostname(ssh: "paramiko.SSHClient", new_hostname: str) -> None:
    _ssh_run(ssh, f"hostnamectl set-hostname {shlex.quote(new_hostname)}")
    _ssh_run(ssh, "sed -i '/^127\\.0\\.1\\.1/d' /etc/hosts")
    _ssh_run(ssh, f"echo '127.0.1.1 {new_hostname}' >> /etc/hosts")

def _fix_ssh_host_keys(ssh: "paramiko.SSHClient") -> None:
    _ssh_run(ssh, "rm -f /etc/ssh/ssh_host_*"); _ssh_run(ssh, "ssh-keygen -A")

def _clean_cloud_init(ssh: "paramiko.SSHClient") -> None:
    code, _, _ = _ssh_run(ssh, "command -v cloud-init 2>/dev/null")
    if code == 0: _ssh_run(ssh, "cloud-init clean --logs 2>/dev/null || true")

def _fix_static_ip(ssh: "paramiko.SSHClient", old_ip: str) -> None:
    code, out, _ = _ssh_run(ssh, "ls /etc/netplan/*.yaml /etc/netplan/*.yml 2>/dev/null")
    if code != 0 or not out.strip(): return
    yaml_files = out.strip().split()
    patch_script = r"""
import yaml, sys, os
ADDR_KEYS  = ('addresses', 'gateway4', 'gateway6', 'nameservers', 'routes', 'routing-policy')
IFACE_KINDS = ('ethernets', 'wifis', 'bonds', 'bridges', 'vlans', 'tunnels', 'dummies', 'modems')
def patch_iface(cfg):
    if not isinstance(cfg, dict): return cfg
    for k in ADDR_KEYS: cfg.pop(k, None)
    cfg['dhcp4'] = True
    cfg.setdefault('dhcp6', False)
    return cfg
changed = False
for path in sys.argv[1:]:
    try:
        with open(path) as fh: data = yaml.safe_load(fh) or {}
    except: continue
    net = data.get('network', {})
    file_changed = False
    for kind in IFACE_KINDS:
        for name, icfg in net.get(kind, {}).items():
            orig = yaml.dump(icfg)
            net[kind][name] = patch_iface(icfg or {})
            if yaml.dump(net[kind][name]) != orig: file_changed = True
    if file_changed:
        with open(path, 'w') as fh: yaml.dump(data, fh, default_flow_style=False)
        changed = True
sys.exit(0 if changed else 2)
"""
    files_arg = " ".join(shlex.quote(f) for f in yaml_files)
    code, _, _ = _ssh_run(ssh, f"python3 -c {shlex.quote(patch_script)} {files_arg}", timeout=30)
    if code == 2: return
    gen_code, _, gen_err = _ssh_run(ssh, "netplan generate 2>&1", timeout=15)
    if gen_code != 0: return
    _ssh_run(ssh, "netplan apply 2>/dev/null || true", timeout=20)

def _sync_time(ssh: "paramiko.SSHClient") -> None:
    _ssh_run(ssh, "chronyc makestep 2>/dev/null || timedatectl set-ntp true 2>/dev/null || true")
    _ssh_run(ssh, "systemctl restart systemd-timesyncd 2>/dev/null || true")

def _reset_kubernetes_state(ssh: "paramiko.SSHClient") -> None:
    _ssh_run(ssh, "kubeadm reset --force 2>/dev/null || true", timeout=120)
    for path in ("/etc/kubernetes", "/var/lib/kubelet", "/var/lib/etcd", "/root/.kube", "/home/*/.kube", "/etc/cni/net.d", "/opt/cni/bin", "/var/run/kubernetes", "/tmp/kubeadm*"):
        _ssh_run(ssh, f"rm -rf {path}")
    for svc in ("kubelet", "containerd", "docker", "cri-o", "iscsid"):
        _ssh_run(ssh, f"systemctl stop {svc} 2>/dev/null || true")
        _ssh_run(ssh, f"systemctl disable {svc} 2>/dev/null || true")
    for table in ("", "-t nat", "-t mangle", "-t raw"):
        _ssh_run(ssh, f"iptables {table} -F 2>/dev/null || true")
        _ssh_run(ssh, f"iptables {table} -X 2>/dev/null || true")
    _ssh_run(ssh, "ip6tables -F 2>/dev/null || true")
    _ssh_run(ssh, "ip6tables -X 2>/dev/null || true")
    for iface in ("cni0", "flannel.1", "calico", "tunl0", "weave", "cilium_host"):
        _ssh_run(ssh, f"ip link delete {iface} 2>/dev/null || true")

def _get_kubeadm_join_command(master_ip: str) -> Optional[str]:
    if not master_ip or not HAS_PARAMIKO: return None
    try:
        ssh = _ssh_connect(master_ip, K8S_MASTER_SSH_USER, K8S_MASTER_SSH_KEY, K8S_MASTER_SSH_PASS)
        try:
            code, out, err = _ssh_run(ssh, "kubeadm token create --print-join-command", timeout=60)
            if code == 0 and "kubeadm join" in out: return out.strip()
        finally:
            ssh.close()
    except Exception: pass
    return None

def _delete_stale_k8s_node(node_name: str) -> None:
    if not k8s_core or not node_name: return
    try: k8s_core.delete_node(node_name)
    except ApiException as e:
        if e.status != 404: pass

def _join_kubernetes_cluster(ssh: "paramiko.SSHClient", join_cmd: str, new_hostname: str) -> bool:
    for svc in ("containerd", "docker", "cri-o"): _ssh_run(ssh, f"systemctl enable --now {svc} 2>/dev/null || true")
    _ssh_run(ssh, "systemctl enable kubelet 2>/dev/null || true")
    code, out, err = _ssh_run(ssh, join_cmd, timeout=300)
    return code == 0

async def _wait_for_k8s_node_ready(node_name: str, timeout: int = 300) -> bool:
    if not k8s_core: return False
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            node_obj = await asyncio.to_thread(k8s_core.read_node, node_name)
            for cond in (node_obj.status.conditions or[]):
                if cond.type == "Ready" and cond.status == "True": return True
        except ApiException: pass
        await asyncio.sleep(10)
    return False

def _get_k8s_node_name_for_vm(vm: dict) -> str:
    if not k8s_core: return vm.get("name", "")
    vm_ip = vm.get("ip", "")
    try:
        nodes_list = k8s_core.list_node()
        for node in nodes_list.items:
            for addr in (node.status.addresses or[]):
                if addr.address == vm_ip: return node.metadata.name
    except Exception: pass
    return vm.get("name", "")

async def _replicate_pods_to_node(source_node_name: str, target_node_name: str) -> None:
    if not k8s_apps or not k8s_core: return
    try: pod_list = await asyncio.to_thread(k8s_core.list_pod_for_all_namespaces, field_selector=f"spec.nodeName={source_node_name}")
    except ApiException: return
    handled: set[str] = set(); bare_pods: list =[]
    for pod in pod_list.items:
        ns = pod.metadata.namespace; owners = pod.metadata.owner_references or[]
        if not owners: bare_pods.append(pod); continue
        for owner in owners:
            key = f"{ns}/{owner.kind}/{owner.name}"
            if key in handled: continue
            handled.add(key)
            if owner.kind == "ReplicaSet":
                try:
                    rs = await asyncio.to_thread(k8s_apps.read_namespaced_replica_set, owner.name, ns)
                    for dep_owner in (rs.metadata.owner_references or[]):
                        if dep_owner.kind == "Deployment":
                            dep_key = f"{ns}/Deployment/{dep_owner.name}"
                            if dep_key not in handled:
                                handled.add(dep_key); await _scale_deployment_to_node(dep_owner.name, ns, target_node_name)
                except ApiException: pass
            elif owner.kind == "Deployment": await _scale_deployment_to_node(owner.name, ns, target_node_name)
            elif owner.kind == "StatefulSet": await _scale_statefulset_to_node(owner.name, ns, target_node_name)
    for pod in bare_pods: await _recreate_pod_on_node(pod, target_node_name)

async def _scale_deployment_to_node(dep_name: str, namespace: str, node_name: str) -> None:
    try:
        dep = await asyncio.to_thread(k8s_apps.read_namespaced_deployment, dep_name, namespace)
        current = dep.spec.replicas or 1
        prefer_term = client.V1PreferredSchedulingTerm(weight=100, preference=client.V1NodeSelectorTerm(match_expressions=[client.V1NodeSelectorRequirement(key="kubernetes.io/hostname", operator="In", values=[node_name])]))
        affinity = dep.spec.template.spec.affinity or client.V1Affinity()
        na = affinity.node_affinity or client.V1NodeAffinity()
        preferred = na.preferred_during_scheduling_ignored_during_execution or[]
        preferred.append(prefer_term); na.preferred_during_scheduling_ignored_during_execution = preferred; affinity.node_affinity = na
        patch = {"spec": {"replicas": current + 1, "template": {"spec": {"affinity": client.ApiClient().sanitize_for_serialization(affinity)}}}}
        await asyncio.to_thread(k8s_apps.patch_namespaced_deployment, dep_name, namespace, patch)
    except Exception: pass

async def _scale_statefulset_to_node(sts_name: str, namespace: str, node_name: str) -> None:
    try:
        sts = await asyncio.to_thread(k8s_apps.read_namespaced_stateful_set, sts_name, namespace)
        await asyncio.to_thread(k8s_apps.patch_namespaced_stateful_set, sts_name, namespace, {"spec": {"replicas": (sts.spec.replicas or 1) + 1}})
    except Exception: pass

async def _recreate_pod_on_node(pod, target_node_name: str) -> None:
    ns, spec = pod.metadata.namespace, pod.spec
    new_pod = client.V1Pod(metadata=client.V1ObjectMeta(generate_name=f"{pod.metadata.name}-clone-", namespace=ns, labels=pod.metadata.labels), spec=client.V1PodSpec(containers=spec.containers, init_containers=spec.init_containers, volumes=spec.volumes, node_selector={"kubernetes.io/hostname": target_node_name}, restart_policy=spec.restart_policy or "Always", service_account_name=spec.service_account_name, image_pull_secrets=spec.image_pull_secrets, security_context=spec.security_context))
    try: await asyncio.to_thread(k8s_core.create_namespaced_pod, ns, new_pod)
    except Exception: pass

async def _grow_filesystem_on_vm(vmid: int, node: str, disk_id: str = "scsi0") -> None:
    if not HAS_PARAMIKO: return
    vm_ip = await _wait_for_vm_ip(vmid, node, timeout=60)
    if not vm_ip or not await _wait_for_ssh(vm_ip, CLONE_SSH_PORT, timeout=60): return
    def _do_grow() -> None:
        try:
            ssh = _ssh_connect(vm_ip, CLONE_SSH_USER, CLONE_SSH_KEY_PATH, CLONE_SSH_PASSWORD, CLONE_SSH_PORT)
            try:
                code, out, _ = _ssh_run(ssh, "lsblk -rno NAME,MOUNTPOINT | awk '$2==\"/\"{print $1}' | head -1")
                if code != 0 or not out.strip(): _, out, _ = _ssh_run(ssh, "df / | tail -1 | awk '{print $1}'")
                root_dev = out.strip().lstrip("/dev/")
                m = re.match(r"([a-z]+)(\d+)$", root_dev)
                if not m: return
                disk_name, part_num = m.group(1), m.group(2)
                disk_path, part_path = f"/dev/{disk_name}", f"/dev/{disk_name}{part_num}"
                _ssh_run(ssh, "apt-get install -y cloud-guest-utils 2>/dev/null || yum install -y cloud-utils-growpart 2>/dev/null || true", timeout=120)
                _ssh_run(ssh, f"growpart {disk_path} {part_num}", timeout=60)
                _, fs_type, _ = _ssh_run(ssh, f"blkid -s TYPE -o value {part_path}")
                if "xfs" in fs_type.strip(): _ssh_run(ssh, f"xfs_growfs /", timeout=60)
                else: _ssh_run(ssh, f"resize2fs {part_path}", timeout=60)
            finally: ssh.close()
        except Exception: pass
    await asyncio.to_thread(_do_grow)

async def _remediate_clone(new_vmid: int, clone_node: str, source_vmid: int, new_hostname: str, old_ip: str = "", source_k8s_node_name: str = "") -> None:
    if not HAS_PARAMIKO: return
    vm_ip = await _wait_for_vm_ip(new_vmid, clone_node, timeout=CLONE_BOOT_TIMEOUT)
    if not vm_ip or not await _wait_for_ssh(vm_ip, CLONE_SSH_PORT, timeout=120): return

    def _phase1() -> None:
        ssh = _ssh_connect(vm_ip, CLONE_SSH_USER, CLONE_SSH_KEY_PATH, CLONE_SSH_PASSWORD, CLONE_SSH_PORT)
        try:
            _fix_machine_id(ssh); _fix_hostname(ssh, new_hostname); _fix_ssh_host_keys(ssh); _clean_cloud_init(ssh)
            _reset_kubernetes_state(ssh); _sync_time(ssh); _fix_static_ip(ssh, old_ip)
        finally:
            try: ssh.close()
            except Exception: pass

    try: await asyncio.to_thread(_phase1)
    except Exception: return
    await asyncio.sleep(15)
    new_ip = await _wait_for_vm_ip(new_vmid, clone_node, timeout=60) or vm_ip

    if source_k8s_node_name: _delete_stale_k8s_node(source_k8s_node_name)
    if CLONE_K8S_ROLE == "none" or not K8S_MASTER_IP: return
    join_cmd = await asyncio.to_thread(_get_kubeadm_join_command, K8S_MASTER_IP)
    if not join_cmd: return

    def _phase2() -> bool:
        ssh2 = _ssh_connect(new_ip, CLONE_SSH_USER, CLONE_SSH_KEY_PATH, CLONE_SSH_PASSWORD, CLONE_SSH_PORT)
        try: return _join_kubernetes_cluster(ssh2, join_cmd, new_hostname)
        finally:
            try: ssh2.close()
            except Exception: pass

    try:
        if not await asyncio.to_thread(_phase2): return
    except Exception: return

    if not await _wait_for_k8s_node_ready(new_hostname, timeout=300): return
    if CLONE_REPLICATE_PODS and source_k8s_node_name: await _replicate_pods_to_node(source_k8s_node_name, new_hostname)

def _update_delta_history(state: dict) -> None:
    nodes = state["nodes"]
    for kind in ("vms", "lxc"):
        for vm in state[kind].values():
            for node_deltas in _compute_raw_deltas(vm, nodes).values():
                for metric, value in node_deltas.items():
                    _delta_history[metric].append(value)

def _compute_raw_deltas(vm: dict, nodes: dict) -> dict:
    vm_cpu    = vm.get("cpu_actual", vm.get("cores", 1) * vm.get("cpu", 1.0))
    vm_ram    = vm.get("ram_actual", vm.get("maxmem", 0) * vm.get("mem", 1.0) if vm.get("mem") else vm.get("maxmem", 0))
    vm_io     = vm.get("io_actual", 0.0)
    vm_energy = vm.get("energy_actual", vm.get("energy_needed", 0.0))
    deltas = {}
    for name, info in nodes.items():
        if not info or info.get("status") != "online": continue
        maxcpu = info.get("maxcpu") or 1
        maxmem = info.get("maxmem") or 1
        if not maxcpu or not maxmem: continue
        deltas[name] = {
            "cpu":    vm_cpu    - maxcpu * (1.0 - info.get("cpu", 0.0)),
            "ram":    vm_ram    - (maxmem - info.get("mem", 0)),
            "io":     vm_io     - (NODE_IO_CAPACITY_BPS - info.get("io_used", 0.0)),
            "energy": vm_energy - (info.get("energy_capacity", 0) - info.get("energy_used", 0.0)),
        }
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
        for n in deltas: normalized[n][metric] = (deltas[n][metric] - mean) / std
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
    vms_data  = cluster_state[state_key]
    valid_vm_ids  =[v for v in vm_ids if v in vms_data]
    online_nodes  =[n for n, info in nodes.items() if info and info.get("status") == "online"]
    if not valid_vm_ids or not online_nodes: return {}

    N, T, F, CR, b, woa_frac = max(DE_WOA_N, 6), DE_WOA_T, DE_WOA_F, DE_WOA_CR, DE_WOA_B, DE_WOA_WOA_FRACTION
    population = [[random.choice(online_nodes) for _ in valid_vm_ids] for _ in range(N)]
    fitnesses  =[_mapping_fitness(mp, valid_vm_ids, vms_data, nodes, weights) for mp in population]

    best_idx          = min(range(N), key=lambda i: fitnesses[i])
    x_star, f_star    = population[best_idx].copy(), fitnesses[best_idx]
    all_indices       = list(range(N)); random.shuffle(all_indices)
    n_woa             = max(2, int(N * woa_frac))
    woa_indices       = all_indices[:n_woa]
    de_indices        = all_indices[n_woa:]

    for t in range(T):
        a = 2.0 * (1.0 - t / T)
        for i in woa_indices:
            xi         = population[i]
            r1, r2, p  = random.random(), random.random(), random.random()
            A, C       = 2.0 * a * r1 - a, 2.0 * r2
            if p < 0.5:
                target_map = x_star if abs(A) < 1.0 else population[random.choice([j for j in woa_indices if j != i] or [best_idx])]
                D        = sum(1 for k in range(len(xi)) if xi[k] != target_map[k]) * C
                strength = abs(A) * D
                diff_pos =[k for k in range(len(xi)) if xi[k] != target_map[k]]
                n_copy   = min(len(diff_pos), max(0, int(strength)))
                for k in (random.sample(diff_pos, n_copy) if n_copy > 0 else []): xi[k] = target_map[k]
            else:
                l       = random.uniform(-1.0, 1.0)
                p_adopt = max(0.0, min(1.0, (math.exp(b * l) * math.cos(2 * math.pi * l) + math.exp(b)) / (2.0 * math.exp(b))))
                for k in range(len(xi)):
                    if random.random() < p_adopt: xi[k] = x_star[k]
            fitnesses[i] = _mapping_fitness(xi, valid_vm_ids, vms_data, nodes, weights)

        woa_best = min(woa_indices, key=lambda i: fitnesses[i])
        if fitnesses[woa_best] < f_star: f_star, x_star = fitnesses[woa_best], population[woa_best].copy()

        donor_pool =[j for j in all_indices if j not in de_indices]
        for j in de_indices:
            eligible =[k for k in donor_pool if k != j]
            if len(eligible) < 2: continue
            xr1, xr2 = population[eligible[0]], population[eligible[1]]
            donor =[xr1[k] if xr1[k] != xr2[k] and random.random() < F else x_star[k] for k in range(len(x_star))]
            trial = [donor[k] if random.random() <= CR else population[j][k] for k in range(len(x_star))]
            f_trial = _mapping_fitness(trial, valid_vm_ids, vms_data, nodes, weights)
            if f_trial < fitnesses[j]: population[j], fitnesses[j] = trial, f_trial

        de_best = min(de_indices, key=lambda i: fitnesses[i])
        if fitnesses[de_best] < f_star: f_star, x_star = fitnesses[de_best], population[de_best].copy()

    if f_star == float("inf"): return {}
    return {vmid: node for vmid, node in zip(valid_vm_ids, x_star)}

def _log_action(action: str, vmid: int, params: dict, outcome: str, reason: str, triggered_by: str = "scaledown") -> None:
    action_log.appendleft({
        "timestamp":    datetime.now(timezone.utc).isoformat(),
        "action":       action,
        "vmid":         vmid,
        "params":       params,
        "outcome":      outcome,
        "reason":       reason,
        "triggered_by": triggered_by,
    })
    _current_cycle_actions.setdefault(vmid, {})[action] = 1

def _node_has_headroom(node_info: dict, vm: dict, extra_cores: int = 1, extra_mem_frac: float = 0.20) -> bool:
    if not node_info or node_info.get("status") != "online": return False
    if node_info.get("maxcpu", 0) * (1.0 - node_info.get("cpu", 1.0)) < extra_cores: return False
    vm_maxmem     = float(vm.get("maxmem", 0))
    node_ram_free = node_info.get("maxmem", 0) - node_info.get("mem", 0)
    if node_ram_free < vm_maxmem * extra_mem_frac: return False
    extra_energy     = extra_cores * ENERGY_PER_CPU_CORE + (vm_maxmem * extra_mem_frac / (1024**3)) * ENERGY_PER_GB_MEMORY
    node_energy_free = node_info.get("energy_capacity", 0) - node_info.get("energy_used", 0)
    return node_energy_free >= extra_energy

def _add_to_warning_queue(identifier: str, target_type: str, alert_type: AlertType, value: float, summary: str, target: dict, action: str = ""):
    target_cp = copy.deepcopy(target)
    for e in warning_queue:
        if e["identifier"] == identifier and e["target_type"] == target_type:
            e["alerts"][alert_type.value] = {"value": value, "summary": summary, "action": action}
            e["target"] = target_cp
            return
    warning_queue.append({
        "identifier":  identifier,
        "target_type": target_type,
        "alerts":      {alert_type.value: {"value": value, "summary": summary, "action": action}},
        "target":      target_cp,
    })

_K8S_MEM_UNIT_BYTES = {"": 1, "Ki": 1024, "K": 1000, "Mi": 1024 ** 2, "M": 10 ** 6, "Gi": 1024 ** 3, "G": 10 ** 9, "Ti": 1024 ** 4, "T": 10 ** 12}

def _parse_k8s_memory(q: str) -> int:
    q = (q or "").strip()
    if not q: return 0
    for suffix in ("Ti", "Gi", "Mi", "Ki", "T", "G", "M", "K"):
        if q.endswith(suffix):
            try: return int(float(q[:-len(suffix)]) * _K8S_MEM_UNIT_BYTES[suffix])
            except ValueError: return 0
    try: return int(q)
    except ValueError: return 0

def _parse_k8s_cpu(q: str) -> float:
    q = (q or "").strip()
    if not q: return 0.0
    try:
        if q.endswith("m"): return float(q[:-1]) / 1000.0
        return float(q)
    except ValueError: return 0.0

async def process_warning_queue() -> None:
    if not warning_queue:
        return

    proxmox_items =[e for e in list(warning_queue) if e["target_type"] in ("vm", "lxc")]
    k8s_items     =[e for e in list(warning_queue) if e["target_type"] == "k8s"]

    state = cluster_state

    # ── Kubernetes ───────────────────────────────────────────────────────────
    for item in k8s_items:
        ns, dep  = item["identifier"].split("/", 1)
        needs_retry = False

        for t, alert_data in item["alerts"].items():
            at = AlertType(t)
            action_label = alert_data.get("action", "")

            try:
                dep_obj   = state["deployments"].get(f"{ns}/{dep}")
                if not dep_obj or not k8s_apps:
                    needs_retry = True
                    continue

                # CORRECTED: Trigger Horizontal Scaling if requested by alert label
                if "horizontal" in action_label or at in (AlertType.NETWORK, AlertType.HTTP_5XX):
                    new_replicas = (dep_obj.get("replicas") or 1) + 1
                    logger.info(f"[Scale] {ns}/{dep}: replicas {dep_obj.get('replicas')} → {new_replicas} "
                                f"(triggered by {at.value}, action={action_label})")
                    k8s_apps.patch_namespaced_deployment(dep, ns, {"spec": {"replicas": new_replicas}})
                    _log_action("scaleout_replicas", 0, {"namespace": ns, "deployment": dep, "replicas": new_replicas},
                                "success", f"horizontal_scaling triggered by {at.value}", "scaleup")
                    continue

                # Fallback to Vertical Scaling
                container = k8s_apps.read_namespaced_deployment(dep, ns).spec.template.spec.containers[0]
                limits    = container.resources.limits or {}

                if at == AlertType.CPU:
                    cur = _parse_k8s_cpu(limits.get("cpu", "0.5"))
                    limits["cpu"] = f"{int(cur * 1.25 * 1000)}m"
                elif at == AlertType.MEMORY:
                    cur_bytes = _parse_k8s_memory(limits.get("memory", "512Mi"))
                    new_mb    = max(1, int(cur_bytes * 1.20 / (1024 * 1024)))
                    limits["memory"] = f"{new_mb}Mi"
                elif at == AlertType.DISK_IO:
                    continue

                patch = {"spec": {"template": {"spec": {"containers":[{"name": container.name, "resources": {"limits": limits}}]}}}}
                k8s_apps.patch_namespaced_deployment(dep, ns, patch)
                _log_action("scaleup_limits", 0, {"namespace": ns, "deployment": dep, "limits": limits},
                            "success", f"vertical_scaling triggered by {at.value}", "scaleup")
            except Exception as e:
                logger.error(f"K8s scaling error for {ns}/{dep}: {e}")
                needs_retry = True
        if not needs_retry and item in warning_queue:
            warning_queue.remove(item)

    # ── Proxmox VMs / LXC ────────────────────────────────────────────────────
    async with _state_lock:
        nodes_snapshot = copy.deepcopy(state["nodes"])
    async with _weights_lock:
        weights_snapshot = (W_CPU, W_RAM, W_IO, W_E)

    for ttype_str, ttype_enum in (("vm", TargetType.VM), ("lxc", TargetType.LXC)):
        group =[e for e in proxmox_items if e["target_type"] == ttype_str]
        if not group:
            continue
        vm_ids = [int(item["identifier"]) for item in group if item["identifier"].isdigit()]
        best_mapping = await asyncio.to_thread(hybrid_de_woa_provisioning, vm_ids, ttype_enum, nodes_snapshot, weights_snapshot)
        state_key    = "vms" if ttype_str == "vm" else "lxc"

        for item in group:
            vmid         = int(item["identifier"])
            target       = state[state_key].get(vmid)
            if not target:
                continue

            current_node = target.get("node")
            best_node    = best_mapping.get(vmid)
            node_info    = nodes_snapshot.get(current_node, {})

            if best_node and best_node != current_node:
                if not _node_has_headroom(node_info, target):
                    reason = "current node lacks headroom for scale-up"
                else:
                    reason = "DE-WOA recommends better placement"
                try:
                    if ttype_enum == TargetType.VM:
                        await asyncio.to_thread(px.nodes(current_node).qemu(vmid).migrate.post, target=best_node, online=1)
                        _log_action("migrate", vmid, {"from": current_node, "to": best_node}, "success", reason, "scaleup")
                        await asyncio.sleep(5)
                        current_node = best_node
                except Exception as e:
                    _log_action("migrate", vmid, {"from": current_node, "to": best_node}, "error", str(e), "scaleup")

            for alert_key, alert_data in item["alerts"].items():
                at          = AlertType(alert_key)
                alert_value = float(alert_data.get("value", 0))

                if at == AlertType.CPU:
                    new_cores = int(target.get("cores", 1)) + 1
                    try:
                        if ttype_enum == TargetType.VM:
                            await asyncio.to_thread(px.nodes(current_node).qemu(vmid).config.put, cores=new_cores)
                        else:
                            await asyncio.to_thread(px.nodes(current_node).lxc(vmid).config.put, cores=new_cores)
                        _log_action("scaleup_cores", vmid, {"cores": new_cores}, "success", "CPU threshold exceeded", "scaleup")
                    except Exception as e:
                        _log_action("scaleup_cores", vmid, {"cores": new_cores}, "error", str(e), "scaleup")

                elif at == AlertType.MEMORY:
                    new_mem_mb = int(int(target.get("maxmem", 512 * 1024 * 1024)) * 1.20 / (1024 * 1024))
                    try:
                        if ttype_enum == TargetType.VM:
                            await asyncio.to_thread(px.nodes(current_node).qemu(vmid).config.put, memory=new_mem_mb)
                        else:
                            await asyncio.to_thread(px.nodes(current_node).lxc(vmid).config.put, memory=new_mem_mb)
                        _log_action("scaleup_memory", vmid, {"memory_mb": new_mem_mb}, "success", "RAM threshold exceeded", "scaleup")
                    except Exception as e:
                        _log_action("scaleup_memory", vmid, {"memory_mb": new_mem_mb}, "error", str(e), "scaleup")

                elif at == AlertType.DISK_IO:
                    try:
                        if ttype_enum == TargetType.VM:
                            await asyncio.to_thread(px.nodes(current_node).qemu(vmid).resize.put, disk="scsi0", size="+15G")
                            asyncio.create_task(_grow_filesystem_on_vm(vmid, current_node, "scsi0"))
                        else:
                            await asyncio.to_thread(px.nodes(current_node).lxc(vmid).resize.put, volume="rootfs", size="+15G")
                        _log_action("scaleup_disk", vmid, {"disk": "+15G"}, "success", "IO free capacity below threshold", "scaleup")
                    except Exception as e:
                        _log_action("scaleup_disk", vmid, {"disk": "+15G"}, "error", str(e), "scaleup")

                elif at in (AlertType.NETWORK, AlertType.HTTP_5XX):
                    try:
                        new_vmid         = int(px.cluster.nextid.get())
                        target_clone_node = best_node or current_node
                        old_ip               = target.get("ip", "")
                        source_k8s_node_name = _get_k8s_node_name_for_vm(target)
                        clone_name           = f"clone-{vmid}-{new_vmid}"

                        if ttype_enum == TargetType.VM:
                            await asyncio.to_thread(px.nodes(current_node).qemu(vmid).clone.post, newid=new_vmid, target=target_clone_node, full=1)
                            await asyncio.sleep(3)
                            await asyncio.to_thread(px.nodes(target_clone_node).qemu(new_vmid).status.start.post)
                        else:
                            await asyncio.to_thread(px.nodes(current_node).lxc(vmid).clone.post, newid=new_vmid, hostname=clone_name)
                            await asyncio.sleep(3)
                            await asyncio.to_thread(px.nodes(target_clone_node).lxc(new_vmid).status.start.post)
                        _clone_registry[new_vmid] = vmid

                        await _register_vm_with_sd(str(new_vmid), clone_name, vmid=new_vmid)

                        asyncio.create_task(
                            _remediate_clone(
                                new_vmid=new_vmid, clone_node=target_clone_node, source_vmid=vmid,
                                new_hostname=clone_name, old_ip=old_ip, source_k8s_node_name=source_k8s_node_name,
                            )
                        )

                        _log_action("scaleout_clone", vmid, {"new_vmid": new_vmid, "node": target_clone_node, "remediation": "scheduled"}, "success", "Horizontal scale-out triggered", "scaleup")
                    except Exception as e:
                        _log_action("scaleout_clone", vmid, {}, "error", str(e), "scaleup")

            if item in warning_queue:
                warning_queue.remove(item)

def handle_alert(alert: PrometheusAlert) -> None:
    try:
        severity    = AlertSeverity(alert.labels.severity.lower())
        alert_type  = AlertType(alert.labels.type.lower())
        target_type = TargetType(alert.labels.target_type.lower())
    except ValueError:
        logger.warning(
            f"[Alert] {alert.labels.alertname}: dropped — "
            f"unsupported labels (severity='{alert.labels.severity}', "
            f"type='{alert.labels.type}', target_type='{alert.labels.target_type}')"
        )
        return

    state = cluster_state
    value = float(alert.annotations.value or 0)

    # ── Kubernetes path ──────────────────────────────────────────────────────
    if target_type == TargetType.K8S:
        ns  = alert.labels.namespace or ""
        dep = alert.labels.deployment or ""

        if (not ns or not dep) and "__" in alert.labels.upstream:
            u_ns, u_dep = alert.labels.upstream.split("__", 1)
            ns  = ns  or u_ns
            dep = dep or u_dep

        if not ns or not dep:
            logger.warning(
                f"[Alert] {alert.labels.alertname}: cannot resolve namespace/deployment "
                f"(ns='{ns}', dep='{dep}', upstream='{alert.labels.upstream}') — dropping"
            )
            return

        key = f"{ns}/{dep}"
        target = state["deployments"].get(key)
        if target is None:
            logger.warning(
                f"[Alert] {alert.labels.alertname}: deployment '{key}' not in cluster_state. "
                f"Known deployments: {list(state['deployments'].keys())[:5]}... — dropping"
            )
            return

        logger.info(f"[Alert] queueing {alert.labels.alertname} for k8s deployment {key} "
                    f"(action={alert.labels.action}, type={alert_type.value})")
        _add_to_warning_queue(key, target_type.value, alert_type, value, alert.annotations.summary, target, alert.labels.action)
        return

    # ── VM / LXC path ────────────────────────────────────────────────────────
    pool = state["vms"] if target_type == TargetType.VM else state["lxc"]
    vmid: Optional[int] = None

    if alert.labels.vmid:
        try: vmid = int(alert.labels.vmid)
        except ValueError: vmid = None

    if vmid is None and alert.labels.instance:
        ip = alert.labels.instance.split(":", 1)[0]
        for vid, vm in pool.items():
            if vm.get("ip") == ip:
                vmid = int(vid)
                break

    if vmid is None:
        logger.warning(
            f"[Alert] {alert.labels.alertname}: cannot resolve vmid "
            f"(label='{alert.labels.vmid}', instance='{alert.labels.instance}') "
            f"— pool has {len(pool)} {target_type.value}s — dropping"
        )
        return

    target = pool.get(vmid)
    if target is None:
        logger.warning(
            f"[Alert] {alert.labels.alertname}: vmid={vmid} not in {target_type.value} pool — dropping"
        )
        return

    logger.info(f"[Alert] queueing {alert.labels.alertname} for {target_type.value}={vmid} "
                f"(action={alert.labels.action}, type={alert_type.value})")
    _add_to_warning_queue(str(vmid), target_type.value, alert_type, value, alert.annotations.summary, target, alert.labels.action)

async def check_scaledown() -> None:
    now, now_ts = datetime.now(timezone.utc), datetime.now(timezone.utc).timestamp()
    async with _state_lock:
        vms_snapshot = copy.deepcopy(cluster_state["vms"])
        lxc_snapshot = copy.deepcopy(cluster_state["lxc"])

    async with _approval_lock:
        expired =[a for a in approval_queue if a.get("expires_at", 0) < now_ts]
        for entry in expired:
            approval_queue.remove(entry)
            _log_action("approval_expired", entry.get("vmid", 0), {}, "expired", f"Action {entry['id']} exceeded TTL", "system")

    async with _ml_config_lock:
        t_cpu_low  = _ml_config.get("thresh_cpu_low",  SCALEDOWN_CPU_LOW_PCT)
        t_ram_low  = _ml_config.get("thresh_ram_low",  SCALEDOWN_RAM_LOW_PCT)
        t_http_low = _ml_config.get("thresh_http_low", SCALEDOWN_HTTP_LOW_PCT)

    for ttype_str, snapshot in (("vm", vms_snapshot), ("lxc", lxc_snapshot)):
        for vmid, vm in snapshot.items():
            if vm.get("status") != "running":
                _scaledown_tracker.pop(vmid, None)
                continue

            cpu_pct  = float(vm.get("cpu", 0.0)) * 100.0
            ram_pct  = float(vm.get("mem", 0)) / max(float(vm.get("maxmem", 1)), 1) * 100.0
            http_5xx = float(vm.get("http_5xx_rate", 0.0)) * 100.0

            tracker = _scaledown_tracker.setdefault(vmid, {"cpu_low": 0, "ram_low": 0, "http_low": 0})
            tracker["cpu_low"]  = tracker["cpu_low"]  + 1 if cpu_pct  < t_cpu_low  else 0
            tracker["ram_low"]  = tracker["ram_low"]  + 1 if ram_pct  < t_ram_low  else 0
            tracker["http_low"] = tracker["http_low"] + 1 if http_5xx < t_http_low else 0

            sustained_cpu  = tracker["cpu_low"]  >= SCALEDOWN_SUSTAINED_POLLS
            sustained_ram  = tracker["ram_low"]  >= SCALEDOWN_SUSTAINED_POLLS
            sustained_http = tracker["http_low"] >= SCALEDOWN_SUSTAINED_POLLS
            is_clone       = vmid in _clone_registry
            node           = vm.get("node", "")

            if is_clone and sustained_cpu and sustained_ram and sustained_http:
                if not any(a.get("vmid") == vmid and a.get("type") == "delete_vm" for a in approval_queue):
                    action_id = f"delete_{vmid}_{int(now_ts)}"
                    async with _approval_lock:
                        approval_queue.append({
                            "id": action_id, "type": "delete_vm",
                            "target_type": ttype_str, "vmid": vmid, "node": node,
                            "parent_vmid": _clone_registry[vmid],
                            "reason": "Sustained low load on clone",
                            "risk": "high",
                            "queued_at": now.isoformat(),
                            "expires_at": now_ts + APPROVAL_TTL_SECONDS,
                        })
                tracker["cpu_low"] = tracker["ram_low"] = tracker["http_low"] = 0
                _current_cycle_actions.setdefault(vmid, {})["delete_vm"] = 1
                continue

            current_cores = int(vm.get("cores", 1))
            if sustained_cpu and current_cores > MIN_VM_CORES:
                new_cores = max(MIN_VM_CORES, current_cores - 1)
                try:
                    if ttype_str == "vm":
                        await asyncio.to_thread(px.nodes(node).qemu(vmid).config.put, cores=new_cores)
                    else:
                        await asyncio.to_thread(px.nodes(node).lxc(vmid).config.put, cores=new_cores)
                    _log_action("scaledown_cores", vmid, {"cores": new_cores}, "success", "CPU low sustained")
                    tracker["cpu_low"] = 0
                except Exception as e:
                    _log_action("scaledown_cores", vmid, {"cores": new_cores}, "error", str(e))

            current_mem_mb = int(vm.get("maxmem", 0)) // (1024 * 1024)
            if sustained_ram and current_mem_mb > MIN_VM_MEMORY_MB:
                new_mem_mb = max(MIN_VM_MEMORY_MB, int(current_mem_mb * 0.80))
                try:
                    if ttype_str == "vm":
                        await asyncio.to_thread(px.nodes(node).qemu(vmid).config.put, memory=new_mem_mb)
                    else:
                        await asyncio.to_thread(px.nodes(node).lxc(vmid).config.put, memory=new_mem_mb)
                    _log_action("scaledown_memory", vmid, {"memory_mb": new_mem_mb}, "success", "RAM low sustained")
                    tracker["ram_low"] = 0
                except Exception as e:
                    _log_action("scaledown_memory", vmid, {"memory_mb": new_mem_mb}, "error", str(e))

async def _execute_approved_action(entry: dict) -> str:
    action_type = entry.get("type")
    vmid        = entry.get("vmid")
    node        = entry.get("node")
    ttype_str   = entry.get("target_type", "vm")

    if action_type == "delete_vm":
        try:
            if ttype_str == "vm":
                try: await asyncio.to_thread(px.nodes(node).qemu(vmid).status.stop.post); await asyncio.sleep(5)
                except Exception: pass
                await asyncio.to_thread(px.nodes(node).qemu(vmid).delete)
            else:
                try: await asyncio.to_thread(px.nodes(node).lxc(vmid).status.stop.post); await asyncio.sleep(3)
                except Exception: pass
                await asyncio.to_thread(px.nodes(node).lxc(vmid).delete)

            _clone_registry.pop(vmid, None)
            _scaledown_tracker.pop(vmid, None)

            vm_ip = cluster_state.get("vms", {}).get(vmid, {}).get("ip", str(vmid))
            await _deregister_vm_from_sd(str(vm_ip))

            _log_action("delete_vm", vmid, {"node": node}, "success", "Admin approved deletion", "admin_approval")
            return "success"
        except Exception as e:
            _log_action("delete_vm", vmid, {"node": node}, "error", str(e), "admin_approval")
            return f"error: {e}"
    return f"unknown action type: {action_type}"

def _load_ml_models():
    global _ml_boosters, _vlan_encoder
    for target in ALL_TARGETS:
        try:
            b = xgb.Booster()
            b.load_model(f"{MODELS_DIR}/{target}.ubj")
            _ml_boosters[target] = b
        except Exception as e:
            logger.warning(f"Could not load local ML model for {target}: {e}")
    if _ml_boosters:
        logger.info(f"Local ML fallback: loaded {len(_ml_boosters)} XGBoost models")

    enc_path = f"{MODELS_DIR}/vlan_encoder.joblib"
    try: _vlan_encoder = joblib.load(enc_path)
    except Exception as e: logger.warning(f"vlan_encoder not found: {e}")

def _flush_csv_buffers():
    if not _ml_metrics_buffer or not _ml_logs_buffer: return None, None
    m_path, l_path = f"{DATA_BUFFER}/metrics_latest.csv", f"{DATA_BUFFER}/logs_latest.csv"

    with open(m_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=_ml_metrics_buffer[0].keys())
        writer.writeheader(); writer.writerows(_ml_metrics_buffer)

    with open(l_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=_ml_logs_buffer[0].keys())
        writer.writeheader(); writer.writerows(_ml_logs_buffer)

    _ml_metrics_buffer.clear(); _ml_logs_buffer.clear()
    return m_path, l_path

async def ml_incremental_update_job():
    logger.info("ML incremental update: flushing buffers...")
    try:
        new_metrics, new_logs = await asyncio.to_thread(_flush_csv_buffers)
        if not new_metrics or not new_logs: return

        from incremental_learning import incremental_update
        report = await asyncio.to_thread(incremental_update, new_metrics, new_logs, N_NEW_TREES, MODELS_DIR)
        await asyncio.to_thread(_load_ml_models)

        improved = sum(1 for v in report.values() if v.get("delta", 0) < 0)
        logger.info(f"ML incremental update complete: {improved}/{len(report)} models improved")
    except Exception as exc: logger.error(f"ML incremental update failed: {exc}")

scheduler = AsyncIOScheduler()

@asynccontextmanager
async def lifespan(app: FastAPI):
    global cluster_state, k8s_apps, k8s_core, W_CPU, W_RAM, W_IO, W_E

    try:
        kube_config.load_incluster_config()
        k8s_apps = client.AppsV1Api()
        k8s_core = client.CoreV1Api()
        logger.info("Kubernetes client connected (in-cluster config).")
    except kube_config.ConfigException:
        try:
            kubeconfig_path = os.getenv("KUBECONFIG", "")
            if kubeconfig_path and Path(kubeconfig_path).exists(): kube_config.load_kube_config(config_file=kubeconfig_path)
            else: kube_config.load_kube_config()
            k8s_apps = client.AppsV1Api()
            k8s_core = client.CoreV1Api()
            logger.info("Kubernetes client connected (kubeconfig).")
        except Exception as e:
            logger.warning(f"Kubernetes client unavailable — k8s features disabled: {e}")

    w_sum = W_CPU + W_RAM + W_IO + W_E
    if abs(w_sum - 1.0) >= 0.01: raise RuntimeError(f"Initial weights sum to {w_sum:.4f} — must equal 1.0")
    elif abs(w_sum - 1.0) > 1e-6: W_CPU, W_RAM, W_IO, W_E = W_CPU/w_sum, W_RAM/w_sum, W_IO/w_sum, W_E/w_sum

    _load_ml_models()

    proxmox, k8s_deps = await asyncio.gather(
        asyncio.to_thread(fetch_proxmox_state),
        asyncio.to_thread(fetch_kubernetes_state),
    )
    cluster_state = {**proxmox, "deployments": k8s_deps}

    scheduler.add_job(poll_cluster,             "interval", seconds=10,                    id="poll_cluster")
    scheduler.add_job(process_warning_queue,    "interval", seconds=10,                    id="process_warning_queue")
    scheduler.add_job(check_scaledown,          "interval", seconds=SCALEDOWN_INTERVAL_S,  id="check_scaledown")
    scheduler.add_job(ml_incremental_update_job,"interval", minutes=5,                     id="ml_incremental_update")
    scheduler.add_job(ml_config_fallback_poll,  "interval", minutes=5,                     id="ml_config_fallback")
    scheduler.start()
    yield
    scheduler.shutdown()

app = FastAPI(lifespan=lifespan, title="PFE Cluster Manager")

@app.post("/alert")
async def receive_alert(webhook: AlertmanagerWebhook):
    """Receive Alertmanager webhook and enqueue scale actions.

    DEBUG: logs every webhook arrival, so an empty journal definitively
    means Alertmanager isn't reaching us.  If you see "[Alert] received N"
    lines but no "[Alert] queueing" lines, the alerts are being dropped
    by handle_alert (look at the WARNING messages it emits).
    """
    processed = 0
    firing    = sum(1 for a in webhook.alerts if a.status == "firing")
    resolved  = sum(1 for a in webhook.alerts if a.status == "resolved")
    logger.info(f"[Alert] received batch of {len(webhook.alerts)} "
                f"(firing={firing}, resolved={resolved})")

    for alert in webhook.alerts:
        if alert.status != "firing":
            continue
        try:
            handle_alert(alert)
            processed += 1
        except Exception as exc:
            logger.warning(f"[Alert] Handler raised on {alert.labels.alertname}: {exc}")
    return {"status": "ok", "processed": processed, "received": len(webhook.alerts)}

@app.get("/state")
async def get_state(_: str = Depends(verify_api_key)):
    async with _state_lock: return cluster_state

@app.get("/actions")
async def get_actions():
    return list(action_log)

@app.get("/approval-queue")
async def get_approval_queue(_: str = Depends(verify_api_key)):
    async with _approval_lock: return list(approval_queue)

@app.post("/approve/{action_id}")
async def approve_action(action_id: str, _: str = Depends(verify_api_key)):
    async with _approval_lock:
        entry = next((a for a in approval_queue if a["id"] == action_id), None)
        if not entry: raise HTTPException(404, f"Action '{action_id}' not found")
        approval_queue.remove(entry)
    result = await _execute_approved_action(entry)
    return {"action_id": action_id, "result": result}

@app.post("/migrate/{vmid}")
async def migrate_vm(vmid: int, req: MigrateRequest, _: str = Depends(verify_api_key)):
    async with _state_lock:
        vm = cluster_state["vms"].get(vmid) or cluster_state["lxc"].get(vmid)
    if not vm: raise HTTPException(404, f"VMID {vmid} not found")

    is_vm, node = vmid in cluster_state["vms"], vm.get("node")
    try:
        if is_vm: await asyncio.to_thread(px.nodes(node).qemu(vmid).migrate.post, target=req.target_node, online=1)
        else: await asyncio.to_thread(px.nodes(node).lxc(vmid).migrate.post, target=req.target_node, online=1)
        _log_action("migrate", vmid, {"from": node, "to": req.target_node}, "success", "manual request", "api")
        return {"status": "ok", "vmid": vmid, "to": req.target_node}
    except Exception as e: raise HTTPException(500, str(e))

@app.post("/migrate-from-node")
async def migrate_from_node(req: MigrateFromNodeRequest, _: str = Depends(verify_api_key)):
    failed_node = req.node
    logger.warning(f"[Migration] Evacuation requested for node '{failed_node}': {req.reason}")

    async with _state_lock:
        nodes_snapshot = copy.deepcopy(cluster_state["nodes"])
        vms_on_node    =[vmid for vmid, vm in cluster_state["vms"].items() if vm.get("node") == failed_node]
        lxc_on_node    = [vmid for vmid, vm in cluster_state["lxc"].items() if vm.get("node") == failed_node]

    async with _weights_lock:
        weights_snapshot = (W_CPU, W_RAM, W_IO, W_E)

    migrated, failed_ids = [],[]
    for ttype_str, ttype_enum, vm_ids in (("vm", TargetType.VM, vms_on_node), ("lxc", TargetType.LXC, lxc_on_node)):
        if not vm_ids: continue
        mapping = await asyncio.to_thread(hybrid_de_woa_provisioning, vm_ids, ttype_enum, nodes_snapshot, weights_snapshot)
        for vmid, target_node in mapping.items():
            if target_node == failed_node:
                failed_ids.append(vmid)
                continue
            try:
                if ttype_str == "vm": await asyncio.to_thread(px.nodes(failed_node).qemu(vmid).migrate.post, target=target_node, online=1)
                else: await asyncio.to_thread(px.nodes(failed_node).lxc(vmid).migrate.post, target=target_node, online=1)
                _log_action("migrate", vmid, {"from": failed_node, "to": target_node}, "success", req.reason, "pm_failure")
                migrated.append({"vmid": vmid, "to": target_node})
            except Exception as e:
                _log_action("migrate", vmid, {"from": failed_node, "to": target_node}, "error", str(e), "pm_failure")
                failed_ids.append(vmid)

    return {"status": "ok", "failed_node": failed_node, "migrated": migrated, "failed_vmids": failed_ids}

@app.post("/update-ml-config")
async def update_ml_config(payload: MLConfigUpdate, _: str = Depends(verify_api_key)):
    await _apply_ml_config(payload.config)
    return {"status": "ok", "applied": payload.config}

@app.get("/ml-config")
async def get_ml_config(_: str = Depends(verify_api_key)):
    async with _ml_config_lock:
        return {"config": _ml_config.copy(), "weights": {"w_cpu": W_CPU, "w_ram": W_RAM, "w_io": W_IO, "w_energy": W_E}}

@app.get("/predict-config/{instance_id}")
async def predict_config_endpoint(instance_id: str, _: str = Depends(verify_api_key)):
    if not _ml_boosters: raise HTTPException(503, "Local ML models not loaded")
    async with _state_lock:
        vm = cluster_state["vms"].get(int(instance_id) if instance_id.isdigit() else instance_id) \
          or cluster_state["lxc"].get(int(instance_id) if instance_id.isdigit() else instance_id)
    if not vm: raise HTTPException(404, f"Instance {instance_id} not found")

    name = vm.get("name", str(instance_id)).lower()
    features = {
        "up": 1.0 if vm.get("status") == "running" else 0.0,
        "scrape_duration_seconds": vm.get("scrape_duration", 0.1),
        "cpu_busy_pct": float(vm.get("cpu", 0.0)) * 100,
        "ram_usage_pct": (float(vm.get("mem", 0)) / max(float(vm.get("maxmem", 1)), 1)) * 100,
        "io_util_pct": vm.get("disk", 0.0) * 100,
        "http_5xx_rate": vm.get("http_5xx_rate", 0.0),
        "net_drop_rate": vm.get("net_drop_rate", 0.0),
        "power_watts": vm.get("power_watts", 50.0),
        "is_worker": int("worker" in name),
        "is_master": int("master" in name),
        "is_monitor": int(any(r in name for r in["monitoring", "influx", "snmp"])),
        "vlan_enc": int(_vlan_encoder.transform([vm.get("vlan", "")])[0]) if _vlan_encoder is not None and vm.get("vlan", "") in _vlan_encoder.classes_ else 0,
    }

    X_input = xgb.DMatrix(pd.DataFrame([features])[FEATURE_COLS], feature_names=FEATURE_COLS)
    raw     = {t: float(_ml_boosters[t].predict(X_input)[0]) for t in ALL_TARGETS}

    w_sum = sum(max(0.0, raw[t]) for t in TARGET_WEIGHTS)
    for t in TARGET_WEIGHTS: raw[t] = max(0.0, raw[t]) / w_sum if w_sum > 0 else 0.25

    raw["thresh_cpu_warn"]  = float(np.clip(raw["thresh_cpu_warn"],  50.0, 95.0))
    raw["thresh_ram_warn"]  = float(np.clip(raw["thresh_ram_warn"],  55.0, 95.0))
    raw["thresh_disk_warn"] = float(np.clip(raw["thresh_disk_warn"], 10.0, 95.0))
    raw["thresh_http_warn"] = float(np.clip(raw["thresh_http_warn"],  0.1,  5.0))
    raw["thresh_cpu_crit"]  = float(np.clip(raw["thresh_cpu_crit"],  50.0, 95.0))
    raw["thresh_ram_crit"]  = float(np.clip(raw["thresh_ram_crit"],  55.0, 95.0))
    raw["thresh_disk_crit"] = float(np.clip(raw["thresh_disk_crit"], 10.0, 95.0))
    raw["thresh_http_crit"] = float(np.clip(raw["thresh_http_crit"],  0.1,  5.0))
    raw["thresh_net_crit"]  = float(np.clip(raw["thresh_net_crit"],   0.1,  5.0))
    raw["thresh_cpu_low"]   = float(np.clip(raw["thresh_cpu_low"],   10.0, 50.0))
    raw["thresh_ram_low"]   = float(np.clip(raw["thresh_ram_low"],   10.0, 50.0))
    raw["thresh_http_low"]  = float(np.clip(raw["thresh_http_low"],   0.1,  1.0))

    return {"instance": instance_id, "config": raw}

@app.get("/health")
async def health():
    async with _state_lock:
        n_vms, n_nodes  = len(cluster_state["vms"]), len(cluster_state["nodes"])
        n_deployments   = len(cluster_state.get("deployments", {}))
        sample_deps     = list(cluster_state.get("deployments", {}).keys())[:5]
    return {
        "status": "ok",
        "nodes": n_nodes, "vms": n_vms,
        # NEW: critical for cluster-manager-on-monitoring-VM diagnostics.
        # k8s_connected=False means the kubeconfig is missing or the
        # apiserver is unreachable — every k8s scaling alert will fail
        # silently until this is fixed.
        "k8s_connected":   k8s_apps is not None,
        "deployments":     n_deployments,
        "sample_deployments": sample_deps,
        "warning_queue":   len(warning_queue),
        "approval_queue":  len(approval_queue),
        "ml_models_local": len(_ml_boosters),
        "ml_url":          ML_URL,
        "app_url":         APP_URL,
    }
