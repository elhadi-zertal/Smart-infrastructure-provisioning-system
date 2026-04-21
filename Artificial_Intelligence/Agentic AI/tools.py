import requests
import sqlite3
import json
import os
import logging

AGENT_DB_PATH = os.getenv("AGENT_DB_PATH", "./agent_state.db")
BASE = os.getenv("CLUSTER_API_URL", "http://localhost:8000")   # Main Cluster Manager API
AGENT_BASE = os.getenv("AGENT_BASE_URL", "http://localhost:8002")  # Agent Service API
CLUSTER_API_KEY = os.getenv("CLUSTER_API_KEY", "dev-secure-key-123") # Security integration

# Standardized headers for authenticated inter-component communication
HEADERS = {"X-API-Key": CLUSTER_API_KEY}

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
#  READ TOOLS — Planner calls these during reasoning.
#
#  CRITICAL: docstrings are mandatory.
#  Agno sends the docstring to the LLM as the tool description.
# ─────────────────────────────────────────────────────────────

def get_health():
    """Returns cluster overview: node count, VM count, LXC count, warning queue length."""
    return requests.get(f"{BASE}/health", headers=HEADERS, timeout=5).json()

def list_nodes():
    """Returns all physical Proxmox nodes with CPU utilisation fraction, RAM used/total, energy used/capacity."""
    return requests.get(f"{BASE}/nodes", headers=HEADERS, timeout=5).json()

def get_node(name: str):
    """Returns detailed CPU, RAM, IO, energy stats for a single Proxmox node. Use node names like pfe0, pfe1, pfe2."""
    return requests.get(f"{BASE}/nodes/{name}", headers=HEADERS, timeout=5).json()

def list_vms():
    """Returns all VMs across the cluster with status (running/stopped), CPU cores, RAM allocation, and which node they run on."""
    return requests.get(f"{BASE}/vms", headers=HEADERS, timeout=10).json()

def get_vm(vmid: int):
    """Returns detailed stats for a single VM: node placement, CPU cores, RAM, disk, current CPU and RAM utilisation."""
    return requests.get(f"{BASE}/vms/{vmid}", headers=HEADERS, timeout=5).json()

def list_deployments():
    """Returns all Kubernetes deployments with current replica count, ready replicas, CPU and memory limits."""
    return requests.get(f"{BASE}/deployments", headers=HEADERS, timeout=10).json()

def get_deployment(namespace: str, name: str):
    """Returns detailed info for a single Kubernetes deployment including replica status and resource limits."""
    return requests.get(f"{BASE}/deployments/{namespace}/{name}", headers=HEADERS, timeout=5).json()

def get_warning_queue():
    """Returns VMs and Kubernetes deployments currently queued for automated provisioning by DE-WOA algorithm."""
    return requests.get(f"{BASE}/warning-queue", headers=HEADERS, timeout=5).json()

def get_history_stats():
    """Returns mean and standard deviation of resource gap observations (CPU, RAM, IO, energy) from historical data."""
    return requests.get(f"{BASE}/delta-history/stats", headers=HEADERS, timeout=5).json()

def get_xgboost_prediction():
    """Returns the current cluster bottleneck class (CPU/RAM/IO/Energy/None), active fitness weight vector, confidence score, and feature importances."""
    return requests.get(f"{BASE}/prediction", headers=HEADERS, timeout=5).json()

def get_action_log():
    """Returns the last 100 automated actions taken by the system. Always check this before proposing an action to avoid duplicates."""
    return requests.get(f"{AGENT_BASE}/action-log", headers=HEADERS, timeout=5).json()

def list_lxc():
    """Returns all LXC containers across the cluster with status, CPU cores, RAM allocation, and host node."""
    return requests.get(f"{BASE}/lxc", headers=HEADERS, timeout=10).json()

def get_lxc(vmid: int):
    """Returns detailed stats for a single LXC container: node placement, CPU, RAM, disk, current utilisation."""
    return requests.get(f"{BASE}/lxc/{vmid}", headers=HEADERS, timeout=5).json()

def get_vm_action_history(vmid: int, limit: int = 20):
    """Returns previous actions on a specific VM to prevent execution loops."""
    if not os.path.exists(AGENT_DB_PATH):
        return []
    con = sqlite3.connect(AGENT_DB_PATH)
    try:
        rows = con.execute(
            """SELECT timestamp, tool, inputs, result, outcome, triggered_by
               FROM executed_log
               WHERE json_extract(inputs, '$.vmid') = ?
               ORDER BY timestamp DESC LIMIT ?""",
            (vmid, limit)
        ).fetchall()
    except sqlite3.OperationalError as e:
        logger.warning(f"Failed to query DB for action history: {e}")
        rows = []
    finally:
        con.close()

    return [
        {"timestamp": r[0], "tool": r[1], "inputs": json.loads(r[2]),
         "result": json.loads(r[3]) if r[3] else None, "outcome": r[4], "triggered_by": r[5]}
        for r in rows
    ]

# ─────────────────────────────────────────────────────────────
#  WRITE TOOLS — executed autonomously for low/medium risk.
# ─────────────────────────────────────────────────────────────

def scale_vm(vmid: int, cores: int = None, memory: int = None):
    """Add vCPUs or RAM to a running VM. cores = new total core count. memory = new RAM in MB. Low risk — executes immediately after Critic approval."""
    body = {k: v for k, v in {"cores": cores, "memory": memory}.items() if v is not None}
    return requests.put(f"{BASE}/vms/{vmid}/resources", json=body, headers=HEADERS, timeout=15).json()

def scale_down_vm(vmid: int, cores: int = None, memory: int = None):
    """Reduce vCPUs or RAM of a VM. cores = new LOWER total core count. memory = new LOWER RAM in MB.
    WARNING: Reducing RAM on a running VM may cause OOM kills. Medium risk — executes after Critic approval."""
    body = {k: v for k, v in {"cores": cores, "memory": memory}.items() if v is not None}
    return requests.put(f"{BASE}/vms/{vmid}/resources", json=body, headers=HEADERS, timeout=15).json()

def clone_vm(vmid: int):
    """Clone a VM to create a second instance for horizontal scaling. Medium risk — executes immediately after Critic approval."""
    return requests.post(f"{BASE}/vms/{vmid}/clone", headers=HEADERS, timeout=45).json()

def clone_lxc(vmid: int):
    """Clone an LXC container to create a second instance for horizontal scaling. Medium risk — executes immediately after Critic approval."""
    return requests.post(f"{BASE}/lxc/{vmid}/clone", headers=HEADERS, timeout=45).json()

def scale_up_deployment(namespace: str, name: str, replicas: int):
    """Increase replica count of a K8s deployment. replicas must be greater than current. Low risk."""
    return requests.patch(f"{BASE}/deployments/{namespace}/{name}/replicas", params={"replicas": replicas}, headers=HEADERS, timeout=10).json()

def scale_down_deployment(namespace: str, name: str, replicas: int):
    """Decrease replica count of a K8s deployment. replicas must be less than current. Medium risk."""
    return requests.patch(f"{BASE}/deployments/{namespace}/{name}/replicas", params={"replicas": replicas}, headers=HEADERS, timeout=10).json()

def restart_vm(vmid: int):
    """Reboot a VM. Causes a brief service interruption. Medium risk."""
    return requests.post(f"{BASE}/vms/{vmid}/restart", headers=HEADERS, timeout=30).json()

def restart_lxc(vmid: int):
    """Reboot an LXC container. Medium risk. Causes brief service interruption."""
    return requests.post(f"{BASE}/lxc/{vmid}/restart", headers=HEADERS, timeout=30).json()

def migrate_vm(vmid: int, target_node: str):
    """Migrate a VM from its current node to target_node for load balancing. Medium risk."""
    return requests.post(f"{BASE}/vms/{vmid}/migrate", json={"target_node": target_node}, headers=HEADERS, timeout=60).json()

# ─────────────────────────────────────────────────────────────
#  HIGH RISK TOOLS — Always deferred to email approval queue
# ─────────────────────────────────────────────────────────────

def stop_vm(vmid: int):
    """Gracefully shut down a running VM. HIGH RISK — irreversible without manual restart."""
    return {"status": "queued", "tool": "stop_vm", "vmid": vmid}

def stop_lxc(vmid: int):
    """Gracefully stop a running LXC container. HIGH RISK."""
    return {"status": "queued", "tool": "stop_lxc", "vmid": vmid}

def delete_vm(vmid: int):
    """Permanently delete a VM and all its data. IRREVERSIBLE. HIGH RISK."""
    return {"status": "queued", "tool": "delete_vm", "vmid": vmid}

def delete_lxc(vmid: int):
    """Permanently delete an LXC container. IRREVERSIBLE. HIGH RISK."""
    return {"status": "queued", "tool": "delete_lxc", "vmid": vmid}

def delete_deployment(namespace: str, name: str):
    """Delete a Kubernetes deployment entirely. All pods will be terminated. HIGH RISK."""
    return {"status": "queued", "tool": "delete_deployment", "namespace": namespace, "name": name}

def drain_node(node_name: str):
    """Mark a Proxmox node as needing maintenance: logs a drain recommendation. HIGH RISK."""
    return {"status": "queued", "tool": "drain_node", "node": node_name}

# ─────────────────────────────────────────────────────────────
#  TOOL LISTS & CLASSIFICATIONS
# ─────────────────────────────────────────────────────────────

READ_TOOLS = [
    get_health, list_nodes, get_node, list_vms, get_vm, list_lxc, get_lxc,
    list_deployments, get_deployment, get_warning_queue, get_history_stats,
    get_xgboost_prediction, get_action_log, get_vm_action_history,
]

WRITE_TOOLS = [
    scale_vm, scale_down_vm, clone_vm, clone_lxc,
    scale_up_deployment, scale_down_deployment, restart_vm, restart_lxc, migrate_vm,
]

HIGH_RISK_TOOLS = [
    stop_vm, stop_lxc, delete_vm, delete_lxc, delete_deployment, drain_node,
]

ALL_TOOLS = READ_TOOLS + WRITE_TOOLS + HIGH_RISK_TOOLS

RISK_LEVELS = {
    "scale_vm":              "low",
    "scale_down_vm":         "medium",
    "clone_vm":              "medium",
    "clone_lxc":             "medium",
    "scale_up_deployment":   "low",
    "scale_down_deployment": "medium",
    "restart_vm":            "medium",
    "restart_lxc":           "medium",
    "migrate_vm":            "medium",
    "stop_vm":               "high",
    "stop_lxc":              "high",
    "delete_vm":             "high",
    "delete_lxc":            "high",
    "delete_deployment":     "high",
    "drain_node":            "high",
}

def get_risk_level(tool_name: str) -> str:
    return RISK_LEVELS.get(tool_name, "high")
