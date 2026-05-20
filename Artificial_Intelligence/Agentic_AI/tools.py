import requests
import sqlite3
import json
import os

AGENT_DB_PATH = os.getenv("AGENT_DB_PATH", "./agent_memory.db")
BASE          = os.getenv("ALERT_MANAGER_BASE", "http://localhost:8000")


# ─────────────────────────────────────────────────────────────
#  READ TOOLS
#  These are the only tools available to the Planner agent.
#  The Planner is a Q&A assistant — it never writes anything.
#  All cluster actions are owned exclusively by main.py.
# ─────────────────────────────────────────────────────────────

def get_health() -> dict:
    """Returns cluster overview: node count, VM count, LXC count, warning queue length."""
    return requests.get(f"{BASE}/health").json()

def list_nodes() -> list:
    """Returns all physical Proxmox nodes with CPU utilisation fraction, RAM used/total, energy used/capacity."""
    return requests.get(f"{BASE}/nodes").json()

def get_node(name: str) -> dict:
    """Returns detailed CPU, RAM, IO, energy stats for a single Proxmox node. Use names like pfe0–pfe7."""
    return requests.get(f"{BASE}/nodes/{name}").json()

def list_vms() -> list:
    """Returns all VMs across the cluster with status, CPU cores, RAM allocation, and host node."""
    return requests.get(f"{BASE}/vms").json()

def get_vm(vmid: int) -> dict:
    """Returns detailed stats for a single VM: node placement, CPU cores, RAM, disk, current utilisation."""
    return requests.get(f"{BASE}/vms/{vmid}").json()

def list_lxc() -> list:
    """Returns all LXC containers across the cluster with status, CPU cores, RAM allocation, and host node."""
    return requests.get(f"{BASE}/lxc").json()

def get_lxc(vmid: int) -> dict:
    """Returns detailed stats for a single LXC container: node placement, CPU, RAM, disk, current utilisation."""
    return requests.get(f"{BASE}/lxc/{vmid}").json()

def list_deployments() -> list:
    """Returns all Kubernetes deployments with current replica count, ready replicas, CPU and memory limits."""
    return requests.get(f"{BASE}/deployments").json()

def get_deployment(namespace: str, name: str) -> dict:
    """Returns detailed info for a single Kubernetes deployment including replica status and resource limits."""
    return requests.get(f"{BASE}/deployments/{namespace}/{name}").json()

def get_warning_queue() -> dict:
    """Returns VMs and K8s deployments currently queued for automated provisioning by the DE-WOA algorithm."""
    return requests.get(f"{BASE}/warning-queue").json()

def get_history_stats() -> dict:
    """Returns mean and standard deviation of resource gap observations (CPU, RAM, IO, energy) from historical data."""
    return requests.get(f"{BASE}/delta-history/stats").json()

def get_xgboost_prediction() -> dict:
    """Returns the current cluster bottleneck class (CPU/RAM/IO/Energy/None), active fitness weights, confidence score, and feature importances."""
    return requests.get(f"{BASE}/predict-config").json()

def get_approval_queue() -> list:
    """Returns all high-risk actions currently pending admin email approval, with their age and expiry time."""
    return requests.get(f"{BASE}/approval-queue").json()

def get_action_log() -> list:
    """Returns the last 100 automated actions executed by the cluster management system, including outcomes."""
    return requests.get(f"{BASE}/action-log").json()

def get_vm_action_history(vmid: int, limit: int = 20) -> list:
    """Returns the last N actions taken on a specific VM from the persisted action log."""
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
    except sqlite3.OperationalError:
        rows = []
    finally:
        con.close()
    return [
        {"timestamp": r[0], "tool": r[1], "inputs": json.loads(r[2]),
         "result": json.loads(r[3]), "outcome": r[4], "triggered_by": r[5]}
        for r in rows
    ]


# ─────────────────────────────────────────────────────────────
#  TOOL LIST
# ─────────────────────────────────────────────────────────────

READ_TOOLS = [
    get_health,
    list_nodes, get_node,
    list_vms, get_vm,
    list_lxc, get_lxc,
    list_deployments, get_deployment,
    get_warning_queue,
    get_history_stats,
    get_xgboost_prediction,
    get_approval_queue,
    get_action_log,
    get_vm_action_history,
]
