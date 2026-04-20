import requests
import sqlite3
import json
import os

AGENT_DB_PATH = os.getenv("AGENT_DB_PATH", "./agent_state.db")

BASE = "http://localhost:8000"   # Alert Manager API
AGENT_BASE = "http://localhost:8002"  # Agent Service API 

# ─────────────────────────────────────────────────────────────
#  READ TOOLS — Planner calls these during reasoning.
#
#  CRITICAL: docstrings are mandatory.
#  Agno sends the docstring to the LLM as the tool description.
#  Missing or vague docstring = LLM does not know when to call it.
# ─────────────────────────────────────────────────────────────

def get_health():
    """Returns cluster overview: node count, VM count, LXC count, warning queue length."""
    return requests.get(f"{BASE}/health").json()

def list_nodes():
    """Returns all physical Proxmox nodes with CPU utilisation fraction, RAM used/total, energy used/capacity."""
    return requests.get(f"{BASE}/nodes").json()

def get_node(name: str):
    """Returns detailed CPU, RAM, IO, energy stats for a single Proxmox node. Use node names like pfe0, pfe1, pfe2."""
    return requests.get(f"{BASE}/nodes/{name}").json()

def list_vms():
    """Returns all VMs across the cluster with status (running/stopped), CPU cores, RAM allocation, and which node they run on."""
    return requests.get(f"{BASE}/vms").json()

def get_vm(vmid: int):
    """Returns detailed stats for a single VM: node placement, CPU cores, RAM, disk, current CPU and RAM utilisation."""
    return requests.get(f"{BASE}/vms/{vmid}").json()

def list_deployments():
    """Returns all Kubernetes deployments with current replica count, ready replicas, CPU and memory limits."""
    return requests.get(f"{BASE}/deployments").json()

def get_deployment(namespace: str, name: str):
    """Returns detailed info for a single Kubernetes deployment including replica status and resource limits."""
    return requests.get(f"{BASE}/deployments/{namespace}/{name}").json()

def get_warning_queue():
    """Returns VMs and Kubernetes deployments currently queued for automated provisioning by DE-WOA algorithm."""
    return requests.get(f"{BASE}/warning-queue").json()

def get_history_stats():
    """Returns mean and standard deviation of resource gap observations (CPU, RAM, IO, energy) from historical data."""
    return requests.get(f"{BASE}/delta-history/stats").json()

def get_xgboost_prediction():
    """Returns the current cluster bottleneck class (CPU/RAM/IO/Energy/None), active fitness weight vector, confidence score, and feature importances."""
    return requests.get(f"{BASE}/prediction").json()

def get_action_log():
    """Returns the last 100 automated actions taken by the system. Always check this before proposing an action to avoid duplicates."""
    return requests.get(f"{AGENT_BASE}/action-log").json()

def list_lxc():
    """Returns all LXC containers across the cluster with status, CPU cores, RAM allocation, and host node."""
    return requests.get(f"{BASE}/lxc").json()

def get_lxc(vmid: int):
    """Returns detailed stats for a single LXC container: node placement, CPU, RAM, disk, current utilisation."""
    return requests.get(f"{BASE}/lxc/{vmid}").json()


def get_vm_action_history(vmid: int, limit: int = 20):
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
#  WRITE TOOLS — executed autonomously for low/medium risk.
#  High risk actions go through email approval queue instead.
#  they are reference definitions for documentation
# ─────────────────────────────────────────────────────────────

def scale_vm(vmid: int, cores: int = None, memory: int = None):
    """Add vCPUs or RAM to a running VM. cores = new total core count. memory = new RAM in MB. Low risk — executes immediately after Critic approval."""
    body = {k: v for k, v in {"cores": cores, "memory": memory}.items() if v is not None}
    return requests.put(f"{BASE}/vms/{vmid}/resources", json=body, timeout=10).json()

def scale_down_vm(vmid: int, cores: int = None, memory: int = None):
    """Reduce vCPUs or RAM of a VM. cores = new LOWER total core count. memory = new LOWER RAM in MB.
    WARNING: Reducing RAM on a running VM may cause OOM kills. Medium risk — executes after Critic approval.
    Only propose if current utilisation is consistently below 40%."""
    body = {k: v for k, v in {"cores": cores, "memory": memory}.items() if v is not None}
    return requests.put(f"{BASE}/vms/{vmid}/resources", json=body, timeout=10).json()

def clone_vm(vmid: int):
    """Clone a VM to create a second instance for horizontal scaling. Medium risk — executes immediately after Critic approval."""
    return requests.post(f"{BASE}/vms/{vmid}/clone", timeout=30).json()

def clone_lxc(vmid: int):
    """Clone an LXC container to create a second instance for horizontal scaling. Medium risk — executes immediately after Critic approval."""
    return requests.post(f"{BASE}/lxc/{vmid}/clone", timeout=30).json()

def scale_up_deployment(namespace: str, name: str, replicas: int):
    """Increase replica count of a K8s deployment. replicas must be greater than current.
    Low risk — executes immediately after Critic approval."""
    return requests.patch(
        f"{BASE}/deployments/{namespace}/{name}/replicas",
        params={"replicas": replicas}, timeout=10
    ).json()

def scale_down_deployment(namespace: str, name: str, replicas: int):
    """Decrease replica count of a K8s deployment. replicas must be less than current.
    Medium risk — can cause dropped requests if traffic is high.
    Only propose if current replica utilisation is consistently below 30%."""
    return requests.patch(
        f"{BASE}/deployments/{namespace}/{name}/replicas",
        params={"replicas": replicas}, timeout=10
    ).json()

def stop_vm(vmid: int):
    """Gracefully shut down a running VM. HIGH RISK — irreversible without manual restart.
    Only propose when the VM is confirmed problematic and admin approval is expected."""
    return {"status": "queued", "tool": "stop_vm", "vmid": vmid}

def restart_vm(vmid: int):
    """Reboot a VM. Causes a brief service interruption. Medium risk.
    Use when the VM is unresponsive but not worth deleting."""
    return requests.post(f"{BASE}/vms/{vmid}/restart", timeout=30).json()

def stop_lxc(vmid: int):
    """Gracefully stop a running LXC container. HIGH RISK."""
    return {"status": "queued", "tool": "stop_lxc", "vmid": vmid}

def restart_lxc(vmid: int):
    """Reboot an LXC container. Medium risk. Causes brief service interruption."""
    return requests.post(f"{BASE}/lxc/{vmid}/restart", timeout=30).json()

def delete_vm(vmid: int):
    """Permanently delete a VM and all its data. IRREVERSIBLE. HIGH RISK.
    Always classify as high risk. Admin email approval required before execution.
    Include rollback plan (e.g. restore from Proxmox snapshot) in your action JSON."""
    # This function is defined for the Planner to propose — execution goes through admin approval
    return {"status": "queued", "tool": "delete_vm", "vmid": vmid}

def delete_lxc(vmid: int):
    """Permanently delete an LXC container. IRREVERSIBLE. HIGH RISK. Admin approval required."""
    return {"status": "queued", "tool": "delete_lxc", "vmid": vmid}

def delete_deployment(namespace: str, name: str):
    """Delete a Kubernetes deployment entirely. All pods will be terminated. HIGH RISK. Admin approval required."""
    return {"status": "queued", "tool": "delete_deployment", "namespace": namespace, "name": name}

def migrate_vm(vmid: int, target_node: str):
    """Migrate a VM from its current node to target_node for load balancing.
    Use when a node is overloaded and vertical scaling is not possible.
    Medium risk — causes a brief pause during live migration.
    Always call list_nodes() first to verify target_node has sufficient headroom."""
    return requests.post(f"{BASE}/vms/{vmid}/migrate",
                         json={"target_node": target_node}, timeout=60).json()


def drain_node(node_name: str):
    """Mark a Proxmox node as needing maintenance: logs a drain recommendation.
    HIGH RISK — after draining, all VMs must be migrated manually.
    Only propose if the node has hardware failure symptoms or needs firmware updates.
    Admin approval required."""
    return {"status": "queued", "tool": "drain_node", "node": node_name}

# ─────────────────────────────────────────────────────────────
#  TOOL LISTS
# ─────────────────────────────────────────────────────────────

READ_TOOLS = [
    get_health, list_nodes, get_node,
    list_vms, get_vm,
    list_lxc, get_lxc,                      # ← ADDED
    list_deployments, get_deployment,
    get_warning_queue, get_history_stats,
    get_xgboost_prediction, get_action_log, get_vm_action_history,
]


WRITE_TOOLS = [
    scale_vm, scale_down_vm,                 # ← scale_down_vm ADDED
    clone_vm, clone_lxc,                     # ← clone_lxc ADDED
    scale_up_deployment, scale_down_deployment,  # ← replaced scale_deployment
    restart_vm, restart_lxc,                 # ← ADDED
    migrate_vm,                              # ← ADDED
]

# Planner can propose these — they NEVER auto-execute, always go to email queue
HIGH_RISK_TOOLS = [
    stop_vm, stop_lxc,                       # ← ADDED
    delete_vm, delete_lxc,                   # ← ADDED
    delete_deployment,                       # ← ADDED
    drain_node,                              # ← ADDED
]

ALL_TOOLS = READ_TOOLS + WRITE_TOOLS + HIGH_RISK_TOOLS


# ─────────────────────────────────────────────────────────────
#  RISK CLASSIFICATION
#  Used by agent_service.py to decide auto-execute vs email.
# ─────────────────────────────────────────────────────────────

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
    return RISK_LEVELS.get(tool_name, "high")  # default to high if unknown

