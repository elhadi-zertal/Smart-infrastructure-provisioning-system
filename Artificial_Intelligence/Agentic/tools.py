# all tool functions



```python
import requests

BASE = "http://localhost:8000"   # Alert Manager API

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
    """Returns the last 50 automated actions taken by the system. Always check this before proposing an action to avoid duplicates."""
    return requests.get(f"{BASE}/action-log").json()


# ─────────────────────────────────────────────────────────────
#  WRITE TOOLS — executed autonomously for low/medium risk.
#  High risk actions go through email approval queue instead.
# ─────────────────────────────────────────────────────────────

def scale_vm(vmid: int, cores: int = None, memory: int = None):
    """Add vCPUs or RAM to a running VM. cores = new total core count. memory = new RAM in MB. Low risk — executes immediately after Critic approval."""
    body = {k: v for k, v in {"cores": cores, "memory": memory}.items() if v is not None}
    return requests.put(f"{BASE}/vms/{vmid}/resources", json=body, timeout=10).json()

def clone_vm(vmid: int):
    """Clone a VM to create a second instance for horizontal scaling. Medium risk — executes immediately after Critic approval."""
    return requests.post(f"{BASE}/vms/{vmid}/clone", timeout=30).json()

def scale_deployment(namespace: str, name: str, replicas: int):
    """Change the replica count of a Kubernetes deployment. Low risk for scale-up, medium for scale-down — executes after Critic approval."""
    return requests.patch(
        f"{BASE}/deployments/{namespace}/{name}/replicas",
        params={"replicas": replicas},
        timeout=10
    ).json()


# ─────────────────────────────────────────────────────────────
#  TOOL LISTS
# ─────────────────────────────────────────────────────────────

READ_TOOLS = [
    get_health, list_nodes, get_node,
    list_vms, get_vm,
    list_deployments, get_deployment,
    get_warning_queue, get_history_stats,
    get_xgboost_prediction, get_action_log
]

# Only low/medium risk write tools are available to the Planner.
# High risk actions are constructed directly by agent_service.py
# based on Planner output — they never auto-execute.
WRITE_TOOLS = [scale_vm, clone_vm, scale_deployment]

ALL_TOOLS = READ_TOOLS + WRITE_TOOLS


# ─────────────────────────────────────────────────────────────
#  RISK CLASSIFICATION
#  Used by agent_service.py to decide auto-execute vs email.
# ─────────────────────────────────────────────────────────────

RISK_LEVELS = {
    "scale_vm":          "low",     # reversible, additive
    "clone_vm":          "medium",  # creates resources, reversible
    "scale_deployment":  "low",     # K8s handles it gracefully
    # High risk actions defined as string names only —
    # they cannot be called as tools (no auto-execution path)
    "delete_vm":         "high",
    "delete_deployment": "high",
    "stop_vm":           "high",
}

def get_risk_level(tool_name: str) -> str:
    return RISK_LEVELS.get(tool_name, "high")  # default to high if unknown
```
