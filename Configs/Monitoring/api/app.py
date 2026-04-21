import json
import os
import threading
from typing import Optional

from fastapi import FastAPI, HTTPException, Query, Header, Depends
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv()

CLUSTER_API_KEY = os.getenv("CLUSTER_API_KEY", "dev-secure-key-123")
DB_FILE = os.path.join(os.path.dirname(__file__), "targets.json")

# Database concurrency lock to prevent data corruption during simultaneous registrations
_db_lock = threading.Lock()

app = FastAPI(
    title="Prometheus Service Discovery API",
    description="Manages dynamic monitoring targets for Prometheus HTTP SD.",
    version="1.1.0"
)

# ─────────────────────────────────────────────────────────────
#  Security & DB Helpers
# ─────────────────────────────────────────────────────────────

def verify_api_key(x_api_key: str = Header(...)):
    """Enforces perimeter security for mutative API actions."""
    if x_api_key != CLUSTER_API_KEY:
        raise HTTPException(status_code=403, detail="Invalid or missing API Key")

def load_db() -> list:
    with _db_lock:
        if not os.path.exists(DB_FILE):
            return []
        with open(DB_FILE, "r") as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                return []

def save_db(data: list):
    with _db_lock:
        with open(DB_FILE, "w") as f:
            json.dump(data, f, indent=2)

def init_db():
    with _db_lock:
        if not os.path.exists(DB_FILE):
            with open(DB_FILE, "w") as f:
                json.dump([], f, indent=2)

init_db()

# ─────────────────────────────────────────────────────────────
#  Schemas & Defaults
# ─────────────────────────────────────────────────────────────

class VMEntry(BaseModel):
    ip:       str
    port:     int = 9100
    env:      str = "prod"
    job:      str = "node_exporter"
    type:     str = "vm"
    hostname: str = ""

DEFAULT_PORTS = {
    "node_exporter":     9100,
    "scaphandre":        8080,
    "snmp_exporter":     9116,
    "blackbox_exporter": 9115,
}

DEFAULT_TYPES = {
    "node_exporter":     "vm",
    "scaphandre":        "pm",
    "snmp_exporter":     "network",
    "blackbox_exporter": "probe",
}

def fetch_targets(job: str):
    """Formats targets exactly as Prometheus HTTP SD expects."""
    return [
        {
            "targets": [f"{t['ip']}:{t['port']}"],
            "labels": {
                "job":      t["job"],
                "env":      t["env"],
                "type":     t.get("type", DEFAULT_TYPES.get(job, "unknown")),
                "hostname": t.get("hostname", t["ip"]),
                "instance": t["ip"],
            }
        }
        for t in load_db() if t["job"] == job
    ]

# ─────────────────────────────────────────────────────────────
#  READ Endpoints (Open for Prometheus Scraper)
# ─────────────────────────────────────────────────────────────

@app.get("/targets")
def get_targets(job: Optional[str] = Query(default=None)):
    targets = load_db()
    if job:
        targets = [t for t in targets if t["job"] == job]
    return [
        {
            "targets": [f"{t['ip']}:{t['port']}"],
            "labels": {
                "job":      t["job"],
                "env":      t["env"],
                "type":     t.get("type", "unknown"),
                "hostname": t.get("hostname", t["ip"]),
                "instance": t["ip"],
            }
        }
        for t in targets
    ]

@app.get("/targets/node_exporter")
def targets_node_exporter():
    return fetch_targets("node_exporter")

@app.get("/targets/scaphandre")
def targets_scaphandre():
    return fetch_targets("scaphandre")

@app.get("/targets/snmp_exporter")
def targets_snmp():
    return fetch_targets("snmp_exporter")

@app.get("/targets/blackbox_exporter")
def targets_blackbox():
    return fetch_targets("blackbox_exporter")

@app.get("/list")
def list_targets():
    return load_db()

@app.get("/list/{job}")
def list_by_job(job: str):
    targets = [t for t in load_db() if t["job"] == job]
    if not targets:
        raise HTTPException(status_code=404, detail=f"No targets found for job '{job}'")
    return targets

@app.get("/health")
def health():
    return {"status": "ok", "total_targets": len(load_db())}

# ─────────────────────────────────────────────────────────────
#  WRITE Endpoints (Protected by API Key)
# ─────────────────────────────────────────────────────────────

@app.post("/register", status_code=201, dependencies=[Depends(verify_api_key)])
def register(vm: VMEntry):
    if vm.port == 9100 and vm.job in DEFAULT_PORTS:
        vm.port = DEFAULT_PORTS[vm.job]
    if vm.type == "vm" and vm.job in DEFAULT_TYPES:
        vm.type = DEFAULT_TYPES[vm.job]

    targets = load_db()

    # Idempotent registration
    for t in targets:
        if t["ip"] == vm.ip and t["job"] == vm.job:
            return {"message": "already registered", "ip": vm.ip, "job": vm.job}

    targets.append({
        "ip":       vm.ip,
        "port":     vm.port,
        "env":      vm.env,
        "job":      vm.job,
        "type":     vm.type,
        "hostname": vm.hostname or vm.ip,
    })

    save_db(targets)
    return {"message": "registered", "ip": vm.ip, "job": vm.job, "type": vm.type, "port": vm.port}

@app.post("/unregister", dependencies=[Depends(verify_api_key)])
def unregister(vm: VMEntry):
    targets = load_db()
    new_targets = [t for t in targets if not (t["ip"] == vm.ip and t["job"] == vm.job)]

    if len(new_targets) == len(targets):
        raise HTTPException(status_code=404, detail="Target not found")

    save_db(new_targets)
    return {"message": "removed", "ip": vm.ip, "job": vm.job}

@app.delete("/clear", dependencies=[Depends(verify_api_key)])
def clear_targets():
    save_db([])
    return {"message": "all targets cleared"}

@app.delete("/clear/{job}", dependencies=[Depends(verify_api_key)])
def clear_by_job(job: str):
    targets = load_db()
    new_targets = [t for t in targets if t["job"] != job]
    count = len(targets) - len(new_targets)
    save_db(new_targets)
    return {"message": f"cleared {count} targets for job '{job}'"}
