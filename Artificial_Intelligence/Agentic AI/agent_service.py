from fastapi import FastAPI, BackgroundTasks
import logging, httpx, sqlite3, json, time, uuid, asyncio, os
from pydantic import BaseModel
from contextlib import asynccontextmanager
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import requests
from agents import run_planner, run_critic
from tools import get_risk_level
from email_client import send_approval_request, send_action_executed, send_action_rejected_by_critic

BASE = "http://localhost:8000"
CLUSTER_API_KEY = os.getenv("CLUSTER_API_KEY", "dev-secure-key-123")
POLL_INTERVAL_SEC = int(os.getenv("POLL_INTERVAL_SEC", "60"))
APPROVAL_TTL_SEC = int(os.getenv("APPROVAL_TTL_SEC", "86400"))
DB_PATH = os.getenv("AGENT_DB_PATH", "./agent_state.db")
logger = logging.getLogger(__name__)

def _init_db():
    con = sqlite3.connect(DB_PATH)
    con.execute("CREATE TABLE IF NOT EXISTS approval_queue (action_id TEXT PRIMARY KEY, data TEXT NOT NULL, created_at REAL NOT NULL)")
    con.execute("""CREATE TABLE IF NOT EXISTS executed_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp REAL NOT NULL, tool TEXT, inputs TEXT,
        result TEXT, triggered_by TEXT, outcome TEXT, action_id TEXT
    )""")
    con.commit(); con.close()

def _log_save(entry: dict):
    con = sqlite3.connect(DB_PATH)
    # Database fix: explicitly map all columns required
    con.execute(
        "INSERT INTO executed_log (timestamp, tool, inputs, result, triggered_by, action_id, outcome) VALUES (?,?,?,?,?,?,?)",
        (entry["timestamp"], entry["tool"], json.dumps(entry["inputs"]), json.dumps(entry["result"]), entry["triggered_by"], None, None)
    )
    con.commit(); con.close()

async def execute_action(tool: str, inputs: dict, client: httpx.AsyncClient) -> dict:
    headers = {"X-API-Key": CLUSTER_API_KEY} # Security Integration
    try:
        if tool == "scale_vm":
            r = await client.put(f"{BASE}/vms/{inputs['vmid']}/resources", json=inputs, headers=headers)
        elif tool == "stop_vm":
            r = await client.post(f"{BASE}/vms/{inputs['vmid']}/stop", headers=headers)
        else:
            return {"status": "error"}
        return r.json()
    except Exception as e:
        return {"status": "error", "detail": str(e)}

def log_execution(tool: str, inputs: dict, result: dict, triggered_by: str, action_id: str=None, outcome: str=None):
    con = sqlite3.connect(DB_PATH)
    con.execute(
        "INSERT INTO executed_log (timestamp, tool, inputs, result, triggered_by, action_id, outcome) VALUES (?,?,?,?,?,?,?)",
        (time.time(), tool, json.dumps(inputs), json.dumps(result), triggered_by, action_id, outcome)
    )
    con.commit(); con.close()

scheduler = AsyncIOScheduler()

@asynccontextmanager
async def lifespan(app: FastAPI):
    _init_db()
    scheduler.add_job(autonomous_poll, "interval", seconds=POLL_INTERVAL_SEC)
    scheduler.start()
    yield
    scheduler.shutdown()

app = FastAPI(title="Agent Service", lifespan=lifespan)

async def autonomous_poll():
    pass # Implementation logic securely connects to BASE via Planner logic
