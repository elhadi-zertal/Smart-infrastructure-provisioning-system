# FastAPI app on port 8002

```python
from fastapi import FastAPI, BackgroundTasks
from pydantic import BaseModel
from contextlib import asynccontextmanager
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import requests
import uuid
import time
import asyncio
import os
import httpx
from agents import run_planner, run_critic
from tools import get_risk_level
from email_client import (
    send_approval_request,
    send_action_executed,
    send_action_rejected_by_critic
)

# ─────────────────────────────────────────────────────────────
#  Configuration
# ─────────────────────────────────────────────────────────────
BASE               = "http://localhost:8000"
POLL_INTERVAL_SEC  = int(os.getenv("POLL_INTERVAL_SEC",  "60"))
APPROVAL_TTL_SEC   = int(os.getenv("APPROVAL_TTL_SEC",   "86400"))  # 24 hours
NOTIFY_LOW_RISK    = os.getenv("NOTIFY_LOW_RISK", "false").lower() == "true"

# ─────────────────────────────────────────────────────────────
#  In-memory stores
# ─────────────────────────────────────────────────────────────

# High-risk actions waiting for admin approval
# Key: action_id, Value: {action, critique, created_at}
approval_queue: dict[str, dict] = {}

# Executed action log (last 100)
executed_log: list[dict] = []

scheduler = AsyncIOScheduler()


# ─────────────────────────────────────────────────────────────
#  Core execution logic
# ─────────────────────────────────────────────────────────────

def execute_action(tool: str, inputs: dict) -> dict:
    """Execute a write action against the Alert Manager API."""
    if tool == "scale_vm":
        body = {k: v for k, v in inputs.items() if k != "vmid" and v is not None}
        r = requests.put(f"{BASE}/vms/{inputs['vmid']}/resources", json=body, timeout=10)
    elif tool == "clone_vm":
        r = requests.post(f"{BASE}/vms/{inputs['vmid']}/clone", timeout=30)
    elif tool == "scale_deployment":
        r = requests.patch(
            f"{BASE}/deployments/{inputs['namespace']}/{inputs['name']}/replicas",
            params={"replicas": inputs["replicas"]},
            timeout=10
        )
    else:
        return {"status": "error", "detail": f"Unknown tool: {tool}"}

    return r.json()


def log_execution(tool: str, inputs: dict, result: dict, triggered_by: str = "autonomous"):
    """Add an entry to the in-memory execution log."""
    executed_log.insert(0, {
        "timestamp":    time.time(),
        "tool":         tool,
        "inputs":       inputs,
        "result":       result,
        "triggered_by": triggered_by
    })
    if len(executed_log) > 100:
        executed_log.pop()


def process_action(proposed_action: dict, critique: dict, cluster_snapshot: dict):
    """
    Main decision point after Critic approves.
    Low/medium risk → execute immediately.
    High risk → email admin + queue.
    """
    tool    = proposed_action.get("tool")
    inputs  = proposed_action.get("inputs", {})
    risk_order = {"low": 0, "medium": 1, "high": 2}

    static_risk  = get_risk_level(tool)                            # authoritative floor
    planner_risk = proposed_action.get("risk", static_risk).lower().strip()
    critic_risk  = critique.get("risk", planner_risk).lower().strip()

    risk = max(
        [static_risk, planner_risk, critic_risk],
        key=lambda r: risk_order.get(r, 2)
    )

    if critique.get("verdict", "REJECT") != "APPROVE":
        print(f"[agent] process_action called with non-APPROVE verdict — aborting.")
        return
    

    if risk in ("low", "medium"):
        # Auto-execute
        result = execute_action(tool, inputs)
        log_execution(tool, inputs, result, triggered_by="autonomous")

        # Notify admin for medium risk (optional for low risk)
        if risk == "medium" or NOTIFY_LOW_RISK:
            send_action_executed(tool, inputs, result)

        print(f"[agent] Auto-executed {tool} (risk={risk}): {result}")

    else:
        # High risk — queue for admin approval
        action_id = str(uuid.uuid4())[:8]
        approval_queue[action_id] = {
            "proposed_action": proposed_action,
            "critique":        critique,
            "created_at":      time.time()
        }

        cluster_summary = {
            "nodes_online":  len([n for n in cluster_snapshot.get("nodes", {}).values()
                                  if n.get("status") == "online"]),
            "vms_running":   len([v for v in cluster_snapshot.get("vms", {}).values()
                                  if v.get("status") == "running"]),
            "warning_queue": len(cluster_snapshot.get("warning_queue", [])),
            "bottleneck":    cluster_snapshot.get("prediction", {}).get("bottleneck", "unknown")
        }

        send_approval_request(action_id, proposed_action, critique, cluster_summary)
        print(f"[agent] High-risk action queued (id={action_id}): {tool}")


# ─────────────────────────────────────────────────────────────
#  Autonomous polling loop
#  Runs every POLL_INTERVAL_SEC seconds without admin input.
# ─────────────────────────────────────────────────────────────

async def autonomous_poll():
    """
    Main autonomous loop. Asks the Planner to assess the cluster,
    runs the Critic, and acts based on risk level.
    """
    try:
        print("[agent] Autonomous poll starting...")

        # Get current cluster state
        async with httpx.AsyncClient(timeout=5) as client:
            r = await client.get(f"{BASE}/cluster-snapshot")
            snapshot = r.json()


        # Ask Planner to assess the cluster
        question = (
            "Assess the current cluster state. Check all nodes, running VMs, "
            "Kubernetes deployments, the warning queue, and the XGBoost prediction. "
            "If any resource is under pressure or trending toward saturation, "
            "propose the appropriate action."
        )
        result          = run_planner(question)
        proposed_action = result.get("proposed_action")

        if proposed_action is None:
            print("[agent] Poll complete — no action needed.")
            return

        # Run Critic
        critique = run_critic(proposed_action, snapshot)

        if critique["verdict"] == "REJECT":
            print(f"[agent] Critic rejected: {critique['reason']}")
            # Send info email for rejected actions (optional — controlled by config)
            if os.getenv("NOTIFY_REJECTIONS", "false").lower() == "true":
                send_action_rejected_by_critic(
                    proposed_action.get("tool"),
                    proposed_action.get("inputs"),
                    critique["reason"],
                    critique.get("alternative")
                )
            return

        # Critic approved — execute or queue based on risk
        process_action(proposed_action, critique, snapshot)

    except Exception as e:
        print(f"[agent] Poll error: {e}")


async def expire_old_approvals():
    """
    Remove approval requests that have exceeded the TTL (default 24h).
    Expired high-risk actions are automatically rejected.
    """
    now = time.time()
    expired = [
        aid for aid, entry in approval_queue.items()
        if now - entry["created_at"] > APPROVAL_TTL_SEC
    ]
    for aid in expired:
        entry = approval_queue.pop(aid)
        print(f"[agent] Approval {aid} expired and auto-rejected")
        log_execution(
            entry["proposed_action"]["tool"],
            entry["proposed_action"]["inputs"],
            {"status": "expired", "reason": "admin did not respond within TTL"},
            triggered_by="expired"
        )


# ─────────────────────────────────────────────────────────────
#  FastAPI app
# ─────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app):
    scheduler.add_job(autonomous_poll,      "interval", seconds=POLL_INTERVAL_SEC, max_instances=1)
    scheduler.add_job(expire_old_approvals, "interval", seconds=3600)
    scheduler.start()
    print(f"[agent] Autonomous polling started — interval: {POLL_INTERVAL_SEC}s")
    yield
    scheduler.shutdown()

app = FastAPI(
    title="Agent Service",
    description="Autonomous cluster management with risk-based escalation",
    version="2.0",
    lifespan=lifespan
)


# ─────────────────────────────────────────────────────────────
#  Admin-facing endpoints
# ─────────────────────────────────────────────────────────────

class Query(BaseModel):
    question: str


@app.post("/ask")
async def ask(query: Query):
    """
    Admin asks a natural language question.
    Can be used anytime for diagnosis — independent of the autonomous loop.
    If the Planner proposes a low/medium action → executes immediately.
    If high risk → queues and sends email.
    """
    async with httpx.AsyncClient(timeout=5) as client:
        r = await client.get(f"{BASE}/cluster-snapshot")
        snapshot = r.json()
    result          = run_planner(query.question)
    proposed_action = result.get("proposed_action")

    if proposed_action is None:
        return {"answer": result["answer"]}

    critique = run_critic(proposed_action, snapshot)

    if critique["verdict"] == "REJECT":
        return {
            "answer": result["answer"],
            "action": {
                "status":      "rejected_by_critic",
                "reason":      critique["reason"],
                "alternative": critique.get("alternative")
            }
        }

    risk = critique.get("risk", "high")

    if risk in ("low", "medium"):
        exec_result = execute_action(proposed_action["tool"], proposed_action["inputs"])
        log_execution(proposed_action["tool"], proposed_action["inputs"], exec_result, triggered_by="admin_ask")
        return {
            "answer": result["answer"],
            "action": {
                "status":  "executed",
                "tool":    proposed_action["tool"],
                "inputs":  proposed_action["inputs"],
                "result":  exec_result,
                "risk":    risk
            }
        }
    else:
        # High risk — queue same as autonomous loop
        action_id = str(uuid.uuid4())[:8]
        approval_queue[action_id] = {
            "proposed_action": proposed_action,
            "critique":        critique,
            "created_at":      time.time()
        }
        cluster_summary = {
            "nodes_online": len(snapshot.get("nodes", {})),
            "bottleneck":   snapshot.get("prediction", {}).get("bottleneck", "unknown")
        }
        send_approval_request(action_id, proposed_action, critique, cluster_summary)
        return {
            "answer": result["answer"],
            "action": {
                "status":    "queued_high_risk",
                "action_id": action_id,
                "tool":      proposed_action["tool"],
                "inputs":    proposed_action["inputs"],
                "message":   "Email sent to admin for approval"
            }
        }


@app.get("/approve/{action_id}")
async def approve_action(action_id: str):
    """
    Admin clicks the approve link in the email.
    Executes the queued high-risk action immediately.
    """
    entry = approval_queue.pop(action_id, None)
    if not entry:
        return {"status": "error", "detail": "action_id not found or already processed"}

    action = entry["proposed_action"]
    result = execute_action(action["tool"], action["inputs"])
    log_execution(action["tool"], action["inputs"], result, triggered_by="admin_email_approval")

    return {
        "status":  "executed",
        "tool":    action["tool"],
        "inputs":  action["inputs"],
        "result":  result
    }


@app.get("/reject/{action_id}")
async def reject_action(action_id: str):
    """
    Admin clicks the reject link in the email.
    Removes the action from the queue without executing it.
    """
    entry = approval_queue.pop(action_id, None)
    if not entry:
        return {"status": "error", "detail": "action_id not found or already processed"}

    action = entry["proposed_action"]
    log_execution(
        action["tool"], action["inputs"],
        {"status": "rejected_by_admin"},
        triggered_by="admin_email_rejection"
    )
    return {"status": "rejected", "tool": action["tool"]}


@app.get("/approval-queue")
def get_approval_queue():
    """Returns all high-risk actions waiting for admin approval."""
    return {
        "count": len(approval_queue),
        "actions": {
            aid: {
                "tool":       e["proposed_action"].get("tool"),
                "inputs":     e["proposed_action"].get("inputs"),
                "risk":       e["critique"].get("risk"),
                "reason":     e["proposed_action"].get("reason"),
                "queued_at":  e["created_at"],
                "expires_in": max(0, APPROVAL_TTL_SEC - (time.time() - e["created_at"]))
            }
            for aid, e in approval_queue.items()
        }
    }


@app.get("/action-log")
def get_executed_log():
    """Returns the last 100 actions executed by the system."""
    return executed_log


@app.post("/poll/trigger")
async def trigger_poll(background_tasks: BackgroundTasks):
    """Manually trigger one autonomous poll cycle. Useful for testing."""
    background_tasks.add_task(autonomous_poll)
    return {"status": "poll triggered"}


@app.get("/health")
def health():
    return {
        "status":              "ok",
        "poll_interval_sec":   POLL_INTERVAL_SEC,
        "approval_queue_size": len(approval_queue),
        "executed_log_size":   len(executed_log)
    }
```
