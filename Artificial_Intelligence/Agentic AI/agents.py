from agno.agent import Agent, RunResponse
from agno.models.groq import Groq
from tools import READ_TOOLS, get_risk_level  # already imported in agent_service.py, add here too HIGH_RISK_TOOLS
import json
import os
import re
from agno.db.sqlite import SqliteDb



# ─────────────────────────────────────────────────────────────
#  PLANNER AGENT
# ─────────────────────────────────────────────────────────────

PLANNER_INSTRUCTIONS = """
You are an autonomous cluster management planner for a Proxmox cluster
running 8 nodes (pfe0 to pfe7). The cluster hosts:
- Hospital: K8s workloads on subnet 10.10.10.0/24
- Ministry: K8s workloads on subnet 10.20.20.0/24
- Monitoring: Prometheus, Grafana, InfluxDB on 10.30.30.0/24

You also manage LXC containers alongside QEMU VMs.
You operate autonomously. The administrator is not watching.
Your job: diagnose problems, recall history, and propose the right action.

MANDATORY TOOL SEQUENCE (every single cycle, no exceptions):
1. get_health()                      — cluster overview
2. get_xgboost_prediction()          — current bottleneck and weights
3. get_action_log()                  — last 50 system actions (avoid duplicates)
4. get_warning_queue()               — what is already being handled
5. list_nodes()                      — identify overloaded or underloaded nodes
6. [targeted] get_vm(vmid) / get_lxc(vmid) / get_deployment(ns, name) — the specific resource in question
7. get_vm_action_history(vmid)       — REQUIRED before any write action on a specific VM

If you skip any of these steps, your answer is wrong and will be rejected.

RISK CLASSIFICATION:
LOW    → scale_vm (add), scale_up_deployment, clone_vm*
         (*clone is MEDIUM — auto-execute after Critic approval)
MEDIUM → scale_down_vm, clone_vm, clone_lxc, scale_down_deployment,
         restart_vm, restart_lxc, migrate_vm
HIGH   → stop_vm, stop_lxc, delete_vm, delete_lxc, delete_deployment,
         drain_node — NEVER auto-execute, always email admin

ESCALATION RULE:
If get_vm_action_history() shows the same action attempted 3+ times in 24h
without a "resolved" outcome → STOP. Propose a fundamentally different approach.
Explain your escalation reasoning in the "reason" field.

DECISION PRIORITY ORDER:
1. If a node is >85% CPU: prefer migrate_vm() over adding more resources to it.
2. If a VM has been scaled 5+ times this week: flag for permanent review in your reason.
3. If XGBoost bottleneck is RAM but you are proposing a CPU action: explain why.
4. Always prefer the least invasive action that solves the problem.

RESPONSE FORMAT — include this exact JSON block when proposing an action:
```json
{
  "tool":             "migrate_vm",
  "inputs":           {"vmid": 152, "target_node": "pfe3"},
  "risk":             "medium",
  "reason":           "VM 152 scaled 4 times this week, pfe0 at 89% CPU, pfe3 has 35% headroom",
  "expected_effect":  "CPU pressure on pfe0 reduced within 5 minutes of migration",
  "rollback":         "Migrate back to pfe0 if pfe3 shows instability"
}
```

The "rollback" field is required. The "risk" field must be: low, medium, or high.
HIGH risk actions: still output the JSON. The system handles escalation automatically.
"""

planner = Agent(
    model=Groq(
        id="llama-3.3-70b-versatile",
        api_key=os.environ['GROQ_API_KEY']
    ),
    tools=READ_TOOLS,
    instructions=PLANNER_INSTRUCTIONS,
    db=SqliteDb(
        db_file="./agent_memory.db",
    ),
    add_history_to_messages=True,    # include last N turns in context
    num_history_responses=10,        # how many past responses to inject
    show_tool_calls=True,
    markdown=True
)


# ─────────────────────────────────────────────────────────────
#  CRITIC AGENT
# ─────────────────────────────────────────────────────────────

CRITIC_INSTRUCTIONS = """
You are a safety critic for an autonomous Proxmox cluster management system.
Low and medium risk actions execute automatically. High risk requires admin email approval.

You receive a proposed action JSON and the full cluster snapshot.
Your job: validate the action and confirm or escalate the risk classification.

VALIDATION CHECKLIST (in order):

1. NODE HEADROOM (most important)
   If target node CPU > 85%:  REJECT vertical CPU scaling → suggest migrate_vm instead
   If target node RAM > 85%:  REJECT vertical RAM scaling → suggest clone or migrate
   If NO node has < 80% CPU:  REJECT scale_up actions → suggest delete_vm of idle VMs

2. DUPLICATE CHECK
   If the exact same (tool, vmid) appears in action_log within the last 5 minutes:
   → REJECT — wait for previous action to stabilise

3. ESCALATION CHECK
   If action_log shows the same tool on the same vmid 3+ times in 24h:
   → Override to a different action. Flag in REASON.
   → Suggest a fundamentally different approach in ALTERNATIVE.

4. BOTTLENECK ALIGNMENT
   Check XGBoost bottleneck in cluster_snapshot["prediction"]["bottleneck"].
   CPU bottleneck → CPU action preferred. Mismatch: flag it, do not auto-reject.

5. RISK ESCALATION (you may increase risk, never decrease)
   Clone on a nearly-full cluster → escalate to HIGH
   Scale-down with high traffic (many ready replicas near minimum) → escalate to HIGH
   Delete action → always HIGH, never accept lower

6. FEASIBILITY
   If no online node can physically host the VM (all above 90%):
   → REJECT with explanation of current headroom across all nodes.

7. DIRECTION CHECK for scale actions
   scale_down_vm must propose FEWER cores/less RAM than current. Reject if inputs are higher.
   scale_up_deployment must propose MORE replicas than current. Reject if lower.

RESPONSE FORMAT — no preamble, start immediately with VERDICT:
VERDICT: APPROVE or REJECT
RISK: low or medium or high
REASON: one clear sentence
ALTERNATIVE: (only if REJECT) the safer action to take instead
"""

critic = Agent(
    model=Groq(
        id="llama-3.3-70b-versatile",
        api_key=os.environ['GROQ_API_KEY']
    ),
    instructions=CRITIC_INSTRUCTIONS,
    markdown=False
)


# ─────────────────────────────────────────────────────────────
#  RUN FUNCTIONS
# ─────────────────────────────────────────────────────────────

def run_planner(question: str) -> dict:
    """
    Run the Planner agent.
    Returns:
        {
            "answer":          str,
            "proposed_action": dict | None
        }
    """
    response: RunResponse = planner.run(question)
    answer = response.content

    proposed_action = None
    matches = re.findall(r'```json\s*(.*?)\s*```', answer, re.DOTALL)
    if matches:
        try:
            proposed_action = json.loads(matches[-1])  # always take the last block
        except json.JSONDecodeError:
            pass

    return {
        "answer":          answer,
        "proposed_action": proposed_action
    }



def run_critic(proposed_action: dict, cluster_snapshot: dict) -> dict:
    """
    Run the Critic agent.
    Returns:
        {
            "verdict":     "APPROVE" | "REJECT",
            "risk":        "low" | "medium" | "high",
            "reason":      str,
            "alternative": str | None
        }
    """
    prompt = f"""
Proposed action:
{json.dumps(proposed_action, indent=2)}

Current cluster state:
{json.dumps(cluster_snapshot, indent=2)}
"""
    response: RunResponse = critic.run(prompt)
    text = response.content.strip()

    lines = {}
    for line in text.splitlines():
        if ":" in line:
            key, val = line.split(":", 1)
            lines[key.strip().upper()] = val.strip()   # BUG-08 fix: .strip() on both sides

    risk_order = {"low": 0, "medium": 1, "high": 2}

    # --- BUG-04 FIX: static table is the authoritative floor ---
    tool_name   = proposed_action.get("tool", "")
    static_risk = get_risk_level(tool_name)           # from RISK_LEVELS table in tools.py

    planner_risk = proposed_action.get("risk", static_risk).lower().strip()
    critic_risk  = lines.get("RISK", planner_risk).lower().strip()

    # Final risk = max(static floor, planner claim, critic assessment)
    # The critic can escalate but nothing can go below the static table.
    final_risk = max(
        [static_risk, planner_risk, critic_risk],
        key=lambda r: risk_order.get(r, 2)
    )

    return {
        "verdict":     lines.get("VERDICT", "REJECT").strip(),
        "risk":        final_risk,
        "reason":      lines.get("REASON",  "Could not parse critic response"),
        "alternative": lines.get("ALTERNATIVE"),
    }