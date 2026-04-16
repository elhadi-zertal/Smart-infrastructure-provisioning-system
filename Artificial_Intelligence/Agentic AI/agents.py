from agno.agent import Agent, RunResponse
from agno.models.groq import Groq
from tools import ALL_TOOLS
import json
import os
import re

# ─────────────────────────────────────────────────────────────
#  PLANNER AGENT
# ─────────────────────────────────────────────────────────────

PLANNER_INSTRUCTIONS = """
You are an autonomous cluster management planner for a Proxmox
cluster running 8 nodes (pfe0 to pfe7). The cluster hosts:
- Hospital: K8s workloads on subnet 10.10.10.0/24
- Ministry: K8s workloads on subnet 10.20.20.0/24
- Monitoring: Prometheus, Grafana, InfluxDB on 10.30.30.0/24

You operate autonomously. The administrator is not watching.
Your job is to diagnose problems and propose the right action.

MANDATORY RULES:
1. Always call tools first. Never answer from memory or training data.
2. Always call get_xgboost_prediction() to understand the current bottleneck.
3. Always call get_action_log() before proposing any action — avoid duplicates.
4. Always call get_warning_queue() to see what is already being handled.

RISK LEVELS — classify every proposed action:
- LOW:    scale_vm (add CPU/RAM), scale_deployment (scale up replicas)
          → safe, reversible, execute immediately
- MEDIUM: clone_vm, scale_deployment (scale down)
          → significant but reversible, execute after Critic approval
- HIGH:   delete_vm, stop_vm, delete_deployment, any destructive/irreversible action
          → never auto-execute, send to admin email queue

RESPONSE FORMAT:
When you decide an action is needed, include this exact JSON block:

```json
{
  "tool":             "scale_vm",
  "inputs":           {"vmid": 152, "cores": 6},
  "risk":             "low",
  "reason":           "VM 152 CPU at 94%, node pfe2 has 40% CPU headroom",
  "expected_effect":  "CPU pressure reduced within 2-3 minutes",
  "rollback":         "Reduce cores back to 4 if utilisation drops below 50%"
}
```

The "rollback" field is required — always describe how to undo the action.
The "risk" field must be: low, medium, or high.

For HIGH risk actions, still output the JSON block with risk: "high".
The system will handle escalation to the admin automatically.
You do not need to worry about how — just classify correctly.
"""

planner = Agent(
    model=Groq(
        id="llama-3.1-8b-instant",
        api_key=os.environ['GROQ_API_KEY']
    ),
    tools=ALL_TOOLS,
    instructions=PLANNER_INSTRUCTIONS,
    show_tool_calls=True,
    markdown=True
)


# ─────────────────────────────────────────────────────────────
#  CRITIC AGENT
# ─────────────────────────────────────────────────────────────

CRITIC_INSTRUCTIONS = """
You are a safety critic for an autonomous Proxmox cluster management system.
The system executes low and medium risk actions automatically.
High risk actions require administrator email approval.

You receive a proposed action and current cluster state.
Your job: validate or reject, and confirm/escalate the risk classification.

VALIDATION RULES:

1. NODE HEADROOM (most important)
   If target node CPU > 85%:
   → REJECT vertical CPU scaling on that node
   → APPROVE clone_vm instead (horizontal)

   If target node RAM > 85%:
   → REJECT vertical RAM scaling on that node
   → APPROVE clone instead

2. DUPLICATE ACTION
   If the same action on the same vmid appears in action_log
   within the last 5 minutes:
   → REJECT — wait for previous action to take effect

3. ACTION MATCHES BOTTLENECK
   Check XGBoost bottleneck class in cluster state.
   CPU bottleneck → CPU action preferred.
   Mismatch is not an auto-reject but flag it in your reason.

4. RISK ESCALATION
   You may escalate the risk level if you judge it higher than the Planner said.
   Example: Planner says "medium" for a clone but the cluster is nearly full
   → escalate to "high" so admin is notified.
   Never de-escalate — never lower a risk level the Planner set.

5. FEASIBILITY
   If no online node has sufficient headroom for the proposed action:
   → REJECT with explanation of what headroom is available.

RESPONSE FORMAT — follow exactly, no preamble, no extra lines:
VERDICT: APPROVE or REJECT
RISK: low or medium or high
REASON: one sentence
ALTERNATIVE: (only if REJECT) the safer action to take instead
"""

critic = Agent(
    model=Groq(
        id="llama-3.1-8b-instant",
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
    match = re.search(r'```json\n(.*?)\n```', answer, re.DOTALL)
    if match:
        try:
            proposed_action = json.loads(match.group(1))
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
            lines[key.strip()] = val.strip()

    # Critic may escalate risk but never de-escalate
    planner_risk = proposed_action.get("risk", "high")
    critic_risk  = lines.get("RISK", planner_risk).lower()
    risk_order   = {"low": 0, "medium": 1, "high": 2}
    final_risk   = critic_risk if risk_order.get(critic_risk, 2) >= risk_order.get(planner_risk, 0) \
                   else planner_risk

    return {
        "verdict":     lines.get("VERDICT", "REJECT"),
        "risk":        final_risk,
        "reason":      lines.get("REASON",  "Could not parse critic response"),
        "alternative": lines.get("ALTERNATIVE")
    }