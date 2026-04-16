import smtplib
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# ── Configuration — set these in your environment ──────────────────────
SMTP_HOST     = os.getenv("SMTP_HOST",     "smtp.gmail.com")
SMTP_PORT     = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER     = os.getenv("SMTP_USER",     "")   # your email address
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")   # app password
ADMIN_EMAIL   = os.getenv("ADMIN_EMAIL",   "")   # where to send alerts
AGENT_BASE_URL = os.getenv("AGENT_BASE_URL", "http://172.25.5.168:8002")


def send_approval_request(action_id: str, proposed_action: dict, critique: dict, cluster_summary: dict):
    """
    Send an email to the administrator requesting approval for a high-risk action.
    The email contains full context and approve/reject links.
    """
    tool       = proposed_action.get("tool", "unknown")
    inputs     = proposed_action.get("inputs", {})
    reason     = proposed_action.get("reason", "")
    expected   = proposed_action.get("expected_effect", "")
    rollback   = proposed_action.get("rollback", "")
    critic_reason = critique.get("reason", "")

    approve_url = f"{AGENT_BASE_URL}/approve/{action_id}"
    reject_url  = f"{AGENT_BASE_URL}/reject/{action_id}"

    subject = f"[CLUSTER ALERT] High-risk action requires approval — {tool} — ID {action_id}"

    body = f"""
CLUSTER MANAGEMENT SYSTEM — HIGH RISK ACTION APPROVAL REQUIRED
===============================================================

Action ID:      {action_id}
Action:         {tool}
Parameters:     {inputs}
Risk level:     HIGH

WHY THIS ACTION WAS PROPOSED
─────────────────────────────
{reason}

EXPECTED EFFECT
───────────────
{expected}

HOW TO ROLL BACK IF NEEDED
──────────────────────────
{rollback}

CRITIC VALIDATION
─────────────────
{critic_reason}

CURRENT CLUSTER STATE
─────────────────────
Nodes online:   {cluster_summary.get("nodes_online", "?")}
VMs running:    {cluster_summary.get("vms_running", "?")}
Warning queue:  {cluster_summary.get("warning_queue", "?")} items
Bottleneck:     {cluster_summary.get("bottleneck", "unknown")}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  APPROVE this action:
  {approve_url}

  REJECT this action:
  {reject_url}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

This action will remain in the queue until you respond.
The system continues operating normally while waiting.

If you do not respond within 24 hours, the action will be
automatically rejected and logged.
"""

    _send(subject, body)


def send_action_executed(tool: str, inputs: dict, result: dict):
    """
    Send a notification that a low/medium risk action was executed automatically.
    """
    subject = f"[CLUSTER INFO] Automated action executed — {tool}"
    body = f"""
CLUSTER MANAGEMENT SYSTEM — AUTOMATED ACTION LOG
=================================================

Action:     {tool}
Parameters: {inputs}
Result:     {result}

This action was classified as low/medium risk and executed automatically
by the cluster management system without requiring your approval.

If this action looks wrong, you can review the action log at:
{AGENT_BASE_URL}/action-log
"""
    _send(subject, body)


def send_action_rejected_by_critic(tool: str, inputs: dict, reason: str, alternative: str):
    """
    Notify admin when the Critic rejected an action the Planner proposed.
    Informational only — no approval needed.
    """
    subject = f"[CLUSTER INFO] Proposed action rejected by Critic — {tool}"
    body = f"""
CLUSTER MANAGEMENT SYSTEM — CRITIC REJECTION LOG
=================================================

Proposed action:  {tool}
Parameters:       {inputs}

CRITIC REJECTION REASON
───────────────────────
{reason}

ALTERNATIVE SUGGESTED
─────────────────────
{alternative or "None — no action taken"}

The system will continue monitoring and may propose a different action.
"""
    _send(subject, body)


def _send(subject: str, body: str):
    """Internal: send a plain text email via SMTP."""
    if not SMTP_USER or not ADMIN_EMAIL:
        print(f"[email_client] Email not configured — would have sent: {subject}")
        return

    msg = MIMEMultipart()
    msg["From"]    = SMTP_USER
    msg["To"]      = ADMIN_EMAIL
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.sendmail(SMTP_USER, ADMIN_EMAIL, msg.as_string())
        print(f"[email_client] Sent: {subject}")
    except Exception as e:
        print(f"[email_client] Failed to send email: {e}")