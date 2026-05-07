"""
app.py — Prometheus HTTP Service Discovery API
===============================================
Roles
-----
1. Prometheus SD  — /targets/{job}  feeds node_exporter, scaphandre, snmp,
                                    blackbox_exporter jobs
2. Target CRUD    — /hosts          main.py registers new VM clones / unregisters deleted VMs
                                    /probes registers HTTP probe URLs (services through the LB)
3. Threshold API  — /thresholds     ml_service.py pushes updated alert thresholds;
                                    regenerates vm_scaling.yml + hot-reloads Prometheus
4. PM watchdog    — background task queries Prometheus for failed physical machines
                    and calls main.py /migrate-from-node to evacuate their VMs

What's new vs the previous version
----------------------------------
* The `blackbox_exporter` job is no longer fed VM IPs.  Its target list is
  the set of HTTP application URLs (services exposed via the NGINX LB)
  registered with type="probe" in targets.json.  The relabel rule in
  prometheus.yml hard-codes __address__ to NGINX_VM_IP:9115, so the
  probes are executed by the central blackbox_exporter sitting on the
  LB VM — exactly the conception in memoire § 4.x ("ICMP reachability
  probes from the NGINX VM to VXLAN targets").
* `_generate_vm_scaling_yml` now uses nginx_vts_upstream_responses_total
  for HTTP5xxWarning / HTTP5xxCritical, with `target_type=k8s` and a
  `type=http_5xx` label so main.py /alert can decode it directly.

Inter-service URLs (env vars)
------------------------------
APP_URL   = this service         (default http://localhost:5000)
MAIN_URL  = main.py              (default http://localhost:8000)
ML_URL    = ml_service.py        (default http://localhost:8001)
PROMETHEUS_URL                   (default http://localhost:9090)
VM_SCALING_YML                   (default /etc/prometheus/rules/vm_scaling.yml)
"""

import asyncio
import json
import logging
import os
import threading
import time
from contextlib import asynccontextmanager
from typing import Optional

import httpx
import requests
import urllib3
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# Disable SSL warnings for Proxmox self-signed certificates
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("sd_api")

# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────
PROXMOX_HOST     = os.getenv("PROXMOX_HOST")
PROXMOX_USER     = os.getenv("PROXMOX_USER")       # user@realm!tokenid
PROXMOX_PASSWORD = os.getenv("PROXMOX_PASSWORD")   # API token secret
PROXMOX_NODE     = os.getenv("PROXMOX_NODE", "pve")
DB_FILE          = os.path.join(os.path.dirname(__file__), "targets.json")

# External service URLs
MAIN_URL           = os.getenv("MAIN_URL",           "http://localhost:8000")
PROMETHEUS_URL     = os.getenv("PROMETHEUS_URL",     "http://localhost:9090")
VM_SCALING_YML     = os.getenv("VM_SCALING_YML",     "/etc/prometheus/rules/vm_scaling.yml")
CLUSTER_API_KEY    = os.getenv("CLUSTER_API_KEY",    "")
PM_CHECK_INTERVAL  = int(os.getenv("PM_CHECK_INTERVAL_S", "30"))

# ── Port each scrape job listens on ──────────────────────────────────────────
PORT_MAP: dict[str, int | None] = {
    "node_exporter":     9100,  # ip:9100 — VMs + PMs
    "scaphandre":        8080,  # ip:8080 — PMs only
    "blackbox_exporter": None,  # raw URL (http://host[:port]/path) — probes only
    "snmp_exporter":     None,  # device IP only — network devices
}

# ── Which host TYPES are included in each job ────────────────────────────────
# Note: "vm" is intentionally REMOVED from blackbox_exporter.  VM-level
# liveness is handled by the VMUnreachable alert which uses the up{} metric
# from node_exporter — far more reliable than HTTP-probing every VM IP.
JOB_TYPES: dict[str, set[str]] = {
    "node_exporter":     {"vm", "pm"},
    "scaphandre":        {"pm"},
    "blackbox_exporter": {"probe"},     # probes only — service URLs
    "snmp_exporter":     {"network"},
}

_db_lock = threading.Lock()

# Globals for caching Proxmox VM lookups
_vm_cache: dict[str, dict] = {}
_vm_cache_lock = threading.Lock()

# ─────────────────────────────────────────────────────────────────────────────
# Pydantic models
# ─────────────────────────────────────────────────────────────────────────────

class HostEntry(BaseModel):
    ip: str
    hostname: str = ""
    type: str = "vm"    # vm | pm | network | probe
    env: str = "prod"
    vmid: Optional[str] = None    # FIX: allow main.py to register a clone WITH its vmid

class ProbeEntry(BaseModel):
    """An HTTP probe target — service URL exposed through the NGINX LB."""
    url: str                  # e.g. http://10.30.30.50:8081/probe/pfe_app
    service_name: str         # e.g. pfe_app (== nginx upstream block name)
    namespace: str = "default"
    deployment: str = ""      # K8s Deployment name (defaults to service_name)
    env: str = "prod"

class ThresholdUpdate(BaseModel):
    # Warning thresholds
    thresh_cpu_warn:  Optional[float] = None
    thresh_ram_warn:  Optional[float] = None
    thresh_disk_warn: Optional[float] = None   # free I/O % below which we warn
    thresh_http_warn: Optional[float] = None   # 5xx rate %
    # Critical thresholds
    thresh_cpu_crit:  Optional[float] = None
    thresh_ram_crit:  Optional[float] = None
    thresh_disk_crit: Optional[float] = None   # free I/O % below which we alert
    thresh_http_crit: Optional[float] = None
    thresh_net_crit:  Optional[float] = None   # packet loss %
    # Scale-down thresholds
    thresh_cpu_low:   Optional[float] = None
    thresh_ram_low:   Optional[float] = None
    thresh_http_low:  Optional[float] = None

# ─────────────────────────────────────────────────────────────────────────────
# DB helpers
# ─────────────────────────────────────────────────────────────────────────────

def load_db() -> list:
    """Load static targets (PMs, network devices, probes, …) from targets.json."""
    with _db_lock:
        if not os.path.exists(DB_FILE):
            return []
        with open(DB_FILE, "r") as f:
            try:
                return json.load(f)
            except Exception:
                return []

def _save_db(entries: list) -> None:
    """Persist the target list to targets.json."""
    with _db_lock:
        with open(DB_FILE, "w") as f:
            json.dump(entries, f, indent=2)

# ─────────────────────────────────────────────────────────────────────────────
# Proxmox helpers (Optimized with HTTP Sessions and Background Caching)
# ─────────────────────────────────────────────────────────────────────────────

def _auth_headers() -> dict:
    return {
        "Authorization": f"PVEAPIToken={PROXMOX_USER}={PROXMOX_PASSWORD}",
        "Content-Type": "application/json",
    }

def _get_vm_ip(session: requests.Session, node: str, vmid: int, vm_name: str) -> str:
    """Resolve a running VM's primary IP using a shared session."""
    base = f"https://{PROXMOX_HOST}:8006/api2/json"

    # 1. QEMU Guest Agent
    try:
        r = session.get(f"{base}/nodes/{node}/qemu/{vmid}/agent/network-get-interfaces", timeout=4)
        if r.status_code == 200:
            for iface in r.json().get("data", {}).get("result", []):
                if iface.get("name", "").startswith("lo"):
                    continue
                for addr in iface.get("ip-addresses", []):
                    if addr.get("ip-address-type") == "ipv4":
                        ip = addr["ip-address"]
                        if not ip.startswith("127."):
                            return ip
    except Exception:
        pass

    # 2. Cloud-init ipconfig0
    try:
        r = session.get(f"{base}/nodes/{node}/qemu/{vmid}/config", timeout=4)
        if r.status_code == 200:
            ipconfig = r.json().get("data", {}).get("ipconfig0", "")
            for part in ipconfig.split(","):
                if part.startswith("ip="):
                    return part[3:].split("/")[0]
    except Exception:
        pass

    # 3. Fall back to VM name (DNS)
    return vm_name


def _get_lxc_ip(session: requests.Session, node: str, vmid: int, name: str) -> str:
    """Resolve a running LXC container's primary IP."""
    base = f"https://{PROXMOX_HOST}:8006/api2/json"
    try:
        r = session.get(f"{base}/nodes/{node}/lxc/{vmid}/config", timeout=4)
        if r.status_code == 200:
            cfg = r.json().get("data", {})
            for k, v in cfg.items():
                if k.startswith("net") and isinstance(v, str) and "ip=" in v:
                    for part in v.split(","):
                        if part.startswith("ip="):
                            ip = part[3:].split("/")[0]
                            if ip and not ip.startswith("dhcp"):
                                return ip
    except Exception:
        pass

    return name

def discover_vms_from_proxmox() -> dict[str, dict]:
    """Query the Proxmox cluster for all running VMs and LXC Containers across ALL nodes."""
    vm_hosts: dict[str, dict] = {}
    if not PROXMOX_HOST:
        return vm_hosts

    url = f"https://{PROXMOX_HOST}:8006/api2/json/cluster/resources"

    try:
        with requests.Session() as session:
            session.verify = False
            session.headers.update(_auth_headers())

            response = session.get(url, timeout=5)
            if response.status_code != 200:
                logger.warning(f"[Proxmox] Unexpected status {response.status_code}")
                return vm_hosts

            for guest in response.json().get("data", []):
                if guest.get("status") != "running":
                    continue

                guest_type = guest.get("type")
                if guest_type not in ("vm", "qemu", "lxc"):
                    continue

                vmid      = guest["vmid"]
                vm_name   = guest.get("name", str(vmid))
                node_name = guest.get("node")

                if guest_type == "lxc":
                    ip = _get_lxc_ip(session, node_name, vmid, vm_name)
                else:
                    ip = _get_vm_ip(session, node_name, vmid, vm_name)

                vm_hosts[ip] = {
                    "hostname": vm_name,
                    "type":     "vm", # We treat both LXC and QEMU as 'vm' for Prometheus groupings
                    "env":      "prod",
                    "vmid":     vmid,
                }
    except Exception as exc:
        logger.error(f"[Proxmox Error] {exc}")

    return vm_hosts

def _build_target_address(entry_key: str, entry: dict, job: str) -> str:
    """
    Format the Prometheus SD target string for the given job type.

    For blackbox_exporter, entry_key IS the full URL (probe target).
    For node_exporter / scaphandre, entry_key is the IP and we append the port.
    For snmp_exporter, entry_key is the device IP (passed as __param_target).
    """
    port = PORT_MAP.get(job)

    if job == "blackbox_exporter":
        # The URL is already complete — pass it through as the probe target.
        return entry_key

    if job == "snmp_exporter":
        return entry_key   # relabeling moves this to __param_target

    return f"{entry_key}:{port}"

# ─────────────────────────────────────────────────────────────────────────────
# Threshold management
# ─────────────────────────────────────────────────────────────────────────────

_current_thresholds: dict = {
    "thresh_cpu_warn":  80.0,
    "thresh_cpu_crit":  95.0,
    "thresh_cpu_low":   30.0,
    "thresh_ram_warn":  60.0,
    "thresh_ram_crit":  80.0,
    "thresh_ram_low":   30.0,
    "thresh_disk_warn": 20.0,
    "thresh_disk_crit": 10.0,
    "thresh_http_warn": 1.0,
    "thresh_http_crit": 5.0,
    "thresh_http_low":  0.5,
    "thresh_net_crit":  2.0,
}

def _generate_vm_scaling_yml(t: dict) -> str:
    """
    Produce the full vm_scaling.yml content with all alert names from
    memoire.tex Tab. alert_rules.

    FIX (scalability bug-fix):
      * HTTP5xx alerts now propagate `namespace` + `deployment` labels via
        label_replace on the upstream label (convention: <ns>__<dep>).
        Without these labels, main.py cannot resolve the deployment to scale.
      * VM-level alerts (CPU/RAM/Disk/Network) now propagate `vmid` via a
        label-join with vm_info{job="node_exporter"} — built implicitly from
        the SD `vmid` label that this app.py now exposes.
      * Recording rules added to keep alert expressions readable.
    """
    return f"""\
# Auto-generated by app.py — do not edit by hand; modify the template
# inside _generate_vm_scaling_yml() instead.
groups:

  - name: aggregations
    interval: 15s
    rules:
      - record: pfe:nginx_5xx_rate_per_upstream
        expr: |
          (
            sum by (upstream) (rate(nginx_vts_upstream_responses_total{{code="5xx"}}[2m]))
            /
            (sum by (upstream) (rate(nginx_vts_upstream_responses_total[2m])) > 0)
          ) * 100

      - record: pfe:nginx_5xx_rate_per_backend
        expr: |
          (
            sum by (upstream, backend) (rate(nginx_vts_upstream_responses_total{{code="5xx"}}[2m]))
            /
            (sum by (upstream, backend) (rate(nginx_vts_upstream_responses_total[2m])) > 0)
          ) * 100

      # vm_info: info-style series with vmid + hostname per VM (from SD labels)
      - record: vm_info
        expr: up{{job="node_exporter"}}
        labels:
          source: "service_discovery"

  - name: http_errors
    rules:
      - alert: HTTP5xxWarning
        expr: |
          label_replace(
            label_replace(
              pfe:nginx_5xx_rate_per_upstream > {t['thresh_http_warn']},
              "namespace",  "$1", "upstream", "(.+?)__.+"
            ),
            "deployment", "$1", "upstream", ".+?__(.+)"
          )
        for: 1m
        labels:
          severity:    warning
          type:        http_5xx
          target_type: k8s
          action:      horizontal_scaling
        annotations:
          summary:     "HTTP 5xx > {t['thresh_http_warn']}% on upstream {{{{ $labels.upstream }}}}"
          description: "5xx error rate = {{{{ $value | printf \\"%.2f\\" }}}}% on ns={{{{ $labels.namespace }}}} dep={{{{ $labels.deployment }}}} — Horizontal Scaling"

      - alert: HTTP5xxCritical
        expr: |
          label_replace(
            label_replace(
              pfe:nginx_5xx_rate_per_upstream > {t['thresh_http_crit']},
              "namespace",  "$1", "upstream", "(.+?)__.+"
            ),
            "deployment", "$1", "upstream", ".+?__(.+)"
          )
        for: 30s
        labels:
          severity:    critical
          type:        http_5xx
          target_type: k8s
          action:      horizontal_scaling
        annotations:
          summary:     "HTTP 5xx > {t['thresh_http_crit']}% on upstream {{{{ $labels.upstream }}}}"
          description: "5xx error rate = {{{{ $value | printf \\"%.2f\\" }}}}% on ns={{{{ $labels.namespace }}}} dep={{{{ $labels.deployment }}}} — Horizontal Scaling required"

      - alert: HTTP5xxBackendCulprit
        expr: |
          pfe:nginx_5xx_rate_per_backend > {t['thresh_http_crit']}
          and
          sum by (upstream, backend) (rate(nginx_vts_upstream_responses_total[2m])) > 0.1
        for: 2m
        labels:
          severity:    warning
          type:        http_5xx
          target_type: backend
          action:      drain_backend
        annotations:
          summary:     "Backend {{{{ $labels.backend }}}} of {{{{ $labels.upstream }}}} faulty"
          description: "5xx = {{{{ $value | printf \\"%.2f\\" }}}}% — drain pod / cordon node."

  - name: network
    rules:
      - alert: PacketLossCritical
        expr: |
          (
            rate(node_network_receive_drop_total{{device!="lo"}}[5m])
            / (rate(node_network_receive_packets_total{{device!="lo"}}[5m]) + 1)
          ) * 100
          * on(instance) group_left(vmid, hostname) (vm_info{{job="node_exporter"}} > 0)
          > {t['thresh_net_crit']}
        for: 1m
        labels:
          severity:    critical
          type:        network
          target_type: vm
          action:      horizontal_scaling
        annotations:
          summary:     "Packet loss > {t['thresh_net_crit']}% on {{{{ $labels.instance }}}} (vmid={{{{ $labels.vmid }}}})"
          description: "Packet loss = {{{{ $value | printf \\"%.2f\\" }}}}% on {{{{ $labels.device }}}} — Horizontal Scaling required"

  - name: cpu
    rules:
      - alert: CPUWarning
        expr: |
          (100 - (avg by(instance, job) (rate(node_cpu_seconds_total{{mode="idle"}}[2m])) * 100))
          * on(instance) group_left(vmid, hostname) (vm_info{{job="node_exporter"}} > 0)
          > {t['thresh_cpu_warn']}
        for: 1m
        labels:
          severity:    warning
          type:        cpu
          target_type: vm
          action:      vertical_scaling_cpu
        annotations:
          summary:     "CPU > {t['thresh_cpu_warn']}% on {{{{ $labels.instance }}}} (vmid={{{{ $labels.vmid }}}})"
          description: "CPU = {{{{ $value | printf \\"%.2f\\" }}}}% — Monitor. If PM insufficient, migrate VM."

      - alert: CPUCritical
        expr: |
          (100 - (avg by(instance, job) (rate(node_cpu_seconds_total{{mode="idle"}}[2m])) * 100))
          * on(instance) group_left(vmid, hostname) (vm_info{{job="node_exporter"}} > 0)
          > {t['thresh_cpu_crit']}
        for: 30s
        labels:
          severity:    critical
          type:        cpu
          target_type: vm
          action:      vertical_scaling_cpu
        annotations:
          summary:     "CPU > {t['thresh_cpu_crit']}% on {{{{ $labels.instance }}}} (vmid={{{{ $labels.vmid }}}})"
          description: "CPU = {{{{ $value | printf \\"%.2f\\" }}}}% — Add 1 vCPU. If PM insufficient, migrate VM."

  - name: memory
    rules:
      - alert: RAMWarning
        expr: |
          (1 - (node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes)) * 100
          * on(instance) group_left(vmid, hostname) (vm_info{{job="node_exporter"}} > 0)
          > {t['thresh_ram_warn']}
        for: 1m
        labels:
          severity:    warning
          type:        memory
          target_type: vm
          action:      vertical_scaling_ram
        annotations:
          summary:     "RAM > {t['thresh_ram_warn']}% on {{{{ $labels.instance }}}} (vmid={{{{ $labels.vmid }}}})"
          description: "RAM used = {{{{ $value | printf \\"%.2f\\" }}}}% — Monitor."

      - alert: RAMCritical
        expr: |
          (1 - (node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes)) * 100
          * on(instance) group_left(vmid, hostname) (vm_info{{job="node_exporter"}} > 0)
          > {t['thresh_ram_crit']}
        for: 1m
        labels:
          severity:    critical
          type:        memory
          target_type: vm
          action:      vertical_scaling_ram
        annotations:
          summary:     "RAM > {t['thresh_ram_crit']}% on {{{{ $labels.instance }}}} (vmid={{{{ $labels.vmid }}}})"
          description: "RAM used = {{{{ $value | printf \\"%.2f\\" }}}}% — Add 20% RAM."

  - name: disk
    rules:
      - alert: DiskIOWarning
        expr: |
          (1 - rate(node_disk_io_time_seconds_total{{device!~"sr.*|loop.*"}}[5m])) * 100
          * on(instance) group_left(vmid, hostname) (vm_info{{job="node_exporter"}} > 0)
          < {t['thresh_disk_warn']}
        for: 5m
        labels:
          severity:    warning
          type:        disk_io
          target_type: vm
          action:      vertical_scaling_disk
        annotations:
          summary:     "Disk I/O free < {t['thresh_disk_warn']}% on {{{{ $labels.instance }}}} (vmid={{{{ $labels.vmid }}}})"
          description: "Free I/O on {{{{ $labels.device }}}} = {{{{ $value | printf \\"%.2f\\" }}}}% — Monitor."

      - alert: DiskIOCritical
        expr: |
          (1 - rate(node_disk_io_time_seconds_total{{device!~"sr.*|loop.*"}}[5m])) * 100
          * on(instance) group_left(vmid, hostname) (vm_info{{job="node_exporter"}} > 0)
          < {t['thresh_disk_crit']}
        for: 2m
        labels:
          severity:    critical
          type:        disk_io
          target_type: vm
          action:      vertical_scaling_disk
        annotations:
          summary:     "Disk I/O free < {t['thresh_disk_crit']}% on {{{{ $labels.instance }}}} (vmid={{{{ $labels.vmid }}}})"
          description: "Free I/O on {{{{ $labels.device }}}} = {{{{ $value | printf \\"%.2f\\" }}}}% — Add 15G disk."

  - name: vm_availability_detailed
    rules:
      - alert: VMUnreachable
        expr: up{{job="node_exporter"}} == 0
        for: 2m
        labels:
          severity:    critical
          action:      check_network_then_migrate
          cause:       network_or_host_failure
        annotations:
          summary:     "VM {{{{ $labels.instance }}}} inaccessible"
          description: "No node_exporter response for 2m."

      - alert: ScaphandreDown
        expr: up{{job="scaphandre"}} == 0
        for: 1m
        labels:
          severity:    warning
          action:      restart_scaphandre
          cause:       scaphandre_failure
        annotations:
          summary:     "Scaphandre down on {{{{ $labels.instance }}}}"
          description: "Scaphandre isn't responding — Energy metrics missing."

      - alert: VMHighLatency
        expr: prometheus_target_scrape_duration_seconds{{job=~"node_exporter|scaphandre"}} > 2
        for: 3m
        labels:
          severity:    warning
          action:      investigate_network
          cause:       network_degradation
        annotations:
          summary:     "High Latency on {{{{ $labels.instance }}}}"
          description: "Scrape duration > 2s — Network or VM overloaded."

      - alert: VMRebootDetected
        expr: (node_time_seconds - node_boot_time_seconds < 300) and (changes(node_boot_time_seconds[10m]) > 0)
        for: 1m
        labels:
          severity:    info
          action:      monitor_stability
          cause:       reboot
        annotations:
          summary:     "VM {{{{ $labels.instance }}}} recently rebooted"
          description: "Uptime < 5m."

      - alert: VMOOMKilled
        expr: increase(node_vmstat_oom_kill[5m]) > 0
        for: 1m
        labels:
          severity:    critical
          action:      add_ram_or_kill_processes
          cause:       out_of_memory
        annotations:
          summary:     "OOM Killer active on {{{{ $labels.instance }}}}"
          description: "Processes killed due to lack of memory."

      - alert: VMDiskAlmostFull
        expr: node_filesystem_avail_bytes{{mountpoint="/"}} / node_filesystem_size_bytes{{mountpoint="/"}} < 0.02
        for: 1m
        labels:
          severity:    critical
          action:      emergency_disk_cleanup
          cause:       disk_full_service_crash
        annotations:
          summary:     "VM {{{{ $labels.instance }}}} — Disk almost full!"
          description: "Disk space < 2% on /."

  - name: energy
    rules:
      - alert: VMHighPowerConsumption
        expr: sum by(instance, vm_id, vm_name) (scaph_vm_power_microwatts{{job="scaphandre"}}) / 1000000 > 50
        for: 5m
        labels:
          severity:    warning
          action:      investigate_power
        annotations:
          summary:     "High consumption on VM {{{{ $labels.vm_name }}}}"
          description: "Power = {{{{ $value | printf \\"%.2f\\" }}}}W — VM consumes too much."

  - name: load_balancer_health
    rules:
      - alert: NginxLBDown
        expr: up{{job="nginx_lb"}} == 0
        for: 1m
        labels:
          severity: critical
          action:   restart_nginx_or_failover
        annotations:
          summary:     "NGINX LB {{{{ $labels.instance }}}} unreachable"
          description: "Prometheus cannot scrape the VTS metrics endpoint."

      - alert: BlackboxProbeFailure
        expr: probe_success == 0
        for: 2m
        labels:
          severity: warning
          action:   investigate_service
        annotations:
          summary:     "Blackbox probe failed for {{{{ $labels.instance }}}}"
          description: "HTTP probe to {{{{ $labels.instance }}}} fails for 2 min."
"""

def _apply_thresholds(new_values: dict) -> bool:
    _current_thresholds.update({k: v for k, v in new_values.items() if v is not None})
    content = _generate_vm_scaling_yml(_current_thresholds)

    try:
        os.makedirs(os.path.dirname(os.path.abspath(VM_SCALING_YML)), exist_ok=True)
        with open(VM_SCALING_YML, "w") as fh:
            fh.write(content)
        logger.info(f"[Thresholds] Written updated rules to {VM_SCALING_YML}")
    except Exception as exc:
        logger.error(f"[Thresholds] Could not write {VM_SCALING_YML}: {exc}")
        return False

    try:
        r = requests.post(f"{PROMETHEUS_URL}/-/reload", timeout=5)
        if r.status_code == 200:
            logger.info("[Thresholds] Prometheus rules reloaded successfully")
        else:
            logger.warning(f"[Thresholds] Prometheus reload HTTP {r.status_code}")
    except Exception as exc:
        logger.warning(f"[Thresholds] Prometheus reload failed: {exc}")

    return True

# ─────────────────────────────────────────────────────────────────────────────
# Background Tasks (PM Watchdog & VM SD Cache)
# ─────────────────────────────────────────────────────────────────────────────

_failed_pms: set[str] = set()

async def _check_pm_health() -> None:
    """Evacuate VMs from physical machines whose node_exporter is unreachable."""
    if not PROMETHEUS_URL or not MAIN_URL:
        return

    try:
        query = 'up{job="node_exporter",type="pm"} == 0'
        r = requests.get(f"{PROMETHEUS_URL}/api/v1/query", params={"query": query}, timeout=5)
        if r.status_code != 200:
            return

        currently_failed: set[str] = set()
        for series in r.json().get("data", {}).get("result", []):
            hostname = series["metric"].get("hostname", "")
            instance = series["metric"].get("instance", "")
            node_id  = hostname or instance.split(":")[0]
            if node_id:
                currently_failed.add(node_id)

        newly_failed = currently_failed - _failed_pms
        for node in newly_failed:
            logger.warning(f"[PM Watchdog] PM '{node}' is DOWN — requesting VM migration")
            try:
                async with httpx.AsyncClient(timeout=10) as client:
                    resp = await client.post(
                        f"{MAIN_URL}/migrate-from-node",
                        json={"node": node, "reason": f"PM '{node}' unreachable"},
                        headers={"X-API-Key": CLUSTER_API_KEY},
                    )
                    if resp.status_code == 200:
                        logger.info(f"[PM Watchdog] Migration request accepted for '{node}'")
                    else:
                        logger.warning(f"[PM Watchdog] main.py returned {resp.status_code} for '{node}'")
            except Exception as exc:
                logger.error(f"[PM Watchdog] Could not notify main.py for '{node}': {exc}")

        recovered = _failed_pms - currently_failed
        for node in recovered:
            logger.info(f"[PM Watchdog] PM '{node}' has recovered")

        _failed_pms.clear()
        _failed_pms.update(currently_failed)

    except Exception as exc:
        logger.warning(f"[PM Watchdog] Health check failed: {exc}")


async def _refresh_vm_cache() -> None:
    """Periodically caches Proxmox VM lists so Prometheus SD queries return instantly."""
    global _vm_cache
    try:
        vms = await asyncio.to_thread(discover_vms_from_proxmox)
        with _vm_cache_lock:
            _vm_cache = vms
    except Exception as exc:
        logger.error(f"[SD Cache] Failed to refresh VM cache: {exc}")

# ─────────────────────────────────────────────────────────────────────────────
# Lifespan / Scheduler
# ─────────────────────────────────────────────────────────────────────────────

_scheduler = AsyncIOScheduler()

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Initializing Proxmox VM/LXC cache...")
    await _refresh_vm_cache()

    # Bootstrap vm_scaling.yml on first launch so Prometheus can load
    # rules immediately even if ml_service.py hasn't called /thresholds yet.
    try:
        if not os.path.exists(VM_SCALING_YML) or os.path.getsize(VM_SCALING_YML) == 0:
            _apply_thresholds({})
            logger.info(f"[Bootstrap] Wrote initial {VM_SCALING_YML}")
    except Exception as exc:
        logger.warning(f"[Bootstrap] Could not write initial vm_scaling.yml: {exc}")

    _scheduler.add_job(
        _check_pm_health, "interval",
        seconds=PM_CHECK_INTERVAL, id="pm_health_check",
    )
    _scheduler.add_job(
        _refresh_vm_cache, "interval",
        seconds=15, id="vm_cache_refresh",
    )
    _scheduler.start()
    logger.info(f"SD API started — PM watchdog every {PM_CHECK_INTERVAL}s")
    yield
    _scheduler.shutdown()

app = FastAPI(title="Prometheus SD API", version="3.1.0", lifespan=lifespan)

# ─────────────────────────────────────────────────────────────────────────────
# API Endpoints
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/targets/{job}")
def get_targets(job: str):
    """Prometheus HTTP SD endpoint."""
    if job not in PORT_MAP:
        return []

    allowed_types = JOB_TYPES[job]
    out = []

    # 1. Static targets from targets.json (PMs, network devices, probes)
    for entry in load_db():
        entry_type = entry.get("type", "pm")
        if entry_type not in allowed_types:
            continue

        if job == "blackbox_exporter":
            # Probe entries store the full URL in `url` plus rich
            # K8s-aware labels so HTTP5xx alerts can identify the
            # deployment to clone.
            url = entry.get("url") or entry.get("ip", "")
            if not url:
                continue
            out.append({
                "targets": [url],
                "labels": {
                    "service":    entry.get("service_name", entry.get("hostname", "")),
                    "namespace":  entry.get("namespace",   "default"),
                    "deployment": entry.get("deployment", entry.get("service_name", "")),
                    "env":        entry.get("env", "prod"),
                },
            })
        else:
            raw_ip = entry.get("ip", "").removeprefix("http://").removeprefix("https://")
            if not raw_ip:
                continue
            # FIX: propagate vmid label if it was registered with /hosts.
            labels = {
                "hostname": entry.get("hostname", raw_ip),
                "type":     entry_type,
                "env":      entry.get("env", "prod"),
            }
            if entry.get("vmid") is not None:
                labels["vmid"] = str(entry["vmid"])
            out.append({
                "targets": [_build_target_address(raw_ip, entry, job)],
                "labels":  labels,
            })

    # 2. Dynamic VM targets — only for jobs that include "vm"
    if "vm" in allowed_types:
        with _vm_cache_lock:
            cached_vms = _vm_cache.copy()

        seen_ips = {e.get("ip", "").removeprefix("http://").removeprefix("https://")
                    for e in load_db() if e.get("ip")}
        for ip, info in cached_vms.items():
            if ip in seen_ips:
                continue
            # FIX (scalability bug-fix): include `vmid` in the SD label set so
            # alerts emitted by node_exporter / scaphandre carry it through
            # to main.py — without it, handle_alert() raises ValueError on
            # int(vmid="") and the entire /alert request returns 500.
            labels = {
                "hostname": info.get("hostname", ip),
                "type":     info.get("type", "vm"),
                "env":      info.get("env", "prod"),
            }
            vmid_val = info.get("vmid")
            if vmid_val is not None:
                labels["vmid"] = str(vmid_val)
            out.append({
                "targets": [_build_target_address(ip, info, job)],
                "labels":  labels,
            })

    return out


@app.post("/hosts", status_code=201)
def add_host(entry: HostEntry):
    raw_ip = entry.ip.removeprefix("http://").removeprefix("https://")
    entries = load_db()

    for e in entries:
        if e.get("ip") == raw_ip and e.get("type", "vm") != "probe":
            logger.info(f"[SD] Host {raw_ip} already registered — no-op")
            return {"status": "already_exists", "ip": raw_ip}

    new_entry = {
        "ip":       raw_ip,
        "hostname": entry.hostname or raw_ip,
        "type":     entry.type,
        "env":      entry.env,
    }
    if entry.vmid:
        new_entry["vmid"] = entry.vmid
    entries.append(new_entry)
    _save_db(entries)
    logger.info(f"[SD] Registered {entry.type} host: {raw_ip} ({entry.hostname}) vmid={entry.vmid}")
    return {"status": "added", "ip": raw_ip}


@app.delete("/hosts/{ip:path}")
def remove_host(ip: str):
    ip = ip.removeprefix("http://").removeprefix("https://")
    entries = load_db()
    new_entries = [e for e in entries if e.get("ip") != ip]

    if len(new_entries) == len(entries):
        raise HTTPException(status_code=404, detail=f"Host '{ip}' not found in targets.json")

    _save_db(new_entries)
    logger.info(f"[SD] Removed host: {ip}")
    return {"status": "removed", "ip": ip}


# ─── Probe management ────────────────────────────────────────────────────────
@app.post("/probes", status_code=201)
def add_probe(probe: ProbeEntry):
    """
    Register a new HTTP probe URL — typically the /probe/<svc> endpoint
    on the NGINX LB which forwards to the corresponding upstream.

    Example payload:
        {
          "url":          "http://10.30.30.50:8081/probe/pfe_app",
          "service_name": "pfe_app",
          "namespace":    "pfe",
          "deployment":   "app",
          "env":          "prod"
        }
    """
    entries = load_db()
    for e in entries:
        if e.get("type") == "probe" and e.get("url") == probe.url:
            return {"status": "already_exists", "url": probe.url}

    entries.append({
        "type":         "probe",
        "url":          probe.url,
        "ip":           probe.url,                  # for backward compat
        "hostname":     probe.service_name,
        "service_name": probe.service_name,
        "namespace":    probe.namespace,
        "deployment":   probe.deployment or probe.service_name,
        "env":          probe.env,
    })
    _save_db(entries)
    logger.info(f"[SD] Registered probe: {probe.url} -> {probe.namespace}/{probe.deployment or probe.service_name}")
    return {"status": "added", "url": probe.url}


@app.delete("/probes")
def remove_probe(url: str):
    entries = load_db()
    new_entries = [e for e in entries if not (e.get("type") == "probe" and e.get("url") == url)]

    if len(new_entries) == len(entries):
        raise HTTPException(status_code=404, detail=f"Probe '{url}' not found")

    _save_db(new_entries)
    logger.info(f"[SD] Removed probe: {url}")
    return {"status": "removed", "url": url}


@app.post("/thresholds")
def update_thresholds(payload: ThresholdUpdate):
    new_values = {k: v for k, v in payload.model_dump().items() if v is not None}
    if not new_values:
        raise HTTPException(status_code=422, detail="No threshold values provided")

    success = _apply_thresholds(new_values)
    return {
        "status":   "updated" if success else "write_error",
        "applied":  new_values,
        "current":  _current_thresholds.copy(),
    }


@app.get("/thresholds")
def get_thresholds():
    return {"thresholds": _current_thresholds.copy()}


@app.get("/health")
def health():
    return {
        "status":              "ok",
        "managed_jobs":        list(PORT_MAP.keys()),
        "routing":             {job: sorted(types) for job, types in JOB_TYPES.items()},
        "current_thresholds":  _current_thresholds.copy(),
        "failed_pms":          sorted(_failed_pms),
        "cached_vms_count":    len(_vm_cache),
        "vm_scaling_yml":      VM_SCALING_YML,
        "prometheus_url":      PROMETHEUS_URL,
        "main_url":            MAIN_URL,
    }
