import json
import re
import uuid
import requests
from typing import Any, Dict, Optional
import time

from apscheduler.schedulers.background import BackgroundScheduler
from scheduler_db import (
    fetch_all_jobs,
    fetch_enabled_jobs,
    fetch_job,
    save_job,
    delete_job,
    enable_job,
    disable_job,
    log_job_run,
)

# ══════════════════════════════════════════════════════════════
#  Config
# ══════════════════════════════════════════════════════════════

AGENTS = {
    "stock_agent": "https://stockagent-fsdkdgfyhhe6cfcp.francecentral-01.azurewebsites.net/prompt",
    "mentor_agent" : "https://mentoragent-eaftcvb3cjf6gzfr.francecentral-01.azurewebsites.net/prompt"
}

scheduler = BackgroundScheduler()


# ══════════════════════════════════════════════════════════════
#  Core job runner
# ═════════════════════════════════════════════════════════════

def send_agent_prompt(job_id: str, agent: str, prompt: str) -> None:
    """Called by APScheduler at the scheduled time."""
    if agent not in AGENTS:
        msg = f"Unknown agent '{agent}'. Known: {list(AGENTS.keys())}"
        log_job_run(job_id, status="error", message=msg)
        print(f"[ERROR] {msg}")
        return
    
    max_attempts = 5
    for attempt in range(max_attempts):
        try:
            response = requests.post(
                AGENTS[agent],
                json={"prompt": prompt},
                headers={"Content-Type": "application/json"},
                timeout=100,  # Short timeout for responsiveness
            )
            response.raise_for_status()
            
            if response.text:
                log_job_run(job_id, status="success", message=response.text[:500])
                print(f"[OK] job_id={job_id}  agent={agent}  status={response.status_code}")
                return  # Exit on success
            
            print(f"[WARNING] job_id={job_id}  Received empty response.")
        
        except requests.exceptions.Timeout:
            print(f"[WARNING] job_id={job_id}  Attempt {attempt+1} timed out.")
        except Exception as e:
            log_job_run(job_id, status="error", message=str(e))
            print(f"[ERROR] job_id={job_id}  error={e}")

        # Wait before retrying, with a simple increasing backoff
        time.sleep(60)  # Back off: 1, 2, 4, 8, ... seconds

    print(f"[ERROR] job_id={job_id}  Max attempts reached. Request failed.")



# ══════════════════════════════════════════════════════════════
#  Load / Reload jobs from MongoDB into APScheduler
# ══════════════════════════════════════════════════════════════

def load_jobs_into_scheduler() -> None:
    """Clears all APScheduler jobs and reloads enabled ones from MongoDB."""
    for job in scheduler.get_jobs():
        scheduler.remove_job(job.id)

    jobs = fetch_enabled_jobs()
    for j in jobs:
        scheduler.add_job(
            send_agent_prompt,
            j["trigger"],
            id=j["job_id"],
            kwargs={
                "job_id": j["job_id"],
                "agent":  j["agent"],
                "prompt": j["prompt"],
            },
            max_instances=1,
            coalesce=True,
            misfire_grace_time=120,
            replace_existing=True,
            **j["trigger_args"],
        )
    print(f"[Scheduler] Loaded {len(jobs)} enabled job(s) from MongoDB.")


# ══════════════════════════════════════════════════════════════
#  Add / Update
# ══════════════════════════════════════════════════════════════

def add_or_update_task(
    agent: str,
    prompt: str,
    trigger: str,
    trigger_args: Dict[str, Any],
    *,
    job_id: Optional[str] = None,
    enabled: bool = True,
) -> str:
    """
    Save a job to MongoDB and register it with APScheduler.

    trigger examples:
      cron     → {"hour": 9, "minute": 0, "timezone": "Asia/Taipei"}
      interval → {"hours": 2}
      date     → {"run_date": "2026-03-10T09:00:00+00:00"}
    """
    if agent not in AGENTS:
        raise ValueError(f"Unknown agent '{agent}'. Known: {list(AGENTS.keys())}")
    if trigger not in {"cron", "interval", "date"}:
        raise ValueError("trigger must be one of: cron, interval, date")
    if not isinstance(trigger_args, dict):
        raise ValueError("trigger_args must be a dict")

    job_id = save_job(
        agent=agent,
        prompt=prompt,
        trigger=trigger,
        trigger_args=trigger_args,
        enabled=enabled,
        job_id=job_id,
    )

    if enabled and scheduler.running:
        scheduler.add_job(
            send_agent_prompt,
            trigger,
            id=job_id,
            kwargs={"job_id": job_id, "agent": agent, "prompt": prompt},
            max_instances=1,
            coalesce=True,
            misfire_grace_time=120,
            replace_existing=True,
            **trigger_args,
        )
        print(f"[Scheduler] Job '{job_id}' registered.")
    else:
        print(f"[Scheduler] Job '{job_id}' saved but not scheduled (disabled or scheduler not running).")

    return job_id


# ══════════════════════════════════════════════════════════════
#  Enable / Disable / Delete
# ══════════════════════════════════════════════════════════════

def update_job_status(job_id: str, enabled: bool) -> bool:
    job = fetch_job(job_id)
    if not job:
        return False
    if enabled:
        enable_job(job_id)
        if scheduler.running:
            scheduler.add_job(
                send_agent_prompt,
                job["trigger"],
                id=job_id,
                kwargs={"job_id": job_id, "agent": job["agent"], "prompt": job["prompt"]},
                max_instances=1,
                coalesce=True,
                misfire_grace_time=120,
                replace_existing=True,
                **job["trigger_args"],
            )
    else:
        disable_job(job_id)
        try:
            scheduler.remove_job(job_id)
        except Exception:
            pass
    return True


def delete_job_from_db(job_id: str) -> bool:
    job = fetch_job(job_id)
    if not job:
        return False
    delete_job(job_id)
    try:
        scheduler.remove_job(job_id)
    except Exception:
        pass
    return True


# ══════════════════════════════════════════════════════════════
#  handle_user_text — LINE command parser
# ══════════════════════════════════════════════════════════════

def handle_user_text(text: str) -> str:
    parts = text.strip().split(maxsplit=1)
    if not parts or not parts[0]:
        return "Please provide a command. Type 'help' for available commands."

    command  = parts[0].lower()
    args_str = parts[1] if len(parts) > 1 else ""

    response = "Unknown command or invalid format. Type 'help' for available commands."

    # ── help ──────────────────────────────────────────────────
    if command == "help":
        response = """
Available commands:

1. schedule <agent_name> "<prompt>" <trigger_type> <trigger_args_json> [job_id=<id>]
   - agent_name  : name of the agent (e.g. stock_agent)
   - prompt      : in double quotes
   - trigger_type: cron | interval | date
   - trigger_args: JSON string
     cron     → {"day_of_week":"mon-fri","hour":9,"minute":0,"timezone":"Asia/Taipei"}
     interval → {"minutes": 15}
     date     → {"run_date": "2026-03-10T09:00:00+00:00"}
   - job_id (optional): provide to update an existing job

   Example:
   schedule stock_agent "Give me a market summary" cron {"day_of_week":"mon-fri","hour":9,"minute":0,"timezone":"Asia/Taipei"}

2. list jobs
3. enable job <job_id>
4. disable job <job_id>
5. delete job <job_id>
6. help
"""

    # ── schedule ──────────────────────────────────────────────
    elif command == "schedule":
        try:
            args_str = args_str.replace("\u201c", '"').replace("\u201d", '"')
            match = re.match(r'(\w+)\s+"([^"]*)"\s+(\w+)\s+(.*)', args_str.strip())
            if not match:
                raise ValueError(
                    "Invalid format. Use: schedule <agent_name> \"<prompt>\" <trigger_type> <trigger_args_json>"
                )

            agent_name   = match.group(1)
            if agent_name not in AGENTS:
                raise ValueError(f"Unknown agent '{agent_name}'. Known: {list(AGENTS.keys())}")
            prompt       = match.group(2)
            trigger_type = match.group(3).lower()
            remaining    = match.group(4).strip()

            job_id = None
            job_id_match = re.search(r'\s+job_id=([\w-]+)$', remaining)
            if job_id_match:
                job_id    = job_id_match.group(1)
                remaining = re.sub(r'\s+job_id=[\w-]+$', '', remaining).strip()

            trigger_args = json.loads(remaining)
            new_job_id   = add_or_update_task(
                agent=agent_name,
                prompt=prompt,
                trigger=trigger_type,
                trigger_args=trigger_args,
                job_id=job_id,
            )

            if scheduler.running:
                response = f"Job '{new_job_id}' scheduled successfully."
            else:
                response = f"Job '{new_job_id}' saved. Start the scheduler for it to take effect."

        except ValueError as e:
            response = f"Error scheduling job: {e}"
        except json.JSONDecodeError:
            response = "Error parsing trigger_args JSON. Ensure it is valid JSON."
        except Exception as e:
            response = f"Unexpected error: {e}"

    # ── list jobs ─────────────────────────────────────────────
    elif command == "list" and args_str.lower() == "jobs":
        try:
            jobs = fetch_all_jobs()
            if not jobs:
                response = "No jobs scheduled."
            else:
                lines = ["── Scheduled Jobs ──────────────────────"]
                for job in jobs:
                    status = "enabled" if job.get("enabled", True) else "disabled"
                    last   = job.get("last_run_at", "never")
                    lines.append(f"\nID     : {job['job_id']}")
                    lines.append(f"Agent  : {job['agent']}")
                    lines.append(f"Prompt : \"{job['prompt']}\"")
                    lines.append(f"Trigger: {job['trigger']} {json.dumps(job['trigger_args'])}")
                    lines.append(f"Status : {status}  |  Last run: {last}")
                    lines.append("─" * 40)
                response = "\n".join(lines)
        except Exception as e:
            response = f"Error listing jobs: {e}"

    # ── enable / disable / delete ─────────────────────────────
    elif command in ("enable", "disable", "delete"):
        try:
            cmd_parts = args_str.strip().split(maxsplit=1)
            if len(cmd_parts) < 2 or cmd_parts[0].lower() != "job":
                raise ValueError(f"Use: {command} job <job_id>")
            job_id = cmd_parts[1]

            if command in ("enable", "disable"):
                enabled = command == "enable"
                if update_job_status(job_id, enabled):
                    action = "enabled" if enabled else "disabled"
                    response = f"Job '{job_id}' {action}."
                else:
                    response = f"Job '{job_id}' not found."

            elif command == "delete":
                if delete_job_from_db(job_id):
                    response = f"Job '{job_id}' deleted."
                else:
                    response = f"Job '{job_id}' not found."

        except ValueError as e:
            response = f"Error: {e}"
        except Exception as e:
            response = f"Unexpected error: {e}"

    return response


# ══════════════════════════════════════════════════════════════
#  Startup
# ══════════════════════════════════════════════════════════════

_scheduler_started = False

def start_scheduler():
    global _scheduler_started
    if _scheduler_started:
        return

    print("[Scheduler] Initializing...")
    scheduler.start()
    print("[Scheduler] Running:", scheduler.running)

    load_jobs_into_scheduler()
    _scheduler_started = True

    print("[Scheduler] Jobs currently loaded:")
    for job in scheduler.get_jobs():
        print(job)
