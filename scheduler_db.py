import os
import uuid
from datetime import datetime, timezone
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
from dotenv import load_dotenv

load_dotenv()

# ══════════════════════════════════════════════════════════════
#  Connection
# ══════════════════════════════════════════════════════════════

#DB_PASSWORD = os.environ.get("MONGO_DB_PASSWORD")

#if not DB_PASSWORD:
#    raise ValueError("MONGO_DB_PASSWORD environment variable not set.")

MONGO_URI = f"mongodb+srv://vincejim91126_db_user:phmyoFTi1aAEhY73@schedulerdb.rf9dlg4.mongodb.net/scheduled_jobs?retryWrites=true&w=majority&appName=schedulerdb"

COLLECTION_NAME = "schedulerdb"

try:
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    print("scheduler_db: Connected to MongoDB!")
except (ConnectionFailure, ServerSelectionTimeoutError) as e:
    print(f"scheduler_db: Failed to connect to MongoDB: {e}")
    client = None

db = client.get_database() if client else None


def _col():
    """Returns the scheduled_jobs collection."""
    if db is None:
        raise RuntimeError("Database connection not available.")
    return db[COLLECTION_NAME]


# ══════════════════════════════════════════════════════════════
#  Save / Upsert a job
# ══════════════════════════════════════════════════════════════

def save_job(agent: str, prompt: str, trigger: str, trigger_args: dict,
             enabled: bool = True, job_id: str = None) -> str:
    """
    Insert or update a scheduled job.
    job_id is auto-generated if not provided.

    Parameters
    ----------
    agent        : agent name e.g. 'stock_agent'
    prompt       : the prompt string to send to the agent
    trigger      : APScheduler trigger type — 'cron', 'interval', or 'date'
    trigger_args : dict of trigger kwargs, e.g.
                     cron     → {'hour': 9, 'minute': 0, 'timezone': 'Asia/Taipei'}
                     interval → {'hours': 2}
                     date     → {'run_date': '2026-03-10 09:00:00'
    enabled      : whether the job is active
    job_id       : optional — auto-generated as '{agent}_{uuid}' if not provided
    """
    if job_id is None:
        job_id = f"{agent}_{uuid.uuid4().hex[:8]}"

    doc = {
        "job_id":       job_id,
        "agent":        agent,
        "prompt":       prompt,
        "trigger":      trigger,
        "trigger_args": trigger_args,
        "enabled":      enabled,
        "created_at":   datetime.now(timezone.utc).isoformat(),
        "updated_at":   datetime.now(timezone.utc).isoformat(),
    }
    _col().update_one(
        {"job_id": job_id},
        {"$set": doc},
        upsert=True
    )
    return job_id


# ══════════════════════════════════════════════════════════════
#  Fetch jobs
# ══════════════════════════════════════════════════════════════

def fetch_job(job_id):
    """Fetch a single job by job_id. Returns None if not found."""
    return _col().find_one({"job_id": job_id}, {"_id": 0})


def fetch_all_jobs() -> list:
    """Fetch all jobs regardless of enabled status."""
    return list(_col().find({}, {"_id": 0}))


def fetch_enabled_jobs() -> list:
    """Fetch only enabled jobs — used on app startup to re-register with APScheduler."""
    return list(_col().find({"enabled": True}, {"_id": 0}))


# ══════════════════════════════════════════════════════════════
#  Enable / Disable
# ══════════════════════════════════════════════════════════════

def enable_job(job_id: str) -> str:
    result = _col().update_one(
        {"job_id": job_id},
        {"$set": {"enabled": True, "updated_at": datetime.now(timezone.utc).isoformat()}}
    )
    return f"Job '{job_id}' enabled." if result.modified_count else f"Job '{job_id}' not found."


def disable_job(job_id: str) -> str:
    result = _col().update_one(
        {"job_id": job_id},
        {"$set": {"enabled": False, "updated_at": datetime.now(timezone.utc).isoformat()}}
    )
    return f"Job '{job_id}' disabled." if result.modified_count else f"Job '{job_id}' not found."


# ══════════════════════════════════════════════════════════════
#  Delete
# ══════════════════════════════════════════════════════════════

def delete_job(job_id: str) -> str:
    result = _col().delete_one({"job_id": job_id})
    return f"Job '{job_id}' deleted." if result.deleted_count else f"Job '{job_id}' not found."


def delete_all_jobs() -> str:
    result = _col().delete_many({})
    return f"Deleted {result.deleted_count} job(s)."


# ══════════════════════════════════════════════════════════════
#  Log last run result (optional but useful)
# ══════════════════════════════════════════════════════════════

def log_job_run(job_id: str, status: str, message: str = "") -> None:
    """
    Update a job's last_run info after execution.
    status: 'success' | 'error'
    """
    _col().update_one(
        {"job_id": job_id},
        {"$set": {
            "last_run_at":     datetime.now(timezone.utc).isoformat(),
            "last_run_status": status,
            "last_run_msg":    message[:500],
        }}
    )
