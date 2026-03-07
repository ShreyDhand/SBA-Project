import connexion
import logging
import logging.config
import yaml
import json
import os
import requests
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler

# Load configuration files

with open("app_conf.yml", "r") as f:
    app_config = yaml.safe_load(f.read())

with open("log_conf.yml", "r") as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger("basicLogger")

STATS_FILE = app_config["datastore"]["filename"]


# GET /stats endpoint

def get_stats():
    logger.info("GET /stats request received")

    if not os.path.exists(STATS_FILE):
        logger.error("Statistics do not exist")
        return {"message": "Statistics do not exist"}, 404

    with open(STATS_FILE, "r") as f:
        stats = json.load(f)

    logger.info("GET /stats request completed")
    return stats, 200

# Periodic processing job

def populate_stats():
    logger.info("Periodic processing started")

    # Default stats
    stats = {
        "num_shots": 0,
        "num_penalties": 0,
        "max_shots_last_5_minutes": 0,
        "min_shots_last_5_minutes": None,
        "max_penalties_last_5_minutes": 0,
        "min_penalties_last_5_minutes": None,
        "last_updated": datetime.now().isoformat()
    }

    # Load existing stats if present
    if os.path.exists(STATS_FILE):
        with open(STATS_FILE, "r") as f:
            stats = json.load(f)

    # Ensure keys exist
    stats.setdefault("num_shots", 0)
    stats.setdefault("num_penalties", 0)
    stats.setdefault("max_shots_last_5_minutes", 0)
    stats.setdefault("min_shots_last_5_minutes", None)
    stats.setdefault("max_penalties_last_5_minutes", 0)
    stats.setdefault("min_penalties_last_5_minutes", None)
    stats.setdefault("last_updated", datetime.now().isoformat())

    # Time window
    try:
        start_ts = int(datetime.fromisoformat(stats["last_updated"]).timestamp())
    except Exception:
        start_ts = 1

    end_ts = int(datetime.now().timestamp())

    # Fetch shots

    try:
        logger.info("Fetching shot events")
        shots_resp = requests.get(
            app_config["eventstore"]["shots"]["url"],
            params={
                "start_timestamp": start_ts,
                "end_timestamp": end_ts
            }
        )

        if shots_resp.status_code == 200:
            shots = shots_resp.json()
            logger.info("Received %d shot events", len(shots))
            stats["num_shots"] += len(shots)

            if shots:
                current_max = max(s["shots_last_5_minutes"] for s in shots)
                current_min = min(s["shots_last_5_minutes"] for s in shots)

                stats["max_shots_last_5_minutes"] = max(
                    stats["max_shots_last_5_minutes"], current_max
                )

                if stats["min_shots_last_5_minutes"] is None:
                    stats["min_shots_last_5_minutes"] = current_min
                else:
                    stats["min_shots_last_5_minutes"] = min(
                        stats["min_shots_last_5_minutes"], current_min
                    )
        else:
            logger.error("Shot request failed with status code %d", shots_resp.status_code)

    except Exception as e:
        logger.error("Error fetching shot events: %s", e)

    # Fetch penalties

    try:
        logger.info("Fetching penalty events")
        penalties_resp = requests.get(
            app_config["eventstore"]["penalties"]["url"],
            params={
                "start_timestamp": start_ts,
                "end_timestamp": end_ts
            }
        )

        if penalties_resp.status_code == 200:
            penalties = penalties_resp.json()
            logger.info("Received %d penalty events", len(penalties))
            stats["num_penalties"] += len(penalties)

            if penalties:
                current_max = max(p["penalties_last_5_minutes"] for p in penalties)
                current_min = min(p["penalties_last_5_minutes"] for p in penalties)

                stats["max_penalties_last_5_minutes"] = max(
                    stats["max_penalties_last_5_minutes"], current_max
                )

                if stats["min_penalties_last_5_minutes"] is None:
                    stats["min_penalties_last_5_minutes"] = current_min
                else:
                    stats["min_penalties_last_5_minutes"] = min(
                        stats["min_penalties_last_5_minutes"], current_min
                    )
        else:
            logger.error("Penalty request failed with status code %d", penalties_resp.status_code)

    except Exception as e:
        logger.error("Error fetching penalty events: %s", e)

    # Update timestamp
    stats["last_updated"] = datetime.now().isoformat()

    # Write stats to file
    with open(STATS_FILE, "w") as f:
        json.dump(stats, f, indent=4)

    logger.debug("Updated stats: %s", stats)
    logger.info("Periodic processing ended")


# Scheduler setup

def init_scheduler():
    sched = BackgroundScheduler(daemon=True)
    sched.add_job(
        populate_stats,
        "interval",
        seconds=app_config["scheduler"]["interval"]
    )
    sched.start()
    logger.info("Scheduler started")


# App startup

app = connexion.FlaskApp(__name__, specification_dir="")
app.add_api("projectPt1API.yaml", strict_validation=True, validate_responses=True)

if __name__ == "__main__":
    init_scheduler()
    app.run(port=8100, host="0.0.0.0")
