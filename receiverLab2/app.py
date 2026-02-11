import connexion
from connexion import NoContent
import json
import os
from datetime import datetime
import httpx
import uuid
import yaml
import logging
import logging.config


with open("app_conf.yml", "r") as f:
    app_config = yaml.safe_load(f.read())

with open("log_conf.yml", "r") as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

# Create logger instance 
logger = logging.getLogger("basicLogger")


# Extract storage service endpoints from config
shots_url = app_config["eventstore"]["shots"]["url"]
penalties_url = app_config["eventstore"]["penalties"]["url"]



MAX_BATCH_EVENTS = 5
SHOTS_FILE = "shots.json"
PENALTIES_FILE = "penalties.json"

def update_storage(file_path, body, record_key, value_to_average, count_key, avg_key_name):
    if os.path.exists(file_path):
            with open(file_path, 'r', encoding='utf-8') as f:
                store = json.load(f)
    else:
        store = {count_key: 0, "recent_batch_data": []}

    # Update total batches
    store[count_key] += 1

    # Loop to get average
    records = body.get(record_key, [])
    total_val = 0
    num_records = len(records)

    for item in records:
        # read shots/penalties_last_5_minutes (value_to_average) add to total 
        total_val += int(item.get(value_to_average, 0))
    # get avrage, if num_recors is 0, then 0
    average_val = total_val / num_records if num_records > 0 else 0

    # batch summery
    new_entry = {
    avg_key_name: round(average_val, 2),
    f"num_{record_key}_readings": num_records,
    "received_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
}

    # Adds batch summary to the end of the list
    store["recent_batch_data"].append(new_entry)
    # if list is over 5, remove the odlest one
    if len(store["recent_batch_data"]) > MAX_BATCH_EVENTS:
        store["recent_batch_data"].pop(0)
    # overwrite old
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(store, f, indent=4)

def report_shot_batch(body):
        status_code = 201  # default success

        # Generate a unique trace_id per batch to track events
        trace_id = str(uuid.uuid4()) # uuid4 = random

        # Log shot batch with trace ID 
        logger.info(f"Received shot batch event with trace id {trace_id}")


        # give me list under shots if missing use empty list
        for shot in body.get("shots", []):
            # build dict with all the stuff
            payload = {
                "trace_id": trace_id,
                "arena_id": body["arena_id"],
                "batch_timestamp": body["batch_timestamp"],
                "batch_count": body["batch_count"],
                "game_id": shot["game_id"],
                "period": shot["period"],
                "shot_type": shot["shot_type"],
                "game_time_seconds": shot["game_time_seconds"],
                "shots_last_5_minutes": shot["shots_last_5_minutes"],
            }

            r = httpx.post(shots_url, json=payload)
            status_code = r.status_code

        # if anything fails, stop early
            if status_code >= 400:
                return NoContent, status_code

        return NoContent, status_code

def report_penalty_batch(body):
    status_code = 201  # default success

    trace_id = str(uuid.uuid4())
    logger.info(f"Received penalty batch event with trace id {trace_id}")

    for pen in body.get("penalties", []):
        payload = {
            "trace_id": trace_id,
            "arena_id": body["arena_id"],
            "batch_timestamp": body["batch_timestamp"],
            "batch_count": body["batch_count"],
            "game_id": pen["game_id"],
            "period": pen["period"],
            "penalty_type": pen["penalty_type"],
            "game_time_seconds": pen["game_time_seconds"],
            "penalties_last_5_minutes": pen["penalties_last_5_minutes"],
        }

        r = httpx.post(penalties_url, json=payload)

        status_code = r.status_code

        if status_code >= 400:
            return NoContent, status_code

    return NoContent, status_code


app = connexion.FlaskApp(__name__, specification_dir="")
app.add_api("projectPt1API.yaml", strict_validation=True, validate_responses=True)

if __name__ == "__main__":
    app.run(port=8080)
