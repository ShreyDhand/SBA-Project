import connexion
from connexion import NoContent
import json
import uuid
import yaml
import logging
import logging.config
import datetime
import time
from pykafka import KafkaClient 

# Config & Logging 
with open("/config/receiver_config.yml", "r") as f:
    app_config = yaml.safe_load(f.read())

with open("/config/receiver_log_config.yml", "r") as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger("basicLogger")

# Kafka Setup with Retry Logic (Part 3) 
producer = None

def init_kafka():
    """ Keeps trying to connect to Kafka until successful """
    global producer
    hostname = f'{app_config["events"]["hostname"]}:{app_config["events"]["port"]}'
    
    while producer is None:
        try:
            logger.info(f"Connecting to Kafka at {hostname}...")
            # This is the line that was crashing!
            client = KafkaClient(hosts=hostname) 
            topic = client.topics[str.encode(app_config["events"]["topic"])]
            
            producer = topic.get_producer() 
            logger.info("Successfully connected to Kafka!")
        except Exception as e:
            # Instead of crashing, we log the error and wait
            logger.error(f"Kafka connection failed: {e}. Retrying in 5 seconds...")
            time.sleep(5)

# Initialize Kafka before the app starts
init_kafka()

def report_shot_batch(body):
    trace_id = str(uuid.uuid4())
    logger.info(f"Received shot batch event with trace id {trace_id}")

    for shot in body.get("shots", []):
        msg = {
            "type": "shot",
            "datetime": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "payload": {
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
        }
        msg_str = json.dumps(msg)
        # Use the producer initialized in the retry loop
        producer.produce(msg_str.encode('utf-8'))

    return NoContent, 201

def report_penalty_batch(body):
    trace_id = str(uuid.uuid4())
    logger.info(f"Received penalty batch event with trace id {trace_id}")

    for pen in body.get("penalties", []):
        msg = {
            "type": "penalty",
            "datetime": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "payload": {
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
        }
        msg_str = json.dumps(msg)
        producer.produce(msg_str.encode('utf-8'))

    return NoContent, 201

def check_health():
    return {"status": "OK"}, 200

app = connexion.FlaskApp(__name__, specification_dir="")
app.add_api("projectPt1API.yaml", base_path="/receiver", strict_validation=True, validate_responses=True)

if __name__ == "__main__":
    app.run(port=8080, host="0.0.0.0")