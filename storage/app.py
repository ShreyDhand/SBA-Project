import connexion
from connexion import NoContent
import functools
from datetime import datetime
import logging
import logging.config
import yaml
from datetime import datetime as dt
from sqlalchemy import select
import threading
import json
from pykafka import KafkaClient
from pykafka.common import OffsetType
from threading import Thread

# From your local files
from db import make_session
from models import Shot, Penalty

from models import Base
from db import ENGINE

Base.metadata.create_all(ENGINE)

# --- Load Configurations ---
with open("/config/storage_config.yml", "r") as f:
    app_config = yaml.safe_load(f.read())

with open("/config/storage_log_config.yml", "r") as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger("basicLogger")

# --- Database Decorator ---
def use_db_session(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        session = make_session()
        try:
            return func(session, *args, **kwargs)
        finally:
            session.close()
    return wrapper

# --- Storage Logic ---
@use_db_session
def store_shot(session, body):
    """ Stores a shot event to the database """
    event = Shot( 
        trace_id=body["trace_id"],
        arena_id=body["arena_id"], 
        # Handles both Z and +00:00 formats
        batch_timestamp=datetime.fromisoformat(body["batch_timestamp"].replace("Z", "+00:00")),
        batch_count=body["batch_count"],
        game_id=body["game_id"],
        period=body["period"],
        shot_type=body["shot_type"],
        game_time_seconds=body["game_time_seconds"],
        shots_last_5_minutes=body["shots_last_5_minutes"],
    )
    session.add(event)
    session.commit()
    logger.debug(f"Stored shot event with trace id {body['trace_id']}")
    return NoContent, 201

@use_db_session
def store_penalty(session, body):
    """ Stores a penalty event to the database """
    event = Penalty(
        trace_id=body["trace_id"],
        arena_id=body["arena_id"],
        batch_timestamp=datetime.fromisoformat(body["batch_timestamp"].replace("Z", "+00:00")),
        batch_count=body["batch_count"],
        game_id=body["game_id"],
        period=body["period"],
        penalty_type=body["penalty_type"],
        game_time_seconds=body["game_time_seconds"],
        penalties_last_5_minutes=body["penalties_last_5_minutes"],
    )
    session.add(event)
    session.commit()
    logger.debug(f"Stored penalty event with trace id {body['trace_id']}")
    return NoContent, 201

# --- Kafka Consumer Logic (Part 4) ---
def process_messages():
    """ Process event messages from Kafka """
    hostname = f"{app_config['events']['hostname']}:{app_config['events']['port']}"
    client = KafkaClient(hosts=hostname)
    topic = client.topics[str.encode(app_config['events']['topic'])]

    # Create a consumer group as required by lab
    consumer = topic.get_simple_consumer(
        consumer_group=b'event_group_v3', # New name = New pointer
        reset_offset_on_start=True,        # Force it to look at the start
        auto_offset_reset=OffsetType.EARLIEST
    )

    logger.info("Kafka consumer started listening...")

    # This loop is blocking - it waits for new messages
    for msg in consumer:
        msg_str = msg.value.decode('utf-8')
        msg_obj = json.loads(msg_str)
        logger.info(f"Consumed message: {msg_obj}")

        payload = msg_obj["payload"]
        event_type = msg_obj["type"]

        # Call the existing store functions
        if event_type == "shot":
            try:
                store_shot(body=payload)
            except Exception as e:
                logger.error(f"Error storing shot: {e}")
        elif event_type == "penalty":
            try:
                store_penalty(body=payload)
            except Exception as e:
                logger.error(f"Error storing penalty: {e}")

        # Commit the message as being read
        consumer.commit_offsets()

# --- API Get Endpoints ---
def get_shots(start_timestamp, end_timestamp):
    session = make_session()
    
    # DEBUG: Let's see what's actually in the DB
    all_shots = session.query(Shot).all()
    logger.info(f"DEBUG: Total shots in DB: {len(all_shots)}")
    if len(all_shots) > 0:
        logger.info(f"DEBUG: First shot in DB was created at: {all_shots[0].date_created}")

    start = dt.utcfromtimestamp(start_timestamp)
    end = dt.utcfromtimestamp(end_timestamp)
    logger.info(f"DEBUG: Querying from {start} to {end}")
    
    statement = (select(Shot).where(Shot.date_created >= start).where(Shot.date_created < end))
    results = [shot.to_dict() for shot in session.execute(statement).scalars().all()]
    session.close()
    logger.debug("Found %d shot events", len(results))
    return results, 200

def get_penalties(start_timestamp, end_timestamp):
    session = make_session()
    start = dt.utcfromtimestamp(start_timestamp)
    end = dt.utcfromtimestamp(end_timestamp)
    statement = (select(Penalty).where(Penalty.date_created >= start).where(Penalty.date_created < end))
    results = [penalty.to_dict() for penalty in session.execute(statement).scalars().all()]
    session.close()
    logger.debug("Found %d penalty events", len(results))
    return results, 200

def check_health():
    """ 
    Health check endpoint that always returns 200 
    if the service is reachable.
    """
    return {"status": "OK"}, 200

# --- App Initialization ---
app = connexion.FlaskApp(__name__, specification_dir="")
app.add_api("projectPt1API.yaml", strict_validation=True, validate_responses=True)

# Create the specific function requested in the lab [cite: 118-120]
def setup_kafka_thread():
    """ Function to start the Kafka consumer thread """
    # Use Thread directly as per the lab example 
    t1 = Thread(target=process_messages)
    t1.setDaemon(True)
    t1.start()

if __name__ == "__main__":
    # Call the setup function BEFORE the app.run call [cite: 121-122]
    setup_kafka_thread()
    app.run(port=8090, host="0.0.0.0")
