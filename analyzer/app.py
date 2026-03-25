import connexion
from connexion import NoContent
import yaml
import json
import logging
import logging.config
from pykafka import KafkaClient
from connexion.middleware import MiddlewarePosition 
from starlette.middleware.cors import CORSMiddleware

# Load Configuration
with open('/config/analyzer_config.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

# Load Logging
with open('/config/analyzer_log_config.yml', 'r') as f:
    log_conf = yaml.safe_load(f)
    logging.config.dictConfig(log_conf)

logger = logging.getLogger('basicLogger')

def get_shot_reading(index):
    """ Get a shot event from the Kafka queue at a specific index """

    kafka_host = f"{app_config['events']['hostname']}:{app_config['events']['port']}"
    client = KafkaClient(hosts=kafka_host)
    topic = client.topics[app_config['events']['topic'].encode()]

    consumer = topic.get_simple_consumer(reset_offset_on_start=True, 
                                         consumer_timeout_ms=1000)

    counter = 0
    logger.info(f"Searching for shot at index {index}...")

    for msg in consumer:
        message = msg.value.decode('utf-8')
        msg_payload = json.loads(message)

        # Filter by the type (Receiver)
        if msg_payload.get('type') == 'shot':
            if counter == index:
                logger.info(f"Found shot at index {index}")
                return msg_payload['payload'], 200
            counter += 1

    logger.error(f"Shot at index {index} not found in Kafka")
    return {"message": f"Shot event at index {index} not found"}, 404

def get_penalty_reading(index):
    """ Get a penalty event from the Kafka queue at a specific index """
    kafka_host = f"{app_config['events']['hostname']}:{app_config['events']['port']}"
    client = KafkaClient(hosts=kafka_host)
    topic = client.topics[app_config['events']['topic'].encode()]

    consumer = topic.get_simple_consumer(reset_offset_on_start=True, 
                                         consumer_timeout_ms=1000)

    counter = 0
    logger.info(f"Searching for penalty at index {index}...")

    for msg in consumer:
        message = msg.value.decode('utf-8')
        msg_payload = json.loads(message)

        if msg_payload.get('type') == 'penalty':
            if counter == index:
                logger.info(f"Found penalty at index {index}")
                return msg_payload['payload'], 200
            counter += 1

    logger.error(f"Penalty at index {index} not found in Kafka")
    return {"message": f"Penalty event at index {index} not found"}, 404

def get_reading_stats():
    """ Get counts of all events currently sitting in the Kafka topic """
    kafka_host = f"{app_config['events']['hostname']}:{app_config['events']['port']}"
    client = KafkaClient(hosts=kafka_host)
    topic = client.topics[app_config['events']['topic'].encode()]
    
    consumer = topic.get_simple_consumer(reset_offset_on_start=True, 
                                         consumer_timeout_ms=1000)

    num_shots = 0
    num_penalties = 0

    for msg in consumer:
        message = msg.value.decode('utf-8')
        msg_payload = json.loads(message)
        
        if msg_payload.get('type') == 'shot':
            num_shots += 1
        elif msg_payload.get('type') == 'penalty':
            num_penalties += 1

    logger.info(f"Stats calculated: {num_shots} shots, {num_penalties} penalties")
    return {
        "num_shot_readings": num_shots,
        "num_penalty_readings": num_penalties
    }, 200

def check_health():
    """ 
    Health check endpoint that always returns 200 
    if the service is reachable.
    """
    return {"status": "OK"}, 200

app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("projectPt1API.yaml", strict_validation=True, validate_responses=True)

app.add_middleware(
    CORSMiddleware,
    position=MiddlewarePosition.BEFORE_EXCEPTION,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

if __name__ == "__main__":
    app.run(port=8110, host="0.0.0.0")