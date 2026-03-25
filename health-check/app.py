import connexion
import json
import logging
import logging.config
import yaml
import requests
import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from flask_cors import CORS

# Load Configs
with open('app_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

with open('log_conf.yml', 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('health_logger')

def init_scheduler():
    sched = BackgroundScheduler(daemon=True)
    sched.add_job(check_health, 'interval', seconds=app_config['scheduler']['interval_seconds'])
    sched.start()

def check_health():
    """Polls the health endpoints of all services."""
    logger.info("Starting health check for all services")
    
    statuses = {
        "receiver": "Down",
        "storage": "Down",
        "processing": "Down",
        "analyzer": "Down",
        "last_update": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    }

    # Define the mapping of service names to their URLs from config
    services = {
        "receiver": app_config['receiver']['url'],
        "storage": app_config['storage']['url'],
        "processing": app_config['processing']['url'],
        "analyzer": app_config['analyzer']['url']
    }

    for service_name, url in services.items():
        try:
            # Using a 5-second timeout as required by the assignment [cite: 14]
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                statuses[service_name] = "Up"
        except Exception as e:
            logger.error(f"Error connecting to {service_name}: {e}")

    # Store the results in a JSON file [cite: 15]
    with open(app_config['datastore']['filename'], 'w') as f:
        json.dump(statuses, f, indent=4)
    
    logger.info(f"Recorded service statuses: {statuses}")

def get_all_service_statuses():
    """GET /health/status endpoint logic [cite: 16]"""
    logger.info("Request for all service statuses received")
    try:
        with open(app_config['datastore']['filename'], 'r') as f:
            status_data = json.load(f)
        return status_data, 200
    except FileNotFoundError:
        return {"message": "Health data not yet initialized"}, 404

app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("openapi.yaml")
CORS(app.app)

if __name__ == "__main__":
    init_scheduler()
    app.run(host='0.0.0.0', port=8120)