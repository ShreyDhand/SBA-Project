import connexion
from connexion import NoContent
import functools
from datetime import datetime
import logging
import logging.config
import yaml
from datetime import datetime as dt
from sqlalchemy import select



from db import make_session
from models import Shot, Penalty


with open("log_conf.yml", "r") as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger("basicLogger")


def use_db_session(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        session = make_session()
        try:
            return func(session, *args, **kwargs)
        finally:
            session.close()
    return wrapper

@use_db_session
def store_shot(session, body):
    event = Shot( 
        trace_id=body["trace_id"],
        arena_id=body["arena_id"], 
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



def get_shots(start_timestamp, end_timestamp):
# Gets shot events between the start and end timestamps

    # Create a database session
    session = make_session()

    # Convert UNIX timestamps to datetime objects
    start = dt.fromtimestamp(start_timestamp)
    end = dt.fromtimestamp(end_timestamp)

    # Build a query to select shot events in the time range
    statement = (select(Shot).where(Shot.date_created >= start).where(Shot.date_created < end))

    # Execute the query and convert results to dictionaries
    results = [shot.to_dict() for shot in session.execute(statement).scalars().all()]

    # Close database session
    session.close()

    # Log how many shot events were found
    logger.debug("Found %d shot events (start: %s, end: %s)",len(results), start, end)

    return results, 200


def get_penalties(start_timestamp, end_timestamp):
# Gets penalty events between the start and end timestamps
    session = make_session()

    start = dt.fromtimestamp(start_timestamp)
    end = dt.fromtimestamp(end_timestamp)

    statement = (select(Penalty).where(Penalty.date_created >= start).where(Penalty.date_created < end))

    results = [penalty.to_dict() for penalty in session.execute(statement).scalars().all()]

    session.close()

    logger.debug("Found %d penalty events (start: %s, end: %s)",len(results), start, end)

    return results, 200




app = connexion.FlaskApp(__name__, specification_dir="")
app.add_api("projectPt1API.yaml", strict_validation=True, validate_responses=True)

if __name__ == "__main__":
    app.run(port=8090)
