from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import yaml

# Load DB configuration from file
with open("/config/storage_config.yml", "r") as f:
    app_config = yaml.safe_load(f.read())

db_conf = app_config["datastore"]

# MySQL connection string from config
DB_URL = (
    f"mysql+pymysql://{db_conf['user']}:{db_conf['password']}"
    f"@{db_conf['hostname']}:{db_conf['port']}/{db_conf['db']}"
)

# FIX FOR PART 4: Add Pooling Parameters
ENGINE = create_engine(
    DB_URL,
    pool_size=5,          # Keeps 5 connections ready in the "pool"
    pool_recycle=300,      # Closes/restarts connections every 5 minutes (300s)
    pool_pre_ping=True     # "Taps" the DB before using it to see if it's awake
)

def make_session():
    return sessionmaker(bind=ENGINE)()