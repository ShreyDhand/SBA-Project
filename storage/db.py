from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import yaml

# Load DB configuration from file
with open("app_conf.yml", "r") as f:
    app_config = yaml.safe_load(f.read())

db_conf = app_config["datastore"]

# MySQL connection string from config
DB_URL = (
    f"mysql+pymysql://{db_conf['user']}:{db_conf['password']}"
    f"@{db_conf['hostname']}:{db_conf['port']}/{db_conf['db']}"
)

ENGINE = create_engine(DB_URL)

def make_session():
    return sessionmaker(bind=ENGINE)()
