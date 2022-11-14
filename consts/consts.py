import os

DATA_HOST = os.environ["DATA_HOST"] # "data.gharchive.org"
GH_FILE_URL_TEMPLATE = "https://{data_host}/{date_hour}.json.gz"
POSTGRES_JDBC_PATH = "./drivers/postgresql-42.5.0.jar"
SPARK_APP_NAME = "gh-data-processor"
GAINED_OWNERSHIP_EVENTS = ["ForkEvent", "CreateEvent", "PublicEvent"]
LOST_OWNERSHIP_EVENTS = ["DeleteEvent", "PrivateEvent"]

class Event:
    TYPE = "type"
    PAYLOAD = "payload"
    ACTOR_ID = "actor.id"
    ACTOR_LOGIN = "actor.login"
USER_TABLE = "Users"
class User:
    ID = "id"
    LOGIN = "login"

class Counts:
    REP_OWNERSHIP = "rep_ownership_count"
    COMMITS = "commits_count"

class DB_Tables:
    USERS = "Users"
    COMMITS = "Commits"
class DB_Creds:
    HOST = os.environ["db_host"]
    PORT = os.environ["db_port"]
    NAME = os.environ["db_name"]
    USER = os.environ["db_user"]
    PASSWORD = os.environ["db_password"]
