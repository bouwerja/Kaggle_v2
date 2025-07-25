import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_HOSTNAME = os.getenv("DATABASE_HOST_IP")
ACTIVE_USERNAME = os.getenv("DATABASE_USERNAME")
ACTIVE_USER_PWD = os.getenv("USER_PASSWORD")
ACTIVE_DATABASE = os.getenv("DATABASE_NAME")