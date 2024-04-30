"""Module contains all project wide constants."""
import logging
import os
from pathlib import Path
from enum import Enum

import dotenv

dotenv.load_dotenv()


# ---------------- PATH CONSTANTS -------------------
#  Source folder path
constants_path = Path(__file__)
SRC_PATH = constants_path.parent
PROJECT_PATH = SRC_PATH.parent
SECRETS_PATH = PROJECT_PATH / "secrets"

#  Data related paths
DATA_PATH = Path(os.getenv("DATA_PATH"))
RESULTS_PATH = Path(os.getenv("RESULTS_PATH"))
USER_PATH = Path(os.getenv("USER_PATH"))

JRC_PATH = DATA_PATH / os.getenv("AFC_SUBDIR")
GLAD_PATH = DATA_PATH / os.getenv("GLAD_SUBDIR")
BASEMAP_DIR = DATA_PATH / os.getenv("GLAD_BASEMAP_SUBDIR")

# ---------------- API KEYS -------------------------


# ---------------- GOOGLE APIS ----------------------
class GoogleAuthMode(Enum):
    OAUTH_BROWSER = "oauth_browser"
    SERVICE_ACCOUNT = "service_account"


GOOGLE_SA_CREDENTIALS_PATH = (SECRETS_PATH / "gcp_service_credentials.json").as_posix()

GOOGLE_OAUTH_CREDENTIALS_PATH = (
    SECRETS_PATH / "personal_google_oauth_credentials.json"
).as_posix()

# ---------------- LOGGING CONSTANTS ----------------
DEFAULT_FORMATTER = logging.Formatter(
    (
        "%(asctime)s %(levelname)s: %(message)s "
        "[in %(funcName)s at %(pathname)s:%(lineno)d]"
    )
)
DEFAULT_LOG_FILE = LOG_PATH / "default_log.log"
DEFAULT_LOG_LEVEL = logging.INFO  # verbose logging per default

# ---------------- PROJECT CONSTANTS ----------------
# Coordinate reference systems (crs)
WGS84 = "EPSG:4326"  # WGS84 standard crs (latitude, longitude)
WEBMERCATOR = "EPSG:3857"  # CRS for web maps
SIRGAS_BRAZIL = "EPSG:5880"  # Polyconic projected CRS for Brazil

WGS84_UTM18S = "EPSG:32718"  # https://epsg.io/32718
WGS84_UTM19S = "EPSG:32719"  # https://epsg.io/32719
WGS84_UTM20S = "EPSG:32720"  # https://epsg.io/32720
WGS84_UTM21S = "EPSG:32721"  # https://epsg.io/32721
WGS84_UTM22S = "EPSG:32722"  # https://epsg.io/32722
WGS84_UTM23S = "EPSG:32723"  # https://epsg.io/32723
WGS84_UTM24S = "EPSG:32724"  # https://epsg.io/32724

SIRGAS2000_UTM18S = "EPSG:31978"  # https://epsg.io/31978
SIRGAS2000_UTM19S = "EPSG:31979"  # https://epsg.io/31979
SIRGAS2000_UTM20S = "EPSG:31980"  # https://epsg.io/31980
SIRGAS2000_UTM21S = "EPSG:31981"  # https://epsg.io/31981
SIRGAS2000_UTM22S = "EPSG:31982"  # https://epsg.io/31982
SIRGAS2000_UTM23S = "EPSG:31983"  # https://epsg.io/31983
SIRGAS2000_UTM24S = "EPSG:31984"  # https://epsg.io/31984

# ---------------- DATABASE CONSTANTS ----------------
DB_HOST = os.getenv("DB_HOST")  # JASMIN database server
DB_PORT = "5432"
DB_NAME = os.getenv("DB_NAME")  # Database for GEDI shots
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_POSTGRES = "postgresql"
DB_CONFIG = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
