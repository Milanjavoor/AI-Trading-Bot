
import os

# Obtain these from the hackathon organisers.
BASE_URL = os.getenv(
    "MANTHAN_BASE_URL",
    "http://127.0.0.1:3002/api/v1"
)

API_TOKEN = os.getenv("MANTHAN_API_TOKEN", "")

# Strategy settings
FAST_WINDOW = 5
SLOW_WINDOW = 20

# Risk controls — initial practice settings
MAX_POSITION_FRACTION = 0.10
MAX_DAILY_LOSS_FRACTION = 0.02

# Bot settings
HEARTBEAT_INTERVAL_SECONDS = 10
REQUEST_TIMEOUT_SECONDS = 5
