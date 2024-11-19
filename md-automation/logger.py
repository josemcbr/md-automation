import logging
from config import config

# Configure logging
logger = logging.getLogger(__name__)

log_level_str = config.get("logging", "level").upper()
log_level = getattr(logging, log_level_str, logging.WARNING)

logging.basicConfig(
    level=log_level, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)