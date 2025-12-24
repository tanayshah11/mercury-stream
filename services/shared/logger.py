import os
import sys

from loguru import logger

# Remove default handler
logger.remove()

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# Custom log levels for different consumer types
logger.level("VWAP", no=25, color="<blue><bold>")
logger.level("VOLATILITY", no=25, color="<magenta><bold>")
logger.level("VOLUME", no=25, color="<yellow><bold>")
logger.level("HEALTH", no=25, color="<cyan><bold>")
logger.level("FORENSICS", no=25, color="<red><bold>")

# Structured format for containers
logger.add(
    sys.stderr,
    level=LOG_LEVEL,
    format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <10}</level> | <level>{message}</level>",
    colorize=True,
)

log = logger
