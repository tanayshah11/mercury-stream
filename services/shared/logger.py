import os
import sys

from loguru import logger

# Remove default handler
logger.remove()

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# Structured format for containers
logger.add(
    sys.stderr,
    level=LOG_LEVEL,
    format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
    colorize=True,
)

log = logger
