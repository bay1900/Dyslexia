import logging
from logging.handlers import RotatingFileHandler
import os

def get_logger(name: str = __name__, log_file: str = "app.log"):
    """
    Returns a logger with consistent formatting and rotation.
    Usage: logger = get_logger(__name__)
    """

    # Create logs directory if it doesn't exist
    os.makedirs("logs", exist_ok=True)
    log_path = os.path.join("logs", log_file)

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)        # Change level if needed

    # Avoid adding handlers multiple times
    if logger.hasHandlers():
        return logger

    # --- Handlers ---
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # File handler (rotates logs when size exceeds 5 MB)
    file_handler = RotatingFileHandler(log_path, maxBytes=5_000_000, backupCount=3)
    file_handler.setLevel(logging.DEBUG)

    # --- Formatters ---
    formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s",
        "%Y-%m-%d %H:%M:%S"
    )
    
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    # Attach handlers
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger
