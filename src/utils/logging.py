import os
import logging
from datetime import datetime

def get_logger(name: str = __name__,
               log_dir: str = "/Workspace/Users/hilgdna.belagavi@gmail.com/sample_repo/logs/",
               level: int = logging.INFO) -> logging.Logger:
    """
    Minimal logger: writes logs to a timestamped file.
    """
    # Ensure logs directory exists
    os.makedirs(log_dir, exist_ok=True)

    # Timestamped log file name
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_file = os.path.join(log_dir, f"app_{timestamp}.log")

    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(level)

    if not logger.handlers:  # prevent duplicate handlers
        # File handler
        fh = logging.FileHandler(log_file)
        fh.setLevel(level)

        # Formatter
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        fh.setFormatter(formatter)

        # Attach handler
        logger.addHandler(fh)

    return logger
