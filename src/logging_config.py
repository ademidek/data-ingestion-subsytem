import logging
from logging.handlers import RotatingFileHandler

def setup_logging():
    logger = logging.getLogger("etl")
    
    if logger.hasHandlers():
        return logger 
    
    logger.setLevel(logging.INFO)

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s"
    ))

    file_handler = RotatingFileHandler(
        "logs/etl.log", maxBytes=5_000_000, backupCount=5
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    ))

    logger.addHandler(console)
    logger.addHandler(file_handler)

    return logger