"""
Logging con Rich para output bonito en consola + archivo.
"""
import logging
import os
from datetime import datetime
from rich.logging import RichHandler
from rich.console import Console

console = Console()

_loggers: dict[str, logging.Logger] = {}


def get_logger(name: str, level: str = "INFO") -> logging.Logger:
    if name in _loggers:
        return _loggers[name]

    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    if not logger.handlers:
        # Console handler con Rich
        rich_handler = RichHandler(
            console=console,
            rich_tracebacks=True,
            show_path=False,
            markup=True,
        )
        rich_handler.setLevel(logging.DEBUG)
        logger.addHandler(rich_handler)

        # File handler
        os.makedirs("logs", exist_ok=True)
        log_file = f"logs/polyedge_{datetime.now().strftime('%Y%m%d')}.log"
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(
            logging.Formatter("%(asctime)s | %(levelname)-8s | %(name)s | %(message)s")
        )
        logger.addHandler(file_handler)

    _loggers[name] = logger
    return logger
