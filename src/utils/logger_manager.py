import logging
import sys

class LoggerManager:
    _logger = None

    @classmethod
    def get_logger(cls, name="AppLogger"):
        if cls._logger is None:
            logger = logging.getLogger(name)
            logger.setLevel(logging.DEBUG)
            formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
            stdout_handler = logging.StreamHandler(sys.stdout)
            stdout_handler.setLevel(logging.INFO)
            stdout_handler.setFormatter(formatter)
            stderr_handler = logging.StreamHandler(sys.stderr)
            stderr_handler.setLevel(logging.WARNING)
            stderr_handler.setFormatter(formatter)
            if not logger.handlers:
                logger.addHandler(stdout_handler)
                logger.addHandler(stderr_handler)
            cls._logger = logger
        return cls._logger
