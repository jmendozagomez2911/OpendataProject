import logging
import sys


class LoggerManager:
    _logger = None

    @classmethod
    def get_logger(cls, name="AppLogger"):
        if cls._logger is None:
            logger = logging.getLogger(name)
            logger.setLevel(logging.INFO)

            formatter = logging.Formatter(
                "%(asctime)s - %(levelname)s - %(message)s"
            )

            # Enviar INFO y DEBUG a stdout, y WARNING, ERROR, CRITICAL a stderr
            stdout_handler = logging.StreamHandler(sys.stdout)
            stdout_handler.setLevel(logging.INFO)
            stdout_handler.setFormatter(formatter)

            stderr_handler = logging.StreamHandler(sys.stderr)
            stderr_handler.setLevel(logging.WARNING)
            stderr_handler.setFormatter(formatter)

            if not logger.handlers:
                logger.addHandler(stdout_handler)  # INFO a stdout
                logger.addHandler(stderr_handler)  # WARNING+ a stderr

            cls._logger = logger

        return cls._logger
