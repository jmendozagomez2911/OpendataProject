import os
from utils.logger_manager import LoggerManager

logger = LoggerManager.get_logger()


class FileWriter:
    def __init__(self, config):
        self.config = config

    def write(self, df):
        path = self.config["path"]
        fmt = self.config["format"]
        mode = self.config["save_mode"]
        if path.startswith("/"):
            path = os.path.abspath(path[1:])
        os.makedirs(os.path.dirname(path), exist_ok=True)
        logger.info(f"Guardando archivo en '{path}', formato: {fmt}, modo: {mode}")
        try:
            df.coalesce(1).write.format(fmt).mode(mode).save(path)
            logger.info(f"Archivo guardado en '{path}'")
        except Exception as e:
            logger.error(f"Error al guardar archivo en '{path}': {e}", exc_info=True)
            raise
