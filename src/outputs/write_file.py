import os
from logger_manager import LoggerManager

logger = LoggerManager.get_logger()


class FileWriter:
    def __init__(self, config):
        self.config = config

    def write(self, df):
        path = self.config["path"]
        fmt = self.config["format"]
        mode = self.config["save_mode"]

        try:
            # Convertir la ruta en absoluta si empieza con "/"
            if path.startswith("/"):
                path = os.path.abspath(path[1:])

            # Crear directorios si no existen
            os.makedirs(os.path.dirname(path), exist_ok=True)

            logger.info(f"Guardando archivo en '{path}' con formato '{fmt}' y modo '{mode}'.")

            writer = df.write.format(fmt).mode(mode)
            partition_col = self.config.get("partition")
            if partition_col:
                writer = writer.partitionBy(partition_col)

            writer.save(path)
            logger.info(f"Archivo guardado exitosamente en '{path}'.")
        except Exception as e:
            logger.error(f"Error al escribir archivo en '{path}': {e}", exc_info=True)
            raise
