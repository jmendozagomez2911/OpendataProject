import json
import sys
from utils.logger_manager import LoggerManager

logger = LoggerManager.get_logger()

class MetadataManager:
    _instance = None
    _metadata = None

    def __new__(cls, metadata_path=None):
        if cls._instance is None:
            cls._instance = super(MetadataManager, cls).__new__(cls)
            if metadata_path:
                try:
                    with open(metadata_path, "r") as f:
                        cls._metadata = json.load(f)
                    logger.info(f"Metadata cargado desde {metadata_path}")
                    if "dataflows" not in cls._metadata:
                        raise ValueError("La metadata no contiene 'dataflows'")
                except Exception as e:
                    logger.error(f"Error al cargar metadata: {e}")
                    sys.exit(1)
        return cls._instance

    @classmethod
    def get_metadata(cls):
        if cls._instance is None:
            raise ValueError("MetadataManager no inicializado.")
        return cls._metadata
