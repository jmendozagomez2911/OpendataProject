import json
import sys
from logger_manager import LoggerManager

logger = LoggerManager.get_logger()


class MetadataManager:
    """
    Singleton mínimo para:
    - Leer el JSON de metadatos (una sola vez)
    - Guardar variables adicionales (e.g., año) en un diccionario interno
    """
    _instance = None
    _metadata = None
    _variables = {}

    def __new__(cls, metadata_path=None, variables=None):
        """
        Se ejecuta solo la primera vez que se crea la instancia.
        :param metadata_path: Ruta al fichero metadata.json
        :param variables: Diccionario con variables adicionales (p.ej. {'year': '2024'})
        """
        if cls._instance is None:
            cls._instance = super(MetadataManager, cls).__new__(cls)

            # Leemos el JSON de metadatos si nos pasan la ruta
            if metadata_path:
                try:
                    with open(metadata_path, "r") as f:
                        cls._metadata = json.load(f)
                    logger.info(f"Metadata cargado desde {metadata_path}")
                except (FileNotFoundError, json.JSONDecodeError) as e:
                    logger.error(f"Error al leer metadata: {e}")
                    sys.exit(1)

            # Guardamos variables extra en _variables (por ejemplo el year)
            if variables:
                cls._variables.update(variables)

        return cls._instance

    @classmethod
    def get_metadata(cls):
        """
        Retorna el contenido de metadata.json que leímos.
        """
        if cls._instance is None:
            raise ValueError("MetadataManager no está inicializado.")
        return cls._metadata

    @classmethod
    def get_variable(cls, key, default=None):
        """
        Devuelve una variable concreta, p.ej. 'year', si existe en _variables.
        """
        if cls._instance is None:
            raise ValueError("MetadataManager no está inicializado.")
        return cls._variables.get(key, default)
