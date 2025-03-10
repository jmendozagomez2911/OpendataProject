from delta.tables import DeltaTable
import pyspark.sql.utils
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from utils.logger_manager import LoggerManager

logger = LoggerManager.get_logger()
LOG_PREFIX = "DELTA:"  # Prefijo para DeltaWriter
ZORDER_FILE_THRESHOLD = 500  # Umbral para aplicar ZORDER


class DeltaWriter:
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config
        self.table_name = self.config["table"]
        self.delta_path = f"delta_tables/{self.table_name}"
        self.primary_key = self.config.get("primary_key", None)  # Evita KeyError si no está definido

    def write(self, df):
        logger.info(
            f"{LOG_PREFIX} Iniciando escritura en Delta Table '{self.table_name}' (modo: {self.config['save_mode']}).")
        try:
            if self.config["save_mode"] == "append":
                self._append(df)
            elif self.config["save_mode"] == "overwrite":
                self._overwrite(df)
            elif self.config["save_mode"] == "merge":
                self._merge(df)
            else:
                raise NotImplementedError(f"{LOG_PREFIX} Modo '{self.config['save_mode']}' no soportado.")
        except Exception as e:
            logger.error(f"{LOG_PREFIX} Error al escribir en '{self.table_name}': {e}", exc_info=True)
            raise

    def _append(self, df):
        try:
            self.spark.read.format("delta").load(self.delta_path)
            df.write.format("delta").mode("append").save(self.delta_path)
            logger.info(f"{LOG_PREFIX} Datos añadidos a '{self.delta_path}'.")
        except pyspark.sql.utils.AnalysisException:
            logger.warning(f"{LOG_PREFIX} La tabla '{self.delta_path}' no existe. Creando nueva tabla.")
            df.write.format("delta").mode("overwrite").save(self.delta_path)
        except Exception as e:
            logger.error(f"{LOG_PREFIX} Error en modo append: {e}", exc_info=True)
            raise

    def _overwrite(self, df):
        df.write.format("delta").mode("overwrite").save(self.delta_path)
        logger.info(f"{LOG_PREFIX} Tabla '{self.table_name}' sobrescrita correctamente.")

    def _merge(self, df):
        if not self.primary_key:
            raise ValueError(f"{LOG_PREFIX} La tabla '{self.table_name}' requiere clave primaria para MERGE.")
        df = self._remove_duplicates(df)
        try:
            delta_table = DeltaTable.forPath(self.spark, self.delta_path)
        except pyspark.sql.utils.AnalysisException:
            logger.warning(f"{LOG_PREFIX} La tabla '{self.delta_path}' no existe. Creando nueva tabla.")
            df.write.format("delta").mode("overwrite").save(self.delta_path)
            return
        except Exception as e:
            logger.error(f"{LOG_PREFIX} Error al acceder a '{self.delta_path}': {e}", exc_info=True)
            raise

        merge_condition = " AND ".join([f"target.{pk} = source.{pk}" for pk in self.primary_key])
        try:
            (delta_table.alias("target")
             .merge(df.alias("source"), merge_condition)
             .whenMatchedUpdateAll()
             .whenNotMatchedInsertAll()
             # .whenNotMatchedBySourceDelete()
             .execute())
            logger.info(f"{LOG_PREFIX} Merge realizado correctamente en '{self.delta_path}'.")

            # Comando OPTIMIZE
            self._optimize_table()
        except Exception as e:
            logger.error(f"{LOG_PREFIX} Error en merge: {e}", exc_info=True)
            raise

    def _remove_duplicates(self, df):
        window_spec = Window.partitionBy(self.primary_key).orderBy(df["_metadata"].desc())
        return (df.withColumn("row_number", row_number().over(window_spec))
                .filter("row_number = 1")
                .drop("row_number"))

    def _optimize_table(self):
        num_files = len(self.spark.read.format("delta").load(self.delta_path).inputFiles())
        if num_files > ZORDER_FILE_THRESHOLD:
            self.spark.sql(f"OPTIMIZE '{self.delta_path}' ZORDER BY ({', '.join(self.primary_key)})")
            logger.info(
                f"{LOG_PREFIX} ZORDER aplicado en '{self.delta_path}' (más de {ZORDER_FILE_THRESHOLD} archivos).")
        else:
            logger.info(
                f"{LOG_PREFIX} ZORDER omitido en '{self.delta_path}' (menos de {ZORDER_FILE_THRESHOLD} archivos).")
