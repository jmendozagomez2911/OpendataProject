import sys
import json
import argparse
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from data_processor import DataProcessor
from logger_manager import LoggerManager

logger = LoggerManager.get_logger()


def main():
    try:
        # Parseo de argumentos
        parser = argparse.ArgumentParser(description="Data Processing Pipeline")
        parser.add_argument("--year", required=True, help="Año de la fuente (2024, 2025, etc.)")
        parser.add_argument("--metadata", required=True, help="Ruta al fichero metadata.json")
        args = parser.parse_args()

        logger.info(f"Iniciando pipeline con año: {args.year} y metadata: {args.metadata}")

        # Configuración de Spark con Delta
        builder = SparkSession.builder \
            .appName("LocalPipeline") \
            .master("local[*]") \
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        logger.info("Sesión de Spark creada exitosamente")

        # Cargar metadata
        try:
            with open(args.metadata, "r") as f:
                metadata = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logger.error(f"Error al leer el archivo de metadata: {e}")
            sys.exit(1)

        # Procesamiento de datos
        processor = DataProcessor(spark, metadata, {"year": args.year})
        processor.run()

        logger.info("Pipeline ejecutado correctamente")

    except Exception as e:
        logger.error(f"Error inesperado en la ejecución: {e}", exc_info=True)
        sys.exit(1)

    finally:
        if 'spark' in locals():
            spark.stop()
            logger.info("Sesión de Spark cerrada")


if __name__ == "__main__":
    main()
