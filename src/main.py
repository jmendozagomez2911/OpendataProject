import sys
import argparse
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from utils.data_processor import DataProcessor
from utils.logger_manager import LoggerManager
from metadata_manager import MetadataManager

logger = LoggerManager.get_logger()


def main():
    try:
        # Parseo de argumentos
        parser = argparse.ArgumentParser(description="Data Processing Pipeline")
        parser.add_argument("--year", required=True, help="Año de la fuente (2024, 2025, etc.)")
        parser.add_argument("--metadata", required=True, help="Ruta al fichero metadata.json")
        args = parser.parse_args()

        logger.info(f"Iniciando pipeline con año: {args.year} y metadata: {args.metadata}")

        # 1) Instanciamos el Singleton con la ruta y la variable 'year'
        MetadataManager(metadata_path=args.metadata, variables={"year": args.year})

        # 2) Configuración de Spark con Delta
        builder = (
            SparkSession.builder
            .appName("LocalPipeline")
            .master("local[*]")
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )

        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        logger.info("Sesión de Spark creada exitosamente")

        # 3) Ejecutamos el DataProcessor (sin pasar metadata ni year, ya se obtiene del Singleton)
        processor = DataProcessor(spark)
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
