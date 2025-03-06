import sys
import argparse
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from utils.data_processor import DataProcessor
from utils.logger_manager import LoggerManager
from metadata_manager import MetadataManager

logger = LoggerManager.get_logger()


def print_spark_resources(spark):
    """Imprime los recursos utilizados por Spark."""
    sc = spark.sparkContext
    logger.info(f"Recursos de Spark:")
    logger.info(f"- Aplicación: {spark.conf.get('spark.app.name', 'No configurado')}")
    logger.info(f"- Master: {sc.master}")
    logger.info(f"- Núcleos asignados: {sc.defaultParallelism}")


def create_spark_session(app_name="LocalPipeline", master="local[*]"):
    builder = SparkSession.builder.appName(app_name).master(master)
    builder = (
        builder.config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def main():
    try:
        parser = argparse.ArgumentParser(description="Pipeline de Procesamiento de Datos")
        parser.add_argument("--metadata", required=True, help="Ruta al archivo metadata.json")
        parser.add_argument("--years_file", required=True, help="Ruta al archivo .txt con la lista de años")
        parser.add_argument("--master", default="local[*]", help="Master de Spark (default: local[*])")
        args = parser.parse_args()

        logger.info(f"Iniciando pipeline con metadata: {args.metadata} y years_file: {args.years_file}")

        # 1) Inicializa la MetadataManager solo con el JSON
        MetadataManager(metadata_path=args.metadata)

        # 2) Crea la sesión de Spark
        spark = create_spark_session(master=args.master)
        logger.info("Sesión de Spark creada.")

        print_spark_resources(spark)

        # 3) Instancia el DataProcessor pasando la ruta del years_file
        processor = DataProcessor(spark, years_file=args.years_file)
        processor.run()
        logger.info("Pipeline ejecutado correctamente.")

    except Exception as e:
        logger.error(f"Error en ejecución: {e}", exc_info=True)
        sys.exit(1)
    finally:
        if 'spark' in locals():
            spark.stop()
            logger.info("Sesión de Spark cerrada.")


if __name__ == "__main__":
    main()
