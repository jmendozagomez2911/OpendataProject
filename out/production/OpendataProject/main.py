import sys
import json
import argparse
from pyspark.sql import SparkSession
from data_processor import DataProcessor


def main():
    parser = argparse.ArgumentParser(description="Data Processing Pipeline")
    parser.add_argument("--year", required=True, help="Año de la fuente (2024, 2025, etc.)")
    parser.add_argument("--metadata", required=True, help="Ruta al fichero metadata.json")
    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName("LocalPipeline") \
        .master("local[*]") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .getOrCreate()

    with open(args.metadata, "r") as f:
        metadata = json.load(f)

    # Inyección de variable year a la configuración
    processor = DataProcessor(spark, metadata, {"year": args.year})
    processor.run()

    spark.stop()


if __name__ == "__main__":
    main()
