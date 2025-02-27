from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

def main():
    # Configurar Spark con Delta
    builder = SparkSession.builder \
        .appName("Test") \
        .master("local[*]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # Crear DataFrame de prueba
    df = spark.createDataFrame([(1, "A"), (2, "B")], ["id", "value"])

    # Definir rutas de salida
    parquet_path = "C:/Users/josem/test_output/parquet"
    delta_path = "C:/Users/josem/test_output/delta"

    # Guardar en formato Parquet
    df.write.format("parquet") \
        .mode("overwrite") \
        .save(parquet_path)
    print(f"✅ Archivo guardado en formato Parquet en: {parquet_path}")

    # Guardar en formato Delta
    df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(delta_path)
    print(f"✅ Archivo guardado en formato Delta en: {delta_path}")

if __name__ == "__main__":
    main()
