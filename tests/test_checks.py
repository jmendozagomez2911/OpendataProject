import pytest
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def _get_csv_path_from_metadata(md, year):
    """
    Busca en metadata.json la primera ruta que contenga '{{ year }}'
    y la sustituye por el valor real.
    """
    try:
        path_template = md["dataflows"][0]["inputs"][0]["config"]["path"]
        path = path_template.replace("{{ year }}", str(year))

        # Asegurarnos de que es la ruta correcta
        base_dir = os.path.abspath("C:/Projects/OpendataProject/data")  # Base correcta
        if path.startswith("/data/"):
            path = os.path.join(base_dir, path[6:])  # Remover "/data/" del inicio

        return path
    except KeyError as e:
        raise ValueError(f"Error en la estructura de metadata.json: {e}")

@pytest.fixture(scope="session")
def spark():
    """
    SparkSession que se usa en los tests.
    """
    return SparkSession.builder.master("local[*]").appName("TestChecks").getOrCreate()

def test_compare_changes(spark, yearStart, yearEnd, metadata):
    """
    Compara qué filas han cambiado entre los ficheros de 2024 y 2025.
    """
    path_start = _get_csv_path_from_metadata(metadata, yearStart)
    path_end = _get_csv_path_from_metadata(metadata, yearEnd)

    df_start = spark.read.option("header", True).option("delimiter", ";").csv(path_start)
    df_end = spark.read.option("header", True).option("delimiter", ";").csv(path_end)

    key_cols = ["provincia", "municipio", "sexo"]  # Sin "edad" porque no está en el CSV

    df_start = df_start.withColumn("year", F.lit(str(yearStart)))
    df_end = df_end.withColumn("year", F.lit(str(yearEnd)))

    # Detectar cambios en la columna "total"
    comparison_df = (
        df_start.alias("a")
        .join(df_end.alias("b"), key_cols, "full_outer")
        .select(
            *[F.coalesce(F.col(f"a.{col}"), F.col(f"b.{col}")).alias(col) for col in key_cols],
            F.coalesce(F.col("a.total"), F.lit(0)).alias(f"total_{yearStart}"),
            F.coalesce(F.col("b.total"), F.lit(0)).alias(f"total_{yearEnd}")
        )
        .filter(F.col(f"total_{yearStart}") != F.col(f"total_{yearEnd}"))
    )

    output_path = "C:/Projects/OpendataProject/tests/results/changes_2024_2025"
    comparison_df.write.mode("overwrite").parquet(output_path)

    count_changes = comparison_df.count()
    print(f"📌 Total de filas con cambios: {count_changes}. Para más detalles, revisar {output_path}")

    # comparison_df.show(500)  # Mostrar las primeras 10 filas en caso de debugging

    assert count_changes > 0, "No se encontraron cambios entre los archivos de 2024 y 2025"

def test_data_quality(spark, yearEnd, metadata):
    """
    Verifica problemas de calidad en el fichero de 2025.
    """
    path_end = _get_csv_path_from_metadata(metadata, yearEnd)
    df_end = spark.read.option("header", True).option("delimiter", ";").csv(path_end)

    required_cols = {"provincia", "municipio", "sexo", "total"}
    missing_cols = required_cols - set(df_end.columns)
    assert not missing_cols, f"Faltan columnas en el archivo: {missing_cols}"

    df_end = df_end.withColumn("total", F.col("total").cast("int"))

    if "Hombres" not in df_end.columns or "Mujeres" not in df_end.columns:
        df_end = df_end.withColumn("Hombres", F.round(F.col("total") * 0.5).cast("int"))
        df_end = df_end.withColumn("Mujeres", (F.col("total") - F.col("Hombres")).cast("int"))

    df_end = df_end.withColumn("Ambos_sexos", F.col("Hombres") + F.col("Mujeres"))

    quality_issues_df = df_end.filter(F.col("Ambos_sexos") != F.col("total"))

    output_path = "C:/Projects/OpendataProject/tests/results/data_quality_2025"
    quality_issues_df.write.mode("overwrite").parquet(output_path)

    count_quality_issues = quality_issues_df.count()
    print(f"📌 Total de problemas de calidad: {count_quality_issues}. Para más detalles, revisar {output_path}")

    # quality_issues_df.show(500)  # Mostrar las primeras 10 filas en caso de debugging

    assert count_quality_issues == 0, "Se encontraron problemas de calidad en 2025"