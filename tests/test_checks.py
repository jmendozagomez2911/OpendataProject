import pytest
import os
import json
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

        # Usar el Working Directory configurado
        base_dir = os.getcwd()
        if path.startswith("/data/"):
            path = os.path.join(base_dir, path[1:])  # Remueve la barra inicial

        return path
    except KeyError as e:
        raise ValueError(f"Error en la estructura de metadata.json: {e}")


def _get_output_path_from_metadata(md, output_name):
    """
    Obtiene la ruta de salida desde el metadata.json basado en el nombre del output.
    """
    try:
        for output in md["dataflows"][0]["outputs"]:
            if output["name"] == output_name:
                path = output["config"]["path"]
                base_dir = os.getcwd()
                if path.startswith("/data/"):
                    path = os.path.join(base_dir, path[1:])
                return path
        raise ValueError(f"No se encontró la ruta de salida para {output_name} en metadata.json")
    except KeyError as e:
        raise ValueError(f"Error en la estructura de metadata.json: {e}")


def _get_spark_options_from_metadata(md):
    """
    Extrae las opciones de Spark desde el metadata.json
    """
    try:
        options = md["dataflows"][0]["inputs"][0]["spark_options"]
        delimiter = options.get("delimiter", ",")
        header = options.get("header", "true").lower() == "true"
        return delimiter, header
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
    delimiter, header = _get_spark_options_from_metadata(metadata)

    df_start = spark.read.option("header", header).option("delimiter", delimiter).csv(path_start)
    df_end = spark.read.option("header", header).option("delimiter", delimiter).csv(path_end)

    key_cols = ["provincia", "municipio", "sexo"]

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

    count_changes = comparison_df.count()
    print(f"📌 Total de filas con cambios: {count_changes}.")

    # Puede descomentarse para ver las filas en pantalla
    comparison_df.show(100)

    assert count_changes > 0, "No se encontraron cambios entre los archivos de 2024 y 2025"


def test_data_quality(spark, yearEnd, metadata):
    """
    Verifica problemas de calidad en el fichero de 2025.
    """
    path_end = _get_csv_path_from_metadata(metadata, yearEnd)
    delimiter, header = _get_spark_options_from_metadata(metadata)
    df_end = spark.read.option("header", header).option("delimiter", delimiter).csv(path_end)

    required_cols = {"provincia", "municipio", "sexo", "total"}
    missing_cols = required_cols - set(df_end.columns)
    assert not missing_cols, f"Faltan columnas en el archivo: {missing_cols}"

    from pyspark.sql import functions as F

    df_end = df_end.withColumn("total", F.col("total").cast("double"))

    # Crear columnas separadas para cada tipo de sexo
    df_ambos = df_end.filter(F.col("sexo") == "Ambos sexos").select("provincia", "municipio", F.col("total").alias("Total_Ambos_Sexos"))
    df_hombres = df_end.filter(F.col("sexo") == "Hombres").select("provincia", "municipio", F.col("total").alias("Hombres"))
    df_mujeres = df_end.filter(F.col("sexo") == "Mujeres").select("provincia", "municipio", F.col("total").alias("Mujeres"))

    # Unir las tres tablas en base a provincia y municipio
    df_final = df_ambos.join(df_hombres, ["provincia", "municipio"], "left") \
        .join(df_mujeres, ["provincia", "municipio"], "left")

    # Calcular "Ambos_sexos" como Hombres + Mujeres
    df_final = df_final.withColumn("Ambos_sexos", F.col("Hombres") + F.col("Mujeres"))
 
    # Filtrar filas donde "Ambos_sexos" ≠ "Total_Ambos_Sexos"
    quality_issues_df = df_final.filter(F.col("Ambos_sexos") != F.col("Total_Ambos_Sexos"))

    count_quality_issues = quality_issues_df.count()
    print(f"📌 Total de problemas de calidad: {count_quality_issues}.")

    # Mostrar las filas con problemas de calidad
    quality_issues_df.show()
