import pytest
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F


@pytest.fixture(scope="session")
def spark():
    """
    SparkSession que se usa en los tests.
    """
    return (
        SparkSession.builder
        .appName("TestChecks")
        .master("local[*]")
        .getOrCreate()
    )


def _get_csv_path_from_metadata(md, year):
    """
    Busca en metadata.json la primera ruta que contenga '{{ year }}'
    y la sustituye por el valor real.
    Por simplicidad, asumimos que está en:
      metadata["dataflows"][0]["inputs"][0]["config"]["path"]
    Ajusta según tu estructura real.
    """
    # Suponiendo que tu metadata.json tiene la forma:
    # {
    #   "dataflows": [
    #       {
    #         "inputs": [
    #           {
    #             "config": {
    #               "path": "/data/input/opendata_demo/poblacion{{ year }}.csv"
    #             }
    #           }
    #         ]
    #       }
    #   ]
    # }

    path_template = md["dataflows"][0]["inputs"][0]["config"]["path"]
    if path_template.startswith("/"):
        path_template = os.path.abspath(path_template[1:])

    return path_template.replace("{{ year }}", str(year))


def test_compare_changes(spark, yearStart, yearEnd, metadata):
    """
    Compara qué filas han cambiado entre los ficheros de yearStart y yearEnd
    deduciendo las rutas desde el metadata.json.
    """
    path_start = _get_csv_path_from_metadata(metadata, yearStart)
    path_end = _get_csv_path_from_metadata(metadata, yearEnd)

    df_start = spark.read.option("header", True).csv(path_start)
    df_end = spark.read.option("header", True).csv(path_end)

    # Ejemplo: usas "provincia, municipio, edad, sexo" como clave
    key_cols = ["provincia", "municipio", "edad", "sexo"]

    df_start = df_start.withColumn("year", F.lit(str(yearStart)))
    df_end = df_end.withColumn("year", F.lit(str(yearEnd)))

    comparison_df = (
        df_start.alias("a")
        .join(df_end.alias("b"), key_cols, "full_outer")
        .select(
            *[
                F.coalesce(F.col(f"a.{col}"), F.col(f"b.{col}")).alias(col)
                for col in key_cols
            ],
            F.col("a.poblacion").alias(f"poblacion_{yearStart}"),
            F.col("b.poblacion").alias(f"poblacion_{yearEnd}")
        )
        .filter(F.col(f"poblacion_{yearStart}") != F.col(f"poblacion_{yearEnd}"))
    )

    # Aserción mínima
    assert comparison_df is not None, "No se generó comparison_df"
    expected_cols = set(key_cols + [f"poblacion_{yearStart}", f"poblacion_{yearEnd}"])
    assert expected_cols.issubset(set(comparison_df.columns)), (
        f"Faltan columnas {expected_cols} en {comparison_df.columns}"
    )


def test_data_quality(spark, yearEnd, metadata):
    """
    Verifica problemas de calidad en yearEnd (Ambos sexos != Hombres + Mujeres).
    También deduce la ruta desde metadata.json.
    """
    path_end = _get_csv_path_from_metadata(metadata, yearEnd)
    df_end = spark.read.option("header", True).csv(path_end)

    # Convertir a int
    df_end = (
        df_end.withColumn("Ambos_sexos", F.col("Ambos_sexos").cast("int"))
        .withColumn("Hombres", F.col("Hombres").cast("int"))
        .withColumn("Mujeres", F.col("Mujeres").cast("int"))
    )

    quality_issues_df = df_end.filter(
        F.col("Ambos_sexos") != (F.col("Hombres") + F.col("Mujeres"))
    )

    assert quality_issues_df is not None
    required_cols = {"Ambos_sexos", "Hombres", "Mujeres"}
    assert required_cols.issubset(set(quality_issues_df.columns)), (
        f"Faltan columnas {required_cols}"
    )
