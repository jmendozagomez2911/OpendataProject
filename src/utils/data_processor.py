import os
import re
import glob
from concurrent.futures import ThreadPoolExecutor
import pyspark.sql.functions as F
from utils.logger_manager import LoggerManager
from metadata_manager import MetadataManager
from outputs.write_file import FileWriter
from outputs.write_delta import DeltaWriter
from transformations.add_fields import AddFieldsTransformation
from transformations.filter_rows import FilterTransformation

logger = LoggerManager.get_logger()

class DataProcessor:
    def __init__(self, spark, years_file=None):
        self.spark = spark
        self.dataframes = {}
        self.metadata = MetadataManager.get_metadata()
        self.years_file = years_file  # Ruta al txt con la lista de años

    def run(self):
        try:
            logger.info("Iniciando procesamiento de datos.")
            for dataflow in self.metadata["dataflows"]:
                logger.info(f"Procesando dataflow: {dataflow['name']}")
                self._process_inputs(dataflow["inputs"])
                self._process_transformations(dataflow["transformations"])
                self._process_outputs(dataflow["outputs"])
        except Exception as e:
            logger.error(f"Error crítico en el pipeline: {e}", exc_info=True)
            raise

    # ========== PROCESO DE INPUTS ==========

    def _process_inputs(self, inputs_config):
        for inp in inputs_config:
            logger.info(f"Procesando input: {inp['name']} ({inp['type']})")
            if inp["type"] == "file":
                try:
                    df_or_dict = self._read_file(inp)
                    self.dataframes[inp["name"]] = df_or_dict
                    # Logs de control
                    if isinstance(df_or_dict, dict):
                        for fid, df in df_or_dict.items():
                            logger.info(f"DataFrame '{inp['name']}' [{fid}] leído con éxito. Filas: {df.count()}")
                            df.printSchema()
                    else:
                        logger.info(f"DataFrame '{inp['name']}' leido con éxito. Filas: {df_or_dict.count()}")
                        df_or_dict.printSchema()
                except Exception as e:
                    logger.error(f"Error al leer '{inp['name']}': {e}", exc_info=True)
                    raise

    # ========== PROCESO DE TRANSFORMACIONES ==========

    def _process_transformations(self, transformations_config):
        for transf in transformations_config:
            t_type = transf["type"]
            t_name = transf["name"]
            t_input = transf["input"]
            logger.info(f"Ejecutando transformación '{t_type}' al inputDF '{t_input}' : Resultado esperado : '{t_name}'DF")

            try:
                input_obj = self.dataframes[t_input]
                if isinstance(input_obj, dict):
                    out_dict = {}
                    for fid, df in input_obj.items():
                        out_dict[fid] = self._apply_transformation(t_type, transf["config"], df)
                        out_dict[fid].printSchema()
                    self.dataframes[t_name] = out_dict
                else:
                    output_df = self._apply_transformation(t_type, transf["config"], input_obj)
                    output_df.printSchema()
                    self.dataframes[t_name] = output_df
            except Exception as e:
                logger.error(f"Error en transformación '{t_type}': {e}", exc_info=True)
                raise

    def _apply_transformation(self, t_type, config, df):
        if t_type == "add_fields":
            return AddFieldsTransformation(config).transform(df)
        elif t_type == "filter":
            return FilterTransformation(config).transform(df)
        else:
            raise NotImplementedError(f"Transformación '{t_type}' no implementada")

    # ========== PROCESO DE OUTPUTS ==========

    def _process_outputs(self, outputs_config):
        for out in outputs_config:
            o_name = out.get("name", "Unnamed Output")
            o_type = out["type"]
            o_input = out["input"]
            logger.info(f"Procesando output '{o_name}' de tipo '{o_type}' desde inputDF '{o_input}'.")
            try:
                input_obj = self.dataframes[o_input]
                if isinstance(input_obj, dict):
                    for fid, df in input_obj.items():
                        logger.info(f"Esquema del DataFrame para output '{o_name}' [{fid}]:")
                        df.printSchema()

                        out_config = out["config"].copy()
                        # Si la ruta de salida contiene {{file_id}}, se reemplaza
                        if "{{file_id}}" in out_config.get("path", ""):
                            out_config["path"] = out_config["path"].replace("{{file_id}}", fid)

                        if o_type == "file":
                            FileWriter(out_config).write(df)
                        elif o_type == "delta":
                            DeltaWriter(self.spark, out_config).write(df)
                        else:
                            raise NotImplementedError(f"Output '{o_type}' no implementado en '{o_name}'")
                        logger.info(f"Output '{o_name}' para [{fid}] escrito correctamente.")
                else:
                    logger.info(f"Esquema del DataFrame para output '{o_name}':")
                    input_obj.printSchema()
                    if o_type == "file":
                        FileWriter(out["config"]).write(input_obj)
                    elif o_type == "delta":
                        DeltaWriter(self.spark, out["config"]).write(input_obj)
                    else:
                        raise NotImplementedError(f"Output '{o_type}' no implementado en '{o_name}'")
                    logger.info(f"Output '{o_name}' escrito correctamente.")
            except Exception as e:
                logger.error(f"Error en output '{o_name}': {e}", exc_info=True)
                raise

    # ========== LECTURA DE FICHEROS (SEGÚN years.txt) ==========

    def _read_file(self, inp: dict):
        """
        1. Lee la lista de años desde self.years_file.
        2. Reemplaza {{ year }} en la ruta del JSON para cada año.
        3. Si solo hay un año, retorna un DF; si hay varios, un dict {file_id: DF} leyendo en paralelo.
        """
        if not self.years_file:
            raise ValueError("No se proporcionó 'years_file'; no se puede reemplazar {{ year }}.")

        # Leemos la lista de años
        with open(self.years_file, "r", encoding="utf-8") as f:
            years = [line.strip() for line in f if line.strip()]

        path_template = inp["config"]["path"]  # por ejemplo: "C:/.../poblacion{{ year }}.csv"
        fmt = inp["config"]["format"]
        spark_options = inp.get("spark_options", {})

        # Construimos las rutas a partir de cada año
        all_paths = []
        for y in years:
            actual_path = path_template.replace("{{ year }}", y)
            all_paths.append(actual_path)

        logger.info(f"{len(years)} años leídos desde {self.years_file}. Generando {len(all_paths)} rutas...")

        # Si solo hay una ruta, lectura directa
        if len(all_paths) == 1:
            single_path = all_paths[0]
            logger.info(f"Procesando un único fichero: {single_path}")
            return self._read_single_file(single_path, fmt, spark_options)

        # Hay varios años => procesamos en paralelo
        result = {}
        with ThreadPoolExecutor(max_workers=len(all_paths)) as executor:
            future_map = {
                executor.submit(self._read_single_file, p, fmt, spark_options): p
                for p in all_paths
            }
            for future in future_map:
                path_done = future_map[future]
                df = future.result()

                # Usamos el nombre base del fichero como file_id
                file_id = os.path.splitext(os.path.basename(path_done))[0]
                result[file_id] = df
        return result

    def _read_single_file(self, path: str, fmt: str, spark_options: dict):
        logger.info(f"Leyendo fichero: {path}, formato: {fmt}")

        if path.startswith("/"):
            # Eliminamos el slash inicial y obtenemos la ruta absoluta relativa al directorio actual.
            path = os.path.join(os.getcwd(), path.lstrip("/"))
        else:
            # En cualquier otro caso, convertimos a ruta absoluta.
            path = os.path.abspath(path)
        if not os.path.isfile(path):
            raise FileNotFoundError(f"No existe el fichero: {path}")

        reader = self.spark.read.format(fmt)
        for key, val in spark_options.items():
            reader = reader.option(key, val)
        return reader.load(path)

