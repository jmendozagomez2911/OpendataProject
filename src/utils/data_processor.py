import os
from concurrent.futures import ThreadPoolExecutor, as_completed

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
        self.years_file = years_file

    def run(self):
        try:
            logger.info("PIPELINE: Inicio del procesamiento de datos.")
            for dataflow in self.metadata["dataflows"]:
                logger.info(f"PIPELINE: Dataflow '{dataflow['name']}' en ejecución.")
                self._process_inputs(dataflow["inputs"])
                self._process_transformations(dataflow["transformations"])
                self._process_outputs(dataflow["outputs"])
        except Exception as e:
            logger.error(f"PIPELINE: Error crítico en el pipeline: {e}", exc_info=True)
            raise

    # ========== PROCESO DE INPUTS ==========
    def _process_inputs(self, inputs_config):
        for inp in inputs_config:
            logger.info(f"INPUT: Iniciando lectura del input '{inp['name']}' (tipo: {inp['type']}).")
            if inp["type"] == "file":
                try:
                    df_or_dict = self._read_file(inp)
                    self.dataframes[inp["name"]] = df_or_dict

                    if isinstance(df_or_dict, dict):
                        for fid, df in df_or_dict.items():
                            logger.info(f"INPUT: '{inp['name']}' [{fid}]: {df.count()} filas leídas.")
                            df.printSchema()
                    else:
                        logger.info(f"INPUT: '{inp['name']}': {df_or_dict.count()} filas leídas.")
                        df_or_dict.printSchema()
                except Exception as e:
                    logger.error(f"INPUT: Error al leer '{inp['name']}': {e}", exc_info=True)
                    raise

    # ========== PROCESO DE TRANSFORMACIONES ==========
    def _process_transformations(self, transformations_config):
        for transf in transformations_config:
            t_type, t_name, t_input = transf["type"], transf["name"], transf["input"]
            logger.info(f"TRANSFORM: Aplicando '{t_type}' sobre '{t_input}' para producir '{t_name}'.")
            try:
                input_obj = self.dataframes[t_input]
                if isinstance(input_obj, dict):
                    out_dict = {}
                    # Ordenamos las claves (p.ej. "2024", "2025") para un procesamiento secuencial definido
                    for fid in sorted(input_obj.keys()):
                        df = input_obj[fid]
                        logger.info(f"TRANSFORM: Procesando '{fid}' de '{t_input}' en orden ascendente.")
                        transformed_df = self._apply_transformation(t_type, transf["config"], df)
                        transformed_df.printSchema()
                        out_dict[fid] = transformed_df

                    self.dataframes[t_name] = out_dict
                else:
                    output_df = self._apply_transformation(t_type, transf["config"], input_obj)
                    output_df.printSchema()
                    self.dataframes[t_name] = output_df
            except Exception as e:
                logger.error(f"TRANSFORM: Error en transformación '{t_type}' para input '{t_input}': {e}",
                             exc_info=True)
                raise

    def _apply_transformation(self, t_type, config, df):
        if t_type == "add_fields":
            return AddFieldsTransformation(config).transform(df)
        elif t_type == "filter":
            return FilterTransformation(config).transform(df)
        else:
            raise NotImplementedError(f"TRANSFORM: Transformación '{t_type}' no implementada.")

    # ========== PROCESO DE OUTPUTS ==========
    def _process_outputs(self, outputs_config):
        for out in outputs_config:
            o_name, o_type, o_input = out.get("name", "Unnamed Output"), out["type"], out["input"]
            logger.info(f"OUTPUT: Procesando salida '{o_name}' (tipo: {o_type}) desde input '{o_input}'.")
            try:
                input_obj = self.dataframes[o_input]
                if isinstance(input_obj, dict):
                    # De nuevo ordenamos las claves para escribir en orden (2024 antes que 2025, etc.)
                    for fid in sorted(input_obj.keys()):
                        df = input_obj[fid]
                        logger.info(f"OUTPUT: '{o_name}' [{fid}]: Esquema del DataFrame:")
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
                            raise NotImplementedError(f"OUTPUT: Output '{o_type}' no implementado para '{o_name}'.")
                        logger.info(f"OUTPUT: Salida '{o_name}' [{fid}] generada correctamente.")
                else:
                    logger.info(f"OUTPUT: '{o_name}': Esquema del DataFrame:")
                    input_obj.printSchema()
                    if o_type == "file":
                        FileWriter(out["config"]).write(input_obj)
                    elif o_type == "delta":
                        DeltaWriter(self.spark, out["config"]).write(input_obj)
                    else:
                        raise NotImplementedError(f"OUTPUT: Output '{o_type}' no implementado para '{o_name}'.")
                    logger.info(f"OUTPUT: Salida '{o_name}' generada correctamente.")
            except Exception as e:
                logger.error(f"OUTPUT: Error al procesar salida '{o_name}': {e}", exc_info=True)
                raise

    # ========== LECTURA DE FICHEROS (SEGÚN years.txt) ==========
    def _read_file(self, inp: dict):
        """
        1. Lee la lista de años desde self.years_file.
        2. Reemplaza {{ year }} en la ruta del JSON para cada año.
        3. Devuelve un dict {file_id: DataFrame}.
           - Decide secuencial vs. paralelo según la volumetría y el número de ficheros.
        """
        if not self.years_file:
            raise ValueError("READ: No se proporcionó 'years_file'; no se puede reemplazar {{ year }}.")

        with open(self.years_file, "r", encoding="utf-8") as f:
            years = [line.strip() for line in f if line.strip()]

        path_template = inp["config"]["path"]
        fmt = inp["config"]["format"]
        spark_options = inp.get("spark_options", {})
        logger.info(f"READ: Años leídos desde '{self.years_file}': {years}.")

        # Creamos la lista de rutas
        all_paths = []
        for y in years:
            actual_path = path_template.replace("{{ year }}", y)
            all_paths.append(actual_path)

        logger.info(f"READ: Se generaron {len(all_paths)} rutas a partir del template: {all_paths}")

        # Preparamos dict resultante
        result = {}

        # (1) Evaluamos la volumetría total (sumando tamaño de cada fichero) y el nº de ficheros
        total_size = 0
        for p in all_paths:
            full_path = self._resolve_full_path(p)
            if os.path.isfile(full_path):
                total_size += os.path.getsize(full_path)
        num_files = len(all_paths)

        # Umbrales ejemplo: 500 MB y 5 ficheros
        threshold_size = 500 * 1024 * 1024  # 500 MB
        threshold_count = 5

        # (2) Decidimos si leer en paralelo o no
        use_parallel = (total_size > threshold_size) or (num_files > threshold_count)

        if use_parallel and num_files > 1:
            logger.info("READ: Se ha superado el umbral, leyendo ficheros en paralelo con ThreadPoolExecutor.")
            max_workers = min(num_files, 4)  # Un límite razonable
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_path = {
                    executor.submit(self._read_single_file, p, fmt, spark_options): p
                    for p in all_paths
                }
                for future in as_completed(future_to_path):
                    done_path = future_to_path[future]
                    file_id = os.path.splitext(os.path.basename(done_path))[0]
                    try:
                        df = future.result()
                        result[file_id] = df
                        logger.info(f"READ: Finalizada lectura de '{done_path}' en paralelo.")
                    except Exception as e:
                        logger.error(f"READ: Error al leer '{done_path}': {e}", exc_info=True)
                        raise
        else:
            logger.info("READ: Volumen pequeño o pocos ficheros, lectura secuencial.")
            for p in all_paths:
                file_id = os.path.splitext(os.path.basename(p))[0]
                df = self._read_single_file(p, fmt, spark_options)
                result[file_id] = df
                logger.info(f"READ: Finalizada lectura secuencial de '{p}'.")

        return result

    def _read_single_file(self, path: str, fmt: str, spark_options: dict):
        logger.info(f"READ_SINGLE: Iniciando lectura del fichero: {path} (formato: {fmt}).")
        full_path = self._resolve_full_path(path)

        if not os.path.isfile(full_path):
            raise FileNotFoundError(f"READ_SINGLE: No existe el fichero: {full_path}")

        reader = self.spark.read.format(fmt)
        for key, val in spark_options.items():
            reader = reader.option(key, val)

        df = reader.load(full_path)
        return df

    def _resolve_full_path(self, path_str: str) -> str:
        """Ayudante para normalizar la ruta, da soporte a paths absolutos o relativos."""
        if path_str.startswith("/"):
            return os.path.join(os.getcwd(), path_str.lstrip("/"))
        else:
            return os.path.abspath(path_str)
