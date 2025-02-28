import os
from transformations.add_fields import AddFieldsTransformation
from transformations.filter_rows import FilterTransformation
from outputs.write_file import FileWriter
from outputs.write_delta import DeltaWriter
from logger_manager import LoggerManager
from metadata_manager import MetadataManager

logger = LoggerManager.get_logger()


class DataProcessor:
    def __init__(self, spark):
        self.spark = spark
        self.dataframes = {}

        # Obtenemos la metadata y el 'year' desde el Singleton
        self.metadata = MetadataManager.get_metadata()
        self.year = MetadataManager.get_variable("year")

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

    def _process_inputs(self, inputs_config):
        for inp in inputs_config:
            logger.info(f"Procesando input: {inp['name']} ({inp['type']})")

            if inp["type"] == "file":
                try:
                    df = self._read_file(inp)
                    self.dataframes[inp["name"]] = df

                    logger.info(f"DataFrame '{inp['name']}' cargado con éxito. Filas: {df.count()}")
                    logger.info(f"Esquema del DataFrame '{inp['name']}':")
                    df.printSchema()
                except Exception as e:
                    logger.error(f"Error al leer '{inp['name']}': {e}", exc_info=True)
                    raise

    def _process_transformations(self, transformations_config):
        for transf in transformations_config:
            t_type, t_name, t_input = transf["type"], transf["name"], transf["input"]
            logger.info(f"Ejecutando transformación '{t_type}' con nombre '{t_name}', input '{t_input}'")

            try:
                input_df = self.dataframes[t_input]

                if t_type == "add_fields":
                    transformer = AddFieldsTransformation(transf["config"])
                    output_df = transformer.transform(input_df)
                elif t_type == "filter":
                    transformer = FilterTransformation(transf["config"])
                    output_df = transformer.transform(input_df)
                else:
                    raise NotImplementedError(f"Transformación '{t_type}' no implementada")

                self.dataframes[t_name] = output_df
                logger.info(f"Esquema del DataFrame transformado '{t_name}':")
                output_df.printSchema()
            except KeyError:
                logger.error(f"Input '{t_input}' no encontrado.", exc_info=True)
                raise
            except Exception as e:
                logger.error(f"Error en transformación '{t_type}': {e}", exc_info=True)
                raise

    def _process_outputs(self, outputs_config):
        for out in outputs_config:
            o_name, o_type, o_input = out.get("name", "Unnamed Output"), out["type"], out["input"]
            logger.info(f"Procesando output '{o_name}' de tipo '{o_type}' desde input '{o_input}'.")

            try:
                df = self.dataframes[o_input]
                logger.info(f"Esquema del DataFrame antes de escribir en '{o_name}':")
                df.printSchema()

                if o_type == "file":
                    FileWriter(out["config"]).write(df)
                elif o_type == "delta":
                    DeltaWriter(self.spark, out["config"]).write(df)
                else:
                    raise NotImplementedError(f"Output '{o_type}' no implementado en '{o_name}'")

                logger.info(f"Output '{o_name}' escrito correctamente.")
            except KeyError:
                logger.error(f"El input '{o_input}' no está disponible.", exc_info=True)
                raise
            except Exception as e:
                logger.error(f"Error en output '{o_name}': {e}", exc_info=True)
                raise

    def _read_file(self, inp):
        path = inp["config"]["path"]

        # Reemplazamos {{ year }} usando la variable del Singleton
        if self.year:
            path = path.replace("{{ year }}", self.year)

        if path.startswith("/"):
            path = os.path.abspath(path[1:])

        fmt = inp["config"]["format"]
        spark_options = inp.get("spark_options", {})

        logger.info(f"Leyendo archivo en '{path}', formato '{fmt}'.")

        reader = self.spark.read.format(fmt)
        for key, val in spark_options.items():
            reader = reader.option(key, val)

        try:
            return reader.load(path)
        except Exception as e:
            logger.error(f"No se pudo cargar el archivo '{path}': {e}", exc_info=True)
            raise RuntimeError(f"No se pudo cargar el archivo '{path}': {e}")
