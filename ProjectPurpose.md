# Proyecto OpenData - Procesamiento de Datos

## ✨ Propósito del Proyecto
Este proyecto tiene como objetivo la **ingestión, transformación y almacenamiento** de datos abiertos relacionados con la población (u otras temáticas), permitiendo su procesamiento mediante **Apache Spark** y **Delta Lake**, y ofreciendo flexibilidad para el manejo de múltiples ficheros de distintos años.

## 🔎 Descripción General
1. **Entrada múltiple y concurrente**: El sistema puede leer uno o varios ficheros CSV simultáneamente, aprovechando la concurrencia (via `ThreadPoolExecutor`) cuando supera ciertos umbrales de tamaño o número de ficheros.
2. **Pipeline flexible (metadata.json)**: La configuración de entradas, transformaciones y salidas se define en un archivo `metadata.json`, que especifica cómo leer, qué campos agregar o filtrar, y a qué formatos/ubicaciones escribir.
3. **Transformaciones genéricas**: Se pueden añadir campos (`AddFieldsTransformation`), aplicar filtros (`FilterTransformation`), y otras operaciones que se definan en la metadata.
4. **Almacenamiento en JSON / Delta**: Permite tanto la generación de archivos JSON (sobrescribiendo el último resultado o acumulando históricos) como la escritura en tablas Delta (con `append` o `merge`), lo que facilita la actualización incremental de datos.

## 📂 Estructura del Pipeline
1. **Lectura de Datos**:
    - Se leen los años a procesar desde un fichero `years.txt` (por ejemplo, 2024 y 2025).
    - Por cada año, se reemplaza `{{ year }}` en la ruta de entrada para obtener la ruta CSV real.
    - Si el total de datos es grande (> 500 MB) o el número de ficheros supera 5, la lectura se realiza en paralelo (multithreading). De lo contrario, se hace secuencialmente para evitar overhead.

2. **Transformaciones** (definidas en `metadata.json`):
    - **Add Fields**: se pueden añadir columnas como `domain`, `load_date` u otras.
    - **Filter**: se filtran filas (por ejemplo, descartar `sexo != 'Ambos sexos'`).
    - Otras transformaciones futuras podrían implementarse (p.ej. agregaciones, joins, etc.).

3. **Outputs / Salidas**:
    - **JSON**:
        - Archivo “last” en modo *overwrite* para los datos más recientes.
        - Archivo “historic” en modo *append* (particionado por `load_date`).
    - **Delta**:
        - Tabla en modo *merge* (ejemplo: `opendata_demo`), usando `provincia` y `municipio` como primary keys.
        - Tabla en modo *append* (ejemplo: `raw_opendata_demo`).

4. **Gestión de Metadatos**:
    - Se usa un **singleton** (`MetadataManager`) que carga el JSON solo una vez.
    - El pipeline consulta esa metadata para saber qué entradas, transformaciones y salidas ejecutar.

## 📅 Ejemplo de Flujo de Trabajo
1. Se lee `metadata.json` para descubrir los “dataflows” configurados (ej., `prueba-acceso`).
2. Se consulta `years.txt` para obtener los años a procesar (ej., 2024 y 2025).
3. **Lectura**: se genera un diccionario `{ 'poblacion2024': df_2024, 'poblacion2025': df_2025, ... }`, ya sea en paralelo o secuencial.
4. **Transformaciones**: se añade la columna `domain = 'demography'`, `load_date = current_date()`, y se filtra `sexo != 'Ambos sexos'`.
5. **Escrituras**:
    - JSON de los últimos datos en `/data/output/opendata_demo/last` (modo overwrite).
    - JSON histórico en `/data/output/opendata_demo/historic` (modo append).
    - **Delta**:
        - Merge en la tabla `opendata_demo`, con clave primaria `provincia + municipio` (evitando duplicados).
        - Append en `raw_opendata_demo`.

## 📒 Ejemplo de Datos de Entrada
- **Formato CSV** con delimitador `;`.
- Ubicación base: `/data/input/opendata_demo/poblacion{{ year }}.csv`.
- Se esperan columnas como: `provincia`, `municipio`, `sexo`, `edad`, `poblacion`, etc.

## ⏩ Lectura en Paralelo vs. Secuencial
- Si se detectan **más de 5 ficheros** o **> 500 MB** de datos, se activa la lectura **en paralelo** mediante `ThreadPoolExecutor`.
- De lo contrario, se procesa cada fichero en un simple bucle secuencial (ver `_read_file`), lo que puede ser más eficiente para datasets pequeños.

## ♻️ Deduplicación y `_metadata`
- En el proceso de `merge` (dentro de `DeltaWriter`), se incluye la posibilidad de **deduplicar** registros basándose en una columna `_metadata` (asumida como indicador de versión o timestamp).
- Si la tabla mantiene varias filas con la misma clave, se aplica una ventana que ordena por `_metadata.desc()` y se queda con la más reciente (`row_number = 1`).

## 📄 Salidas del Pipeline
| Tipo de Salida          | Ubicación                            | Formato | Modo                      | Observaciones                                                           |
|-------------------------|--------------------------------------|---------|---------------------------|-------------------------------------------------------------------------|
| **JSON (Last)**         | `/data/output/opendata_demo/last`    | JSON    | Overwrite                 | Guía los últimos datos procesados; normalmente un único archivo.        |
| **JSON (Historic)**     | `/data/output/opendata_demo/historic`| JSON    | Append (partition `load_date`) | Registra la evolución en el tiempo; particionado por `load_date`.       |
| **Delta (Merge)**       | `opendata_demo`                      | Delta   | Merge (por PK)           | Identifica duplicados/upserts en base a `provincia, municipio`.         |
| **Delta (Append)**      | `raw_opendata_demo`                  | Delta   | Append                    | Almacena datos “brutos” sin merge, útil como capa raw o staging.        |

## 🌐 Casos de Uso
1. **Análisis demográfico en distintos años**: Lectura simultánea de ficheros (2024, 2025...) e integración en tablas Delta.
2. **Histórico de poblaciones**: Con particionado por `load_date`, se rastrea cómo evoluciona la población a lo largo del tiempo.
3. **Concurrencia Controlada**: Optimiza la lectura en paralelo si hay muchos ficheros; evita overhead cuando son pocos.

## 🚀 Tecnologías Utilizadas
- **Apache Spark** (procesamiento distribuido y lectura con `spark.read`).
- **Delta Lake** (ACID, merges, time travel y optimizaciones).
- **Python** (coordinación del pipeline, transforms, multithreading).
- **ThreadPoolExecutor** (solo para la fase de lectura cuando se superan umbrales).

Este proyecto incluye tests automatizados con **Pytest** y **Spark**:

1. **`test_compare_changes`**
    - Compara dos ficheros (p.ej. 2024 vs 2025) para detectar diferencias en la columna `total`.
    - Realiza un *full outer join* en base a `provincia, municipio, sexo` y verifica si el valor `total` cambia entre ambos años.
    - Muestra las filas distintas con `DataFrame.show()` y asegura que exista al menos una diferencia.

2. **`test_data_quality`**
    - Revisa la consistencia entre “Ambos sexos” y la suma de “Hombres” + “Mujeres” en el CSV de un año final (por defecto 2025).
    - Para ello, filtra el dataset en tres subconjuntos: `Ambos sexos`, `Hombres`, `Mujeres`, los junta por `provincia` y `municipio`, y compara.
    - Si `Ambos_sexos` difiere de `Total_Ambos_Sexos`, lo considera un problema de calidad (lo muestra con `show()` y falla el test con un `assert`).

Ambos tests se apoyan en:
- **Un fixture de Spark**: para crear `SparkSession` en modo local (`local[*]`).
- **Funciones utilitarias** que extraen rutas de lectura/escritura y opciones de `spark_options` directamente del `metadata.json`.
- **`pytest`**: para la ejecución y reporte de éxito/fallo.
## 🔧 Futuras Mejoras
1. **Validaciones de Esquema**: Manejar columnas faltantes o distintas en cada año (uso de `mergeSchema`).
2. **Métricas y Alertas**: Incluir logs de rendimiento para saber cuándo la lectura en paralelo es efectiva.
3. **Integración con Orquestadores** (Airflow, Luigi, etc.) para programar y monitorizar tareas.
4. **Más Transformaciones**: Agregaciones o joins con otros datasets, control de calidad, etc.
