# Proyecto OpenData - Procesamiento de Datos

## ✨ Propósito del Proyecto
Este proyecto tiene como objetivo la **ingestión, transformación y almacenamiento** de datos abiertos relacionados con la población, permitiendo su procesamiento mediante Apache Spark y Delta Lake.

## 🔎 Descripción General
El sistema lee archivos de datos de entrada en formato **CSV**, les aplica transformaciones y luego los almacena en distintos formatos (✨ JSON y Delta Lake). Esto permite mantener tanto los datos recientes como un historial completo de la información procesada.

## 📂 Estructura del Pipeline

1. **Ingesta de datos**: Se lee un archivo de población en formato CSV desde un directorio de entrada.
2. **Transformaciones**:
    - Se agregan nuevas columnas como `domain` y `load_date`.
    - Se filtran registros innecesarios (por ejemplo, eliminando registros con "Ambos sexos").
3. **Almacenamiento de resultados**:
    - Se guardan los datos en formato **JSON** (archivos de última ejecución y datos históricos).
    - Se almacena una versión estructurada en **Delta Lake**, permitiendo realizar operaciones de `MERGE` y `APPEND`.

## 📒 Datos de Entrada
- **Formato**: CSV con delimitador `;`
- **Ubicación**: `/data/input/opendata_demo/poblacion{{ year }}.csv`
- **Columnas esperadas**: Provincia, Municipio, Sexo, Edad, Población, etc.

## 📅 Transformaciones Aplicadas
- **Añadir columnas**:
    - `domain` = `'demography'`
    - `load_date` = `current_date()`
- **Filtrar filas**:
    - Se eliminan registros donde `sexo = 'Ambos sexos'`

## 📄 Salidas del Pipeline
| Tipo de Salida       | Ubicación | Formato | Modo |
|----------------------|------------|---------|------|
| Archivo JSON - Último | `/data/output/opendata_demo/last` | JSON | Overwrite |
| Archivo JSON - Histórico | `/data/output/opendata_demo/historic` | JSON | Append (particionado por `load_date`) |
| Delta Table - Merge | `opendata_demo` | Delta | Merge (según `provincia`, `municipio`) |
| Delta Table - Raw | `raw_opendata_demo` | Delta | Append |

## 🌐 Casos de Uso
- **Consulta de datos poblacionales recientes**: Se puede acceder a los últimos datos procesados en JSON.
- **Análisis de tendencias demográficas**: Gracias a la partición por `load_date`, se pueden analizar cambios en el tiempo.
- **Actualización eficiente de datos**: Usando Delta Lake con `MERGE`, se pueden actualizar registros existentes sin duplicar información.

## 🚀 Tecnologías Utilizadas
- **Apache Spark** 💚
- **Delta Lake** 📀
- **Python** 🔄
- **JSON / CSV** 📂

## 🔧 Futuras Mejoras
- Agregar validaciones automáticas de calidad de datos.
- Implementar monitoreo de ingestas y alertas.
- Integración con un Data Warehouse para visualización avanzada.

