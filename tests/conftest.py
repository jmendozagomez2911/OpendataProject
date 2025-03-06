import pytest
import json
import sys
from pathlib import Path

def pytest_addoption(parser):
    """
    Añade opciones de línea de comando para pytest:
      --yearStart 2024
      --yearEnd 2025
      --metadata C:/Projects/OpendataProject/metadata/metadata.json
    """
    parser.addoption(
        "--yearStart",
        action="store",
        default="2024",
        help="Año inicial para comparación."
    )
    parser.addoption(
        "--yearEnd",
        action="store",
        default="2025",
        help="Año final para comparación."
    )
    parser.addoption(
        "--metadata",
        action="store",
        default="C:/Projects/OpendataProject/metadata/metadata.json",
        help="Ruta al fichero metadata.json."
    )

@pytest.fixture(scope="session")
def yearStart(pytestconfig):
    return pytestconfig.getoption("yearStart")

@pytest.fixture(scope="session")
def yearEnd(pytestconfig):
    return pytestconfig.getoption("yearEnd")

@pytest.fixture(scope="session")
def metadata(pytestconfig):
    """
    Lee y parsea el metadata.json en un diccionario Python.
    """
    metadata_path = pytestconfig.getoption("metadata")
    try:
        with open(Path(metadata_path), "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Error al leer metadata: {e}", file=sys.stderr)
        sys.exit(1)
