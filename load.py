"""
load.py
-------
Fase LOAD del ETL de vuelos de salida de los 11 grandes aeropuertos de
España.

Responsabilidades:
1. Persistir el "estado actual" (foto completa de todos los vuelos vistos
    en la última extracción) de CADA aeropuerto por separado en disco,
    para que la siguiente ejecución de Transform pueda comparar ese
    aeropuerto contra su propio último estado conocido.
2. Añadir al histórico SOLO las filas nuevas o cambiadas que detectó
    Transform. Si no hay cambios, no se escribe nada.

Estructura de archivos que gestiona esta fase:
    data/
        estado_actual/
            MAD.pkl             -> última foto completa de Madrid
            AGP.pkl             -> última foto completa de Malaga
            ...                 -> uno por cada código IATA
        historico_vuelos.csv    -> histórico acumulado de cambios de TODOS
                                    los aeropuertos (columna "aeropuerto"
                                    para filtrar/analizar por separado)

IMPORTANTE: el estado actual de un aeropuerto NUNCA debe compararse ni
mezclarse con el de otro — cada uno vive en su propio archivo .pkl.
"""

import logging
from pathlib import Path

import pandas as pd

from extract import AEROPUERTOS

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

ESTADO_ACTUAL_DIR = DATA_DIR / "estado_actual"
ESTADO_ACTUAL_DIR.mkdir(exist_ok=True)

HISTORICO_PATH = DATA_DIR / "historico_vuelos.csv"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


# --- Estado actual (para comparar en la siguiente ejecución) --------------

def _ruta_estado(airport_code: str) -> Path:
    """Ruta del .pkl de estado actual de un aeropuerto concreto."""
    return ESTADO_ACTUAL_DIR / f"{airport_code}.pkl"


def cargar_estado_anterior(airport_code: str) -> pd.DataFrame | None:
    """
    Carga la última foto completa de vuelos de UN aeropuerto, guardada por
    una ejecución previa. Devuelve None si es la primera vez que se
    ejecuta el pipeline para ese aeropuerto (no existe el archivo).

    Usamos pickle (no CSV) para este archivo porque es un archivo interno,
    no pensado para que lo abra una persona: así conservamos los tipos de
    datos exactos (fechas como datetime, etc.) sin tener que re-parsear nada.
    """
    ruta = _ruta_estado(airport_code)
    if not ruta.exists():
        logger.info("[%s] No existe estado_actual todavía (primera ejecución).", airport_code)
        return None

    df = pd.read_pickle(ruta)
    logger.info("[%s] Estado anterior cargado: %d vuelos.", airport_code, len(df))
    return df


def cargar_estados_anteriores(codigos: list[str] | None = None) -> dict[str, pd.DataFrame | None]:
    """
    Carga el estado anterior de varios aeropuertos de una vez (por defecto,
    todos los de AEROPUERTOS). Pensado para pasárselo directamente a
    transform.procesar_todos(estados_anteriores).
    """
    codigos = codigos or list(AEROPUERTOS.keys())
    return {codigo: cargar_estado_anterior(codigo) for codigo in codigos}


def guardar_estado_actual(airport_code: str, df_estado_actualizado: pd.DataFrame) -> None:
    """Sobrescribe el .pkl de estado actual de un aeropuerto con la foto más reciente."""
    df_estado_actualizado.to_pickle(_ruta_estado(airport_code))
    logger.info("[%s] Estado actual guardado (%d vuelos).", airport_code, len(df_estado_actualizado))


# --- Histórico de cambios (el resultado final que te interesa) ------------

def guardar_cambios_en_historico(df_cambios: pd.DataFrame) -> None:
    """
    Añade las filas de df_cambios al final de data/historico_vuelos.csv.

    Como cada fila ya trae su propia columna "aeropuerto" (añadida en
    transform.limpiar_vuelos), un único CSV combinado es suficiente:
    para analizar solo un aeropuerto basta con filtrar esa columna al
    leerlo, sin necesidad de mantener un CSV por aeropuerto.

    - Si df_cambios está vacío, no se escribe nada (ni siquiera se toca el archivo).
    - Si el histórico no existe todavía, se crea con cabecera.
    - Si ya existe, se añade (append) sin repetir la cabecera.
    """
    if df_cambios.empty:
        logger.info("Sin cambios detectados: no se añade nada al histórico.")
        return

    existe_ya = HISTORICO_PATH.exists()
    df_cambios.to_csv(
        HISTORICO_PATH,
        mode="a",
        header=not existe_ya,
        index=False,
        encoding="utf-8-sig",
    )
    logger.info("Añadidas %d fila(s) nueva(s) al histórico (%s).", len(df_cambios), HISTORICO_PATH)


# --- Punto de entrada de la fase Load --------------------------------------

def run_load(airport_code: str, df_cambios: pd.DataFrame, df_estado_actualizado: pd.DataFrame) -> None:
    """
    Ejecuta la fase Load completa para UN aeropuerto:
    1. Añade los cambios detectados al histórico combinado (si los hay).
    2. Actualiza el estado persistente de ESE aeropuerto para la
        siguiente comparación.
    """
    guardar_cambios_en_historico(df_cambios)
    guardar_estado_actual(airport_code, df_estado_actualizado)


def run_load_todos(
    resultados_transform: dict[str, tuple[pd.DataFrame, pd.DataFrame, Path] | None],
) -> None:
    """
    Ejecuta la fase Load para el resultado de transform.procesar_todos(),
    es decir, para los 11 aeropuertos de una vez.

    `resultados_transform` es el dict {codigo_iata: (df_cambios, df_estado_actualizado, ruta) | None}
    que devuelve transform.procesar_todos(). Los aeropuertos con valor None
    (sin raw disponible en esa ejecución) se saltan sin tocar su estado.
    """
    procesados = 0
    for codigo, resultado in resultados_transform.items():
        if resultado is None:
            logger.warning("[%s] Sin resultado de Transform, se omite en Load.", codigo)
            continue

        df_cambios, df_estado_actualizado, _ruta = resultado
        run_load(codigo, df_cambios, df_estado_actualizado)
        procesados += 1

    logger.info("Load completo: %d/%d aeropuertos actualizados.", procesados, len(resultados_transform))