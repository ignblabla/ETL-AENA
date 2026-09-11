"""
transform.py
------------
Fase TRANSFORM del ETL de vuelos de salida de los 11 grandes aeropuertos
de España.

Responsabilidades:
1. Limpiar y estructurar el JSON crudo de una extracción (raw/<IATA>/<fecha>/*.json)
    en un DataFrame con una columna "clave_vuelo" que identifica de forma
    única cada vuelo a lo largo del día, para cualquier aeropuerto.
2. Comparar esa extracción contra el último estado conocido de cada vuelo
    de ESE aeropuerto y devolver SOLO las filas nuevas o que han cambiado
    en los campos que nos interesa vigilar (estado, puerta, hora estimada,
    mostrador).

Este módulo NO escribe en el histórico final ni en el "estado actual"
persistente — eso es responsabilidad de la fase Load. Aquí solo se
calculan los DataFrames en memoria: (cambios, estado_actualizado).

IMPORTANTE: el "estado anterior" de los vuelos se gestiona SIEMPRE por
aeropuerto por separado (nunca mezclando tablas de distintos aeropuertos),
ya que la fase Load persiste un estado independiente por cada uno.
"""

import json
import logging
from pathlib import Path

import pandas as pd

from extract import AEROPUERTOS

RAW_DIR = Path("raw")

CAMPOS_VIGILADOS = ["estado", "puerta", "salida_estimada", "mostrador"]

ESTADOS = {
    "BOR": "Embarcando",
    "RET": "Retrasado",
    "FIN": "Finalizado",
    "CER": "Cerrado",
    "ULT": "Ult.Llamada",
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


# --- Carga de la extracción cruda ----------------------------------------
def cargar_raw(ruta: Path) -> dict:
    """Carga un archivo raw/<IATA>/<fecha>/<hora>.json generado por extract.py."""
    with open(ruta, "r", encoding="utf-8") as f:
        return json.load(f)


def listar_archivos_raw(airport_code: str, dia: str | None = None) -> list[Path]:
    """
    Devuelve, ordenados cronológicamente, los ficheros raw de un aeropuerto.

    Si se especifica `dia` (formato "AAAA-MM-DD"), busca solo en esa carpeta.
    Si no, busca en todas las carpetas de día disponibles para ese aeropuerto
    (raw/<IATA>/*/*.json), lo que evita problemas al cruzar la medianoche.
    """
    carpeta_aeropuerto = RAW_DIR / airport_code
    if not carpeta_aeropuerto.exists():
        return []

    patron = f"{dia}/*.json" if dia else "*/*.json"
    return sorted(carpeta_aeropuerto.glob(patron))


def ultimo_archivo_raw(airport_code: str, dia: str | None = None) -> Path | None:
    """Devuelve la ruta del archivo raw más reciente de un aeropuerto, o None si no hay ninguno."""
    archivos = listar_archivos_raw(airport_code, dia)
    return archivos[-1] if archivos else None


# --- Limpieza / estructuración --------------------------------------------

def limpiar_vuelos(raw: dict) -> pd.DataFrame:
    """
    Convierte el JSON crudo (tal como lo guarda extract.py) en un DataFrame
    limpio, con una clave única por vuelo y los campos vigilados normalizados.

    La clave incluye el código de aeropuerto de origen (raw["airport"]) para
    que sea única incluso comparando entre distintos aeropuertos.
    """
    aeropuerto = raw["airport"]
    vuelos = raw["vuelos"]
    df = pd.DataFrame(vuelos)

    if df.empty:
        return df

    df["aeropuerto"] = aeropuerto

    # Fecha/hora reales como datetime
    df["salida_programada"] = pd.to_datetime(
        df["fecha"] + " " + df["horaProgramada"], format="%d/%m/%Y %H:%M:%S", errors="coerce"
    )

    df["salida_estimada"] = pd.to_datetime(
        df["fechaEstimada"] + " " + df["horaEstimada"], format="%d/%m/%Y %H:%M:%S", errors="coerce"
    )

    # Puerta: usamos la primera; si viniera "null" como texto, lo dejamos vacío
    df["puerta"] = df["puertaPrimera"].replace("null", pd.NA)

    # Mostrador como rango combinado, más fácil de comparar como un solo campo
    df["mostrador"] = df["mostradorDesde"].fillna("") + "-" + df["mostradorHasta"].fillna("")

    df["estado_desc"] = df["estado"].map(ESTADOS).fillna(df["estado"])

    # Clave única del vuelo: aeropuerto + numVuelo + fecha + hora + destino.
    df["clave_vuelo"] = (
        df["aeropuerto"] + "_"
        + df["numVuelo"].astype(str) + "_"
        + df["fecha"] + "_"
        + df["horaProgramada"] + "_"
        + df["iataOtro"]
    )

    df = df.drop_duplicates(subset=["clave_vuelo"]).reset_index(drop=True)

    columnas = {
        "clave_vuelo": "clave_vuelo",
        "aeropuerto": "aeropuerto",
        "numVuelo": "num_vuelo",
        "iataCompania": "iata_compania",
        "nombreCompania": "aerolinea",
        "iataOtro": "destino_iata",
        "ciudadIataOtro": "destino_ciudad",
        "salida_programada": "salida_programada",
        "salida_estimada": "salida_estimada",
        "estado": "estado",
        "estado_desc": "estado_desc",
        "terminal": "terminal",
        "puerta": "puerta",
        "mostrador": "mostrador",
        "tipoAeronave": "tipo_aeronave",
    }

    df = df[list(columnas.keys())].rename(columns=columnas)
    df = df.sort_values("salida_programada").reset_index(drop=True)
    return df


# --- Detección de cambios --------------------------------------------------

def _distinto(a, b) -> bool:
    """
    Compara dos valores tratando NaN/NaT como iguales entre sí.

    Sin esto, dos vuelos sin "salida_estimada" todavía (NaT en ambos casos)
    se marcarían como "cambiados" en cada ejecución, ya que NaT != NaT
    evalúa a True en pandas — con extracciones cada 10-15 minutos eso
    generaría falsos positivos constantes.
    """
    if pd.isna(a) and pd.isna(b):
        return False
    return a != b


def detectar_cambios(
    df_nuevo: pd.DataFrame, df_estado_anterior: pd.DataFrame | None
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Compara la extracción nueva de UN aeropuerto contra el último estado
    conocido de ese mismo aeropuerto.

    IMPORTANTE: df_nuevo y df_estado_anterior deben pertenecer siempre al
    mismo aeropuerto. No mezclar estados de distintos aeropuertos aquí;
    la comparación se hace aeropuerto por aeropuerto.

    Devuelve una tupla (df_cambios, df_estado_actualizado):
    - df_cambios: solo las filas nuevas o con algún campo vigilado distinto
                al último estado conocido. Incluye columna "detectado_en".
    - df_estado_actualizado: el nuevo "último estado conocido" de TODOS los
                vuelos vistos de ese aeropuerto (para pasárselo a la
                siguiente ejecución).
    """
    ahora = pd.Timestamp.now()

    if df_nuevo.empty:
        logger.info("La extracción no trae vuelos.")
        return df_nuevo, df_estado_anterior if df_estado_anterior is not None else df_nuevo

    if df_estado_anterior is None or df_estado_anterior.empty:
        # Primera ejecución para este aeropuerto: todo se considera "nuevo"
        logger.info("No hay estado anterior. Se toman todos los vuelos (%d) como nuevos.", len(df_nuevo))
        df_cambios = df_nuevo.copy()
        df_cambios["detectado_en"] = ahora
        return df_cambios, df_nuevo

    # Convertimos clave_vuelo en el índice de la tabla anterior,
    anterior_indexado = df_estado_anterior.set_index("clave_vuelo")
    filas_cambiadas = []

    for _, fila_nueva in df_nuevo.iterrows():
        clave = fila_nueva["clave_vuelo"]

        if clave not in anterior_indexado.index:
            filas_cambiadas.append(fila_nueva)
            continue

        fila_anterior = anterior_indexado.loc[clave]
        ha_cambiado = any(
            _distinto(fila_nueva[campo], fila_anterior[campo]) for campo in CAMPOS_VIGILADOS
        )
        if ha_cambiado:
            filas_cambiadas.append(fila_nueva)

    if filas_cambiadas:
        df_cambios = pd.DataFrame(filas_cambiadas)
        df_cambios["detectado_en"] = ahora
    else:
        df_cambios = pd.DataFrame(columns=list(df_nuevo.columns) + ["detectado_en"])

    logger.info("%d vuelo(s) nuevo(s) o con cambios detectados.", len(df_cambios))

    df_estado_actualizado = df_nuevo.copy()
    return df_cambios, df_estado_actualizado


# --- Orquestación por aeropuerto y para todos los aeropuertos -------------

def procesar_aeropuerto(
    airport_code: str, df_estado_anterior: pd.DataFrame | None, dia: str | None = None
) -> tuple[pd.DataFrame, pd.DataFrame, Path] | None:
    """
    Procesa la extracción más reciente de UN aeropuerto: la carga, la
    limpia y detecta cambios respecto a su estado anterior.

    Devuelve (df_cambios, df_estado_actualizado, ruta_procesada), o None si
    ese aeropuerto no tiene ningún archivo raw disponible todavía.
    """
    ruta = ultimo_archivo_raw(airport_code, dia)
    if ruta is None:
        logger.warning("[%s] No hay ningún archivo raw disponible.", airport_code)
        return None

    raw = cargar_raw(ruta)
    df_nuevo = limpiar_vuelos(raw)
    df_cambios, df_estado_actualizado = detectar_cambios(df_nuevo, df_estado_anterior)
    return df_cambios, df_estado_actualizado, ruta


def procesar_todos(
    estados_anteriores: dict[str, pd.DataFrame | None], dia: str | None = None
) -> dict[str, tuple[pd.DataFrame, pd.DataFrame, Path] | None]:
    """
    Procesa la extracción más reciente de los 11 aeropuertos.

    `estados_anteriores` debe ser un dict {codigo_iata: df_estado_o_None}
    con el último estado conocido de cada aeropuerto (típicamente cargado
    por la fase Load desde donde lo haya persistido). Los aeropuertos que
    no aparezcan en el dict se tratan como si fuera su primera ejecución.

    Devuelve un dict {codigo_iata: (df_cambios, df_estado_actualizado, ruta) | None},
    manteniendo siempre separado el estado de cada aeropuerto.
    """
    resultados: dict[str, tuple[pd.DataFrame, pd.DataFrame, Path] | None] = {}

    for codigo in AEROPUERTOS:
        estado_anterior = estados_anteriores.get(codigo)
        resultados[codigo] = procesar_aeropuerto(codigo, estado_anterior, dia)

    procesados = [c for c, r in resultados.items() if r is not None]
    logger.info("Transform completo: %d/%d aeropuertos procesados.", len(procesados), len(AEROPUERTOS))

    return resultados