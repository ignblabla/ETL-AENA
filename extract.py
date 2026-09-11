"""
extract.py
----------
Fase EXTRACT del ETL de vuelos de salida de los 11 grandes aeropuertos
de España.

Responsabilidad única de este módulo: llamar al endpoint interno de Aena
para cada aeropuerto y guardar la respuesta cruda (sin transformar) en
disco, en una subcarpeta por aeropuerto, con un nombre de archivo basado
en el timestamp de la extracción.

Pensado para ejecutarse cada 10-15 minutos de forma continua (vía cron,
scheduler, etc.), por lo que además de la subcarpeta por aeropuerto se
añade una subcarpeta por día, para que ninguna carpeta acumule
decenas de miles de ficheros y el archivado/borrado por antigüedad sea
sencillo.

Cada ejecución genera un archivo nuevo en raw/<CODIGO_IATA>/<AAAA-MM-DD>/,
p.ej.:
    raw/SVQ/2026-09-10/14-30-05.json
    raw/MAD/2026-09-10/14-30-07.json

Esto nos permite, en la fase Transform posterior, comparar extracciones
consecutivas de un mismo aeropuerto y detectar cambios (estado del
vuelo, puerta, hora estimada...).
"""

import json
import logging
import time
from datetime import datetime
from pathlib import Path
import requests

# --- Configuración -----------------------------------------------------

BASE_URL = "https://www.aena.es/sites/Satellite"
FLIGHT_TYPE = "S"  # S = Salidas
DOS_DIAS = "si"

AEROPUERTOS = {
    "MAD": "Madrid-Barajas",
    "AGP": "Malaga-Costa del Sol",
    "BCN": "Barcelona-El Prat",
    "VLC": "Valencia",
    "SVQ": "Sevilla",
    "SCQ": "Santiago de Compostela",
    "PMI": "Palma de Mallorca",
    "TFN": "Tenerife Norte",
    "TFS": "Tenerife Sur",
    "LPA": "Gran Canaria",
    "MAH": "Menorca",
}

RAW_DIR = Path("raw")
RAW_DIR.mkdir(exist_ok=True)

MAX_REINTENTOS = 2
TIMEOUT_SEGUNDOS = 8
ESPERA_ENTRE_REINTENTOS = 2
ESPERA_ENTRE_AEROPUERTOS = 1

HEADERS = {
    "accept": "*/*",
    "accept-language": "es-ES,es;q=0.9",
    "origin": "https://www.aena.es",
    "referer": "https://www.aena.es/es/infovuelos.html",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/153.0.0.0 Safari/537.36"
    ),
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


# --- Extracción ----------------------------------------------------------

def extraer_vuelos(airport_code: str) -> list[dict] | None:
    """
    Llama al endpoint de Aena para un aeropuerto concreto y devuelve la
    lista cruda de vuelos (dicts).

    Reintenta hasta MAX_REINTENTOS veces si hay error de red o la API
    responde con un status distinto de 200. Devuelve None si todos los
    intentos fallan (para que el scheduler pueda decidir qué hacer, en
    vez de reventar el proceso).
    """
    params = {
        "pagename": "AENA_ConsultarVuelos",
        "airport": airport_code,
        "flightType": FLIGHT_TYPE,
        "dosDias": DOS_DIAS,
    }

    for intento in range(1, MAX_REINTENTOS + 1):
        try:
            resp = requests.post(
                BASE_URL, params=params, headers=HEADERS, data=b"", timeout=TIMEOUT_SEGUNDOS
            )
            resp.raise_for_status()
            datos = resp.json()
            logger.info("[%s] Extracción OK: %d vuelos recibidos", airport_code, len(datos))
            return datos

        except requests.exceptions.RequestException as e:
            logger.warning(
                "[%s] Intento %d/%d fallido: %s", airport_code, intento, MAX_REINTENTOS, e
            )
            if intento < MAX_REINTENTOS:
                time.sleep(ESPERA_ENTRE_REINTENTOS)
            else:
                logger.error("[%s] Se agotaron los reintentos. Extracción fallida.", airport_code)
                return None

        except json.JSONDecodeError as e:
            logger.error("[%s] La respuesta no es JSON válido: %s", airport_code, e)
            return None


def guardar_raw(airport_code: str, datos: list[dict]) -> Path:
    """
    Guarda los datos crudos en raw/<CODIGO_IATA>/<AAAA-MM-DD>/<HH-MM-SS>.json
    y devuelve la ruta del archivo creado.

    La subcarpeta por día evita que se acumulen decenas de miles de
    ficheros en una sola carpeta cuando la extracción se ejecuta cada
    10-15 minutos de forma continua, y facilita el archivado/borrado
    por antigüedad (basta con operar sobre carpetas de día completas).
    """
    ahora = datetime.now()
    carpeta_dia = RAW_DIR / airport_code / ahora.strftime("%Y-%m-%d")
    carpeta_dia.mkdir(parents=True, exist_ok=True)

    nombre_fichero = ahora.strftime("%H-%M-%S")
    ruta = carpeta_dia / f"{nombre_fichero}.json"

    with open(ruta, "w", encoding="utf-8") as f:
        json.dump(
            {
                "extraido_en": ahora.isoformat(),
                "airport": airport_code,
                "flight_type": FLIGHT_TYPE,
                "num_vuelos": len(datos),
                "vuelos": datos,
            },
            f,
            ensure_ascii=False,
            indent=2,
        )

    logger.info("[%s] Guardado: %s", airport_code, ruta)
    return ruta


def listar_extracciones(airport_code: str, dia: str | None = None) -> list[Path]:
    """
    Devuelve, ordenados cronológicamente, los ficheros raw de un
    aeropuerto para un día concreto (formato 'AAAA-MM-DD'). Si no se
    especifica día, usa el día de hoy.

    Pensado para que la fase Transform pueda comparar extracciones
    consecutivas sin tener que rebuscar entre todo el histórico.
    """
    dia = dia or datetime.now().strftime("%Y-%m-%d")
    carpeta_dia = RAW_DIR / airport_code / dia
    if not carpeta_dia.exists():
        return []
    return sorted(carpeta_dia.glob("*.json"))


def run_extract_aeropuerto(airport_code: str) -> Path | None:
    """Extract de un único aeropuerto: extrae y guarda. Devuelve la ruta o None si falló."""
    datos = extraer_vuelos(airport_code)
    if datos is None:
        return None
    return guardar_raw(airport_code, datos)


def run_extract() -> dict[str, Path | None]:
    """
    Punto de entrada de la fase Extract: recorre todos los aeropuertos
    de AEROPUERTOS, extrae y guarda cada uno por separado.

    Devuelve un dict {codigo_iata: ruta_o_None} para que el scheduler
    pueda saber qué aeropuertos fallaron sin que un fallo puntual
    interrumpa la extracción del resto.
    """
    resultados: dict[str, Path | None] = {}

    codigos = list(AEROPUERTOS.keys())
    for i, codigo in enumerate(codigos):
        resultados[codigo] = run_extract_aeropuerto(codigo)

        # Pausa entre aeropuertos (no tras el último) para no machacar el endpoint.
        if i < len(codigos) - 1:
            time.sleep(ESPERA_ENTRE_AEROPUERTOS)

    ok = [c for c, r in resultados.items() if r is not None]
    fallidos = [c for c, r in resultados.items() if r is None]
    logger.info("Extracción completa: %d/%d aeropuertos OK", len(ok), len(codigos))
    if fallidos:
        logger.warning("Aeropuertos fallidos: %s", ", ".join(fallidos))

    return resultados


if __name__ == "__main__":
    run_extract()