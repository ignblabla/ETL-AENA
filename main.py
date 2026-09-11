"""
main.py
-------
Punto de entrada del pipeline ETL de vuelos de salida de los 11 grandes
aeropuertos de España.

Encadena las tres fases:
    1. Extract   -> extract.run_extract()
    2. Transform -> transform.procesar_todos(estados_anteriores)
    3. Load      -> load.run_load_todos(resultados)

Pensado para ejecutarse periódicamente (cron local, systemd timer, o el
workflow de GitHub Actions en .github/workflows/etl.yml) cada 10-15 min.

raw/ es efímero: solo se usa dentro de esta misma ejecución para pasar
los datos de Extract a Transform. Lo único que necesita persistir entre
ejecuciones es data/ (estado_actual/*.pkl e historico_vuelos.csv), de lo
cual se encarga la fase Load.
"""

import logging

import extract
import load
import transform

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def main() -> None:
    logger.info("=== Inicio del pipeline ETL ===")

    # 1. Extract
    resultados_extract = extract.run_extract()

    # 2. Transform
    estados_anteriores = load.cargar_estados_anteriores()
    resultados_transform = transform.procesar_todos(estados_anteriores)

    # 3. Load
    load.run_load_todos(resultados_transform)

    aeropuertos_ok_extract = [c for c, r in resultados_extract.items() if r is not None]
    logger.info(
        "=== Pipeline completo: %d/%d aeropuertos extraídos correctamente ===",
        len(aeropuertos_ok_extract),
        len(resultados_extract),
    )


if __name__ == "__main__":
    main()