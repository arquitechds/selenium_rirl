from src.asociaciones import *
from src.db import *
from src.selenium_extract import *
from src.asociaciones import *
from concurrent.futures import ThreadPoolExecutor, as_completed
from loguru import logger

uniques = read_table('select url_prefix from archivos.asociaciones_pre_cfcrl where in_s3 = 0 ORDER BY RAND() LIMIT 1000')
uniques['url_prefix'].unique()


def run_task(i):
    asociacion_metadata = {'url_prefix': i}
    logger.info(asociacion_metadata['url_prefix'])
    write_selenium_documents_to_s3_v3(asociacion_metadata, 120)

# Número de hilos (ajusta según tus necesidades)
MAX_WORKERS = 1

with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    futures = [executor.submit(run_task, i) for i in uniques['url_prefix'].unique()]
    
    # Opcional: para manejar errores o saber cuándo termina cada tarea
    for future in as_completed(futures):
        try:
            future.result()
        except Exception as e:
            logger.error(f"Error en una tarea: {e}")
