from src.selenium_extract import *
#https://repositorio.centrolaboral.gob.mx/asociacion/37730
#37729#
#selenium_download_v3('/asociacion/14089','/workspaces/selenium_rirl/pdfs',20)

#from src.paralel import *
#from src.extract import *
# Update data from first 300 pages for all categories
#run_paralel(split_tasks_extract_metadata('asociaciones',limit = 10, offset = 14280))
from src.asociaciones import *
#https://repositorio.centrolaboral.gob.mx/asociacion/14088
from concurrent.futures import ThreadPoolExecutor, as_completed
from loguru import logger

def run_task(i):
    asociacion_metadata = {'url_prefix': f'/asociacion/{i}'}
    logger.info(asociacion_metadata['url_prefix'])
    get_asociaciones(asociacion_metadata)

# Número de hilos (ajusta según tus necesidades)
MAX_WORKERS = 12

with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    futures = [executor.submit(run_task, i) for i in range(26000, 40000)]
    
    # Opcional: para manejar errores o saber cuándo termina cada tarea
    for future in as_completed(futures):
        try:
            future.result()
        except Exception as e:
            logger.error(f"Error en una tarea: {e}")
