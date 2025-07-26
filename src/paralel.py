from multiprocessing import Process
from src.jobs import check_new_urls_for_control_tables,extract_metadata,write_document_to_s3,write_selenium_documents_to_s3
from loguru import logger
from tqdm import tqdm
# pararel for contracts
from src.db import *
from src.extract import *

# Paralel framework to run jobs in many tasks

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def execute_paralel_tasks(processes):
    # start all processes
    for process in processes:
        process.start()
    # wait for all processes to complete
    for process in processes:
        process.join()

def execute_processes_list_in_batches(processes,max_paralel_tasks):
    task_batches = chunks(processes,max_paralel_tasks)
    for batch in tqdm(task_batches):
        execute_paralel_tasks(batch)


def run_paralel(split_task_function):
    ''' 
    Executes the paralel run given a split_task_function
    '''
    tasks_list = split_task_function
    execute_processes_list_in_batches(tasks_list,4)





# split custom tasks 


def split_tasks_check_new_urls_for_control_tables(lowerp = 1,upperp= 10):
    # create all tasks
    processes = [Process(target=check_new_urls_for_control_tables, args=(i,)) for i in range(lowerp,upperp)]
    tasks = lowerp-upperp
    logger.info(f'Created {tasks} tasks')
    return processes

def split_tasks_extract_metadata(table, limit=100,offset=0):
    '''
    Either contract_table is current_contracts or historic_contracts
    '''
    # create all tasks
    available_urls = read_table(f'select * from control.{table} limit {limit} offset {offset}')
    #available_urls = read_table("select * from metadata.contratos_historicos WHERE stamp_created < '2024-06-02'")
    available_urls = list(available_urls['url'])
    processes = [Process(target=extract_metadata, args=(url,)) for url in available_urls[0:limit]]
    tasks = len(available_urls)
    logger.info(f'Created {tasks} tasks')
    return processes


def split_tasks_extract_metadata_last_contratos(table):
    '''
    Either contract_table is current_contracts or historic_contracts
    '''
    # create all tasks

    source_urls = read_table(f"""select url from control.contratos WHERE status = 'archivo_historico'""")
    metadata_urls = read_table(f'select url from metadata.contratos_historicos')
    urls = list(set(source_urls['url']) - set(metadata_urls['url']))
    print(len(urls))
    metadata_urls = read_table("""select url from metadata.contratos_historicos WHERE stamp_created < '2024-06-17'""")
    available_urls = urls + list(metadata_urls['url'])
    print(len(available_urls))
    processes = [Process(target=extract_metadata, args=(url,)) for url in available_urls]
    tasks = len(available_urls)
    logger.info(f'Created {tasks} tasks')
    return processes




# pararel for pdfs
def split_tasks_files_to_s3(table,limit,offset):
    # create all tasks
    available_pdfs = read_table(f'select * from archivos.{table} where in_s3 = "0" limit {limit} offset {offset}')
    logger.info('PDFs urls obtained from mysql')
    #available_pdfs = available_pdfs[available_pdfs['in_s3']=='0']
    #available_pdfs = available_pdfs[::-1]
    available_pdfs = available_pdfs.to_dict('records')
    processes = [Process(target=write_document_to_s3, args=(url,table)) for url in available_pdfs]
    tasks = len(available_pdfs)
    logger.info(f'Created {tasks} tasks')
    return processes

contract_list = [
    "/contrato/3433", "/contrato/3477", "/contrato/3659", "/contrato/3750",
    "/contrato/3855", "/contrato/3866", "/contrato/3873", "/contrato/3927",
    "/contrato/3934", "/contrato/3986", "/contrato/3996", "/contrato/4009",
    "/contrato/4031", "/contrato/4062", "/contrato/4075", "/contrato/4130",
    "/contrato/4134", "/contrato/4154", "/contrato/4197", "/contrato/4209",
    "/contrato/4214", "/contrato/4216", "/contrato/4230", "/contrato/4242",
    "/contrato/4252", "/contrato/4253", "/contrato/4257", "/contrato/4267",
    "/contrato/4283", "/contrato/4300", "/contrato/4303", "/contrato/4309",
    "/contrato/4313", "/contrato/4316", "/contrato/4321", "/contrato/4370",
    "/contrato/4386", "/contrato/4389", "/contrato/4393", "/contrato/4401",
    "/contrato/4405", "/contrato/4410", "/contrato/4413", "/contrato/4414",
    "/contrato/4425", "/contrato/4431", "/contrato/4432", "/contrato/4472",
    "/contrato/4482", "/contrato/4486", "/contrato/4488", "/contrato/4503",
    "/contrato/4510", "/contrato/4525", "/contrato/4550", "/contrato/4552",
    "/contrato/4557", "/contrato/4565", "/contrato/4567", "/contrato/4572",
    "/contrato/4577", "/contrato/4593", "/contrato/4601", "/contrato/4605",
    "/contrato/4610", "/contrato/4616", "/contrato/4628", "/contrato/4631",
    "/contrato/4659", "/contrato/4670", "/contrato/4672", "/contrato/4673",
    "/contrato/4676", "/contrato/4690", "/contrato/4695", "/contrato/4700",
    "/contrato/4703", "/contrato/4716", "/contrato/4727", "/contrato/4742",
    "/contrato/4786", "/contrato/4792", "/contrato/4795", "/contrato/4836",
    "/contrato/4841", "/contrato/4882", "/contrato/4894", "/contrato/4899",
    "/contrato/4911", "/contrato/4945", "/contrato/4958", "/contrato/4967",
    "/contrato/4979", "/contrato/4980", "/contrato/5021", "/contrato/5029",
    "/contrato/5037", "/contrato/5040", "/contrato/5110", "/contrato/5115",
    "/contrato/5139", "/contrato/5215", "/contrato/5264", "/contrato/5268",
    "/contrato/5286", "/contrato/5293"]

def split_tasks_files_to_s3_selenium(table, limit, offset,status  = 'archivo_historico', table_archivos = 'contratos_historicos'):
    # create all tasks
    #available_pdfs = read_table(f'select url,numero_registro from control.contratos where status = "{status}" limit {limit} offset {offset}')
    available_pdfs = read_table(f'select url,numero_registro from control.contratos where status = "{status}"')
    is_vigente = read_table(f'select url from control.visited_contratos_deposito_inicial' )
    is_vigente['url'] = is_vigente['url'].str.replace('https://repositorio.centrolaboral.gob.mx','')
    available_pdfs = available_pdfs[available_pdfs['url'].isin(is_vigente['url'])]

    #visited_urls = read_table(f'select url from control.visited_')
    #visited_urls['prefix'] = visited_urls['url'].str.replace('https://repositorio.centrolaboral.gob.mx','')
    #valid_urls = set(available_pdfs['url']) - set(visited_urls['prefix'])
    valid_urls =  set(available_pdfs['url'])
    processes = [Process(target=write_selenium_documents_to_s3, args=(url,table,table_archivos)) for url in valid_urls]
    tasks = len(valid_urls)
    logger.info(f'Created {tasks} tasks')
    return processes

def split_tasks_files_to_s3_selenium_missing(table,status  = 'archivo_historico', table_archivos = 'contratos_historicos'):
    # create all tasks
    is_imss = read_table(f'select numero_de_registro from control.contratos_historicos_imss where imss = "1"')
    is_imss = list(is_imss['numero_de_registro'].unique())
    available_pdfs = read_table(f'select url,numero_registro from control.{table} where status = "{status}"')
    available_pdfs = available_pdfs[available_pdfs['numero_registro'].isin(is_imss)]
    visited_urls = read_table(f'select url from control.visited_historicos')
    visited_urls['prefix'] = visited_urls['url'].str.replace('https://repositorio.centrolaboral.gob.mx','')
    valid_urls = set(available_pdfs['url']) - set(visited_urls['prefix'])
    processes = [Process(target=write_selenium_documents_to_s3, args=(url,table,table_archivos)) for url in valid_urls]
    tasks = len(valid_urls)
    logger.info(f'Created {tasks} tasks')
    return processes


def contract_files_upload_to_s3_paralel(table,limit,offset):
    tasks_list = split_tasks_files_to_s3(table,limit,offset)
    execute_processes_list_in_batches(tasks_list,8)

def contract_files_upload_to_s3_paralel_selenium(table,limit,offset,status, table_archivos):
    tasks_list = split_tasks_files_to_s3_selenium(table,limit,offset,status, table_archivos)
    execute_processes_list_in_batches(tasks_list,32)

def contract_files_upload_to_s3_paralel_selenium_missing(table,status, table_archivos):
    tasks_list = split_tasks_files_to_s3_selenium_missing(table,status, table_archivos)
    execute_processes_list_in_batches(tasks_list,32)