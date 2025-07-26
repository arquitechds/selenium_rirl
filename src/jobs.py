from src.extract import *
import pandas as pd
from src.db import * 
from tqdm import tqdm
import os 
from loguru import logger
from src.utils import run_twice

##########################
#    JOBS Functions   #
##########################

# The purpose of this functions are to extract in an ordered manner the data (using extract.py package) 
# and then send it to a db (db.py package) all in a single job


rirl = rirl_scrapping_session()

def check_new_urls_for_control_tables(i):
    ''' 
    Extract all possible 4 data dicts from https://repositorio.centrolaboral.gob.mx/ (contratos vigentes, historicos, asociaciones y reglamentos)
    and insert them to mysql table
        inputs 
            i(str) of page number
    '''
    soup = rirl.get_raw_entries_by_page(i)
    reglamentos_entries, asociaciones_entries,contratos_vig_entries, contratos_hist_entries = extract_all_entries(soup)
    if len(asociaciones_entries)>0:
        insert_data(asociaciones_entries,'control.asociaciones')
        logger.info('Asociaciones data added!')
    if len(contratos_vig_entries)>0:
        insert_data(contratos_vig_entries,'control.contratos')
        logger.info('Contratos vigentes data added!')
    if len(reglamentos_entries)>0:
        insert_data(reglamentos_entries,'control.reglamentos')
        logger.info('Reglamentos data added!')
    if len(contratos_hist_entries)>0:
        logger.info(contratos_hist_entries)
        insert_data(contratos_hist_entries,'control.contratos')
        logger.info('Contratos historicos data added!')


def extract_metadata(url):
    ''' 
    Extract all possible data dicts from contratods
    and insert them to mysql table
        inputs 
            url(str) contract url prefix
    '''
    if 'reglamento' in url:
        logger.info('Reglamento url')
        get_data_f = get_data_reglamentos
        urls_table = 'archivos.reglamentos'

    elif 'contrato' in url:
        logger.info('Contrato url')
        get_data_f = get_data_contratos
        urls_table = 'archivos.contratos'

    elif 'asociacion' in url:
        logger.info('Asociacion url')
        get_data_f = get_data_asociaciones
        urls_table = 'archivos.asociaciones'
    metadata, urls, empresas, table = get_data_f(url)

    if len(metadata)> 0:
        insert_data([metadata], table)
    if len(urls)> 0:
        insert_data(urls, urls_table)
    if len(empresas)> 0:
        insert_data(empresas, 'metadata.empresas_relacionadas')

def extract_metadata_asociacion(url):
    ''' 
    Extract all possible data dicts from contratods
    and insert them to mysql table
        inputs 
            url(str) contract url prefix
    '''
    logger.info('Asociacion url')
    get_data_f = get_data_asociaciones
    urls_table = 'archivos.asociaciones'
    metadata, urls, empresas, table = get_data_f(url)
    control ,table_control = create_control_dict_asociaciones(url, [], urls, 'asociacion')
    insert_data([control], table_control)
    if len(urls)> 0:
        insert_data(urls, urls_table)




def correct_subtype(string):
    string = string.replace('á','a')
    string = string.replace('é','e')
    string = string.replace('í','i')
    string = string.replace('ó','o')
    string = string.replace('ú','u')
    string = string.replace('Á','A')
    string = string.replace('É','E')
    string = string.replace('Í','I')
    string = string.replace('Ó','O')
    string = string.replace('Ú','U')
    string = string.replace(' ','_')
    string = string.replace('/','')
    string = string.lower()
    return string




def write_document_to_s3(entry,table):
    ''' 
    Sends document to S3 and uploads control table
    '''
    home = 'https://repositorio.centrolaboral.gob.mx/'

    url = entry['file_url'].strip()

    try:
        # download file
        r = requests.get(url, stream=True)

        if r.status_code == 200:
            # create s3 uri
            #name = 'metadata/archivos/contratos/' + name
            corrected_file_id = entry['file_id'].replace(' ', '_')
            corrected_file_id = corrected_file_id.replace('/', '_')
            if entry['contrato_type'] == None:
                corrected_type = 'unknwown'
            else:
                corrected_type = entry['contrato_type'].replace('contrato ','')
            corrected_subtype = correct_subtype(entry['type'])
            file_name = url.rsplit('/', 1)[-1]
            name = 'contratos/' + corrected_type + '/' + corrected_file_id + '/'+ corrected_subtype + '/' + file_name 
            send_to_s3(name,r.content)
            # send data to control table
            new_entry = [{'file_url' : entry['file_url'],
                            'type': entry['type'],
                            'source': entry['source'],
                            'file_id': entry['file_id'],
                            #'stamp_created': entry['stamp_created'],
                            'url_active' : '1',
                            'in_s3': '1',
                            's3_uri': name,
                            }]
            insert_data(new_entry,f'archivos.{table}')
        else:
            new_entry = [{'file_url' : url,
                            'type': entry['type'],
                            'source': entry['source'],
                            'file_id': entry['file_id'],
                            #'stamp_created': entry['stamp_created'],
                            'url_active' : '0',
                            'in_s3': '0',
                            's3_uri': '',
                            }]
            insert_data(new_entry,f'archivos.{table}')
            logger.info('File NOT inserted to S3')
    except:
        new_entry = [{'file_url' : url,
                        'type': entry['type'],
                        'source': entry['source'],
                        'file_id': entry['file_id'],
                        #'stamp_created': entry['stamp_created'],
                        'url_active' : '0',
                        'in_s3': '0',
                        's3_uri': '',
                        }]
        insert_data(new_entry,f'archivos.{table}')
        logger.info('File NOT inserted to S3')



def write_document_to_s3(entry,table):
    ''' 
    Sends document to S3 and uploads control table
    '''
    home = 'https://repositorio.centrolaboral.gob.mx/'

    url = entry['file_url'].strip()
    if ('https:' not in url):
        new_entry = [{ 'url_prefix': url,
                        'file_url' : url,
                        'type': entry['type'],
                        'source': entry['source'],
                        'asociacion_id': entry['file_id'],
                        #'stamp_created': entry['stamp_created'],
                        'url_active' : '0',
                        'in_s3': '0',
                        's3_uri': '',
                        'size_mb': ''
                        }]
        
        
        
        [{'file_url' : url,
                'type': entry['type'],
                'source': entry['source'],
                'file_id': entry['file_id'],
                #'stamp_created': entry['stamp_created'],
                'url_active' : '0',
                'in_s3': 'selenium',
                's3_uri': '',
                }]
        insert_data(new_entry,f'archivos.{table}')
        logger.info('File tagged for selenium extraction')
    else:
        try:
            # download file
            r = requests.get(url, stream=True)
            # extract name
            #name = re.findall('([^\/]+$)',entry['file_url'])[0].strip()
            url = entry['file_url'].strip()
            # download file
            r = requests.get(url, stream=True)
            # extract name
            name = re.findall('([^\/]+$)',entry['file_url'])[0].strip()

            if (r.url == home):
                new_entry = [{'file_url' : url,
                        'type': entry['type'],
                        'source': entry['source'],
                        'file_id': entry['file_id'],
                        #'stamp_created': entry['stamp_created'],
                        'url_active' : '0',
                        'in_s3': 'selenium',
                        's3_uri': '',
                        }]
                insert_data(new_entry,f'archivos.{table}')
                logger.info('File tagged for selenium extraction')

            elif r.status_code == 200:
                # create s3 uri
                #name = 'metadata/archivos/contratos/' + name
                corrected_file_id = entry['file_id'].replace(' ', '_')
                corrected_file_id = corrected_file_id.replace('/', '_')
                if entry['contrato_type'] == None:
                    corrected_type = 'unknwown'
                else:
                    corrected_type = entry['contrato_type'].replace('contrato ','')
                corrected_subtype = correct_subtype(entry['type'])
                file_name = url.rsplit('/', 1)[-1]
                name = 'contratos/' + corrected_type + '/' + corrected_file_id + '/'+ corrected_subtype + '/' + file_name 
                send_to_s3(name,r.content)
                # send data to control table
                new_entry = [{'file_url' : entry['file_url'],
                                'type': entry['type'],
                                'source': entry['source'],
                                'file_id': entry['file_id'],
                                #'stamp_created': entry['stamp_created'],
                                'url_active' : '1',
                                'in_s3': '1',
                                's3_uri': name,
                                }]
                insert_data(new_entry,f'archivos.{table}')
            else:
                new_entry = [{'file_url' : url,
                                'type': entry['type'],
                                'source': entry['source'],
                                'file_id': entry['file_id'],
                                #'stamp_created': entry['stamp_created'],
                                'url_active' : '0',
                                'in_s3': '0',
                                's3_uri': '',
                                }]
                insert_data(new_entry,f'archivos.{table}')
                logger.info('File NOT inserted to S3')
        except:
            new_entry = [{'file_url' : url,
                            'type': entry['type'],
                            'source': entry['source'],
                            'file_id': entry['file_id'],
                            #'stamp_created': entry['stamp_created'],
                            'url_active' : '0',
                            'in_s3': '0',
                            's3_uri': '',
                            }]
            insert_data(new_entry,f'archivos.{table}')
            logger.info('File NOT inserted to S3')


from src.selenium_extract import *

def get_file_names(folder_path):
    try:
        names = []
        # List all files and directories in the specified folder
        with os.scandir(folder_path) as entries:
            for entry in entries:
                # Check if it's a file
                if entry.is_file():
                    new_name = entry.name
                    new_name = new_name.replace(' ', '_')
                    new_name = new_name.replace('/', '_')
                    new_name = new_name.replace('|', '_')
                    new_name = new_name.replace('.crdownload', '')

                    names = names + [new_name]

                    # Rename the file
                    new_path = os.path.join(folder_path, new_name)
                    old_path = os.path.join(folder_path, entry.name)
                    os.rename(old_path, new_path)
    except: 
        names = []

    return names

# Example usage
def get_last_substring_after_slash(input_string):
    # Split the string by slash and return the last part
    inp = input_string.split('/')[-1]
    inp = inp.replace('|','_')
    inp = inp.replace(' ', '_')
    inp = inp.replace('/', '_')
    return inp

def get_last_substring_after_character(input_string,character):
    # Split the string by slash and return the last part
    inp = input_string.split(character)[-1]
    return inp

import shutil


@run_twice
def write_selenium_documents_to_s3(url_prefix,table, table_archivos):
    ''' 
    Sends document to S3 and uploads control table
    '''
    logger.info(url_prefix)
    download_dir = get_last_substring_after_slash(url_prefix)
    url_original = 'https://repositorio.centrolaboral.gob.mx' + url_prefix

    try:
        url = 'https://repositorio.centrolaboral.gob.mx' + url_prefix
        selenium_download(url_prefix,download_dir)
        a,entries,c,d = get_data_contratos(url_prefix)
        local_document_names = get_file_names(download_dir)        

        if (len(entries) == 0) or (len(local_document_names) == 0):
            control, table = create_control_dict_global(url_original, [], entries, d)
            insert_data([control],table)

            control, table = create_control_dict_specific(url_original, [], entries, d)
            insert_data([control],table)
            logger.info('Nothing downloaded!')
            return 


        for entry in entries:

            document_name = get_last_substring_after_slash(entry['file_url'])
            url = entry['file_url'].strip()

            if document_name in set(local_document_names) :
                # create s3 uri
                #name = 'metadata/archivos/contratos/' + name
                corrected_file_id = entry['file_id'].replace(' ', '_')
                corrected_file_id = corrected_file_id.replace('/', '_')
                corrected_file_id = corrected_file_id.replace('|', '_')

                if entry['contrato_type'] == None:
                    corrected_type = 'unknwown'
                else:
                    corrected_type = entry['contrato_type'].replace('contrato ','')
                corrected_subtype = correct_subtype(entry['type'])
                file_name = url.rsplit('/', 1)[-1]
                name = 'contratos/' + corrected_type + '/' + corrected_file_id + '/'+ corrected_subtype + '/' + document_name
                local_name = download_dir+ '/' + document_name 
                size = os.path.getsize(local_name)/ (1024 ** 2)                                

                send_to_s3_from_local(name,local_name)
                # send data to control table
                new_entry = [{  'url_prefix': url_prefix,
                                'file_url' : entry['file_url'],
                                'type': entry['type'],
                                'source': entry['source'],
                                'file_id': entry['file_id'],
                                #'stamp_created': entry['stamp_created'],
                                'contrato_type': corrected_type,
                                'url_active' : '1',
                                'in_s3': '1',
                                's3_uri': name,
                                'size_mb': size
                                }]
                print(new_entry)
                insert_data(new_entry,f'archivos.{table_archivos}')
            else:
                new_entry = [{'url_prefix': url_prefix,
                              'file_url' : url,
                                'type': entry['type'],
                                'source': entry['source'],
                                'file_id': entry['file_id'],
                                'contrato_type': corrected_type,
                                #'stamp_created': entry['stamp_created'],
                                'url_active' : '0',
                                'in_s3': '0',
                                's3_uri': '',
                                'size_mb': 0
                                }]
                insert_data(new_entry,f'archivos.{table_archivos}')
                logger.info('File NOT inserted to S3')
                print(new_entry)
                insert_data(new_entry,f'archivos.{table}')
        control, table = create_control_dict_global(url_original, local_document_names, entries, d)
        insert_data([control],table)

        control, table = create_control_dict_specific(url_original, local_document_names, entries, d)
        insert_data([control],table)
        shutil.rmtree(download_dir)
    except:
        logger.info('Error')
        try:
            shutil.rmtree(download_dir)
        except:
            pass

def write_selenium_documents_to_s3_v3(url_prefix,table, table_archivos, size_mb):
    ''' 
    Sends document to S3 and uploads control table
    '''
    logger.info(url_prefix)
    download_dir = get_last_substring_after_slash(url_prefix)
    url_original = 'https://repositorio.centrolaboral.gob.mx' + url_prefix

    try:
        url = 'https://repositorio.centrolaboral.gob.mx' + url_prefix
        selenium_download_v3(url_prefix,download_dir,size_mb)
        a,entries,c,d = get_data_contratos(url_prefix)
        local_document_names = get_file_names(download_dir)        

        if (len(entries) == 0) or (len(local_document_names) == 0):
            control, table = create_control_dict_global(url_original, [], entries, d)
            insert_data([control],table)

            control, table = create_control_dict_specific(url_original, [], entries, d)
            insert_data([control],table)
            logger.info('Nothing downloaded!')
            return 


        for entry in entries:

            document_name = get_last_substring_after_slash(entry['file_url'])
            url = entry['file_url'].strip()

            if entry['contrato_type'] == None:
                corrected_type = 'unknwown'
            else:
                corrected_type = entry['contrato_type'].replace('contrato ','')
                
            if document_name in set(local_document_names) :
                # create s3 uri
                #name = 'metadata/archivos/contratos/' + name
                corrected_file_id = entry['file_id'].replace(' ', '_')
                corrected_file_id = corrected_file_id.replace('/', '_')
                corrected_file_id = corrected_file_id.replace('|', '_')

                corrected_subtype = correct_subtype(entry['type'])
                file_name = url.rsplit('/', 1)[-1]
                name = 'contratos/' + corrected_type + '/' + corrected_file_id + '/'+ corrected_subtype + '/' + document_name
                local_name = download_dir+ '/' + document_name 
                size = os.path.getsize(local_name)/ (1024 ** 2)                                

                send_to_s3_from_local(name,local_name)
                # send data to control table
                new_entry = [{  'url_prefix': url_prefix,
                                'file_url' : entry['file_url'],
                                'type': entry['type'],
                                'source': entry['source'],
                                'file_id': entry['file_id'],
                                #'stamp_created': entry['stamp_created'],
                                'contrato_type': corrected_type,
                                'url_active' : '1',
                                'in_s3': '1',
                                's3_uri': name,
                                'size_mb': size,
                                'text': 0
                                }]
                print(new_entry)
                insert_data(new_entry,f'archivos.{table_archivos}')
            else:
                new_entry = [{'url_prefix': url_prefix,
                              'file_url' : url,
                                'type': entry['type'],
                                'source': entry['source'],
                                'file_id': entry['file_id'],
                                'contrato_type': corrected_type,
                                #'stamp_created': entry['stamp_created'],
                                'url_active' : '0',
                                'in_s3': '0',
                                's3_uri': '',
                                'size_mb': 0,
                                'text': 0}]
                insert_data(new_entry,f'archivos.{table_archivos}')
                logger.info('File NOT inserted to S3')
                print(new_entry)
                insert_data(new_entry,f'archivos.{table}')
        control, table = create_control_dict_global(url_original, local_document_names, entries, d)

        print([control])
        print([table])

        insert_data([control],table)
        control, table = create_control_dict_specific(url_original, local_document_names, entries, d)
        print([control])
        print([table])
        insert_data([control],table)
        shutil.rmtree(download_dir)
    except Exception as e:
        logger.info(e)
        try:
            shutil.rmtree(download_dir)
        except:
            pass


def create_control_dict_specific(url_original, local_document_names, entries, ctype):
    ctype = ctype.replace('metadata.','')

    if ctype == 'contratos_historicos':
        control, table = create_control_dict_historico(url_original, local_document_names, entries, ctype)
    elif ctype == 'contratos_vigentes':
        control, table = create_control_dict_vigente(url_original, local_document_names, entries, ctype)
    elif ctype == 'contratos_revision_salarial':
        control, table = create_control_dict_revision(url_original, local_document_names, entries, ctype)
    elif ctype == 'contratos_deposito_inicial':
        control, table = create_control_dict_deposito(url_original, local_document_names, entries, ctype)
    else:
        control, table = create_control_dict_otros(url_original, local_document_names, entries, ctype)
    return control, table

def create_control_dict_global(url_original, local_document_names, entries, ctype):
    ctype = ctype.replace('metadata.','')

    ls = []
    for lr in local_document_names:
        lr = lr.replace('.crdownload','')
        ls = ls + [lr]

    downloded_files = []
    for file in entries:
        if get_last_substring_after_slash(file['file_url']) in ls:
            downloded_files = downloded_files + [file]

    control = {'url':url_original,'all_documents':len(entries),'extracted_documents':len(local_document_names),
               'tipo': ctype}  
    return control , 'control.visited_'



def create_control_dict_vigente(url_original, local_document_names, entries, ctype):
    ls = []
    for lr in local_document_names:
        lr = lr.replace('.crdownload','')
        ls = ls + [lr]

    downloded_files = []
    for file in entries:
        if get_last_substring_after_slash(file['file_url']) in ls:
            downloded_files = downloded_files + [file]

    if 'constancia_de_legitimacion' in [d['type'] for d in downloded_files]:
        constancia = 1
    else:
        constancia = 0
    if 'acta_de_resultados_de_legitimacion' in [d['type'] for d in downloded_files]:
        acta = 1
    else:
        acta = 0
    if 'contrato_colectivo_de_trabajo' in [d['type'] for d in downloded_files]:
        contrato = 1
    else:
        contrato = 0

    control = {'url':url_original,'all_documents':len(entries),'extracted_documents':len(local_document_names),
               'constancia':constancia, 
               'acta': acta , 
               'contrato': contrato, 
               'tipo': ctype}  
    return control , 'control.visited_contratos_vigentes'

def create_control_dict_historico(url_original, local_document_names, entries, ctype):

    ls = []
    for lr in local_document_names:
        lr = lr.replace('.crdownload','')
        ls = ls + [lr]

    downloded_files = []
    for file in entries:
        if get_last_substring_after_slash(file['file_url']) in ls:
            downloded_files = downloded_files + [file]

    if 'Expediente digitalizado en origen' in [d['type'] for d in downloded_files]:
        expediente = 1
    else:
        expediente = 0

    control = {'url':url_original,'all_documents':len(entries),'extracted_documents':len(local_document_names),
               'expediente':expediente, 
               'tipo': ctype}  
    return control  , 'control.visited_contratos_historicos'

def create_control_dict_revision(url_original, local_document_names, entries, ctype):

    ls = []
    for lr in local_document_names:
        lr = lr.replace('.crdownload','')
        ls = ls + [lr]

    downloded_files = []
    for file in entries:
        if get_last_substring_after_slash(file['file_url']) in ls:
            downloded_files = downloded_files + [file]

    if 'acuerdo_de_deposito_ante_la_jca' in [d['type'] for d in downloded_files]:
        acuerdo = 1
    else:
        acuerdo = 0
    if 'convenio_de_revision_salarial' in [d['type'] for d in downloded_files]:
        convenio = 1
    else:
        convenio = 0
    if 'resolucion_final_del_tramite' in [d['type'] for d in downloded_files]:
        resolucion = 1
    else:
        resolucion = 0

    if 'tabulador_de_sueldos' in [d['type'] for d in downloded_files]:
        tabulador = 1
    else:
        tabulador = 0

    control = {'url':url_original,'all_documents':len(entries),'extracted_documents':len(local_document_names),
               'acuerdo':acuerdo, 
               'convenio': convenio , 
               'resolucion': resolucion, 
               'tabulador': tabulador, 
               'tipo': ctype}  
    return control , 'control.visited_contratos_revision_salarial'

def create_control_dict_deposito(url_original, local_document_names, entries, ctype):

    ls = []
    for lr in local_document_names:
        lr = lr.replace('.crdownload','')
        ls = ls + [lr]

    downloded_files = []
    for file in entries:
        if get_last_substring_after_slash(file['file_url']) in ls:
            downloded_files = downloded_files + [file]

    if 'resolucion_de_constancia_de_representatividad' in [d['type'] for d in downloded_files]:
        resolucion_de_constancia = sum(1 for d in downloded_files if d.get('type') == 'resolucion_de_constancia_de_representatividad')
    else:
        resolucion_de_constancia = 0
    if 'contrato_colectivo_de_trabajo' in [d['type'] for d in downloded_files]:
        contrato_colectivo = sum(1 for d in downloded_files if d.get('type') == 'contrato_colectivo_de_trabajo')
    else:
        contrato_colectivo = 0
    if 'tabulador_de_sueldos_y/o_salarios' in [d['type'] for d in downloded_files]:
        tabulador =  sum(1 for d in downloded_files if d.get('type') == 'tabulador_de_sueldos_y/o_salarios')
    else:
        tabulador = 0

    if 'resolucion_de_convocatoria' in [d['type'] for d in downloded_files]:
        resolucion_de_convocatoria = sum(1 for d in downloded_files if d.get('type') == 'resolucion_de_convocatoria')
    else:
        resolucion_de_convocatoria = 0


    if 'resolucion_de_deposito_inicial' in [d['type'] for d in downloded_files]:
        resolucion_de_deposito = sum(1 for d in downloded_files if d.get('type') == 'resolucion_de_deposito_inicial')
    else:
        resolucion_de_deposito = 0


    if 'acta_de_resultados_de_votacion' in [d['type'] for d in downloded_files]:
        acta = sum(1 for d in downloded_files if d.get('type') == 'acta_de_resultados_de_votacion')
    else:
        acta = 0

    control = {'url':url_original,'all_documents':len(entries),'extracted_documents':len(local_document_names),
               'resolucion_de_constancia':resolucion_de_constancia, 
               'contrato_colectivo': contrato_colectivo , 
               'tabulador': tabulador, 
               'resolucion_de_convocatoria': resolucion_de_convocatoria, 
               'resolucion_de_deposito': resolucion_de_deposito, 
               'acta': acta, 
               'tipo': ctype}  
    return control , 'control.visited_contratos_deposito_inicial'

def create_control_dict_otros(url_original, local_document_names, entries, ctype):

    ls = []
    for lr in local_document_names:
        lr = lr.replace('.crdownload','')
        ls = ls + [lr]

    downloded_files = []
    for file in entries:
        if get_last_substring_after_slash(file['file_url']) in ls:
            downloded_files = downloded_files + [file]

    control = {'url':url_original,'all_documents':len(entries),'extracted_documents':len(local_document_names),
               'tipo': ctype}  
    return control , 'control.visited_contratos_otros'

def create_control_dict_asociaciones(url_prefix, local_document_names, entries_cfrl, entries):
    ls = []
    for lr in local_document_names:
        lr = lr.replace('.crdownload','')
        ls = ls + [lr]
    all = len(entries_cfrl) + len(entries)
    control = {'url_prefix':url_prefix,'all_documents':all,'extracted_documents':len(local_document_names),'cfrl': len(entries_cfrl), 'regular':len(entries)
               }  
    return control , 'control.visited_asociaciones_v2'
