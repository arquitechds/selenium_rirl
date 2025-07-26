import requests
from bs4 import BeautifulSoup
from src.utils import tukan_text_treater
from src.aws import S3Handler
import os
from loguru import logger
import shutil

import os
import shutil

def send_html_to_s3(soup, title, asociacion_metadata):
    s3 = S3Handler()
    
    # Extraer ID o número de carpeta (por ejemplo: '2999')
    folder = asociacion_metadata['url_prefix'].split("/")[-1]
    
    # Asegurar que el folder existe
    os.makedirs(folder, exist_ok=True)
    
    filename = title + '.html'
    local_path = os.path.join(folder, filename)
    
    # Guardar HTML localmente
    with open(local_path, "w", encoding="utf-8") as file:
        file.write(str(soup))
    
    # Construir ruta completa en S3
    s3_uri = f's3://rirl-documents{asociacion_metadata["url_prefix"]}/{filename}'
    
    # Subir a S3
    s3.write_any_file_to_s3(s3_uri, local_path)
    
    # Limpiar carpeta temporal
    shutil.rmtree(folder)

from src.db import insert_data

def parse_informacion_general(soup,asociacion_metadata):
    data = {'url_prefix':asociacion_metadata['url_prefix'] }

    for item in soup.select("div.dato-extra"):
        key_el = item.find("b")
        value_el = item.find("span")

        if not key_el or not value_el:
            continue

        # Limpieza de texto
        key = key_el.get_text(strip=True).rstrip(":")
        value = value_el.get_text(strip=True)

        # Convertir claves a formato tipo snake_case
        key_snake = key.lower().replace("(", "").replace(")", "")
        key_snake = key_snake.replace("á", "a").replace("é", "e").replace("í", "i").replace("ó", "o").replace("ú", "u")
        key_snake = key_snake.replace(" ", "_").replace("-", "_")

        # Si el valor es "No disponible", dejar como None
        if value.lower() == "no disponible":
            value = None

        data[key_snake] = value
    return data
    


def populate_files_metadata(soup,asociacion_metadata):
    documents = soup.find_all('a', {'class': 'data-tracking-document'})
    ds = []
    for entry  in documents:
        d = {'url_prefix': asociacion_metadata['url_prefix'],
            'type': tukan_text_treater(entry['data-ga-type']),
            'tramite': tukan_text_treater(entry['data-ga-item']),
            'file_url' : entry['href'],
            'name': tukan_text_treater(entry['data-ga-file']),
            'url_active': '',
            'in_s3': 0,
            's3_uri': ''}
        ds = ds + [d]
    return ds


def populate_cfrl_files_metadata(soup,asociacion_metadata):
    soup = soup.find('table')
    if soup == None:
        logger.info('no table')
        return []
    rows = soup.select("tbody tr")
    documents = []
    for row in rows:
        cols = row.find_all("td")
        button = row.find("button", class_="data-tracking-document")
        doc = {'url_prefix': asociacion_metadata['url_prefix'],
            "name": cols[0].get_text(strip=True),
            "type": cols[1].get_text(strip=True),
            "date": cols[2].get_text(strip=True),
            "size_mb": cols[3].get_text(strip=True),
            "directorio": button["data-directorio"],
            "estado": button["data-estado"],
            "entidad": button["data-ga-entity"],
            "url_pdf": f"https://registro.centrolaboral.gob.mx/documento-publico/download/{button['data-ga-file'].replace('_', '|')}",
            'url_active': '',
            'in_s3': 0,
            's3_uri': '',
        }
        documents.append(doc)
    return documents

def get_data_asociaciones(asociacion_metadata):
    try:
        url = f"https://repositorio.centrolaboral.gob.mx{asociacion_metadata['url_prefix']}"
        #url = "https://repositorio.centrolaboral.gob.mx/asociacion/4149"
        r = requests.get(url)
        soup = BeautifulSoup(r.content,"html.parser")
        data = populate_files_metadata(soup, asociacion_metadata)
    except:
        data = [] 
    return data

def get_data_asociaciones_cfrl(asociacion_metadata):
    try:

        url = f"https://repositorio.centrolaboral.gob.mx{asociacion_metadata['url_prefix']}"
        #url = "https://repositorio.centrolaboral.gob.mx/asociacion/4149"
        r = requests.get(url)
        soup = BeautifulSoup(r.content,"html.parser")
        data =  populate_cfrl_files_metadata(soup, asociacion_metadata)
    except:
        data = []
    return data

def tramites_contratos_relacionados(soup,asociacion_metadata):
    soup = soup.find('table')
    if soup == None:
        logger.info('no table')
        return []
    rows = soup.select("tbody tr")
    documents = []
    for row in rows:
        cols = row.find_all("td")
        doc = {'asociacion_url_prefix': asociacion_metadata['url_prefix'],
               'contrato_url_prefix': row.find('a')['href'],
            "relation_id":  asociacion_metadata['url_prefix'] + '-' + row.find('a')['href'] ,
            "autoridad_de_origen": cols[1].get_text(strip=True),
            "fecha": cols[2].get_text(strip=True),
            "patron_o_empresa": cols[3].get_text(strip=True),
        }
        documents.append(doc)
    return documents

def federaciones_relacionadas(soup,asociacion_metadata):
    soup = soup.find('table')
    if soup == None:
        logger.info('no table')
        return []
    rows = soup.select("tbody tr")
    documents = []
    for row in rows:
        cols = row.find_all("td")
        doc = {'asociacion_url_prefix': asociacion_metadata['url_prefix'],
               'fed_url_prefix': row.find('a')['href'],
            "relation_id":  asociacion_metadata['url_prefix'] + '-' + rows[0].find('a')['href'] ,
            "fed_folio_expediente": cols[0].get_text(strip=True),
            "fed_nombre": cols[1].get_text(strip=True),
            "fed_fecha_constitucion": cols[2].get_text(strip=True),
            'fed_autoridad_de_origen':cols[3].get_text(strip=True),
        }
        documents.append(doc)
    return documents


def iterate_tramites_relacionados_multiple_pages(asociacion_metadata, page):
    url = f"https://repositorio.centrolaboral.gob.mx{asociacion_metadata['url_prefix']}?page={page}"
    r = requests.get(url)
    soup = BeautifulSoup(r.content)
    info = soup.find_all("div",{"class":"detalle-informacion-seccion"})
    for soup in info:
        title = tukan_text_treater(soup.find("span").text).replace(' ','_')
        if title == 'tramites_de_contratos_posiblemente_relacionados':
            d = tramites_contratos_relacionados(soup,asociacion_metadata)
            insert_data(d,'asociaciones.contratos_relacionados')
            logger.info(f'iterate page {page}')
    return len(d)

def parse_cfrl(soup,asociacion_metadata):
    data = {'url_prefix':asociacion_metadata['url_prefix']}

    # Extraer todos los pares <b>...</b> y su <span> correspondiente
    for div in soup.select(".antecedente-plantilla .dato-extra"):
        label = div.find("b")
        value = div.find("span")
        if not label or not value:
            continue

        key = label.get_text(strip=True).rstrip(":")
        val = value.get_text(strip=True)

        # Normalizar clave a snake_case
        key = key.lower().replace("á", "a").replace("é", "e").replace("í", "i").replace("ó", "o").replace("ú", "u")
        key = key.replace("(", "").replace(")", "").replace(" ", "_")

        if val.lower() == "no disponible":
            val = None

        data[key] = val

    # Domicilio(s)
    domicilio_block = soup.select_one(".datos-grupo ul")
    if domicilio_block:
        domicilios = [li.get_text(strip=True) for li in domicilio_block.select("li")]
        data["domicilios"] = domicilios

    return data



def get_asociaciones(asociacion_metadata):
    url = f"https://repositorio.centrolaboral.gob.mx{asociacion_metadata['url_prefix']}"
    #url = "https://repositorio.centrolaboral.gob.mx/asociacion/4149"
    r = requests.get(url)


    soup = BeautifulSoup(r.content,"html.parser")
    ds = populate_files_metadata(soup,asociacion_metadata)
    logger.info('files metadata:')
    insert_data(ds,'archivos.asociaciones_v2')

    name = soup.find('h2').text.strip()
    titulo = {'titulo':name}

    info = soup.find_all("div",{"class":"detalle-informacion-seccion"})
    # guardar metadata
    for soup in info:

        title = tukan_text_treater(soup.find("span").text).replace(' ','_')
        logger.info(f'{title}')
        if title == 'informacion_general':
            d = parse_informacion_general(soup,asociacion_metadata)
            insert_data([{**titulo,**d}],'control.asociaciones_v2')

        if title in ['directiva','tramites_relacionados']:
            d = send_html_to_s3(soup,title,asociacion_metadata)

        if title == 'expedientes_y_tramites_de_autoridades_registrales_anteriores_al_cfcrl':

            d = parse_cfrl(soup,asociacion_metadata)
            insert_data([d],'asociaciones.antecedentes_cfcrl')

            d = populate_cfrl_files_metadata(soup,asociacion_metadata)
            insert_data(d,'archivos.asociaciones_pre_cfcrl')
            
        if title == 'tramites_de_contratos_posiblemente_relacionados':
            d = tramites_contratos_relacionados(soup,asociacion_metadata)
            insert_data(d,'asociaciones.contratos_relacionados')
            n_relations = len(d)
            page = 1
            while n_relations == 20:
                page = page + 1
                n_relations = iterate_tramites_relacionados_multiple_pages(asociacion_metadata, page)

        if title == 'federaciones_o_confederaciones_posiblemente_relacionadas':
            d = federaciones_relacionadas(soup,asociacion_metadata)
            insert_data(d,'asociaciones.federaciones_relacionadas')

from src.jobs import get_last_substring_after_slash, get_file_names,correct_subtype, create_control_dict_asociaciones
from src.selenium_extract import selenium_download_v3
from src.jobs import send_to_s3_from_local
import shutil
from loguru import logger


def write_selenium_documents_to_s3_v3(asociacion_metadata, size_mb):
    ''' 
    Sends document to S3 and uploads control table
    '''
    url_prefix = asociacion_metadata['url_prefix']
    logger.info(url_prefix)
    download_dir = get_last_substring_after_slash(url_prefix)
    url_original = 'https://repositorio.centrolaboral.gob.mx' + url_prefix

    try:
        url = 'https://repositorio.centrolaboral.gob.mx' + url_prefix
        selenium_download_v3(url_prefix,download_dir,size_mb)

        entries = get_data_asociaciones(asociacion_metadata)
        entries_cfrl = get_data_asociaciones_cfrl(asociacion_metadata)
        
        local_document_names = get_file_names(download_dir)        

        # caso zero 

        if (len(entries) + len(entries_cfrl) == 0) or (len(local_document_names) == 0):
            control, table = create_control_dict_asociaciones(url_prefix, local_document_names,entries_cfrl, entries)
            insert_data([control],table)

            logger.info('Nothing downloaded!')
            return 



        for entry in entries_cfrl:

            document_name = get_last_substring_after_slash(entry['url_pdf'])
            url = entry['url_pdf'].strip()


            if document_name in set(local_document_names) :
                # create s3 uri
                #name = 'metadata/archivos/contratos/' + name
                corrected_file_id = entry['url_pdf'].replace(' ', '_')
                corrected_file_id = corrected_file_id.replace('/', '_')
                corrected_file_id = corrected_file_id.replace('|', '_')

                corrected_subtype = correct_subtype(entry['type'])
                file_name = url.rsplit('/', 1)[-1]
                name =  url_prefix  + '/'+ corrected_subtype + '/' + document_name
                local_name = download_dir+ '/' + document_name 
                size = os.path.getsize(local_name)/ (1024 ** 2)                               

                send_to_s3_from_local(name,local_name)
                # send data to control table
                new_entry = [{  'url_prefix': url_prefix,
                                'url_pdf' : entry['url_pdf'],
                                'name' : entry['name'],
                                'type': entry['type'],
                                'date': entry['date'],
                                'size_mb': entry['size_mb'],
                                'size_mb_real': str(size),
                                'directorio': entry['directorio'],
                                'estado': entry['estado'],
                                'entidad': entry['entidad'],
                                'url_active': '1',
                                'in_s3': '1',
                                's3_uri': name,
                                'text_column':'0'}]
                print(new_entry)
                insert_data(new_entry,f'archivos.asociaciones_pre_cfcrl')
            else:
                new_entry = [{  'url_prefix': url_prefix,
                                'url_pdf' : entry['url_pdf'],
                                'name' : entry['name'],
                                'type': entry['type'],
                                'date': entry['date'],
                                'size_mb': entry['size_mb'],
                                'size_mb_real': str(size),
                                'directorio': entry['directorio'],
                                'estado': entry['estado'],
                                'entidad': entry['entidad'],
                                'url_active': '1',
                                'in_s3': '0',
                                's3_uri': name,
                                'text_column':'0'}]
                insert_data(new_entry,f'archivos.asociaciones_pre_cfcrl')
                logger.info('File NOT inserted to S3')



#        for entry in entries:
#
#            document_name = get_last_substring_after_slash(entry['file_url'])
#            url = entry['file_url'].strip()


#            if document_name in set(local_document_names) :
                # create s3 uri
                #name = 'metadata/archivos/contratos/' + name
#                corrected_file_id = entry['file_url'].replace(' ', '_')
#                corrected_file_id = corrected_file_id.replace('/', '_')
#                corrected_file_id = corrected_file_id.replace('|', '_')

 #               corrected_subtype = correct_subtype(entry['type'])
  #              file_name = url.rsplit('/', 1)[-1]
   #             name =  url_prefix  + '/'+ corrected_subtype + '/' + document_name
    #            local_name = download_dir+ '/' + document_name 
    #            size = os.path.getsize(local_name)/ (1024 ** 2)                                

    #            send_to_s3_from_local(name,local_name)
                # send data to control table
    #            new_entry = [{  'url_prefix': url_prefix,
   #                             'file_url' : entry['file_url'],
   #                             'name' : entry['name'],
   #                             'type': entry['type'],
   #                             'tramite': entry['tramite'],
   #                             'size_mb': str(size_mb),
   #                             'url_active': '1',
   #                             'in_s3': '1',
   #                             's3_uri': name,
   #                             'text':'0'}]
   #             print(new_entry)
   #             insert_data(new_entry,f'archivos.asociaciones_v2')
   #         else:
   #             new_entry = [{  'url_prefix': url_prefix,
   #                             'file_url' : entry['file_url'],
   #                             'name' : entry['name'],
   #                             'type': entry['type'],
   #                             'tramite': entry['tramite'],
   #                             'size_mb': str(size_mb),
   #                             'url_active': '1',
   #                             'in_s3': '0',
   #                             's3_uri': name,
   #                             'text':'0'}]
   #             insert_data(new_entry,f'archivos.asociaciones_v2')
   #             logger.info('File NOT inserted to S3')




        control, table = create_control_dict_asociaciones(url_prefix, local_document_names,entries_cfrl, entries)
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