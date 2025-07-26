# Request data
import requests
from bs4 import BeautifulSoup
from loguru import logger
import unicodedata
import pandas as pd


class rirl_scrapping_session:
    ''' 
    Purpose of this class is to reuse same cookies for massive extractions
    '''
    def __init__(self):
        self.cookies = self.get_cookies()
        self.cookies2  = {
    'XSRF-TOKEN': 'eyJpdiI6IitmS0VEWEhsOGx2YUk2aGQvMVdvTEE9PSIsInZhbHVlIjoiSUpIdWd5NExhV1VCRmpzaFo2bFk2aFRvcURTUWpuQXpHSlhLV3o0L0tzRXhRc0k5bEdLQk5IWnRqQThrenUzSnFLSlJha0RSUnBRMHUwQVhxQjdEVFArMlh2T0xEZ0s0aXY3OEdUbmtSOGFFcDRMMlZ5ZHR6SlptYlZ6QWc3T08iLCJtYWMiOiJhMTU2MzhhNzM5YTI2MWFkM2RjYjk5Mzc5M2JmNjNhZDZhYTJjOWJhODZjMzcwNmFiMWI1Mzk0ZjE3OGRkYTlmIiwidGFnIjoiIn0%3D',
    'repositorio_session': 'eyJpdiI6ImU4bnNqbHpDM0grUHdXWEVOeDF3bnc9PSIsInZhbHVlIjoiOXEzOXdmZ3NJYjRFMnVXbFFLc1dVemMxNzVpYmxoc1BPK2hoaE0wdXliZkZQU3ZxU2JOV3FUWEZaK3l0YzYxTDRGaG1UWXR4YUI4dGZwbGpCaEMrUEtmTkhwYkw5ekhPRlI4M0lrZWFHSVlOcG9LRVhZN25FZ0NXc05rNkVxa2ciLCJtYWMiOiIyYmUzMWIzZWVhYTMyNmU3ZGE3NzM5YmMxMzM1MmY2NGRlYzJjOTExMjlkOWFjMGM4OTJiMzBmODIxN2QxNjExIiwidGFnIjoiIn0%3D',
}


    def get_cookies(self):
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:123.0) Gecko/20100101 Firefox/123.0',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'es-MX,es;q=0.8,en-US;q=0.5,en;q=0.3',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
        }

        response = requests.get('https://repositorio.centrolaboral.gob.mx/', headers= headers)
        cookies =  response.cookies.get_dict() 
        aviso =    { 'avisoprivacidad': 'true'}
        cookies = {**cookies, **aviso}
        return cookies

    def get_raw_entries_by_page(self, page):
        ''' 
        Extracts all html given a page number
            input page: page to iterate throw the website
        
        '''
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:123.0) Gecko/20100101 Firefox/123.0',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'es-MX,es;q=0.8,en-US;q=0.5,en;q=0.3',
            'Connection': 'keep-alive',
            'DNT': '1',
            'Sec-GPC': '1',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
        }

        params = {
            'cont': str(page),
            'reg': str(page),
            'asoc':str(page)
        }

        response = requests.get('https://repositorio.centrolaboral.gob.mx/', params=params, cookies=self.cookies2, headers=headers)
        soup = BeautifulSoup(response.text, "html.parser")
        logger.info(f'Extracted page {page}')
        return soup



##########################################
# check_new_urls_for_control_tables 
##########################################



def get_reglamento_entry_data(entry):
    ''' 
    Parses raw html data from reglamentos
        input entry: bs4 obj
    Returns
        data (dict)

    '''
    url = entry.find_all('a')[0]['href']
    titulo = entry.find_all('b')[0].text.strip()
    folio_unico = entry.find_all('span')[0].text.strip()
    num_expediente = entry.find_all('span')[1].text.strip()
    fecha_registro = entry.find_all('span')[2].text.strip()
    autoridad = entry.find_all('span')[3].text.strip()
    patron_empresa = entry.find_all('div')[-1].text.strip().replace('  ','').replace('Patrón, empresa(s) o establecimiento(s):\n','')
    data = {'url' : url,
            'titulo': titulo,
            'folio_unico': folio_unico,
            'num_expediente': num_expediente,
            'fecha_registro':fecha_registro,
            'autoridad': autoridad,
            'patron_empresa': patron_empresa
            }
    return data

def get_contrato_entry_data(entry):
    ''' 
    Parses raw html data from contratos
        input entry: bs4 obj
    Returns 
        data (dict)

    
    '''
    if entry.find('div').text.strip() == 'Expediente de contrato colectivo':
        status = 'vigente'
    else:
        status = 'archivo_historico'

    url = entry.find_all('a')[0]['href']
    numero_registro = entry.find_all('span')[0].text.strip()
    folio_unico = entry.find_all('span')[1].text.strip()
    entidad_origen = entry.find_all('span')[2].text.strip()
    autoridad_registro = entry.find_all('span')[3].text.strip()
    fecha_presentacion = entry.find_all('span')[4].text.strip()
    nombre_asociacion = entry.find_all('span')[5].text.strip()
    pea_antecedentes = entry.find_all('span')[6].text.strip().replace('  ','')
    pea_legitimacion = entry.find_all('span')[7].text.strip().replace('  ','')
    pea_revision_salarial = entry.find_all('span')[8].text.strip().replace('  ','')
    pea_revision_contrato = entry.find_all('span')[9].text.strip().replace('  ','')

    data = {'url' : url,
            'numero_registro': numero_registro,
            'folio_unico': folio_unico,
            'entidad_origen':entidad_origen,
            'fecha_presentacion':fecha_presentacion,
            'autoridad_registro': autoridad_registro,
            'nombre_asociacion': nombre_asociacion,
            'pea_antecedentes': pea_antecedentes,
            'pea_legitimacion':pea_legitimacion,
            'pea_revision_salarial': pea_revision_salarial,
            'pea_revision_contrato': pea_revision_contrato,
            'status': status
            }
    return data


def get_asociacion_entry_data(entry):
    ''' 
    Parses raw html data from asociaciones
        input entry: bs4 obj
        
    returns
        data (dict)
    
    '''
    url = entry.find_all('a')[0]['href']
    folio_tramite = entry.find_all('span')[0].text.strip()
    nombre_asociacion = entry.find_all('span')[1].text.strip().replace('  ','')
    num_expediente = entry.find_all('span')[2].text.strip()
    folio_unico = entry.find_all('span')[3].text.strip()
    fecha_registro = entry.find_all('span')[4].text.strip()
    fecha_ultimo_tramite = entry.find_all('span')[5].text.strip()
    entidad_origen = entry.find_all('span')[6].text.strip()
    autoridad = entry.find_all('span')[7].text.strip()
    data = {'url' : url,
            'folio_tramite': folio_tramite,
            'nombre_asociacion': nombre_asociacion,
            'num_expediente': num_expediente,
            'folio_unico': folio_unico,
            'fecha_registro':fecha_registro,
            'fecha_ultimo_tramite':fecha_ultimo_tramite,
            'entidad_origen': entidad_origen,
            'autoridad': autoridad,
            }
    return data


def extract_all_entries(soup):
    ''' 
    Divides html soup object into 4 lists of each entry (reglamento, asociaciones, contrato vig y contrato hist)
        input: bs4 soup

    Returns 4 lists of 10 items each for each entry group
    
    '''

    reglamento = 'opcion-resultado-item item-reglamento'
    asociacion = 'opcion-resultado-item item-asociacion'
    contrato_vigente = 'opcion-resultado-item item-contrato item-vigente'
    contrato_historico = 'opcion-resultado-item item-contrato item-historico'

    reglamentos = soup('div', {"class": reglamento})
    asociaciones = soup('div', {"class": asociacion})
    contratos_vig = soup('div', {"class": contrato_vigente})
    contratos_hist = soup('div', {"class": contrato_historico})


    reglamentos_entries = []
    for a in reglamentos:
        reglamentos_entries = reglamentos_entries + [get_reglamento_entry_data(a)]

    asociaciones_entries = []
    for b in asociaciones:
        asociaciones_entries = asociaciones_entries + [get_asociacion_entry_data(b)]

    contratos_vig_entries = []
    for c in contratos_vig:
        contratos_vig_entries = contratos_vig_entries + [get_contrato_entry_data(c)]

    contratos_hist_entries = []
    for d in contratos_hist:
        contratos_hist_entries = contratos_hist_entries + [get_contrato_entry_data(d)]

    return reglamentos_entries, asociaciones_entries,contratos_vig_entries, contratos_hist_entries



##########################################
# metadata!
##########################################

def get_soup(url):
    ''' 
    Extracts all html given a page number
        input page: page to iterate throw the website
    
    '''
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:123.0) Gecko/20100101 Firefox/123.0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'es-MX,es;q=0.8,en-US;q=0.5,en;q=0.3',
        # 'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        # 'Cookie': '_ga_XT5D9P1XZZ=GS1.1.1709495574.3.1.1709498582.15.0.0; _ga=GA1.1.886665048.1709169090; XSRF-TOKEN=eyJpdiI6IkxoR1ZVVGJNTHNVTTVKN3RRaStWZVE9PSIsInZhbHVlIjoiaUQzVFJVOTJ2anJiUDlvMkN1OVVqRDd0d0lrOFBDSmxCZjB2MHlIUWliL3Rhb20ybTNFd1YrS3dXb3pxY1Z5UnBQbFVUc3pVWVJMTnRhZEw4RDBNUUQ2Ni9WcmlzcUlwcTgvWTRKMTUzc2tvMVlsOEx2Q0hSMzZWMWgwaWxHWTciLCJtYWMiOiJhMWY0ZDk2ZDY5NmNkNzQ0M2U4NWIxNDMxNDI5OTgzNDQ0Y2RhNzM3OTQxN2ExOTA1NmE5MzAxZjAxOGZiOGYzIiwidGFnIjoiIn0%3D; repositorio_session=eyJpdiI6IjdTS0l2TTV6bTAxK01HSGFCNHkrQVE9PSIsInZhbHVlIjoiemVsRm5ROXBCcWYrdDcvSFdKQVpsdEY0My9PQTZ5U0FBL0o5MUtUVE8rYkM0QUIzclBVNXJXS3Q0MVYybUc1VFUvZmVEbVBLcVNXQWxUSEM5K1Rtd2xBeU5GQ3BYc0xONmFCeGp6RHpFT1pEY0MvTVlMNWhPNXhpMk4yR2F5Qk4iLCJtYWMiOiI5YTQ1ZDcxMzE4YTgzNTNiZjMyMzU5NDgzYjNlODRiZmZhNjc0Y2UzN2U3YThmNDJlM2ExZGE5ZDhmMDBhYzE3IiwidGFnIjoiIn0%3D; avisoprivacidad=true; informativohistorico=true; XSRF-TOKEN=eyJpdiI6InI0U1Ewamo5bGwzREJFR3pWNU1HZmc9PSIsInZhbHVlIjoiakVFbi9yeU9NNXRGRERlb1M1Ky82MVZiZ0MzL05QMVZLNDNDOVJZUmxzc1piY0t2L3BSYkxPeENIdGxKVS80aHlCN3d4OTVjS3FyOVRTVlIxZ2xmUjYxdEQwZkl5QkF3YmpPL0tOYzZqQm8yajgrWGRxMTdmZFZWdnRJRGpSRE8iLCJtYWMiOiJiZmIxNTQyZjY4YWQ3ZjA1NTYyMWZkZWE3NDFiOGQ3NjQ3MTdiNjJiMTA0MGVlYzE5MjJmYzYxNzVhY2JkOTYwIiwidGFnIjoiIn0%3D; repositorio_session=eyJpdiI6Im9oNWFRdDVRV2grcnZuZWQyMlJlb2c9PSIsInZhbHVlIjoiWTh0RGl3UDRHdjJidkNkYnJueVRqOU1CeFpGelc1NExPM1pCSHdSSldyUVZhNTBvOFBhTldKcmVxS050RnVLZjQ1QVc2aFNrd2ptRWxKSytmd1BVck1sNjFHYmRMOFRxMnM0eTdKdWZnTU1aOEJTSU9mazZ5ZkNZTzl4M2VpZ3IiLCJtYWMiOiJiZDMwMzJkOTg2NWQzMGVlYjFkNTQwMWVlMmQ5NmFlZDI2ZmRlNjY0YTgzMmJmZDU5MjE4Yzk4NDE0NWE3ZDc2IiwidGFnIjoiIn0%3D',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        soup = BeautifulSoup(response.text, "html.parser")
        logger.info(f'Extracted page')
    else:
        soup = None
        logger.info(f'{url} was empty')
    return soup


def clean_txt(input_str):
    input_str = input_str.replace('á','a').replace('é','e').replace('í','i').replace('ó','o').replace('ú','u')
    input_str = input_str.replace(' ','_').replace(':','').lower().strip().replace('__','').replace('\n','')
    #input_str = input_str.strip()#.replace('__','').replace('\n','')
    return input_str


def get_metadata(soup,url_prefix):
    values = []
    keys = []
    for entry in soup.find_all('div' , class_="dato-extra"):
        if entry.find('b') == None:
            break
        key = clean_txt(entry.find('b').text.replace('(','').replace(')','')).replace('_|_','')
        #if key in keys:
        #    break
        try:
            if len(entry.find_all('li')) > 1:
                value = []
                for j in entry.find_all('li'):
                    value_n = j.text.strip()
                    value = value + [value_n]
                value = " | ".join(str(x) for x in value)

            else:
                #value = clean_txt(entry.find('li').text)
                value = entry.find('li').text.strip()
        except:
            try:
                #value = clean_txt(entry.find('span').text)
                value = entry.find('span').text.strip()

            except:
                #value = None
                #value = clean_txt(soup.find_all('div' , class_="dato-extra")[-1].find('span').text)
                value = soup.find_all('div' , class_="dato-extra")[-1].find('span').text.strip()

        values = values + [value]
        keys = keys + [key]

    dictionary = dict(map(lambda key, value: (key, value), keys, values))
    dictionary['url'] = url_prefix
    dictionary['url_active'] = 1
    return dictionary

def get_metadata_2(soup,url_prefix):
    '''
    Extract function design for revision salarial
    '''
    values = []
    keys = []
    for entry in soup.find_all('div' , class_="dato-extra"):
        if entry.find('b') == None:
            break
        key = clean_txt(entry.find('b').text.replace('(','').replace(')','')).replace('_|_','')
        try: 
            #value = clean_txt(entry.find('li').text)
            value = entry.find('li').text.strip()
        except:
            try:
                #value = clean_txt(entry.find('span').text)
                value = entry.find('span').text.strip()

            except:
                #value = None
                #value = clean_txt(soup.find_all('div' , class_="dato-extra")[-1].find('span').text)
                value = soup.find_all('div' , class_="dato-extra")[-1].find('span').text.strip()

        values = values + [value]
        keys = keys + [key]

    dictionary = dict(map(lambda key, value: (key, value), keys, values))
    dictionary['url'] = url_prefix
    dictionary['url_active'] = 1
    return dictionary



def get_metadata_reglamento(soup):
    soup_reglamento = soup.find_all('div', class_='detalle-informacion-seccion')[1]
    keys = []
    for i in soup_reglamento.find_all('th'):
        keys = keys + [clean_txt(i.text).strip()]
    values = []
    for i in soup_reglamento.find_all('td'):
        values = values + [i.text.strip()]
    dictionary = dict(map(lambda key, value: (key, value), keys, values))
    return dictionary

def chunks(l, n):
    """
    Yield successive n-sized chunks from list l.
    """
    for i in range(0, len(l), n):
        yield l[i:i + n]

def get_tramites_urls(soup, file_id,contrato_type):
    try:
        types = []
        urls = []
        for entry in soup.find_all('div', class_ = 'document-group-item'):
            types = types + [clean_txt(entry.find('span').text)]
            urls = urls + [entry.find('a')['href']]
        df = pd.DataFrame([urls,types]).T
        df.columns = ['file_url', 'type']
        df['file_id'] = file_id
        df['contrato_type'] = contrato_type
        df['source'] = 'tramites'
        df['url_active'] = 1
        df['in_s3'] = 0
        df['s3_uri'] = ''
        tramites_urls = df.to_dict('records')
    except:
        tramites_urls = []


    return tramites_urls

def get_reglamento_urls(soup,file_id):
    try:
        types = []
        urls = []
        for entry in soup.find_all('a', class_ = 'data-tracking-document'):
            types = types + [entry['data-ga-type'].strip()]
            urls = urls + [entry['href']]
        df = pd.DataFrame([urls,types]).T
        df.columns = ['file_url', 'type']
        df['file_id'] = file_id
        df['source'] = 'info_general'
        df['url_active'] = 1
        df['in_s3'] = 0
        df['s3_uri'] = ''
        tramites_urls = df.to_dict('records')
    except:
        tramites_urls = []
    return tramites_urls

def get_asoc_urls(soup,file_id,url_prefix):
    try:
        types = []
        urls = []
        for entry in soup.find_all('a', class_ = 'data-tracking-document'):
            types = types + [clean_txt(entry['data-ga-type'])]
            urls = urls + [entry['href']]
        df = pd.DataFrame([urls,types]).T
        df.columns = ['file_url', 'type']
        df['file_id'] = file_id
        df['source'] = 'asociaciones'
        df['url_active'] = 1
        df['in_s3'] = 0
        df['s3_uri'] = ''
        df['url_prefix'] = url_prefix
        df['size_mb'] = 0
        df['text'] = 0

        tramites_urls = df.to_dict('records')
    except:
        tramites_urls = []
    return tramites_urls


def related_asocs(asociacion_soup):
    table = asociacion_soup.find('table')
    values = []
    for entry in table.find_all('span'):
        values = values + [clean_txt(entry.text)]
    df = pd.DataFrame(list(chunks(values, 5)))
    df.columns = ['folio','folio2','asociacion','fecha_const','autoridad']
    df.pop('folio2')
    related_asoc = df.to_dict('records')
    return related_asoc


def get_expedientes_urls(tramites_relacionados,file_id,contrato_type):
    try:
        table = tramites_relacionados.find('table')
        if table == None:
            return []
        values = []
        for entry in table.find_all('td'):
            values = values + [entry.text.strip()]
        df = pd.DataFrame(list(chunks(values, 5)))
        df.columns = ['file_url','type','date','size','button']
        df  = df[['file_url','type']]
        df['file_id'] = file_id
        df['contrato_type'] = contrato_type
        df['source'] = 'expedientes'
        df['url_active'] = '1'
        df['in_s3'] = '0'
        df['s3_uri'] = ''
        df['file_url'] =  'https://repositorio.centrolaboral.gob.mx/storage/antecedentes/' + df['file_url']

        related_expedientes = df.to_dict('records')
    except:
        related_expedientes = []

    return related_expedientes

def get_empresas(soup,file_id,contrato_type):
    ''' 
    Extract table of empresas
    '''
    try:
        table = soup.find('table')
        if table == None:
            return []
        values = []
        for entry in table.find_all('td'):
            values = values + [entry.text.strip()]
        df = pd.DataFrame(list(chunks(values, 4)))
        df.columns = ['nombre','rfc','domicilio','actividad']
        df['file_id'] = file_id
        df['relation_to'] = contrato_type
        df['id'] = file_id + ' - ' + df['nombre']
        empresas = df.to_dict('records')
    except:
        empresas = []
    return empresas


def compare_dicts(dict_a, dict_b):
    keys_a = set(dict_a.keys())
    keys_b = set(dict_b.keys())
    intersect = keys_a.intersection(keys_b)
    
    for key in intersect:
        # caso 1 uno es diferente
        if dict_a[key] == 'No disponible' and dict_b[key] != 'No disponible':
            del dict_a[key]
        elif dict_a[key] != 'No disponible' and dict_b[key] == 'No disponible':
            del dict_b[key]
    return dict_a,dict_b
        
        # caso 2, ambos estan disponibles






def get_data_contratos(url_prefix):
    url = 'https://repositorio.centrolaboral.gob.mx'+ url_prefix
    soup = get_soup(url)
    if soup is not None:
        informacion_general_soup = soup.find_all('div', class_='detalle-informacion-seccion')[0]
        tramites_relacionados_soup = soup.find_all('div', class_='detalle-informacion-seccion')[1]
        #asociacion = soup.find_all('div', class_='detalle-informacion-seccion')[2]
        informacion_general = get_metadata(informacion_general_soup,url_prefix)
        tramites_relacionados = get_metadata(tramites_relacionados_soup,url_prefix)
        tramites_relacionados, informacion_general = compare_dicts(tramites_relacionados, informacion_general)
        data = {**tramites_relacionados,**informacion_general}
    else:
        data = {}

    #related_asocs_dict = related_asocs(asociacion)

    keys_alive_contracts = ['autoridad_que_genero_el_registro',
    'empresa_o_persona_empleadora',
    'entidad_federativa_de_origen',
    'entidades',
    'fecha_de_constancia_de_representacion',
    'fecha_de_deposito_inicial',
    'fecha_de_la_constancia_de_legitimacion',
    'fecha_de_votacion',
    'fecha_del_dictamen',
    'fecha_del_evento_de_votacion',
    'folio_del_tramite',
    'nombre_de_la_asociacion',
    'numero_de_contrato',
    'numero_de_trabajadores',
    'ramas_economicas_de_la_industria',
    'resultado_de_la_legitimacion',
    'rfc_de_la_empresa',
    'url',
    'url_active']

    keys_historic_contracts = ['numero_de_registro', 
            'numero_de_expediente', 
            'nombre_del_patron', 
            'entidad_federativa_de_origen', 
            'autoridad_que_genero_el_registro', 
            'nombre_de_la_asociacion', 
            'url', 
            'folio_del_tramite', 
            'fecha_de_constitucion', 
            'fecha_de_registro', 
            'secretarioa_general_u_homologo', 
            'tipo_documental', 
            'numero_de_personas_afiliadas', 
            'federacion_o_confederacion', 
            'federacion', 
            'confederacion', 
            'vigencia_de_la_directiva', 
            'fecha_de_la_ultima_toma_de_nota', 
            'domicilios',
    'url_active',
    'fecha_de_ultima_revision']

    keys_deposito_inicial =  ['folio_del_tramite',
    'fecha_de_resolucion',
    'numero_de_expediente',
    'tipo_de_contrato',
    'jurisdiccion',
    'estado_de_la_jurisdiccion',
    'personas_trabajadoras_con_derecho_a_voto',
    'ámbito_de_aplicacion',
    'url',
    'url_active',
    'numero_de_registro',
    'nombre_del_patron',
    'patron,_empresas_o_establecimientos',
    'nombre_de_persona_empleadora',
    'entidad_federativa_de_origen',
    'autoridad_que_genero_el_registro',
    'estados_vinculados_a_la_jurisdiccion',
    'fecha_de_presentacion',
    'nombre_de_la_asociacion',
    'personas_trabajadoras_cubiertas_por_el_contrato',
    'duracion_del_contrato',
    'fecha_de_ultima_revision_salarial',
    'fecha_de_terminacion_del_contrato',
    'fecha_de_legitimacion',
    'domicilio_donde_se_desarrolla_la_actividad']
    
    
    revision_salarial_filter_1 = ['folio_del_tramite',
    'numero_de_expediente',
    'fecha_de_resolucion',
    'nombre_del_patron',
    'nombre_de_la_asociacion',
    'numero_de_expediente_de_la_asociacion',
    'numero_de_registro_de_la_asociacion',
    'url',
    'url_active',
    'numero_de_registro']

    revision_salarial= ['folio_del_tramite',
    'numero_de_expediente',
    'fecha_de_resolucion',
    'nombre_del_patron',
    'nombre_de_la_asociacion',
    'numero_de_expediente_de_la_asociacion',
    'numero_de_registro_de_la_asociacion',
    'url',
    'url_active',
    'numero_de_registro',
    'nombre_de_persona_empleadora',
    'entidad_feredativa_de_origen',
    'autoridad_que_genero_el_registro',
    'fecha_de_presentacion']

    terminacion  = ['folio_del_tramite',
        'fecha_de_resolucion',
        'numero_de_expediente',
        'nombre_de_persona_empleadora',
        'nombre_de_la_asociacion',
        'numero_de_expediente_de_la_asociacion',
        'numero_de_registro_de_la_asociacion',
        'fechas,_horas_y_lugares_de_votacion',
        'url',
        'url_active',
        'numero_de_registro',
        'nombre_del_patron',
        'entidad_federativa_de_origen',
        'autoridad_que_genero_el_registro',
        'fecha_de_presentacion']
 
    if find_similarity(set(keys_alive_contracts),set(data.keys())) > .8:
        logger.info('Alive contract detected')
        if  set(keys_alive_contracts) > set(data.keys()):
            data = {**data,**complete_keys(keys_alive_contracts,data.keys() )}

        logger.info('Alive contract detected')
        logger.info('Correct columns!')
        table = 'metadata.contratos_vigentes'
        contrato_type = 'contrato vigente'
        data['id'] = data['numero_de_contrato']
        empresas = []

    elif find_similarity(set(keys_historic_contracts),set(data.keys())) > .8:
        logger.info('Historic contract detected')
        if  set(keys_alive_contracts) > set(data.keys()):
            data = {**data,**complete_keys(keys_historic_contracts,data.keys() )}
        logger.info('Correct columns!')
        table = 'metadata.contratos_historicos'
        contrato_type = 'contrato historico'
        data['id'] = data['numero_de_registro']
        empresas = []




    elif find_similarity(set(keys_deposito_inicial),set(data.keys())) > .8:
        logger.info('Deposito inicial contract detected')
        if set(keys_deposito_inicial) <= set(data.keys()):
            data = {**data,**complete_keys(keys_deposito_inicial,data.keys() )}
        logger.info('Deposito inicial contract detected')
        logger.info('Correct columns!')
        data['ambito_de_aplicacion'] = data.pop('ámbito_de_aplicacion')
        data['patron_empresas_o_establecimientos'] = data.pop('patron,_empresas_o_establecimientos')
        table = 'metadata.contratos_deposito_inicial'
        contrato_type = 'contrato deposito_inicial'
        data['id'] = data['numero_de_expediente']
        if data['id'] == 'No disponible':
            data['id'] = data['folio_del_tramite']
        
        empresas = get_empresas(soup,data['id'],contrato_type)

    elif find_similarity(set(revision_salarial_filter_1) ,set(data.keys())) > .8:
        logger.info('Revision salarial contract detected')
        informacion_general = get_metadata_2(informacion_general_soup,url_prefix)
        tramites_relacionados = get_metadata(tramites_relacionados_soup,url_prefix)
        data = {**tramites_relacionados,**informacion_general}
        if set(revision_salarial) <= set(data.keys()):
            data = {**data,**complete_keys(revision_salarial,data.keys() )}
        revision_salarial
        table = 'metadata.contratos_revision_salarial'
        contrato_type = 'contrato revision_salarial'
        data['id'] = data['folio_del_tramite']
        empresas = []

    elif find_similarity(set(terminacion),set(data.keys())) > .8:
        logger.info('Terminacion contract detected')
        if set(terminacion) <= set(data.keys()):
            data = {**data,**complete_keys(terminacion,data.keys() )}
        logger.info('Terminacion contract detected')
        logger.info('Correct columns!')
        data['fechas_horas_y_lugares_de_votacion'] = data.pop('fechas,_horas_y_lugares_de_votacion')
        table = 'metadata.contratos_terminacion'
        contrato_type = 'contrato terminacion'
        data['id'] = data['numero_de_expediente']
        empresas = get_empresas(soup,data['id'],contrato_type)



    else: 
        logger.info('Incorrect columns')
        logger.info(url_prefix)
        data = []
        urls = []
        empresas = []
        table = None

    # extract urls
    try: 
        tramites_urls =  get_tramites_urls(tramites_relacionados_soup,data['id'],contrato_type)
        expedientes_urls = get_expedientes_urls(tramites_relacionados_soup,data['id'],contrato_type)
        urls = tramites_urls + expedientes_urls
    except:
        urls = []
    return data,urls,empresas, table

def complete_keys(all_keys,current_keys):
    missing_keys = all_keys - current_keys
    missing_dict = {}
    for key in list(missing_keys):
        new_missing_dict = {key: 'na'}
        missing_dict = {**missing_dict, **new_missing_dict}
    return missing_dict
def find_similarity(all_keys,missing_keys):
    return len(all_keys.intersection(missing_keys))/len(all_keys)


def get_data_reglamentos(url_prefix):
    url = 'https://repositorio.centrolaboral.gob.mx'+ url_prefix
    soup = get_soup(url)
    if soup is not None:
        informacion_general_soup = soup.find_all('div', class_='detalle-informacion-seccion')[0]
        tramites_relacionados_soup = soup.find_all('div', class_='detalle-informacion-seccion')[1]
        #asociacion = soup.find_all('div', class_='detalle-informacion-seccion')[2]
        informacion_general = get_metadata(informacion_general_soup,url_prefix)
        #tramites_relacionados = get_metadata_reglamento(soup)
        contrato = soup.find_all('h2', class_='titulo')[0].text.strip()
        informacion_general['numero_de_expediente'] = contrato
        expediente = informacion_general['numero_de_expediente']
        urls = get_reglamento_urls(soup,contrato)
        data = informacion_general
        empresas = get_empresas(soup,expediente,'reglamento')
    else:
        data = {}


    keys_reglamento = [
    'numero_de_expediente',
    'nombre_de_la_asociacion',
    'numero_de_registro_de_asociacion',
    'entidad_federativa_de_origen',
    'autoridad_que_genero_el_registro',
    'jurisdiccion',
    'estados_vinculados_a_la_jurisdiccion',
    'fecha_de_registro',
    'fecha_de_ultima_modificacion',
    'url',
    'url_active']

    if set(keys_reglamento) == set(data.keys()):
        logger.info('Reglamento detected')
        logger.info('Correct columns!')
        table = 'metadata.reglamentos'
        data['id'] = data['numero_de_expediente']

    else: 
        logger.info('Incorrect columns')
        logger.info(url_prefix)
        data = []
        urls = []
        table = None

    return data,urls, empresas, table

def get_data_asociaciones(url_prefix):
    url = 'https://repositorio.centrolaboral.gob.mx'+ url_prefix
    soup = get_soup(url)
    if soup is not None:
        informacion_general_soup = soup.find_all('div', class_='detalle-informacion-seccion')[0]
        nombre = soup.find_all('li',class_='breadcrumb-item')[-1].find_all('span')[0].text.strip()
        data = get_metadata(soup,url_prefix)
        data['nombre'] = nombre

    else:
        data = {}

    keys_asoc_basic = ['nombre',
        'numero_de_registro',
    'numero_de_expediente',
    'entidad_federativa_de_origen',
    'autoridad_que_genero_el_registro',
    'folio_unico',
    'fecha_de_constitucion',
    'secretarioa_general_u_homologo',
    'url',
    'url_active']



   # if set(keys_asoc_basic) in set(data.keys()):
    if  set(keys_asoc_basic).issubset(set(data.keys())):

        data = {key: data[key] for key in keys_asoc_basic}
        logger.info('Asoc detected')
        logger.info('Correct columns!')
        table = 'metadata.asociaciones'
        data['id'] = data['nombre']
        urls =  get_asoc_urls(soup,data['id'],url_prefix)

        

    else: 
        logger.info('Incorrect columns')
        logger.info(url_prefix)
        data = []
        urls = []
        table = None

    empresas = []
    return data,urls, empresas, table