
from selenium import webdriver
from selenium.webdriver.chrome.options import Options as Options
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import *
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service

import datetime as dt
import time
import os
import json
from loguru import logger

from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import re

def selenium_download(url_prefix,download_dir):
    '''
    '''

    chrome_options = Options()
    chrome_options.add_experimental_option('prefs', {
        "download.default_directory": download_dir,  # Change default directory for downloads
        "download.prompt_for_download": False,  # To auto download the file
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True  # Enable safe browsing
    })


    # Init driver
    chrome_options.add_argument("--headless") # Runs Chrome in headless mode.

    service = Service("/usr/bin/chromedriver")
    driver = webdriver.Chrome(service=service, options=chrome_options)

    #driver.set_window_size(1500, 700)


    # Navigate to sie
    driver.get("https://repositorio.centrolaboral.gob.mx"+ url_prefix)
    # Evidence 1
    driver.save_screenshot('1-welcomepage.png')

    time.sleep(2)
    element =driver.find_elements(By.XPATH, "(//button[@class='btn btn-primary'])")[0]
    driver.execute_script("arguments[0].click();", element)
    driver.save_screenshot('2-loginpage.png')

    time.sleep(1)

    # Step 1: Store the current window handle
    original_window = driver.current_window_handle
    # Step 2: Execute the JavaScript click
    documents = driver.find_elements(By.XPATH, "//a[@class='data-tracking-document']")
    logger.info(f'detected {len(documents)}')
    for file in documents:
        try:
            file.click()
        except:
            try:
                time.sleep(1)
                file.click()
            except:
                pass

    documents = driver.find_elements(By.XPATH, "//button[@class= 'btn btn-primary btn-generar-liga data-tracking-document']")

    logger.info(f'detected {len(documents)}')
    for file in documents:
        try:
            file.click()
            time.sleep(1)

        except:
            try:
                time.sleep(1)
                file.click()
                time.sleep(1)
            except:
                pass

    elements = driver.find_elements(By.XPATH, "//a[@href]")
    matching_elements1 = [el for el in elements if re.search(r'pdf', el.get_attribute('href'))]
    matching_elements2 = [el for el in elements if re.search(r'xlsx', el.get_attribute('href'))]
    matching_elements = matching_elements1 + matching_elements2
    logger.info(f'detected {len(matching_elements)}')
    for file in matching_elements:
        try:
            file.click()
            time.sleep(1)

        except:
            try:
                time.sleep(1)
                file.click()
                time.sleep(1)
            except:
                pass

    driver.close()


def selenium_download_v2(url_prefix,download_dir,size_mb):

    download_dir =  "" + download_dir

    chrome_options = Options()
    chrome_options.add_experimental_option('prefs', {
        "download.default_directory": download_dir,  # Change default directory for downloads
        "download.prompt_for_download": False,  # To auto download the file
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True  # Enable safe browsing
    })


    chrome_options.add_argument("--headless=new")
    service = Service("/usr/bin/chromedriver")
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.get("https://repositorio.centrolaboral.gob.mx" + url_prefix)

    driver.execute_script("window.scrollBy(0, 1000);")  # scrolls down 500px
    time.sleep(2)
    element =driver.find_elements(By.XPATH, "(//button[@class='btn btn-primary'])")[0]
    driver.execute_script("arguments[0].click();", element)
    driver.save_screenshot('2-loginpage.png')

    time.sleep(1)

    # Step 1: Store the current window handle
    original_window = driver.current_window_handle
    # Step 2: Execute the JavaScript click


    accordions = driver.find_elements(By.XPATH, "//button[@class='accordion-button collapsed data-tracking-click ']")

    #for ac in accordions:
    #    ac.click()

    tramites = driver.find_elements(By.XPATH,"//a[@href='#collapseAntecedentes']")
    tramites[0].click()

    files = driver.find_elements(By.XPATH,"//a[@class='data-tracking-document']")

    print(len(files))
    for file in reversed(files)[0:6]:
        file.click()
        time.sleep(10)


    #subaccordions = driver.find_elements(By.XPATH, "//h2[@id='panelsSecondary-modmiembros-heading-1']//button[@type='button']")
    #print(len(subaccordions))
    #subaccordions[0].click()

    #file = driver.find_elements(By.XPATH, "//div[@id='colDoc-626021-626021-62']//a[@class='data-tracking-document']")

#//h2[@id='panelsSecondary-modmiembros-heading-2']//button[@type='button']

    subaccordions = driver.find_elements(By.XPATH, "//button[@class='accordion-button collapsed ']")
    #print(len(subaccordions))
    driver.save_screenshot('3-loginpage.png')

    for ac in reversed(subaccordions):
        try: 
            ac.click()
            time.sleep(2)
            driver.save_screenshot('5-loginpage.png')

        except:
            pass


    files = driver.find_elements(By.XPATH,"//a[@class='data-tracking-document']")

    print(len(files))
    for file in files:
        file.click()

    accordion_items = driver.find_elements(By.XPATH, "//button[@class='accordion-button collapsed ']")
    driver.save_screenshot('4-loginpage.png')
    for ac in accordion_items:
        colapsed_docs = driver.find_elements(By.XPATH, "//a[@data-bs-toggle='collapse']")
        for colapsed_doc in colapsed_docs:
            colapsed_doc.click()  


        print(len(colapsed_docs))

        time.sleep(3) 

        ac.click()  




    driver.save_screenshot('3-loginpage.png')


    documents = driver.find_elements(By.XPATH, "//a[@class='data-tracking-document']")
    
    driver.execute_script("window.scrollBy(0, 1000);")  # scrolls down 500px

    driver.save_screenshot('3-loginpage.png')

    logger.info(f'detected {len(documents)}')
    for file in documents:
        try:
            file.click()
            time.sleep(max(5, size_mb))

        except:
            try:
                file.click()
                time.sleep(max(5, size_mb))
            except:
                pass

    documents = driver.find_elements(By.XPATH, "//button[@class= 'btn btn-primary btn-generar-liga data-tracking-document']")

    logger.info(f'detected {len(documents)}')
    for file in documents:
        try:
            file.click()
            time.sleep(max(5, size_mb))

        except:
            try:
                file.click()
                time.sleep(max(5, size_mb))
            except:
                pass

    elements = driver.find_elements(By.XPATH, "//a[@href]")
    matching_elements1 = [el for el in elements if re.search(r'pdf', el.get_attribute('href'))]
    matching_elements2 = [el for el in elements if re.search(r'xlsx', el.get_attribute('href'))]
    matching_elements = matching_elements1 + matching_elements2
    logger.info(f'detected {len(matching_elements)}')
    for file in matching_elements:
        try:
            file.click()
            time.sleep(max(5, size_mb))

        except:
            try:
                file.click()
                time.sleep(max(5, size_mb))
            except:
                pass



def selenium_download_v3(url_prefix,download_dir,size_mb):

    download_dir =  "" + download_dir

    chrome_options = Options()
    chrome_options.add_experimental_option('prefs', {
        "download.default_directory": download_dir,  # Change default directory for downloads
        "download.prompt_for_download": False,  # To auto download the file
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True  # Enable safe browsing
    })

    chrome_options.add_argument("--headless=chrome")  # Modo más compatible
    #chrome_options.add_argument("--headless=new")
    service = Service("/usr/bin/chromedriver")
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.get("https://repositorio.centrolaboral.gob.mx" + url_prefix)




    driver.execute_script("window.scrollBy(0, 1000);")  # scrolls down 500px
    time.sleep(2)
    element =driver.find_elements(By.XPATH, "(//button[@class='btn btn-primary'])")[0]
    driver.execute_script("arguments[0].click();", element)
    driver.save_screenshot('2-loginpage.png')

    time.sleep(1)

    # Step 1: Store the current window handle
    original_window = driver.current_window_handle

    # UNPOP
    #accordions = driver.find_elements(By.XPATH, "//button[@class='accordion-button collapsed data-tracking-click ']")
    #print(len(accordions))
    #for ac in reversed(accordions):
    #    driver.save_screenshot('3-loginpage.png')
    #    ac.click()
    #    time.sleep(2)

    #subaccordions = driver.find_elements(By.XPATH, "//button[@class='accordion-button collapsed ']")
    #driver.save_screenshot('3-loginpage.png')
    #for ac in reversed(subaccordions):
    #    try: 
    #        ac.click()
    #        time.sleep(2)
    #        driver.save_screenshot('5-loginpage.png')
    #    except:
    #        pass
        
    # UNPOP
    driver.find_element(By.XPATH,"//a[@id='close-opinion-tool']").click()
    driver.save_screenshot('1-close.png')

    # Step 2: Execute the JavaScript click
    #documents = driver.find_elements(By.XPATH, "//a[@class='data-tracking-document']")
    #logger.info(f'detected {len(documents)}')
    #for file in documents:
    #    try:
    #        # Scroll to the element before clicking
    #        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", file)
    #        time.sleep(1)  # Optional: let the scroll complete
    #        driver.save_screenshot('5-loginpage.png')

 #           driver.execute_script("arguments[0].click();", file)
#
 #           # Espera tiempo razonable para que el navegador descargue
  #          time.sleep(max(5, size_mb))
   #     except Exception as e1:
    #        logger.warning(f'First click failed: {e1}')
     #       try:
      #          # Try JavaScript click as fallback
       #         driver.execute_script("arguments[0].click();", file)
        #        time.sleep(max(5, size_mb))
         #   except Exception as e2:
          #      logger.error(f'Click failed again: {e2}')
    documents = driver.find_elements(By.XPATH, "//a[@class='data-tracking-document']")
                
    for file in documents[0:10]:
        try:
            file.click()
            time.sleep(max(5, size_mb))

        except:
            try:
                file.click()
                time.sleep(max(5, size_mb))
            except:
                pass

    documents = driver.find_elements(By.XPATH, "//button[@class= 'btn btn-primary btn-generar-liga data-tracking-document']")

    logger.info(f'detected {len(documents)}')
    for file in documents[0:10]:
        try:
            file.click()
            time.sleep(max(5, size_mb))

        except:
            try:
                file.click()
                time.sleep(max(5, size_mb))
            except:
                pass

    elements = driver.find_elements(By.XPATH, "//a[@href]")
    matching_elements1 = [el for el in elements if re.search(r'pdf', el.get_attribute('href'))]
    matching_elements2 = [el for el in elements if re.search(r'xlsx', el.get_attribute('href'))]
    matching_elements = matching_elements1 + matching_elements2
    logger.info(f'detected {len(matching_elements)}')
    for file in matching_elements:
        try:
            file.click()
            time.sleep(max(5, size_mb))

        except:
            try:
                file.click()
                time.sleep(max(5, size_mb))
            except:
                pass