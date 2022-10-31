from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from multiprocessing import Queue
from threading import Thread
from selenium import webdriver
import logging
import csv
import re
import time
import mysql.connector

sqlinsertlinks="""INSERT INTO `kontak` (`Name`, `City`, `Country`, `Category`,`Telephone`, `Website`, `Adress`, `Lat`, `Lng`) values (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE `Name`=%s;"""


searchquery= "mobile phone repairs"
country= "FI"
countrylong= "Finland"
ccode= "+358 "
filename = 'cities.csv'


options = Options()
options.headless = True
options.add_argument("--window-size=1920,1080")
options.add_argument('--ignore-certificate-errors')
options.add_argument('--allow-running-insecure-content')
options.add_argument("--log-level=3")
user_agent = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.50 Safari/537.36'
options.add_argument(f'user-agent={user_agent}')

logger = logging.getLogger(__name__)

with open(filename, 'r', encoding='utf-8') as keywordfile:
    datareader = csv.reader(keywordfile)
    cities = [] 
    for city in datareader:
            cities.append(city[0])

cities.append('STOP')

selenium_data_queue = Queue()
worker_queue = Queue()

worker_ids = list(range(4)) #cpu_count()
selenium_workers = {i: webdriver.Chrome(executable_path="chromedriver.exe",options=options) for i in worker_ids}
for worker_id in worker_ids:
    worker_queue.put(worker_id)

def Consent(worker):
    worker.get("https://www.google.com/search?tbm=shop&q=asd")
    consentbutton=WebDriverWait(worker, 5).until(EC.visibility_of_element_located((By.CSS_SELECTOR, "#yDmH0d > c-wiz > div > div > div > div.NIoIEf > div.G4njw > div.AIC7ge > div.CxJub > div.VtwTSb > form:nth-child(1) > div > div > button > span")))
    consentbutton.click()

def GetProducts(worker, city):
    print("Getting "+searchquery+" in "+city)
    db = mysql.connector.connect(
  host="localhost",
  user="root",
  password="",
  database="beverwijk",
) 

    crs = db.cursor()
    crs.autocommit = True  
    
    query=searchquery+" in "+city+" "+countrylong
    url="https://maps.google.com/maps?q="+query
    worker.get(url)
    
    leftbar = WebDriverWait(worker, 5).until(EC.visibility_of_element_located((By.CSS_SELECTOR, "#QA0Szd > div > div > div.w6VYqd > div.bJzME.tTVLSc > div > div.e07Vkf.kA9KIf > div > div > div.m6QErb.DxyBCb.kA9KIf.dS8AEf.ecceSd > div.m6QErb.DxyBCb.kA9KIf.dS8AEf.ecceSd")))
    endofpage=0
    while endofpage == 0:
        leftbar.send_keys(Keys.CONTROL, Keys.END)
        try:
            endofpage = WebDriverWait(worker, 0.1).until(EC.visibility_of_element_located((By.CSS_SELECTOR, ".HlvSq")))
        except:
            print("Scroll down...")
    time.sleep(1)
    results = WebDriverWait(worker, 5).until(EC.visibility_of_all_elements_located((By.CSS_SELECTOR, ".hfpxzc")))
    placesincity=[]
    
    for result in results:
        print(result.get_attribute("href"))
        placesincity.append(result.get_attribute("href"))
        
    for placeurl in placesincity:
        worker.get(placeurl)
        time.sleep(0.5)
        try:
            title = WebDriverWait(worker, 3).until(EC.visibility_of_element_located((By.CSS_SELECTOR, ".DUwDvf.fontHeadlineLarge"))).text
        except:
            title = ''
            
        try:
            category = WebDriverWait(worker, 3).until(EC.visibility_of_element_located((By.CSS_SELECTOR, ".DkEaL.u6ijk"))).text
        except:
            try:
                category = WebDriverWait(worker, 3).until(EC.visibility_of_element_located((By.CSS_SELECTOR, '.DkEaL'))).text
            except:
                category = ''
            
        try:
            address = WebDriverWait(worker, 3).until(EC.visibility_of_element_located((By.CSS_SELECTOR, ".rogA2c"))).text
        except:
            address = ''
            
        try:
            website = WebDriverWait(worker, 3).until(EC.visibility_of_element_located((By.CSS_SELECTOR, '[data-tooltip="Website openen"]'))).get_attribute("href")
        except:
            website = ''
            
        try:
            telephone = WebDriverWait(worker, 3).until(EC.visibility_of_element_located((By.CSS_SELECTOR, '[data-tooltip="Telefoonnummer kopiëren"]'))).text
            if telephone[0:1] != "+":
                telephone = ccode+telephone[1:]
        except:
            telephone = ''
            
        print(telephone)
        kordinat= re.search("(\-?\d+(\.\d+)?)!4d\s*(\-?\d+(\.\d+)?)",placeurl)
        
        
        insertrow=(title,city,country,category,telephone,website,address,kordinat.group(1),kordinat.group(3),title)
        crs.execute(sqlinsertlinks,insertrow)



def selenium_queue_listener(data_queue, worker_queue):
    """
    Monitor a data queue and assign new pieces of data to any available web workers to action
    :param data_queue: The python FIFO queue containing the data to run on the web worker
    :type data_queue: Queue
    :param worker_queue: The queue that holds the IDs of any idle workers
    :type worker_queue: Queue
    :rtype: None
    """
    
    logger.info("Selenium func worker started")
    while True:
        current_data = data_queue.get()
        if current_data == 'STOP':
            # If a stop is encountered then kill the current worker and put the stop back onto the queue
            # to poison other workers listening on the queue
            logger.warning("STOP encountered, killing worker thread")
            data_queue.put(current_data)
            break
        else:
            logger.info(f"Got the item {current_data} on the data queue")
        # Get the ID of any currently free workers from the worker queue
        worker_id = worker_queue.get()
        worker = selenium_workers[worker_id]
        # Assign current worker and current data to your selenium function
        GetProducts(worker, current_data)
        # Put the worker back into the worker queue as  it has completed it's task
        worker_queue.put(worker_id)
    return

# Create one new queue listener thread per selenium worker and start them
logger.info("Starting selenium background processes")

for i in range(4):
        Consent(selenium_workers[i])

selenium_processes = [Thread(target=selenium_queue_listener,
                             args=(selenium_data_queue, worker_queue)) for _ in worker_ids]
for p in selenium_processes:
    p.daemon = True
    p.start()

# Add each item of data to the data queue, this could be done over time so long as the selenium queue listening
# processes are still running
logger.info("Adding data to data queue")
for d in cities:
    selenium_data_queue.put(d)

# Wait for all selenium queue listening processes to complete, this happens when the queue listener returns
logger.info("Waiting for Queue listener threads to complete")
for p in selenium_processes:
    p.join()

# Quit all the web workers elegantly in the background
logger.info("Tearing down web workers")
for b in selenium_workers.values():
    b.quit()