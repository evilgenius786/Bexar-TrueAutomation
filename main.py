import csv
import time
import traceback

import json
import os.path
import threading
import requests
from bs4 import BeautifulSoup

thread_count = 100
semaphore = threading.Semaphore(thread_count)
test = False
encoding = 'utf8'
bexar = "https://bexar.trueautomation.com/clientdb"
s = requests.session()
lock = threading.Lock()
if not os.path.isfile("table-cache.json"):
    with open("table-cache.json", "w") as j:
        json.dump({}, j)
with open("table-cache.json") as j:
    table_cache = json.load(j)
if not os.path.isdir('json'):
    os.mkdir('json')
scraped = [x.replace(".json", "") for x in os.listdir('json')]
params = {'cid': '110'}
headers = ['Property ID', 'Legal Description', 'Geographic ID', 'Type', 'Address', 'Mapsco', 'Map ID', 'Name',
           'Owner ID', 'Mailing Address', '% Ownership', 'Property Use Code', 'Property Use Description',
           'Neighborhood', 'Exemptions', 'Zoning', 'Parent Property ID', 'Neighborhood CD', 'Agent Code',
           'Protest Status', 'Informal Date', 'Formal Date']

threads = []


def getTable(pid, snum, sname, tries=3):
    try:
        with semaphore:
            gdata = getData(BeautifulSoup(s.get(f'{bexar}/propertysearch.aspx', params=params).text, 'lxml'))
            gsoup = BeautifulSoup(s.post(f'{bexar}/propertysearch.aspx', params=params, data=gdata).text, 'lxml')
            s.post(f'{bexar}/propertysearch.aspx', params=params, data=getData(gsoup, snum, sname))
            response = s.get(f'{bexar}/SearchResults.aspx', params=params)
            span = [sp['prop_id'] for sp in BeautifulSoup(response.content, 'lxml').find_all('span', {'prop_id': True})]
            with lock:
                table_cache[pid] = span
                with open('table-cache.json', 'w') as f:
                    json.dump(table_cache, f, indent=4)
            if len(span) > 0:
                if len(span) > 1:
                    print(f"Multiple properties found for {pid} {snum} {sname}")
                else:
                    print(f"Got PID for {pid} {snum} {sname}", span[0])
                scrape({"CAN": span[0]})
            else:
                print(f"Nothing found ({pid}) via property address {snum} {sname}")
    except:
        if tries > 0:
            getTable(pid, snum, sname, tries - 1)
        else:
            print(f"Error getting table for {pid} {snum} {sname}")
            traceback.print_exc()


def scrape(line):
    try:
        pid = line['CAN']
        if pid in table_cache.keys():
            span = table_cache[pid]
            print(f"Found in cache {pid} {span}, Not trying again!")
            return
        with semaphore:
            if pid in scraped:
                print(f"Already scraped {pid}")
                return
            url = f"https://bexar.trueautomation.com/clientdb/Property.aspx?cid=110&prop_id={pid}"
            if test:
                with open('index.html') as ifile:
                    content = ifile.read()
            else:
                content = requests.get(url).text
            table = BeautifulSoup(content, 'lxml').find("table", {"summary": "Property Details"})
            if not table:
                if 'PROPERTY ADDRESS' not in line.keys():
                    print(f"Neither address nor property data found ({pid})")
                    return
                print(f"Property ID ({pid}) not found, searching via property address ({line['PROPERTY ADDRESS']})")
                padd = line['PROPERTY ADDRESS'].split(" ")
                t = threading.Thread(target=getTable, args=(pid, padd[0], " ".join(padd[1:])), )
                t.start()
                threads.append(t)
                return
            data = {}
            for tr in table.find_all('tr'):
                tds = tr.find_all('td')
                if len(tds) % 2 == 0:
                    for i in range(0, len(tds), 2):
                        if tds[i].text.strip() != "" and tds[i + 1].text.strip() != "":
                            data[tds[i].text.strip().replace(":", "")] = tds[i + 1].text.strip()
            print(json.dumps(data, indent=4))
            with open(f'./json/{pid}.json', 'w') as jfile:
                json.dump(data, jfile, indent=4)
            scraped.append(pid)
            append(data)
    except:
        print(f"Error {line}")
        traceback.print_exc()


def main():
    # getHeaders()
    print("Scraped", scraped)
    if not os.path.isfile('out.csv'):
        with open('out.csv', 'w', newline='', encoding=encoding) as csvfile:
            csv.DictWriter(csvfile, fieldnames=headers).writeheader()
        combineJson()
    with open('input.csv') as f:
        for line in csv.DictReader(f):
            pid = line['CAN']
            if pid in table_cache.keys():
                print(f"Found in cache {pid} {table_cache[pid]}, Not trying again!")
            elif pid not in scraped:
                t = threading.Thread(target=scrape, args=(line,))
                t.start()
                # t.join()
                time.sleep(0.1)
                threads.append(t)
            else:
                print(f"Already scraped {pid}")
    for thread in threads:
        thread.join()


def logo():
    print(r"""
  _______                                  _                            _    _               
 |__   __|                    /\          | |                          | |  (_)              
    | | _ __  _   _   ___    /  \   _   _ | |_  ___   _ __ ___    __ _ | |_  _   ___   _ __  
    | || '__|| | | | / _ \  / /\ \ | | | || __|/ _ \ | '_ ` _ \  / _` || __|| | / _ \ | '_ \ 
    | || |   | |_| ||  __/ / ____ \| |_| || |_| (_) || | | | | || (_| || |_ | || (_) || | | |
    |_||_|    \__,_| \___|/_/    \_\\__,_| \__|\___/ |_| |_| |_| \__,_| \__||_| \___/ |_| |_|
================================================================================================
            Bexar.TrueAutomation.com scraper by github.com/evilgenius786
================================================================================================
[+] CSV/JSON Output
[+] Without browser
[+] Multithreaded
[+] Resumable
[+] Response caching
________________________________________________________________________________________________                                                                                             
""")


def getHeaders():
    headers_list = []
    for file in os.listdir('json'):
        with open(f'json/{file}') as f:
            for key in json.load(f).keys():
                if key not in headers_list:
                    headers_list.append(key)
    print(headers_list)
    input()


def append(row):
    with open('out.csv', 'a', newline='', encoding=encoding) as csvfile:
        csv.DictWriter(csvfile, fieldnames=headers).writerow(row)


def combineJson():
    for file in os.listdir('json'):
        with open(f'json/{file}') as f:
            append(json.load(f))


def getData(soup, snum="", sname=""):
    data = {
        '__VIEWSTATE': soup.find('input', {'id': '__VIEWSTATE'})['value'],
        '__VIEWSTATEGENERATOR': '90EF699E',
        '__EVENTVALIDATION': soup.find('input', {'id': '__EVENTVALIDATION'})['value'],
        'propertySearchOptions:streetNumber': snum,
        'propertySearchOptions:streetName': sname,
        'propertySearchOptions:taxyear': '2022',
        'propertySearchOptions:propertyType': 'All',
        'propertySearchOptions:orderResultsBy': 'Owner Name',
        'propertySearchOptions:recordsPerPage': '25',
    }
    if snum != "":
        data['propertySearchOptions:searchAdv'] = 'Search'
    else:
        data['propertySearchOptions:advanced'] = 'Advanced >>'
    return data


def fixCSV():
    for file in os.listdir('json1'):
        with open(f'json1/{file}') as f:
            with open(f'json/{file}', 'w') as jfile:
                jfile.write(f.read().replace(':":', '":'))


if __name__ == '__main__':
    logo()
    main()
    # scrape({"CAN": "1085394", "PROPERTY ADDRESS": "213 PALO ALTO RD"})
    # getTable("203", "COLIMA ST")
