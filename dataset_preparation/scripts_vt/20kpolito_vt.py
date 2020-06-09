#!/usr/bin/env python3
import csv
import json
import requests
from datetime import datetime
import sys
import time
import os


'''
path_polito_dom = '/Volumes/SDPEPPE/polito_py/campus_top_10k_mod_dot.txt'
csv_scan = '/Volumes/SDPEPPE/polito_py/polito_scan_VT2.csv'
'''

def scanVT_polito(api_k, list_dn):
    """
    Args:
        api_k: API key of VT-> academic api 20k req/day, normal api 1k req/day
        list_dn: list of domain to scan -> max lenght : 20k
        
    Returns:
        csv whit the results of scanning
    """
    
    if os.path.exists('scanVT_csvout.csv'):
        os.remove('scanVT_csvout.csv')
        print('OLD CSV REMOVED')
    else:
        print('NO OLD CSV EXISTS')
    
    api_key_in = api_k #token da virus total
    path_polito_dom = list_dn #txt con lista di nomi da scannerizzare 
    url = 'https://www.virustotal.com/vtapi/v2/url/report'

    scanner_dict = {
        'CLEANMX': 0,
        'DNS8': 1,
        'VXVAULT': 2,
        'ZDBZEUS': 3,
        'TENCENT': 4,
        'MALWAREPATROL': 4,
        'ZCLOUDSEC': 5,
        'COMODOVALKYRIEVERDICT': 6,
        'PHISHLABS': 7,
        'K7ANTIVIRUS': 8,
        'FRAUDSENSE': 9,
        'VIRUSDIEEXTERNALSITESCAN': 10,
        'SPAMHAUS': 11,
        'QUTTERA': 12,
        'AEGISLABWEBGUARD': 13,
        'MALWAREDOMAINLIST': 14,
        'ZEUSTRACKER': 15,
        'ZVELO': 17,
        'GOOGLESAFEBROWSING': 18,
        'KASPERSKY': 19,
        'BITDEFENDER': 20,
        'DR.WEB': 21,
        'G-DATA': 22,
        'SEGASEC': 23,
        'CYBERCRIME': 24,
        'MALWAREDOMAINBLOCKLIST': 25,
        'CRDF': 26,
        'TRUSTWAVE': 27,
        'WEBSECURITYGUARD': 28,
        'CYRADAR': 29,
        'DESENMASCARA.ME': 30,
        'ADMINUSLABS': 31,
        'MALWAREBYTESHPHOSTS': 32,
        'OPERA': 33,
        'ALIENVAULT': 35,
        'EMSISOFT': 36,
        'MALC0DEDATABASE': 37,
        'MALWARES.COMURLCHECKER': 38,
        'PHISHTANK': 39,
        'EONSCOPE': 40,
        'MALWARED': 41,
        'AVIRA': 42,
        'NOTMINING': 43,
        'OPENPHISH': 44,
        'ANTIY-AVL': 45,
        'FORCEPOINTTHREATSEEKER': 46,
        'SCUMWARE.ORG': 47,
        'ESTSECURITY-THREATINSIDE': 48,
        'COMODOSITEINSPECTOR': 49,
        'YANDEXSAFEBROWSING': 50,
        'MALEKAL': 51,
        'ESET': 52,
        'SOPHOS': 53,
        'URLHAUS': 54,
        'SECUREBRAIN': 55,
        'NUCLEON': 56,
        'BADWARE.INFO': 57,
        'SUCURISITECHECK': 58,
        'BLUELIV': 59,
        'NETCRAFT': 60,
        'AUTOSHUN': 61,
        'THREATHIVE': 62,
        'FRAUDSCORE': 63,
        'QUICKHEAL': 64,
        'RISING': 65,
        'URLQUERY': 66,
        'STOPBADWARE': 67,
        'FORTINET': 68,
        'ZEROCERT': 69,
        'SPAM404': 70,
        'SECUROLYTICS': 71,
        'BAIDU-INTERNATIONAL': 72,
        'ZEROFOX': 73,
        'CERTLY': 74,
        'C-SIRT': 75,
        'WEBSENSETHREATSEEKER': 76,
        'WEBUTATION': 77,
        'PARETOLOGIC': 78,
        'WEPAWET': 79,
        'SPYEYETRACKER': 80,
        'CLOUDSTAT': 81,
        'PALEVOTRACKER': 82,
        'WOT': 83,
        'TRENDMICRO': 84,
        'BOTVRIJ.EU': 85,
        'SANGFOR': 86,
        'VIRUSDIE': 87,
        'MINOTAUR': 88,
        'PREBYTES': 89
    }
    header = ['domain', 'VT_response_code', 'VT_positives', 'VT_scandate', 'VT_total']
    scanners_dict_keys = scanner_dict.keys()
    #creation of header
    for x in scanners_dict_keys:
        header.append('VT_'+ str(x))

    fields_to_write = []
    list_domain_name = []
    count = 0
    with open(path_polito_dom) as polito_in, open('scanVT_csvout.csv', mode='a') as csv_out:
        lines = polito_in.readlines()
        for line in lines:
            if '\n' in line:
                #list_domain_name.append(line.strip('\n'))
                line = line.strip('\n')
                if 'www8.' in line:
                    line = line.replace('www8.','')
                elif 'www2.' in line:
                    line = line.replace('www2.','')
                    #aggiungere gestione che toglie il 'www.', 'www2.', 'www8.'.. anche sotto
                elif 'www.' in line:
                    line = line.replace('www.','')
                else:
                    print('[debug]probably no new case ')
                list_domain_name.append(line)
            else:
                if 'www8.' in line:
                    line = line.replace('www8.','')
                elif 'www2.' in line:
                    line = line.replace('www2.','')
                    #aggiungere gestione che toglie il 'www.', 'www2.', 'www8.'.. anche sotto
                elif 'www.' in line:
                    line = line.replace('www.','')
                else:
                    print('[debug]2 probably no new case ')
                list_domain_name.append(line)
        #print list of domain names for debug, it could be removed   
        for x in list_domain_name:
            print(x)
        print(len(list_domain_name))
            
                
        writer = csv.writer(csv_out)
        writer.writerow(header)
        for domain in list_domain_name:
            count += 1
            params_d = {'apikey': api_key_in, 'resource': domain}
            response = requests.get(url, params=params_d)
            code = response.status_code
            json_response = response.json()
            if code == 200 and json_response['response_code'] != 0:
                fields_to_write.append(json_response['resource'])
                fields_to_write.append(str(json_response['response_code']))
                fields_to_write.append(str(json_response['positives']))
                fields_to_write.append(str(json_response['scan_date']))
                fields_to_write.append(str(json_response['total']))
                json_keys = []
                json_keys_mod = {}
                for x in json_response['scans'].keys():
                    json_keys.append(str(x))
                    #  print(x)
                for x in json_keys:
                    z = x.strip()
                    z = z.replace(" ", "")
                    z = z.upper()
                    # print("chiave: " + z + " valore :" + x)
                    # json_keys_mod.append(str(x))
                    json_keys_mod[str(z)] = str(x)
                for scanners_key in scanners_dict_keys:
                    if scanners_key in json_keys_mod.keys():
                        scan_part = json_response['scans'][json_keys_mod[str(scanners_key)]]
                        txt_scan = scan_part['result']
                        fields_to_write.append(txt_scan)
                    else:
                        fields_to_write.append('null')
                writer.writerow(fields_to_write)
                fields_to_write = []
                json_keys = []
                json_keys_mod = {}
            elif json_response['response_code'] == 0:
                print(str(count) + " , response code 0")
                fields_to_write.append(json_response['resource'])
                fields_to_write.append(str(json_response['response_code']))
                # positives
                fields_to_write.append('null')
                # scandate
                fields_to_write.append('null')
                # total
                fields_to_write.append('null')
                for x in scanners_dict_keys:
                    fields_to_write.append('null')
                writer.writerow(fields_to_write)
                fields_to_write = []
            else:
                print("new problem")
            #sleep for minute request rate
            time.sleep(0.07)


if __name__ == "__main__":
    api_key_in = sys.argv[1] #token da virus total
    path_polito_dom = sys.argv[2] #txt con lista di nomi da scannerizzare 
    start = datetime.now()
    scanVT_polito(api_key_in,path_polito_dom)
    end = datetime.now()

    print('Duration: {}'.format(end - start))