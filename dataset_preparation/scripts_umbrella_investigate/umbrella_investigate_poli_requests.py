import requests
import json
import csv
import sys
import time
import os
from datetime import datetime
import re




def investigate_requests(token,list_d):
    """
    Args: 
        token: token of umbrella investigate
        list_d: list of domain names to scan , max lenght of list: 2k
        
    Returns:
        csv whit results of scan
    """
    '''
    if os.path.exists('csv_out_investigate'):
        print("folder 'csv_out_investigate' already exists")
    else:
        os.mkdir('csv_out_investigate')
    ''' 

    #token = sys.argv[1]
    #token = '2e7bf9bc-ba55-4a05-a814-28bfe5035618'

    header_api = {'Authorization': 'Bearer '+ token,
                }
    #file txt con lista nomi da scannerizzare (limite 2000 nomi)
    path_nom_dom_txt = list_d


    


    url = 'https://investigate.api.umbrella.com/domains/categorization/'
 
    '''
    header_api = {'Authorization': 'Bearer 2e7bf9bc-ba55-4a05-a814-28bfe5035618',
                }
    '''

    params = {
        'showLabels':''
    }

    #check presence of folder 'scanned'
    folder_base = os.path.dirname(list_dn)
    if os.path.exists(folder_base+'/scanned'):
        print('folder scanned exists')
    else:
        os.mkdir(folder_base+'/scanned')
        
    if os.path.exists(folder_base+'/scanned/'+list_dn.split('/')[-1]):
        os.remove(folder_base+'/scanned/'+list_dn.split('/')[-1])
        print('old '+ list_dn.split('/')[-1]+ ' removed')
    else:
        print('no old '+ list_dn.split('/')[-1]+' exists')
    
    if os.path.exists(folder_base+'/error_umb'):
        print('folder error_umb exists')
    else:
        os.mkdir(folder_base+'/error_umb')
    
    if os.path.exists(folder_base+'/error_umb/'+list_dn.split('/')[-1].split('.')[0]+'_error_umb.txt'):
        print('relative error file exists..removing it')
        os.remove(folder_base+'/error_umb/'+list_dn.split('/')[-1].split('.')[0]+'_error_umb.txt')
    else:
        print('no old error file exists')
    
    #folder out dove inserire file txt con messaggi di errore
    #txt_debug = '/Volumes/SDPEPPE/umbrella_investigate_polito/error_umb/'
    '''
    if os.path.exists('error_umb'):
        print("folder 'error_umb' already exists")
    else:
        os.mkdir('error_umb')
        
    dirname = os.path.basename(path_nom_dom_txt)
    print(dirname)
    name_csv = dirname.split('.')[0]
    print(name_csv)
    '''

    domains = []
    count_null = 0
    re_www = re.compile(r'www.')
    re_wwwn= re.compile(r"www\w+.")
    with open(path_nom_dom_txt) as nom_dom_txt:
        lines = nom_dom_txt.readlines()
        for line in lines:
            if '\n' in line:
                #list_domain_name.append(line.strip('\n'))
                line = line.strip('\n')
                if len(line) == 0:
                    count_null += 1
                    continue
                elif re.match(re_wwwn,line):
                    line = re_wwwn.sub('',line)
                elif re.match(re_www,line):
                    line = re_www.sub('',line)
                else:
                    print('[debug]probably no new case ')
                domains.append(line)
            else:
                if len(line) == 0:
                    count_null += 1
                    continue
                elif re.match(re_wwwn,line):
                    line = re_wwwn.sub('',line)
                elif re.match(re_www,line):
                    line = re_www.sub('',line)
                else:
                    print('[debug]probably no new case ')
                domains.append(line)

    '''
    for dom in domains:
        print(dom)

    '''
    header_vt = ['domain','INV_status','INV_security_categories','INV_content_categories']
    header  = ['domain','status','security_categories','content_categories']
    data_to_write = []
    count_dom = 0
    
    scanned_folder_path = os.path.join(folder_base,'scanned')
    file_name_out = os.path.join(scanned_folder_path,list_dn.split('/')[-1])
    scanned_folder_path = os.path.join(folder_base,'error_umb')
    error_file_name_out = os.path.join(scanned_folder_path,list_dn.split('/')[-1].split('.')[0]+'_error_umb.txt')
    
    with open(file_name_out, mode='a') as csv_out, open(error_file_name_out, mode='w+') as txt_deb:
        writer = csv.writer(csv_out, delimiter=';')
        writer.writerow(header_vt)
        for domain in domains:
            count_dom += 1
            response = requests.get('https://investigate.api.umbrella.com/domains/categorization/' + domain , params=params, headers=header_api)
            code = response.status_code
            if code == 403:
                print("403: Request had Authorization header but token was missing or invalid. Please ensure your API token is valid.")
                txt_deb.write(domain + ',' + str(count_dom) + '\n')
                txt_deb.write("403: Request had Authorization header but token was missing or invalid. Please ensure your API token is valid.\n")
                exit()
            elif code == 404:
                print("404: The requested item doesn't exist, check the syntax of your query or ensure the IP and/or domain are valid.")
                txt_deb.write(domain + ',' + str(count_dom) + '\n')
                txt_deb.write("404: The requested item doesn't exist, check the syntax of your query or ensure the IP and/or domain are valid.\n")
                exit()
            elif code == 429:
                print("429: Too many requests received in a given amount of time. You may have exceeded the rate limits for your organization or package.")
                txt_deb.write(domain + ',' + str(count_dom) + '\n')
                txt_deb.write("429: Too many requests received in a given amount of time. You may have exceeded the rate limits for your organization or package.\n")
                exit()
            elif code == 500 or code == 502 or code == 503 or code == 504:
                print("Something went wrong on Umbrella's end.")
                txt_deb.write(domain + ',' + str(count_dom) + '\n')
                txt_deb.write("Something went wrong on Umbrella's end.\n")
                exit()
            else:
                print(str(code)+ ' OK - ' + str(domain) + ' - ' + str(count_dom))
                response_json = response.json()
            print(response_json[domain])
            data_to_write.append(domain)
            data_to_write.append(str(response_json[domain][header[1]]))
            data_to_write.append(str(response_json[domain][header[2]]))
            data_to_write.append(response_json[domain][header[3]])
            writer.writerow(data_to_write)
            data_to_write = []
            time.sleep(0.3)
        
    



if __name__ == '__main__':
    token = sys.argv[1]
    list_dn = sys.argv[2]
    start = datetime.now()
    investigate_requests(token,list_dn)
    end = datetime.now()
    print("Duration : {}".format(end-start))
    