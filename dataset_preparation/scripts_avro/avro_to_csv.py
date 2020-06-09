#!/usr/bin/env python3
from fastavro import reader
import csv
import pandas as pd
import requests
import json
import time
from datetime import datetime
import sys
import numpy
from glob import glob
import os
import urllib.request as request
from datetime import date, timedelta
import tarfile
import shutil

path_avro_prova = "/Volumes/SDPEPPE/openintel-open-tld-20191120/CO_0C036A72E2C7B9277634A5BDD7FD8228.avro"
path_avrocsv = "/Volumes/SDPEPPE/Py_OpenIntel/prova-avro.csv"
path_all_avrotot = "/Volumes/SDPEPPE/out.avro"
path_1m = '/Volumes/SDPEPPE/Py_OpenIntel/1m.csv'
polito_new = '/Volumes/SDPEPPE/Py_OpenIntel/campus_top_10k_mod_dot.csv'
tranco_vt = '/Volumes/SDPEPPE/Py_OpenIntel/tranco_dom_check.csv'
tranc_csv = '/Volumes/SDPEPPE/Py_OpenIntel/tranco-top-1m.csv'
url = 'https://www.virustotal.com/vtapi/v2/url/report'

avro_umbtot = '/Volumes/SDPEPPE/Py_OpenIntel/avroumb.avro'
avro_concat_06 = '/Volumes/SDPEPPE/Py_OpenIntel/06_avro_concat.avro'

path_opin_umb = '/Volumes/SDPEPPE/Py_OpenIntel/openintel-umbrella1m-20191206'
path_opin5_umb = '/Volumes/SDPEPPE/Py_OpenIntel/openintel-umbrella1m-20191205'
path_opin4_umb = '/Volumes/SDPEPPE/Py_OpenIntel/openintel-umbrella1m-20191204'

path_opin9_01_20 = '/Volumes/SDPEPPE/Py_OpenIntel/openintel-umbrella1m-20200109'

########### TLD ###########
path_opin9_01_20_tld = '/Volumes/SDPEPPE/Py_OpenIntel/openintel-open-tld-20200109'
###########################

'''per ogni avro crea il csv
'''
def multiple_avro_creationcsv(pathfold):
    """
    Args:
        pathfold: path folder that contain the folder unpacked from OI tar
    
    Return:
        in every sub folder, it creates the csv for each avro contained
    """
    path_folder = pathfold
    folders = glob(path_folder+'/*')
    for folder in folders:
        print(folder)
        split_path = folder.split('/')[-1]
        base_name = split_path.split('-')[-1]+'_'
        file_in_dir = glob(folder+'/*.avro')
        file_in_dir.sort(key=os.path.getmtime)
        head = True
        countf = 0
        count = 0
        print('****'+base_name+'****')
        for file in file_in_dir:
            print(file)
            path_file_out = os.path.join(folder,base_name+str(countf)+'.csv')
            with open(file,'rb') as avro_in, open(path_file_out, mode='a') as csv_out:
                f = csv.writer(csv_out)
                avro_reader = reader(avro_in)
                for emp in avro_reader:
                    if head == True:
                        header = emp.keys()
                        f.writerow(header)
                        head = False
                    f.writerow(emp.values())
                head = True
            countf += 1
    '''
        path_folder_openIntel = sys.argv[1]
        split_path= path_folder_openIntel.split("/")[-1]
        base_name = split_path.split('-')[-1] +'_'
        #base_name = "6_01_20_"
        file_in_dir = glob(path_folder_openIntel + "/*.avro")
        file_in_dir.sort(key=os.path.getmtime)
        countf = 0
        head = True
        count = 0
        for file in file_in_dir:
            print(file)
            path_file_out = os.path.join(path_folder_openIntel,base_name+str(countf)+'.csv')
            with open(file, 'rb') as avro_in, open(path_file_out, mode='a') as csv_out:
                f = csv.writer(csv_out)
                avro_reader = reader(avro_in)
                for emp in avro_reader:
                    if head == True:
                        header = emp.keys()
                        f.writerow(header)
                        head = False
                    f.writerow(emp.values())
                head = True
            countf += 1
    '''


def single_avro_creationcsv(folder):
    path_folder_openIntel = folder
    split_path= path_folder_openIntel.split("/")[-1]
    base_name = split_path.split('-')[-1] +'_'
    #base_name = "6_01_20_"
    file_in_dir = glob(path_folder_openIntel + "/*.avro")
    file_in_dir.sort(key=os.path.getmtime)
    countf = 0
    head = True
    count = 0
    for file in file_in_dir:
        print(file)
        path_file_out = os.path.join(path_folder_openIntel,base_name+str(countf)+'.csv')
        with open(file, 'rb') as avro_in, open(path_file_out, mode='a') as csv_out:
            f = csv.writer(csv_out)
            avro_reader = reader(avro_in)
            for emp in avro_reader:
                if head == True:
                    header = emp.keys()
                    f.writerow(header)
                    head = False
                f.writerow(emp.values())
            head = True
        countf += 1

def download_tar(date, type_data):
    """
    Args:
        date (str): string date -> format 'XXXXYYZZ' -> XXXX=year, YY=month,ZZ=day
        type_data (str): 'umbrella1m', 'open-tld','alexa1m'
        
    Returns:
        File rar in 'OI_rar' folder
    """
    if os.path.exists('OI_tar'):
        print("'OI_tar' already exists")
    else:
        os.mkdir('OI_tar')
        
    if os.path.exists('OI_tar/openintel-'+type_data+'-'+date+'.tar'):
        print('tar ',date,' ',type_data,' already exists')
    else:
        year = date[:4]
        path_url= 'https://data.openintel.nl/data/'+type_data+'/'+year+'/openintel-'+type_data+'-'+date+'.tar'
        print('Beginning of download..')
        request.urlretrieve(path_url,'OI_tar/openintel-'+type_data+'-'+date+'.tar')
    
    
def download_tar_interval(start_date, end_date, type_data):
    """
    Args:
        start_date (str): starting date -> format 'XXXXYYZZ' -> XXXX=year, YY=month,ZZ=day
        end_date(str): end date -> format 'XXXXYYZZ' -> XXXX=year, YY=month,ZZ=day
        type_data (str): 'umbrella1m', 'open-tld','alexa1m'
        
    Returns:

        PATH 'OI_rar' folder
    
    """
    if os.path.exists('OI_tar'):
        print("'OI_tar' already exists")
    else:
        os.mkdir('OI_tar')
    #extract year,month and day from the input strings start and end date
    year_start = int(start_date[:4])
    month_start = int(start_date[4:6])
    day_start = int(start_date[6:])
    
    year_end = int(end_date[:4])
    month_end = int(end_date[4:6])
    day_end = int(end_date[6:])
    
    #convert start and end date in format Date
    start_d = date(year_start, month_start, day_start)
    print(start_d)
    end_d = date(year_end, month_end, day_end)
    print(end_d)
    
    #calc the days between end and start
    days = end_d - start_d
    
    #download the rar for each days between end and start
    for i in range(0,int(days.days) + 1):
        dates = start_d + timedelta(days=i)
        year = str(dates.year)
        month = str(dates.month)
        if int(month) <10:
            month = '0'+month
        day = str(dates.day)
        if int(day) <10:
            day = '0'+day
        conc_date = year + month + day
        print('Beginning of download of '+ type_data+', '+conc_date+'.tar')
        path_url = 'https://data.openintel.nl/data/'+ type_data +'/'+year+'/openintel-'+type_data+'-'+conc_date+'.tar'
        print(path_url)
        request.urlretrieve(path_url, 'OI_tar/openintel-'+type_data+'-'+conc_date+'.tar')
            
    return 'OI_tar'
    
    
def untar(path_oi_tar=None):
    """
    Args: 
        path_oi_tar: folder path that contains .tar file unpacked
    
    Returns:
        path OI_tar, a new folder that contains all .tar 
    """
    
    if os.path.exists('OI_folder'):
        print("'OI_folder' already exists")
    else:
        os.mkdir('OI_folder')

    files = []
    if path_oi_tar[-1] == '/':
        files = glob(path_oi_tar+'*.tar')
    else:
        files = glob(path_oi_tar+'/*.tar')
    
    for file in files:
        file_name = file.split('/')[-1].split('.')[0]
        print(file_name)
        
        if os.path.exists('OI_folder/'+file_name+'/'):
            os.remove('OI_folder/'+file_name+'/')
            os.mkdir('OI_folder/'+file_name+'/')
        else:
            os.mkdir('OI_folder/'+file_name+'/')
            
        my_tar = tarfile.open(file)
        my_tar.extractall('OI_folder/'+file_name)
        my_tar.close()
    
    return 'OI_folder'
    
    
def single_untar(path_tar):
    """
    Args: 
        path_oi_tar:  path of .tar file 
    
    Returns:
        path OI_tar, a new folder that contains all .tar 
    """
    
    if os.path.exists('OI_folder'):
        print("'OI_folder' already exists")
    else:
        os.mkdir('OI_folder')

    name = path_tar.split('/')[-1].split('.')[0]
    
    if os.path.exists('OI_folder/'+name):
        shutil.rmtree('OI_folder/'+name+'/')
        #os.remove('OI_folder/'+name)
        os.makedirs('OI_folder/'+name)
    else:
        os.makedirs('OI_folder/'+name)
    
    my_tar = tarfile.open(path_tar)
    my_tar.extractall('OI_folder/'+name+'/')
    my_tar.close()
    
    return 'OI_folder'
    

#no
def avro_csvconcat():
    head = True
    count = 0
    #f = csv.writer(open("test.csv", "w+"))
    with open(avro_concat_06, 'rb') as fo, open('OI_06_concat.csv', mode='a') as csv_out:
        f = csv.writer(csv_out)
        avro_reader = reader(fo)
        for emp in avro_reader:
            print(count)
            #print(emp)
            if head == True:
                header = emp.keys()
                f.writerow(header)
                head = False
            count += 1
            f.writerow(emp.values())
            #print(emp.values())
    print(count)


'''
- domain name 
- vt : [yes/no(1/0), resposne_code, positives, scandate ]
    - vt_in: yes/no
    - vt_resp_code
    - vt_positives
    - vt_scandate
- tranco : -tranco_base_in: 0/1 (non presente nel file csv base tranco/presente)
          - tranco_vt : 0/1( non presente nei file FP/ presente)
 
           

'''
#no
def info_to_csv(api_key):
    with open('csv_info_new.csv', mode='a') as csv_out, open(path_1m,mode='r') as oneM, open(polito_new, mode='r') as pol_new, open(tranco_vt) as tran_vt\
            , open(tranc_csv) as tranc:
        lines_c = list(pol_new.readlines())
        #lines = pd.read_csv(pol_new).values
        lines = []
        for line in lines_c:
            if '\n' in line:
                lines.append(line.strip('\n'))
        writer = csv.writer(csv_out, delimiter=",")
        data = pd.read_csv(path_1m, delimiter=";")
        df = data['domain name'].values
        data.set_index('domain name', inplace=True)
        #print(data.head())
        #print(data['scandate'].loc['netflix.com'])
        data_tranc_s = pd.read_csv(tranc).values
        data_tranc = []
        for x in data_tranc_s:
            if '\n' in x:
                data_tranc.append(x.strip('\n'))
        data_tranc_vt_s= pd.read_csv(tran_vt).values
        data_tranc_vt = []
        for x in data_tranc_vt_s:
            if '\n' in x:
                data_tranc_vt.append(x.strip('\n'))

        count = 0
        count_scan = 0
        header = ["domain_name","vt_in","vt_resp_code","vt_positives","vt_scandate","tranco_base_in","tranco_vt"]
        array_info = []
        #array_info.append("domain_name,VT,Tranco")
        writer.writerow(header)
        for line in lines:
            print(count)
            array_info.append(str(line))
            if line in df:
                #vt_in
                array_info.append(1)
                #resp code
                resp_cod = data['response_code'].loc[str(line)]
                array_info.append(int(resp_cod))
                #positives
                positives = data['positives'].loc[str(line)]
                array_info.append(numpy.nan_to_num(positives))
                #scandate
                scandate = data['scandate'].loc[str(line)]
                array_info.append(scandate)
                #array_info.append(" '[1," + str(resp_cod) + "," + str(positives) + "," + str(scandate) + "]'")
            else:
                time.sleep(0.07)
                params = {'apikey': api_key, 'resource': str(line)}
                response = requests.get(url, params=params)
                json_resp = response.json()
                #response_cod = json_resp['response_code']
                code = response.status_code
                # print(code)
                count_scan += 1
                #print(type(response_cod))
                if json_resp['response_code'] != 0:
                    #array_info.append("'[0,1," + str(json_resp['positives']) + "," + str(json_resp['scan_date']) + "]'")
                    array_info.append(0)
                    array_info.append(1)
                    array_info.append(int(json_resp['positives']))
                    array_info.append(str(json_resp['scan_date']))
                else:
                   # array_info.append("'[0,0,0,0]'")
                    array_info.append(0)
                    array_info.append(0)
                    array_info.append(0)
                    array_info.append(0)
            if line in data_tranc:
                array_info.append(1)
            else:
                array_info.append(0)
            if line in data_tranc_vt:
                array_info.append(1)
            else:
                array_info.append(0)
            count += 1
            writer.writerow(array_info)
            array_info = []
            #if count == 10:
                #break
        print("count scan:" + str(count_scan))


def main(sd,ed,t):
    
    '''To understand the value to pass, please look in 'download_tar_interval' def function (line 145)..
    But I can repeat it here, only for you :D 
    """
    Args:
        start_date (str): starting date -> format 'XXXXYYZZ' -> XXXX=year, YY=month,ZZ=day
        end_date(str): end date -> format 'XXXXYYZZ' -> XXXX=year, YY=month,ZZ=day
        type_data (str): 'umbrella1m', 'open-tld','alexa1m'
        
    For example: if you want download tar of alexa1m from 01-01-2020 to 03-01-2020, you have to write:
        - start_date as '20200101'
        - end_date as '20200103'
        - type_data as 'alexa1m'
    Returns:

        PATH 'OI_rar' folder
    
    """
    '''
    
    start_date = sd
    end_date = ed
    type_data = t
    
  
    
    if start_date == end_date:
        
        print('single downlaod')
        start = datetime.now()
        path_tar = download_tar(start_date,type_data) 
        end = datetime.now()
        print("Duration : {}".format(end - start))
        print('Start untar')
        
        folder_untar= single_untar('OI_tar/'+'openintel-'+type_data+'-'+start_date+'.tar')
        path_folder_singleconversation = os.path.join(folder_untar,'openintel-'+type_data+'-'+start_date)
        single_avro_creationcsv(path_folder_singleconversation)
        
    else:
        print('multiple downlaod')
        start = datetime.now()
        path_tar = download_tar_interval(start_date,end_date,type_data) 
        end = datetime.now()
        print("Duration : {}".format(end - start))
        print('Start untar')
        folder_untar = untar(path_tar)
        print("Start converting avro->csv for each folder in 'OI_fodler' ")
        multiple_avro_creationcsv(folder_untar)
    
    
    

if __name__ == '__main__':
    
    start_date = sys.argv[1] 
    end_date = sys.argv[2]
    type_data = sys.argv[3]
    main(start_date,end_date,type_data)
    
    