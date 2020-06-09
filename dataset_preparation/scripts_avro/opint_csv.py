#!/usr/bin/env python3

import csv
import pandas as pd
import numpy as np
import gc
from glob import glob
import os
import subprocess
from datetime import datetime
import sys

'''
    path_oicsv_0_6 = '/Volumes/SDPEPPE/Py_OpenIntel/openintel-umbrella1m-20191206/6_12_19_0.csv'
    path_oicsv_0_5 = '/Volumes/SDPEPPE/Py_OpenIntel/openintel-umbrella1m-20191206/6_12_19_0.csv'
    path_oicsv_0_4 = '/Volumes/SDPEPPE/Py_OpenIntel/openintel-umbrella1m-20191206/6_12_19_0.csv'

    path_fold_6 = '/Volumes/SDPEPPE/Py_OpenIntel/openintel-umbrella1m-20191206'
    path_fold_4 = '/Volumes/SDPEPPE/Py_OpenIntel/openintel-umbrella1m-20191204'


    top1m_base = '/Volumes/SDPEPPE/Py_OpenIntel/top-1m-new-filter.csv'
    #Onem_oi_dom = '/Volumes/SDPEPPE/Py_OpenIntel/1m_oi_dom.csv'
    csv_inter1 = '/Volumes/SDPEPPE/Py_OpenIntel/inter_dotwww_in_t1m.csv'
    csv_inter2 = '/Volumes/SDPEPPE/Py_OpenIntel/inter_dotwww_notin_t1m.csv'

    csv_inter1 = '/Volumes/SDPEPPE/Py_OpenIntel/inter_dotwww_in_t1m.csv'
    csv_inter2 = '/Volumes/SDPEPPE/Py_OpenIntel/inter_dotwww_notin_t1m.csv'

    csv_inter1_04 = '/Volumes/SDPEPPE/Py_OpenIntel/inter_dotwww_in_t1m_04.csv'
    csv_inter2_04 = '/Volumes/SDPEPPE/Py_OpenIntel/inter_dotwww_notin_t1m_04.csv'

    path_nds_04 = '/Volumes/SDPEPPE/Py_OpenIntel/nds_4.csv'
    path_nds_04_nodot = '/Volumes/SDPEPPE/Py_OpenIntel/nds_4_nodot.csv'
    path_nds_nodot_nowww_04 = '/Volumes/SDPEPPE/Py_OpenIntel/nds_nodot_nowww_04.csv'


    path_allumb = "/Volumes/SDPEPPE/Py_OpenIntel/prova_all_umb.csv"
    path_nds = '/Volumes/SDPEPPE/Py_OpenIntel/nds.csv'
    path_nds_nodot_nowww = '/Volumes/SDPEPPE/Py_OpenIntel/nds_nodot_nowww.csv'

    path_fold_09_01_20 = '/Volumes/SDPEPPE/Py_OpenIntel/openintel-umbrella1m-20200109'
    path_nds_09_01_20 = '/Volumes/SDPEPPE/Py_OpenIntel/nds_09_01_20.csv'
    path_nds_nodot_09_01_20 = '/Volumes/SDPEPPE/Py_OpenIntel/nds_nodot_09_01_20.csv'
    path_nds_nodot_www_09_01_20 = '/Volumes/SDPEPPE/Py_OpenIntel/nds_nodot_www_09_01_20.csv'
    csv_inter1_090120 = '/Volumes/SDPEPPE/Py_OpenIntel/polito_OI_090120/inter1in_polito_OI090120_v2.csv'
    csv_inter2_090120 = '/Volumes/SDPEPPE/Py_OpenIntel/polito_OI_090120/inter2notin_polito_OI090120_v2.csv'
    polito_nds = '/Volumes/SDPEPPE/Py_OpenIntel/campus_top_10k_mod_dot.csv'
    polito_nds_txt = '/Volumes/SDPEPPE/Py_OpenIntel/campus_top_10k_mod_dot.txt'
    ################################# TLD ###########################
    path_fold_09_01_20_tld = '/Volumes/SDPEPPE/Py_OpenIntel/openintel-open-tld-20200109'
    path_nds_09_01_20_tld = '/Volumes/SDPEPPE/Py_OpenIntel/nds_09_01_20_tld.csv'
    path_nds_nodot_09_01_20_tld = '/Volumes/SDPEPPE/Py_OpenIntel/nds_nodot_09_01_20_tld.csv'
    path_nds_nodot_www_09_01_20_tld = '/Volumes/SDPEPPE/Py_OpenIntel/nds_nodot_www_09_01_20_tld.csv'
    csv_inter1_090120_tld = '/Volumes/SDPEPPE/Py_OpenIntel/polito_OI_090120/inter1in_polito_OI090120_tld.csv'
    csv_inter2_090120_tld = '/Volumes/SDPEPPE/Py_OpenIntel/polito_OI_090120/inter2notin_polito_OI090120_tld.csv'
    #################################################################
'''

'''ricavare la lista dei domini su cui vengono inoltrare le richieste.. la lista coniente i domini con il punto finale e 
quelli con 'www.'
Da terminale con il comando 'sed 's/.$//' nomefile.estensione > fileout.estensione' rimuovo tutti i '.' finali dai
nomi di dominio.
Successivamente chiamo la funzione 'del__www' per eliminare tutti i nomi di dominio che iniziano con 'www.' '''

def dom_name_by_OI(folder):
    path_fold = folder
    name = path_fold.split('/')[-1]
    """
    Args:
        folder: folder thaht contains csv obtained from avro 
    
    Return:
        csv'list_domnames_oi.csv' that contain a list of domain names queried by OI in the csvs passed
    """
    file_in_dir = glob(path_fold + "/*.csv")
    file_in_dir.sort(key=os.path.getmtime)
    count_f = 0
    count_tot = 0
    
    
    if os.path.exists('list_domnames_oi_'+name+'.csv'):
        print('old list_domnames_oi_'+name+'.csv removed')
        os.remove('list_domnames_oi_'+name+'.csv')
    else:
        print('no old list_domnames_oi'+name+'.csv exists')
        
    name_csv_out = 'list_domnames_oi_'+name+'.csv'
    with open('list_domnames_oi_'+name+'.csv', mode='a') as nds_out:
        for file_csv in file_in_dir:
            print(count_f)
            print(file_csv)
            df = pd.read_csv(file_csv, delimiter=',',low_memory=False)
            df_dom_names = df['query_name'].drop_duplicates()
            #print(df_dom_names.describe())
            #print(type(df_dom_names))
            df_dom_names.to_csv(nds_out, header=False, index=False)
            count = 0
            for x in df_dom_names.values:
                #print(x)
                count += 1
            print("count: " + str(count))
            count_tot += count
            count_f += 1
            del df
            del df_dom_names
            gc.collect()
    print("count_tot: " + str(count_tot))
    
    return name_csv_out


def del_www(list_dom_nodot):
    """
    Args:
        list_dom_nodot: list of dom names where for each domain names we have to remove the start 'www.','www3.' etc
        
    Return:
        csv 'filtered_OIdomnames.csv' with a lsit of domain names without the start 'www.', 'www3.' etc.
    """
    path_nds_nodot = list_dom_nodot
    name = path_nds_nodot.split('/')[-1].split('.')[0].split('_')[-1]
    path_nds_nodot_www = 'filtered_OIdomnames_'+name+'.csv'
    if os.path.exists(path_nds_nodot_www):
        os.remove(path_nds_nodot_www)
        print('old filtered_OIdomnames_'+name+'.csv removed ')
    else:
        print('no old filtered_OIdomnames_'+name+'.csv exists')
        
    with open(path_nds_nodot) as csv_in, open(path_nds_nodot_www,mode='a') as csv_out:
        writer = csv.writer(csv_out)
        lines = csv_in.readlines()
        for line in lines:
            if '\n' in line:
                nl = line.strip('\n')
                if 'www.' in nl:
                    continue
                else:
                    writer.writerow([nl])
            else:
                if 'www.' in line:
                    continue
                else:
                    writer.writerow([line])
    return path_nds_nodot_www

def del_dot(path):
    name = path.split('/')[-1].split('.')[0].split('_')[-1]
    name_out = 'filtered_nodot_'+name+'.csv'
    
    if os.path.exists(name_out):
        os.remove(name_out)
        print('old filtered_nodot removed')
    with open(path) as csv_in, open(name_out, mode='a') as csv_out:
        lines = csv_in.readlines()
        writer = csv.writer(csv_out)
        for line in lines:
            if '\n' in line:
                line = line.strip('\n')
                writer.writerow([line[:-1]])
            else:
                writer.writerow([line[:-1]])
    return name_out
    
        
            
    

'''
tale funzione ha l'obiettivo di calcoalre due intersezioni:
    - da i nomi di dominio ottenuti dal file O.I umbrella ricavati con le funzioni precedenti, vedere quali sono
    presenti nel file base top1m umbrella e quali no
L'obiettivo è quello di trovare il giorno di OI da cui ricavare i stessi nomi di dominio presenti nel file top1M
'''



def diff_ldn_oi2(list_names_byoi, list_polito, names_folder_out):
    oi_nds_nodotwww = list_names_byoi
    name = oi_nds_nodotwww.split('/')[-1].split('.')[0].split('_')[-1]
    polito_list = list_polito
    name_folder_inter = names_folder_out
    
    #ricaviamo le liste di dominio dai due file
    oi_l = []
    with open(list_names_byoi) as oi_csv:
        lines = oi_csv.readlines()
        for line in lines:
            if '\n' in line:
                oi_l.append(line.strip('\n'))
            else:
                oi_l.append(line)
    print('len list oi:',str(len(oi_l)))
    
    campus_l = []
    with open(list_polito) as campus_csv:
        lines = campus_csv.readlines()
        for line in lines:
            if '\n' in line:
                line = line.strip('\n')
                if 'www8.' in line:
                    campus_l.append(line.replace('www8.',''))
                elif 'www4.' in line:
                    campus_l.append(line.replace('www4.',''))
                elif 'www3.' in line:
                    campus_l.append(line.replace('www3.',''))
                elif 'www2.' in line:
                    campus_l.append(line.replace('www2.',''))
                elif 'www.' in line:
                    campus_l.append(line.replace('www.',''))
                else:
                    campus_l.append(line)
            else:
                if 'www8.' in line:
                    campus_l.append(line.replace('www8.',''))
                elif 'www4.' in line:
                    campus_l.append(line.replace('www4.',''))
                elif 'www3.' in line:
                    campus_l.append(line.replace('www3.',''))
                elif 'www2.' in line:
                    campus_l.append(line.replace('www2.',''))
                elif 'www.' in line:
                    campus_l.append(line.replace('www.',''))
                else:
                    campus_l.append(line)
    print('len list campus:',str(len(campus_l)))
    
    if os.path.exists(names_folder_out):
        print('folder:' + names_folder_out+', already exists')
        folder_inter = names_folder_out
    else:
        print('no ' + names_folder_out+' exists, creating it..')
        os.mkdir(names_folder_out)
        folder_inter = names_folder_out
    
    csv_inter1 = 'inter1_ldn_oi_'+name+'.csv'
    csv_inter2 = 'inter2_ldn_oi_'+name+'.csv'
    
    if os.path.exists(folder_inter+'/'+csv_inter1):
        print(" old 'inter1_ldn_oi_"+name+".csv' removed")
        os.remove(folder_inter+'/inter1_ldn_oi_'+name+'.csv')
    else:
        print(" no old'inter1_ldn_oi_"+name+".csv' exists")

    if os.path.exists(folder_inter +'/'+csv_inter2):
        print(" old 'inter2_ldn_oi_"+name+".csv' removed")
        os.remove(folder_inter+'/inter2_ldn_oi_'+name+'.csv')
    else:
        print(" no old'inter2_ldn_oi_"+name+".csv' exists")
        
    path_csv_inter1 = os.path.join(name_folder_inter,csv_inter1)
    path_csv_inter2 = os.path.join(name_folder_inter,csv_inter2)
    print(path_csv_inter1)
    print(path_csv_inter2)
    
    count_in = 0
    count_out = 0 
    start = datetime.now()
    with open(path_csv_inter1, mode='a') as csv_inter_in, open(path_csv_inter2, mode='a') as csv_inter_out:
        print('csv inter 1 and inter 2 opened')
        writer_in = csv.writer(csv_inter_in)
        writer_out = csv.writer(csv_inter_out)
        for dom in campus_l:
            if dom in oi_l:
                count_in += 1
                writer_in.writerow([dom])
            else:
                count_out += 1
                writer_out.writerow([dom])
    end = datetime.now()
    print("end int 1, duration : {}".format(end - start))
    print('in intersection:', str(count_in))
    print('not in intersection:', str(count_out))            
           
        
        
def diff_ldn_oi(list_names_byoi, list_polito, names_folder_out):
    """
    Args:
        -list_names_byoi: lista di domni di open intel seza dot e senza www
        -list_polito: lista di domini originale
        -names_folder_out: nome della cartella dove salvare i file di intersezione
    """
    #path_nds_nodot_www_09_01_20 OPENINTEL
    #path_nds_nodot_nowww_04
    path_nds_nodot_www = list_names_byoi
    name = path_nds_nodot_www.split('/')[-1].split('.')[0].split('_')[-1]
    polito_nds_txt = list_polito
    name_folder_inter = names_folder_out
    
    if os.path.exists(names_folder_out):
        print('folder:' + names_folder_out+', already exists')
        folder_inter = names_folder_out
    else:
        print('no ' + names_folder_out+' exists, creating it..')
        os.mkdir(names_folder_out)
        folder_inter = names_folder_out
    
    csv_inter1 = 'inter1_ldn_oi_'+name+'.csv'
    csv_inter2 = 'inter2_ldn_oi_'+name+'.csv'
    
    if os.path.exists(folder_inter+'/'+csv_inter1):
        print(" old 'inter1_ldn_oi_"+name+".csv' removed")
        os.remove(folder_inter+'/inter1_ldn_oi_'+name+'.csv')
    else:
        print(" no old'inter1_ldn_oi_"+name+".csv' exists")

    if os.path.exists(folder_inter +'/'+csv_inter2):
        print(" old 'inter2_ldn_oi_"+name+".csv' removed")
        os.remove(folder_inter+'/inter2_ldn_oi_'+name+'.csv')
    else:
        print(" no old'inter2_ldn_oi_"+name+".csv' exists")
        
    path_csv_inter1 = os.path.join(name_folder_inter,csv_inter1)
    path_csv_inter2 = os.path.join(name_folder_inter,csv_inter2)
    print(path_csv_inter1)
    print(path_csv_inter2)
    
    with open(path_nds_nodot_www) as oi_filtered, open(polito_nds_txt) as pol_nodot_nowww, open(path_csv_inter1, mode='a') as inter1_out, open(path_csv_inter2, mode='a') as inter2_out:
        writer_inter1 = csv.writer(inter1_out)
        writer_inter2 = csv.writer(inter2_out)
        df_oi = pd.read_csv(oi_filtered).values.tolist()
        #df_pol_dot_www = pd.read_csv(pol_nodot_nowww).values.tolist()
        df_pol_dot_www = []
        lines = pol_nodot_nowww.readlines()
        for line in lines:
            if '\n' in line:
                #list_domain_name.append(line.strip('\n'))
                line = line.strip('\n')
                if 'www8.' in line:
                    line = line.replace('www8.','')
                    df_pol_dot_www.append(line)
                elif 'www2.' in line:
                    line = line.replace('www2.','')
                    df_pol_dot_www.append(line)
                    #aggiungere gestione che toglie il 'www.', 'www2.', 'www8.'.. anche sotto
                elif 'www3.' in line:
                    line = line.replace('www3.','')
                    df_pol_dot_www.append(line)
                elif 'www.' in line:
                    line = line.replace('www.','')
                    df_pol_dot_www.append(line)
                else:
                    #print('[debug]probably no new case')
                    df_pol_dot_www.append(line)
            else:
                if 'www8.' in line:
                    line = line.replace('www8.','')
                    df_pol_dot_www.append(line)
                elif 'www2.' in line:
                    line = line.replace('www2.','')
                    df_pol_dot_www.append(line)
                    #aggiungere gestione che toglie il 'www.', 'www2.', 'www8.'.. anche sotto
                elif 'www3.' in line:
                    line = line.replace('www3.','')
                    df_pol_dot_www.append(line)
                elif 'www.' in line:
                    line = line.replace('www.','')
                    df_pol_dot_www.append(line)
                else:
                    #print('[debug]2 probably no new case ')
                    df_pol_dot_www.append(line)
        
        df_oi_l = []
        first = True
        for x in df_oi:
            if first:
                print(x)
                first = False
            x = x[0].replace("['",'')
            x = x.replace("']",'')
            df_oi_l.append(x)
                
                
        print("start int1")
        start1 = datetime.now()
        #valori di polito che sono presenti in oi
        #inter_1 = [value for value in df_pol_dot_www if value in df_oi_l]
        count = 0
        for value in df_pol_dot_www:
            if value in df_oi_l:
                count += 1
                writer_inter1.writerow(value)
        end1 = datetime.now()
        print("end int 1, duration : {}".format(end1 - start1))
        print(count)
        '''
        count_int1 = 0
        for x in inter_1:
            count_int1 += 1
            writer_inter1.writerow([x])
        print("len intersection 1: " + str(count_int1))
        '''
        
        '''
        print("start int2")
        start2 = datetime.now()
        inter_2 = [value for value in df_pol_dot_www if value not in inter_1]
        end2 = datetime.now()
        print("end int2 , duration : {}".format(end2 - start2))
        count_int2 = 0
        for x in inter_2:
            count_int2 += 1
            writer_inter2.writerow([x])
            
        '''
        print("start int2")
        start2 = datetime.now()
        #valori di polito non presenti in oi
        #inter_2 = [value for value in df_pol_dot_www if value not in df_oi_l]
        count2= 0
        for value in df_pol_dot_www:
            if value not in df_oi_l:
                count2 += 1
                writer_inter2.writerow(value)
        end2 = datetime.now()
        print(count2)
        print("end int2 , duration : {}".format(end2 - start2))
        
        '''
        count_int2 = 0
        for x in inter_2:
            count_int2 += 1
            writer_inter2.writerow([x])
        print("len intersection 2: " + str(count_int2))
        '''

def main(pf,pdl,nfi):
    '''
    path_nodotnoww = sys.argv[1]
    path_list = sys.argv[2]
    namefolder_intersec = sys.argv[3]
    diff_ldn_oi2(path_nodotnoww,path_list,namefolder_intersec)    
    
    '''
    #fodler that contains csv
    path_folder = pf
    #txt
    path_domlsit = pdl
    #namefolder where save csv of intersection
    path_namefolder_intersec = nfi
    csv_out = dom_name_by_OI(path_folder)

    #nodot_nowww = del_www(no_dot)
    csv_filt = del_dot(csv_out)
    diff_ldn_oi2(csv_filt,path_domlsit,path_namefolder_intersec)
    
    
    '''
    path_nodotnoww = sys.argv[1]
    path_list = sys.argv[2]
    namefolder_intersec = sys.argv[3]
    diff_ldn_oi2(path_nodotnoww,path_list,namefolder_intersec)
    
    '''
    
    
    '''
    path_fold = sys.argv[1]
    dom_name_by_OI(path_fold)
    '''
    '''
    list = sys.argv[1]
    del_www(list)
    
    listoi= sys.argv[1]
    listcamptxt = sys.argv[2]
    name= sys.argv[3]
    diff_ldn_oi(listoi,listcamptxt,name)
    '''

if __name__ == '__main__':
    #fodler that contains csv
    path_folder = sys.argv[1]
    #txt
    path_domlsit = sys.argv[2]
    #namefolder where save csv of intersection
    path_namefolder_intersec = sys.argv[3]
    main(path_folder,path_domlsit,path_namefolder_intersec)