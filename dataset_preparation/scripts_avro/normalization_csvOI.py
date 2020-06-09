#!/usr/bin/env python3
import pandas as pd
import csv
import gc
import statistics as sts
from datetime import datetime
from progress.bar import Bar
import os
import sys
import json

'''
path_csv_oi = '/Volumes/SSD_PEPPE/polito_py/OI_inter1_polito_090120_tld.csv'
path_domain_names = '/Volumes/SSD_PEPPE/polito_py/campus_top_10k_mod_dot.csv'
'''
def normalization(pco,pdn,s):
    #path del csv dei dati ricavati da open intel e in forma sparsa
    path_csv_oi = pco
    #path file csv campus
    path_domain_names = pdn
    '''flag per vedere se si vuole estrarre il feature set:
        - 1: feature set e csv normalizzato
        - 0: solo feature set 
    '''
    set_out = s 

    count = 0
    max_A = max_AAAA = max_TXT = max_MX = max_SOA = max_DS = max_NS = max_DNSKEY = max_CAA = max_CDS = max_CDNSKEY = max_NSEC3PARAM = max_NSEC3 = 0
    dom_A = dom_AAAA = dom_TXT = dom_MX = dom_SOA = dom_DS = dom_NS = dom_DNSKEY = dom_CAA = dom_CDS = dom_CDNSKEY = dom_NSEC3PARAM = dom_NSEC3 = ''

    '''
    mod = sys.argv[1]
    if int(mod) == 0:
        print('DEGUB MODE: CSV IN SCRIPT')
    elif int(mod) == 1:
        path_csv_oi = sys.argv[2]
        path_domain_names = sys.argv[3]
        print('CSV PASSED')
    else:
        print('ARGV MODE MISSED')
        exit()
    '''

    if os.path.isfile('normalization.csv'):
        os.remove('normalization.csv')
        print('OLD CSV REMOVED')
    else:
        print('NO OLD CSV EXISTS')

    ################################################ list of domain names #####################################################
    doms = []
    with open(path_domain_names) as dn:
        lines = dn.readlines()
        for line in lines:
            if '\n'  in line:
                line = line.strip('\n')
                if 'www8.' in line:
                    line = line.replace('www8.','')
                    doms.append(line)
                elif 'www3.' in line:
                    line = line.replace('www3.','')
                    doms.append(line)
                elif 'www4.' in line:
                    line = line.replace('www4.','')
                    doms.append(line)
                elif 'www2.' in line:
                    line = line.replace('www2.','')
                    doms.append(line)
                elif 'www.' in line:
                    line = line.replace('www.','')
                    doms.append(line)
                else:
                    doms.append(line)
                    #print('new case: ' + str(line))
            else:
                if 'www8.' in line:
                    line = line.replace('www8.','')
                    doms.append(line)
                elif 'www4.' in line:
                    line = line.replace('www4.','')
                    doms.append(line)
                elif 'www3.' in line:
                    line = line.replace('www3.','')
                    doms.append(line)
                elif 'www2.' in line:
                    line = line.replace('www2.','')
                    doms.append(line)
                elif 'www.' in line:
                    line = line.replace('www.','')
                    doms.append(line)
                else:
                    doms.append(line)
                    #print('new case: ' + str(line))
                
            


    '''        
    ####################################################################################################
    - creation of dictionary:
    {
        'domain_name1':{
            'A':[['ip1','as1',...,''],['ip2','as2',....'']....],   
            'AAAA':...
            ...
            ...
            'TXT':....
            
        },
        {'domain_name1_www':{
            ...
        }
        },
        .
        .
        .
        'domain_name_n':{
            ......
        }
    }
    '''
    start = datetime.now()
    df_origin = pd.read_csv(path_csv_oi, delimiter=',',low_memory=False, index_col='query_name')
    #df_origin = df_origin.set_index('query_name')
  

    '''
    df_temp = df_orig.loc['ns-tel1.qq.com.']

    for index,row in df_temp.iterrows():
        print(type(row))
        print(row['query_type'])

    print(df_temp)
    '''
    print('----------------------------DICT CREATION----------------------------\n')
    dict_info = {}
    count_key = 0
    df_group = df_origin.groupby('query_name',sort=False)

    bar_dict = Bar('Dict Creation',max=df_group.ngroups)
    for key,item in df_group:
        count_key += 1
        dict_info[key] = {
            'A': [],
            'A_CNAME': [],
            'A_RRSIG': [],
            'A_DNAME':[],
            'AAAA': [],
            'AAAA_CNAME': [],
            'AAAA_RRSIG': [],
            'AAAA_DNAME':[],
            'MX': [],
            'MX_CNAME': [],
            'MX_RRSIG': [],
            'MX_MXHASH': [],
            'MX_DNAME':[],
            'TXT': [],
            'TXT_CNAME': [],
            'TXT_RRSIG': [],
            'TXT_TXTHASH': [],
            'TXT_DNAME':[],
            'SOA': [],
            'SOA_CNAME': [],
            'SOA_RRSIG': [],
            'SOA_DNAME':[],
            'NS': [],
            'NS_CNAME': [],
            'NS_RRSIG': [],
            'NS_NSHASH': [],
            'NS_DNAME':[],
            'DS': [],
            'DS_CNAME': [],
            'DS_RRSIG': [],
            'DS_DNAME':[],
            'DNSKEY': [],
            'DNSKEY_CNAME': [],
            'DNSKEY_RRSIG': [],
            'DNSKEY_DNAME':[],
            'CAA': [],
            'CAA_CNAME': [],
            'CAA_RRSIG': [],
            'CAA_DNAME':[],
            'CDS': [],
            'CDS_CNAME': [],
            'CDS_RRSIG': [],
            'CDNSKEY': [],
            'CDNSKEY_CNAME': [],
            'CDNSKEY_RRSIG': [],
            'NSEC3PARAM': [],
            'NSEC3PARAM_RRSIG': [],
            'NSEC3': [],
            'AFSDB_RRSIG': []
        }
        #print(df_group.get_group(key), "\n\n")

        df_sel_info = df_group.get_group(key).query('query_type==response_type')
        #print(df_sel_info)
        for index,row in df_sel_info.iterrows():
            vect_inf = []
            if row['query_type'] == 'A':
                if len(str(row['ip4_address'])) != 0:
                    #print('indirizzo: ' + str(row['ip4_address']))
                    vect_inf.append(str(row['ip4_address']))
                else:
                    vect_inf.append('')         
                if len(str(row['response_ttl'])) != 0:
                    vect_inf.append(str(row['response_ttl']))
                else:
                    vect_inf.append('')
                if len(str(row['worker_id'])) != 0:
                    vect_inf.append(str(row['worker_id']))
                else:
                    vect_inf.append('')
                if len(str(row['rtt'])) != 0:
                    vect_inf.append(str(row['rtt']))
                else:
                    vect_inf.append('')
                if len(str(row['status_code'])) != 0:
                    vect_inf.append(str(row['status_code']))
                else:
                    vect_inf.append('')       
                if len(str(row['as'])) != 0 :
                    if str(row['as']) == '--':
                        vect_inf.append('null')
                    else:
                        vect_inf.append(str(row['as']))
                else:
                    vect_inf.append('')
                if len(str(row['as_full'])) != 0:
                    if str(row['as_full']) == '[--]':
                        vect_inf.append('null')
                    else:
                        vect_inf.append(str(row['as_full']))
                else:
                    vect_inf.append('')  
                if len(row['country']) != 0:
                    if row['country'] == '-':
                        vect_inf.append('null')
                    else:
                        vect_inf.append(row['country'])
                else:
                    vect_inf.append('')  
                if len(str(row['ip_prefix'])) != 0:
                    if str(row['ip_prefix']) == '--':
                        vect_inf.append('null')
                    else:
                        vect_inf.append(str(row['ip_prefix']))
                else:
                    vect_inf.append('')  
                dict_info[key]['A'].extend([vect_inf])
            else:
                dict_info[key]['A'].extend([])
                
            if row['query_type'] == 'AAAA':
                if len(str(row['ip6_address'])) !=0:
                    vect_inf.append(str(row['ip6_address']))
                else:
                    vect_inf.append('')
                if len(str(row['response_ttl'])) != 0:
                    vect_inf.append(str(row['response_ttl']))
                else:
                    vect_inf.append('')
                if len(str(row['worker_id'])) != 0:
                    vect_inf.append(str(row['worker_id']))
                else:
                    vect_inf.append('')
                if len(str(row['rtt'])) != 0:
                    vect_inf.append(str(row['rtt']))
                else:
                    vect_inf.append('')
                if len(str(row['status_code'])) != 0:
                    vect_inf.append(str(row['status_code']))
                else:
                    vect_inf.append('')
                if len(str(row['as'])) != 0 :
                    if str(row['as']) == '--':
                        vect_inf.append('null')
                    else:
                        vect_inf.append(str(row['as']))
                else:
                    vect_inf.append('')
                if len(str(row['as_full'])) != 0:
                    if str(row['as_full']) == '[--]':
                        vect_inf.append('null')
                    else:
                        vect_inf.append(str(row['as_full']))
                else:
                    vect_inf.append('')  
                if len(row['country']) != 0:
                    if row['country'] == '-':
                        vect_inf.append('null')
                    else:
                        vect_inf.append(row['country'])
                else:
                    vect_inf.append('')  
                if len(str(row['ip_prefix'])) != 0:
                    if str(row['ip_prefix']) == '--':
                        vect_inf.append('null')
                    else:
                        vect_inf.append(str(row['ip_prefix']))
                else:
                    vect_inf.append('') 
                dict_info[key]['AAAA'].extend([vect_inf])
            else:
                dict_info[key]['AAAA'].extend([])
                
            if row['query_type'] == 'MX':
                if len(str(row['mx_address'])) != 0:
                    vect_inf.append(str(row['mx_address']))
                else:
                    vect_inf.append('')
                if len(str(row['mx_preference'])) != 0:
                    vect_inf.append(str(row['mx_preference'])) != 0
                else:
                    vect_inf.append('')
                if len(str(row['response_ttl'])) != 0:
                    vect_inf.append(str(row['response_ttl']))
                else:
                    vect_inf.append('')
                if len(str(row['worker_id'])) != 0:
                    vect_inf.append(str(row['worker_id']))
                else:
                    vect_inf.append('')
                if len(str(row['rtt'])) != 0:
                    vect_inf.append(str(row['rtt']))
                else:
                    vect_inf.append('')
                if len(str(row['status_code'])) != 0:
                    vect_inf.append(str(row['status_code']))
                else:
                    vect_inf.append('')
                dict_info[key]['MX'].extend([vect_inf])
            else:
                dict_info[key]['MX'].extend([])
                
            if row['query_type'] == 'TXT':
                if len(str(row['txt_text'])) != 0:
                    if str(row['txt_text'])[-1] != '"':
                        text = str(row['txt_text']).replace(" ","")
                        text = "[" + text + "]"
                        vect_inf.append(str(text))
                    else:
                        text = "[" + str(row['txt_text']) + "]"
                        vect_inf.append(str(text))
                else:
                    vect_inf.append('')
                if len(str(row['response_ttl'])) != 0:
                    vect_inf.append(str(row['response_ttl']))
                else:
                    vect_inf.append('')
                if len(str(row['worker_id'])) != 0:
                    vect_inf.append(str(row['worker_id']))
                else:
                    vect_inf.append('')
                if len(str(row['rtt'])) != 0:
                    vect_inf.append(str(row['rtt']))
                else:
                    vect_inf.append('')
                if len(str(row['status_code'])) != 0:
                    vect_inf.append(str(row['status_code']))
                else:
                    vect_inf.append('')
                dict_info[key]['TXT'].extend([vect_inf])
            else:
                dict_info[key]['TXT'].extend([])
                
            if row['query_type'] == 'SOA':
                if len(str(row['soa_mname'])) != 0:
                    vect_inf.append(str(row['soa_mname']))
                else:
                    vect_inf.append('')
                if len(str(row['soa_rname'])) != 0:
                    vect_inf.append(str(row['soa_rname']))
                else:
                    vect_inf.append('')
                if len(str(row['soa_serial'])) != 0:
                    vect_inf.append(str(row['soa_serial']))
                else:
                    vect_inf.append('')
                if len(str(row['soa_refresh'])) != 0:
                    vect_inf.append(str(row['soa_refresh']))
                else:
                    vect_inf.append('')
                if len(str(row['soa_retry'])) != 0:
                    vect_inf.append(str(row['soa_retry']))
                else:
                    vect_inf.append('')
                if len(str(row['soa_expire'])) != 0:
                    vect_inf.append(str(row['soa_expire']))
                else:
                    vect_inf.append('')
                if len(str(row['soa_minimum'])) != 0:
                    vect_inf.append(str(row['soa_minimum']))
                else:
                    vect_inf.append('')
                if len(str(row['response_ttl'])) != 0:
                    vect_inf.append(str(row['response_ttl']))
                else:
                    vect_inf.append('')
                if len(str(row['worker_id'])) != 0:
                    vect_inf.append(str(row['worker_id']))
                else:
                    vect_inf.append('')
                if len(str(row['rtt'])) != 0:
                    vect_inf.append(str(row['rtt']))
                else:
                    vect_inf.append('')
                if len(str(row['status_code'])) != 0:
                    vect_inf.append(str(row['status_code']))
                else:
                    vect_inf.append('')
                dict_info[key]['SOA'].extend([vect_inf])
            else:
                dict_info[key]['SOA'].extend([])
            
            if row['query_type'] == 'NS':
                if len(str(row['ns_address'])) != 0:
                    vect_inf.append(str(row['ns_address']))
                else:
                    vect_inf.append('')
                if len(str(row['response_ttl'])) != 0:
                    vect_inf.append(str(row['response_ttl']))
                else:
                    vect_inf.append('')
                if len(str(row['worker_id'])) != 0:
                    vect_inf.append(str(row['worker_id']))
                else:
                    vect_inf.append('')
                if len(str(row['rtt'])) != 0:
                    vect_inf.append(str(row['rtt']))
                else:
                    vect_inf.append('')
                if len(str(row['status_code'])) != 0:
                    vect_inf.append(str(row['status_code']))
                else:
                    vect_inf.append('')
                dict_info[key]['NS'].extend([vect_inf])
            else:
                dict_info[key]['NS'].extend([])
                
            if row['query_type'] == 'DS':
                if len(str(row['ds_key_tag'])) != 0:
                    vect_inf.append(str(row['ds_key_tag']))
                else:
                    vect_inf.append('')
                if len(str(row['ds_algorithm'])) != 0:
                    vect_inf.append(str(row['ds_algorithm']))
                else:
                    vect_inf.append('')
                if len(str(row['ds_digest_type'])) != 0:
                    vect_inf.append(str(row['ds_digest_type']))
                else:
                    vect_inf.append('')
                if len(str(row['ds_digest'])) != 0:
                    vect_inf.append(str(row['ds_digest']))
                else:
                    vect_inf.append('')
                if len(str(row['response_ttl'])) != 0:
                    vect_inf.append(str(row['response_ttl']))
                else:
                    vect_inf.append('')
                if len(str(row['worker_id'])) != 0:
                    vect_inf.append(str(row['worker_id']))
                else:
                    vect_inf.append('')
                if len(str(row['rtt'])) != 0:
                    vect_inf.append(str(row['rtt']))
                else:
                    vect_inf.append('')
                if len(str(row['status_code'])) != 0:
                    vect_inf.append(str(row['status_code']))
                else:
                    vect_inf.append('')
                dict_info[key]['DS'].extend([vect_inf])
            else:
                dict_info[key]['DS'].extend([])
            
            if row['query_type'] == 'DNSKEY':
                if len(str(row['dnskey_flags'])) != 0:
                    vect_inf.append(str(row['dnskey_flags']))
                else:
                    vect_inf.append('')
                if len(str(row['dnskey_protocol'])) != 0:
                    vect_inf.append(str(row['dnskey_protocol']))
                else:
                    vect_inf.append('')
                if len(str(row['dnskey_algorithm'])) != 0:
                    vect_inf.append(str(row['dnskey_algorithm']))
                else:
                    vect_inf.append('')
                if len(str(row['dnskey_pk_eccgost_x'])) != 0:
                    vect_inf.append(str(row['dnskey_pk_eccgost_x']))
                else:
                    vect_inf.append('')
                if len(str(row['response_ttl'])) != 0:
                    vect_inf.append(str(row['response_ttl']))
                else:
                    vect_inf.append('')
                if len(str(row['worker_id'])) != 0:
                    vect_inf.append(str(row['worker_id']))
                else:
                    vect_inf.append('')
                if len(str(row['rtt'])) != 0:
                    vect_inf.append(str(row['rtt']))
                else:
                    vect_inf.append('')
                if len(str(row['status_code'])) != 0:
                    vect_inf.append(str(row['status_code']))
                else:
                    vect_inf.append('')
                dict_info[key]['DNSKEY'].extend([vect_inf])
            else:
                dict_info[key]['DNSKEY'].extend([])
            
            if row['query_type'] == 'CAA':
                if len(str(row['caa_flags'])) != 0:
                    vect_inf.append(str(row['caa_flags']))
                else:
                    vect_inf.append('')
                if len(str(row['caa_tag'])) != 0:
                    vect_inf.append(str(row['caa_tag']))
                else:
                    vect_inf.append('')
                if len(str(row['caa_value'])) != 0:
                    vect_inf.append(str(row['caa_value']))
                else:
                    vect_inf.append('')
                if len(str(row['response_ttl'])) != 0:
                    vect_inf.append(str(row['response_ttl']))
                else:
                    vect_inf.append('')
                if len(str(row['worker_id'])) != 0:
                    vect_inf.append(str(row['worker_id']))
                else:
                    vect_inf.append('')
                if len(str(row['rtt'])) != 0:
                    vect_inf.append(str(row['rtt']))
                else:
                    vect_inf.append('')
                if len(str(row['status_code'])) != 0:
                    vect_inf.append(str(row['status_code']))
                else:
                    vect_inf.append('')
                dict_info[key]['CAA'].extend([vect_inf])
            else:
                dict_info[key]['CAA'].extend([])
            
            if row['query_type'] == 'CDS':
                if len(str(row['cds_key_tag'])) != 0:
                    vect_inf.append(str(row['cds_key_tag']))
                else:
                    vect_inf.append('')
                if len(str(row['cds_algorithm'])) != 0:
                    vect_inf.append(str(row['cds_algorithm']))
                else:
                    vect_inf.append('')
                if len(str(row['cds_digest_type'])) != 0:
                    vect_inf.append(str(row['cds_digest_type']))
                else:
                    vect_inf.append('')
                if len(str(row['cds_digest'])) != 0:
                    vect_inf.append(str(row['cds_digest']))
                else:
                    vect_inf.append('')
                if len(str(row['response_ttl'])) != 0:
                    vect_inf.append(str(row['response_ttl']))
                else:
                    vect_inf.append('')
                if len(str(row['worker_id'])) != 0:
                    vect_inf.append(str(row['worker_id']))
                else:
                    vect_inf.append('')
                if len(str(row['rtt'])) != 0:
                    vect_inf.append(str(row['rtt']))
                else:
                    vect_inf.append('')
                if len(str(row['status_code'])) != 0:
                    vect_inf.append(str(row['status_code']))
                else:
                    vect_inf.append('')
                dict_info[key]['CDS'].extend([vect_inf])
            else:
                dict_info[key]['CDS'].extend([])
            
            if row['query_type'] == 'CDNSKEY':
                if len(str(row['cdnskey_flags'])) != 0:
                    vect_inf.append(str(row['cdnskey_flags']))
                else:
                    vect_inf.append('')
                if len(str(row['cdnskey_protocol'])) != 0:
                    vect_inf.append(str(row['cdnskey_protocol']))
                else:
                    vect_inf.append('')
                if len(str(row['cdnskey_algorithm'])) != 0:
                    vect_inf.append(str(row['cdnskey_algorithm']))
                else:
                    vect_inf.append('')
                if len(str(row['cdnskey_pk_eccgost_x'])) != 0:
                    vect_inf.append(str(row['cdnskey_pk_eccgost_x']))
                else:
                    vect_inf.append('')
                if len(str(row['response_ttl'])) != 0:
                    vect_inf.append(str(row['response_ttl']))
                else:
                    vect_inf.append('')
                if len(str(row['worker_id'])) != 0:
                    vect_inf.append(str(row['worker_id']))
                else:
                    vect_inf.append('')
                if len(str(row['rtt'])) != 0:
                    vect_inf.append(str(row['rtt']))
                else:
                    vect_inf.append('')
                if len(str(row['status_code'])) != 0:
                    vect_inf.append(str(row['status_code']))
                else:
                    vect_inf.append('')
                dict_info[key]['CDNSKEY'].extend([vect_inf])
            else:
                dict_info[key]['CDNSKEY'].extend([])
                
            if row['query_type'] == 'NSEC3PARAM':
                if len(str(row['nsec3param_hash_algorithm'])) != 0:
                    vect_inf.append(str(row['nsec3param_hash_algorithm']))
                else:
                    vect_inf.append('')
                if len(str(row['nsec3param_flags'])) != 0:
                    vect_inf.append(str(row['nsec3param_flags']))
                else:
                    vect_inf.append('')
                if len(str(row['nsec3param_iterations'])) != 0:
                    vect_inf.append(str(row['nsec3param_iterations']))
                else:
                    vect_inf.append('')
                if len(str(row['nsec3param_salt'])) != 0:
                    vect_inf.append(str(row['nsec3param_salt']))
                else:
                    vect_inf.append('')
                if len(str(row['response_ttl'])) != 0:
                    vect_inf.append(str(row['response_ttl']))
                else:
                    vect_inf.append('')
                if len(str(row['worker_id'])) != 0:
                    vect_inf.append(str(row['worker_id']))
                else:
                    vect_inf.append('')
                if len(str(row['rtt'])) != 0:
                    vect_inf.append(str(row['rtt']))
                else:
                    vect_inf.append('')
                if len(str(row['status_code'])) != 0:
                    vect_inf.append(str(row['status_code']))
                else:
                    vect_inf.append('')
                dict_info[key]['NSEC3PARAM'].extend([vect_inf])
            else:
                dict_info[key]['NSEC3PARAM'].extend([])
                        
            if row['query_type'] == 'NSEC3':
                if len(str(row['nsec3_hash_algorithm'])) != 0:
                    vect_inf.append(str(row['nsec3_hash_algorithm']))
                else:
                    vect_inf.append('')
                if len(str(row['nsec3_flags'])) != 0:
                    vect_inf.append(str(row['nsec3_flags']))
                else:
                    vect_inf.append('')
                if len(str(row['nsec3_iterations'])) != 0:
                    vect_inf.append(str(row['nsec3_iterations']))
                else:
                    vect_inf.append('')
                if len(str(row['nsec3_salt'])) != 0:
                    vect_inf.append(str(row['nsec3_salt']))
                else:
                    vect_inf.append('')
                if len(str(row['nsec3_next_domain_name_hash'])) != 0:
                    vect_inf.append(str(row['nsec3_next_domain_name_hash']))
                else:
                    vect_inf.append('')
                if len(str(row['nsec3_owner_rrset_types'])) != 0:
                    vect_inf.append(str(row['nsec3_owner_rrset_types']))
                else:
                    vect_inf.append('')
                if len(str(row['response_name'])) != 0:
                    vect_inf.append(str(row['response_name']))
                else:
                    vect_inf.append('')
                if len(str(row['response_ttl'])) != 0:
                    vect_inf.append(str(row['response_ttl']))
                else:
                    vect_inf.append('')
                if len(str(row['worker_id'])) != 0:
                    vect_inf.append(str(row['worker_id']))
                else:
                    vect_inf.append('')
                if len(str(row['rtt'])) != 0:
                    vect_inf.append(str(row['rtt']))
                else:
                    vect_inf.append('')
                if len(str(row['status_code'])) != 0:
                    vect_inf.append(str(row['status_code']))
                dict_info[key]['NSEC3'].extend([vect_inf])
            else:
                dict_info[key]['NSEC3'].extend([])
                

                
        bar_dict.next()   
        """        
        if count_key == 5:
            break
        """
        """
        for k,v in dict_info.items():
        print(k,v)        
        print('\n')
    """
    bar_dict.finish()          
                


    ####################################### MAX VAL #############################################
    print('----------------------------CALC MAX VAL----------------------------\n\n')
    doms_oi = []
    bar_m = Bar('Calc Max',max=df_group.ngroups)
    for key,item in df_group:
        doms_oi.append(key)
        count += 1
        #print(key +'\n')
        #print(df_group.get_group(key), "\n\n")
        df_group_sel = df_group.get_group(key).query('query_type==response_type').groupby('query_type').size().\
            reset_index(name='query_type_counts')
        #print(df_group.get_group(key).query('query_type==response_type'))
        for index,row in df_group_sel.iterrows():
            #print(str(row['query_type']) +',' + str(row['query_type_counts']))
            if row['query_type'] == 'A':
                temp_A_count = int(row['query_type_counts'])
                if temp_A_count > max_A:
                    max_A = temp_A_count
                    dom_A = str(key)
            elif row['query_type'] == 'AAAA':
                temp_AAAA_count = int(row['query_type_counts'])
                if temp_AAAA_count > max_AAAA:
                    max_AAAA = temp_AAAA_count
                    dom_AAAA = str(key)
            elif row['query_type'] == 'TXT':
                temp_TXT_count = int(row['query_type_counts'])
                if temp_TXT_count > max_TXT:
                    max_TXT = temp_TXT_count
                    dom_TXT = str(key)
            elif row['query_type'] == 'MX':
                temp_MX_count = int(row['query_type_counts'])
                if temp_MX_count > max_MX:
                    max_MX = temp_MX_count
                    dom_MX = str(key)
            elif row['query_type'] == 'SOA':
                temp_SOA_count = int(row['query_type_counts'])
                if temp_SOA_count > max_SOA:
                    max_SOA = temp_SOA_count
                    dom_SOA = str(key)
            elif row['query_type'] == 'DS':
                temp_DS_count = int(row['query_type_counts'])
                if temp_DS_count > max_DS:
                    max_DS = temp_DS_count
                    dom_DS = str(key)
            elif row['query_type'] == 'NS':
                temp_NS_count = int(row['query_type_counts'])
                if temp_NS_count > max_NS:
                    max_NS = temp_NS_count
                    dom_NS = str(key)
            elif row['query_type'] == 'DNSKEY':
                temp_DNSKEY_count = int(row['query_type_counts'])
                if temp_DNSKEY_count > max_DNSKEY:
                    max_DNSKEY = temp_DNSKEY_count
                    dom_DNSKEY = str(key)
            elif row['query_type'] == 'CAA':
                temp_CAA_count = int(row['query_type_counts'])
                if temp_CAA_count > max_CAA:
                    max_CAA = temp_CAA_count
                    dom_CAA = str(key)
            elif row['query_type'] == 'CDS':
                temp_CDS_count = int(row['query_type_counts'])
                if temp_CDS_count > max_CDS:
                    max_CDS = temp_CDS_count
                    dom_CDS = str(key)
            elif row['query_type'] == 'CDNSKEY':
                temp_CDNSKEY_count = int(row['query_type_counts'])
                if temp_CDNSKEY_count > max_CDNSKEY:
                    max_CDNSKEY = temp_CDNSKEY_count
                    dom_CDNSKEY = str(key)
            elif row['query_type'] == 'NSEC3PARAM':
                temp_NSEC3PARAM_count = int(row['query_type_counts'])
                if temp_NSEC3PARAM_count > max_NSEC3PARAM:
                    max_NSEC3PARAM = temp_NSEC3PARAM_count
                    dom_NSEC3PARAM = str(key)
            elif row['query_type'] == 'NSEC3':
                temp_NSEC3_count = int(row['query_type_counts'])
                if temp_NSEC3_count == 0:
                    max_NSEC3 = 0
                    dom_NSEC3 = ''
                else:
                    if temp_NSEC3_count > max_NSEC3:
                        max_NSEC3 = temp_NSEC3_count
                        dom_NSEC3 = str(key)
            else:
                #print('other')
                continue
        bar_m.next()
        #for key,item in df_group_sel:
        #print(df_group.get_group(key).query('query_type==response_type').groupby('query_type').size().reset_index(name='query_type_counts'))
        """
        if count == 25:
            break
        """
    bar_m.finish()

    print('\n\n')
    print('A,'+ str(max_A)+',' + dom_A)
    print('AAAA,'+ str(max_AAAA)+',' + dom_AAAA)
    print('MX,'+ str(max_MX)+',' + dom_MX)
    print('TXT,'+ str(max_TXT)+',' + dom_TXT)
    print('SOA,'+ str(max_SOA)+',' + dom_SOA)
    print('DS,'+ str(max_DS)+',' + dom_DS)
    print('NS,'+ str(max_NS)+',' + dom_NS)
    print('DNSKEY,'+ str(max_DNSKEY)+ ',' + dom_DNSKEY)
    print('CAA,'+ str(max_CAA)+','+dom_CAA)
    print('CDS,'+str(max_CDS)+','+dom_CDS)
    print('CDNSKEY,'+str(max_CDNSKEY)+','+dom_CDNSKEY)
    print('NSEC3PARAM,'+str(max_NSEC3PARAM)+','+dom_NSEC3PARAM)
    print('NSEC3, '+str(max_NSEC3) + ',' + dom_NSEC3)

    ############################################################### DICT PT 2 #################################################
    print('----------------------------CALC MAX VAL 2----------------------------\n\n')
    df_group_2 = df_origin.groupby('query_name', sort=False)

    max_a_cname = max_aaaa_cname = max_mx_cname = max_txt_cname = max_soa_cname = max_dnskey_cname = max_ns_cname = max_caa_cname = max_cdnskey_cname = max_ds_cname = max_cds_cname = 0
    max_a_rrsig = max_aaaa_rrsig = max_mx_rrsig = max_txt_rrsig = max_soa_rrsig = max_dnskey_rrsig = max_ns_rrsig = max_cds_rrsig = max_cdnskey_rrsig = max_caa_rrsig = max_ds_rrsig = max_nsec3param_rrsig = max_afsdb_rrsig = 0
    max_mxhash = max_txthash = max_nshash = 0
    max_a_dname = max_aaaa_dname = max_mx_dname = max_soa_dname = max_txt_dname = max_ns_dname = max_ds_dname = max_dnskey_dname = max_caa_dname = 0
    dom_a_dname = dom_aaaa_dname = dom_mx_dname = dom_soa_dname = dom_txt_dname = dom_ns_dname = dom_ds_dname = dom_dnskey_dname = dom_caa_dname = ''
    dom_a_cname = dom_aaaa_cname = dom_mx_cname = dom_soa_cname = dom_txt_cname = dom_ns_cname = dom_ds_cname = dom_dnskey_cname = dom_caa_cname = dom_cds_cname = dom_cdnskey_cname = ''
    dom_a_rrsig = dom_aaaa_rrsig = dom_mx_rrsig = dom_soa_rrsig = dom_txt_rrsig = dom_ns_rrsig = dom_dnskey_rrsig = dom_ds_rrsig = dom_dnskey_rrsig = dom_caa_rrsig = dom_cds_rrsig = dom_cdnskey_rrsig = dom_nsec3param_rrsig = dom_afsdb_rrsig = ''
    dom_mx_mxhash = dom_txt_txthash = dom_ns_nshash = ''
    count_dom = 0


    start_c = datetime.now()
    bar_m2 = Bar('Calc max 2',max=df_group_2.ngroups)
    for key,item in df_group_2:
        count_dom += 1
        df_group_bykey = df_group_2.get_group(key)
        #print(df_group_bykey.query("response_type=='CNAME'"))
        count_a_cname = count_a_rrsig = 0
        count_aaaa_cname = count_aaaa_rrsig = 0 
        count_mx_cname = count_mx_rrsig = count_mx_mxhash = 0
        count_txt_cname = count_txt_rrsig = count_txt_txthash = 0
        count_soa_cname = count_soa_rrsig = 0
        count_dnskey_cname = count_dnskey_rrsig = 0
        count_cds_cname = count_cds_rrsig = 0
        count_cdnskey_rrsig = count_cdnskey_cname = 0
        count_ns_nshash = count_ns_rrsig = count_ns_cname = 0
        count_caa_cname = count_caa_rrsig = 0
        count_ds_cname = count_ds_rrsig = 0
        count_nsec3param_rrsig = 0
        count_afsdb_rrsig = 0
        count_a_dname = count_aaaa_dname = count_mx_dname = count_soa_dname = count_txt_dname = count_ns_dname = count_ds_dname = count_dnskey_dname = count_caa_dname = 0
        if key[0:4] == 'www.':
            continue
        else:
            for index,row in df_group_bykey.iterrows():
                vect_inf2 = []
                if row['query_type']=='A': 
                    if row['response_type']=='CNAME':
                    #print(key+ ','+ row['response_name']+ ','+row['cname_name'])
                        count_a_cname += 1
                        if len(str(row['cname_name'])) != 0:
                            vect_inf2.append(str(row['cname_name']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_name'])) != 0:
                            vect_inf2.append(str(row['response_name']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_ttl'])) != 0:
                            vect_inf2.append(str(row['response_ttl']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['worker_id'])) != 0:
                            vect_inf2.append(str(row['worker_id']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rtt'])) != 0:
                            vect_inf2.append(str(row['rtt']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['status_code'])) != 0:
                            vect_inf2.append(str(row['status_code']))
                        else:
                            vect_inf2.append('')
                        dict_info[key]['A_CNAME'].extend([vect_inf2])
                    else:
                        dict_info[key]['A_CNAME'].extend([])
                    if count_a_cname > max_a_cname:
                        max_a_cname = count_a_cname
                        dom_a_cname = str(key)
                    if row['response_type'] == 'RRSIG':
                        count_a_rrsig += 1
                        if len(str(row['rrsig_type_covered'])) != 0:
                            vect_inf2.append(str(row['rrsig_type_covered']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_algorithm'])) != 0:
                            vect_inf2.append(str(row['rrsig_algorithm']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_labels'])) != 0:
                            vect_inf2.append(str(row['rrsig_labels']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_original_ttl'])) != 0:
                            vect_inf2.append(str(row['rrsig_original_ttl']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_signature_inception'])) != 0:
                            vect_inf2.append(str(row['rrsig_signature_inception']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_signature_expiration'])) != 0:
                            vect_inf2.append(str(row['rrsig_signature_expiration']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_key_tag'])) != 0:
                            vect_inf2.append(str(row['rrsig_key_tag']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_signer_name'])) != 0:
                            vect_inf2.append(str(row['rrsig_signer_name']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_signature'])) != 0:
                            vect_inf2.append(str(row['rrsig_signature']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_name'])) != 0:
                            vect_inf2.append(str(row['response_name']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_ttl'])) != 0:
                            vect_inf2.append(str(row['response_ttl']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['worker_id'])) != 0:
                            vect_inf2.append(str(row['worker_id']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rtt'])) != 0:
                            vect_inf2.append(str(row['rtt']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['status_code'])) != 0:
                            vect_inf2.append(str(row['status_code']))
                        else:
                            vect_inf2.append('')
                        dict_info[key]['A_RRSIG'].extend([vect_inf2])
                    else:
                        dict_info[key]['A_RRSIG'].extend([])
                    if count_a_rrsig > max_a_rrsig :
                        max_a_rrsig = count_a_rrsig
                        dom_a_rrsig = str(key)
                    if row['response_type']=='DNAME':
                        count_a_dname += 1
                        if len(str(row['dname_name'])) != 0:
                            vect_inf2.append(str(row['dname_name']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_name'])) != 0:
                            vect_inf2.append(str(row['response_name']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_ttl'])) != 0:
                            vect_inf2.append(str(row['response_ttl']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['worker_id'])) != 0:
                            vect_inf2.append(str(row['worker_id']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rtt'])) != 0:
                            vect_inf2.append(str(row['rtt']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['status_code'])) != 0:
                            vect_inf2.append(str(row['status_code']))
                        else:
                            vect_inf2.append('')
                        dict_info[key]['A_DNAME'].extend([vect_inf2])
                    else:
                        dict_info[key]['A_DNAME'].extend([])
                    if count_a_dname > max_a_dname:
                        max_a_dname = count_a_dname
                        dom_a_dname = str(key)
                    
                if row['query_type'] == 'AAAA':
                    if row['response_type']=='CNAME':
                        count_aaaa_cname += 1
                        if len(str(row['cname_name'])) != 0:
                            vect_inf2.append(str(row['cname_name']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_name'])) != 0:
                            vect_inf2.append(str(row['response_name']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_ttl'])) != 0:
                            vect_inf2.append(str(row['response_ttl']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['worker_id'])) != 0:
                            vect_inf2.append(str(row['worker_id']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rtt'])) != 0:
                            vect_inf2.append(str(row['rtt']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['status_code'])) != 0:
                            vect_inf2.append(str(row['status_code']))
                        else:
                            vect_inf2.append('')
                        dict_info[key]['AAAA_CNAME'].extend([vect_inf2])
                    else:
                        dict_info[key]['AAAA_CNAME'].extend([])
                    if count_aaaa_cname > max_aaaa_cname:
                        max_aaaa_cname = count_aaaa_cname
                        dom_aaaa_cname = str(key)
                    if row['response_type'] == 'RRSIG':
                        count_aaaa_rrsig += 1
                        if len(str(row['rrsig_type_covered'])) != 0:
                            vect_inf2.append(str(row['rrsig_type_covered']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_algorithm'])) != 0:
                            vect_inf2.append(str(row['rrsig_algorithm']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_labels'])) != 0:
                            vect_inf2.append(str(row['rrsig_labels']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_original_ttl'])) != 0:
                            vect_inf2.append(str(row['rrsig_original_ttl']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_signature_inception'])) != 0:
                            vect_inf2.append(str(row['rrsig_signature_inception']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_signature_expiration'])) != 0:
                            vect_inf2.append(str(row['rrsig_signature_expiration']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_key_tag'])) != 0:
                            vect_inf2.append(str(row['rrsig_key_tag']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_signer_name'])) != 0:
                            vect_inf2.append(str(row['rrsig_signer_name']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_signature'])) != 0:
                            vect_inf2.append(str(row['rrsig_signature']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_name'])) != 0:
                            vect_inf2.append(str(row['response_name']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_ttl'])) != 0:
                            vect_inf2.append(str(row['response_ttl']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['worker_id'])) != 0:
                            vect_inf2.append(str(row['worker_id']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rtt'])) != 0:
                            vect_inf2.append(str(row['rtt']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['status_code'])) != 0:
                            vect_inf2.append(str(row['status_code']))
                        else:
                            vect_inf2.append('')
                        dict_info[key]['AAAA_RRSIG'].extend([vect_inf2])
                    else:
                        dict_info[key]['AAAA_RRSIG'].extend([])
                    if count_aaaa_rrsig > max_aaaa_rrsig:
                        max_aaaa_rrsig = count_aaaa_rrsig
                        dom_aaaa_rrsig = str(key)
                    if row['response_type']=='DNAME':
                        count_aaaa_dname += 1
                        if len(str(row['dname_name'])) != 0:
                            vect_inf2.append(str(row['dname_name']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_name'])) != 0:
                            vect_inf2.append(str(row['response_name']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_ttl'])) != 0:
                            vect_inf2.append(str(row['response_ttl']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['worker_id'])) != 0:
                            vect_inf2.append(str(row['worker_id']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rtt'])) != 0:
                            vect_inf2.append(str(row['rtt']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['status_code'])) != 0:
                            vect_inf2.append(str(row['status_code']))
                        else:
                            vect_inf2.append('')
                        dict_info[key]['AAAA_DNAME'].extend([vect_inf2])
                    else:
                        dict_info[key]['AAAA_DNAME'].extend([])
                    if count_aaaa_dname > max_aaaa_dname:
                        max_aaaa_dname = count_aaaa_dname
                        dom_aaaa_dname = str(key)
                
                if row['query_type'] == 'MX':
                    if row['response_type']=='CNAME':
                        count_mx_cname += 1
                        if len(str(row['cname_name'])) != 0:
                            vect_inf2.append(str(row['cname_name']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_name'])) != 0:
                            vect_inf2.append(str(row['response_name']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_ttl'])) != 0:
                            vect_inf2.append(str(row['response_ttl']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['worker_id'])) != 0:
                            vect_inf2.append(str(row['worker_id']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rtt'])) != 0:
                            vect_inf2.append(str(row['rtt']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['status_code'])) != 0:
                            vect_inf2.append(str(row['status_code']))
                        else:
                            vect_inf2.append('')
                        dict_info[key]['MX_CNAME'].extend([vect_inf2])
                    else:
                        dict_info[key]['MX_CNAME'].extend([])
                    if count_mx_cname > max_mx_cname:
                        max_mx_cname = count_mx_cname
                        dom_mx_cname = str(key)
                    if row['response_type'] == 'RRSIG':
                        count_mx_rrsig += 1
                        if len(str(row['rrsig_type_covered'])) != 0:
                            vect_inf2.append(str(row['rrsig_type_covered']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_algorithm'])) != 0:
                            vect_inf2.append(str(row['rrsig_algorithm']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_labels'])) != 0:
                            vect_inf2.append(str(row['rrsig_labels']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_original_ttl'])) != 0:
                            vect_inf2.append(str(row['rrsig_original_ttl']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_signature_inception'])) != 0:
                            vect_inf2.append(str(row['rrsig_signature_inception']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_signature_expiration'])) != 0:
                            vect_inf2.append(str(row['rrsig_signature_expiration']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_key_tag'])) != 0:
                            vect_inf2.append(str(row['rrsig_key_tag']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_signer_name'])) != 0:
                            vect_inf2.append(str(row['rrsig_signer_name']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_signature'])) != 0:
                            vect_inf2.append(str(row['rrsig_signature']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_name'])) != 0:
                            vect_inf2.append(str(row['response_name']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_ttl'])) != 0:
                            vect_inf2.append(str(row['response_ttl']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['worker_id'])) != 0:
                            vect_inf2.append(str(row['worker_id']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rtt'])) != 0:
                            vect_inf2.append(str(row['rtt']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['status_code'])) != 0:
                            vect_inf2.append(str(row['status_code']))
                        else:
                            vect_inf2.append('')
                        dict_info[key]['MX_RRSIG'].extend([vect_inf2])
                    else:
                        dict_info[key]['MX_RRSIG'].extend([])
                    if count_mx_rrsig > max_mx_rrsig:
                        max_mx_rrsig = count_mx_rrsig
                        dom_mx_rrsig = str(key)
                    if row['response_type']=='DNAME':
                        count_mx_dname += 1
                        if len(str(row['dname_name'])) != 0:
                            vect_inf2.append(str(row['dname_name']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_name'])) != 0:
                            vect_inf2.append(str(row['response_name']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_ttl'])) != 0:
                            vect_inf2.append(str(row['response_ttl']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['worker_id'])) != 0:
                            vect_inf2.append(str(row['worker_id']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rtt'])) != 0:
                            vect_inf2.append(str(row['rtt']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['status_code'])) != 0:
                            vect_inf2.append(str(row['status_code']))
                        else:
                            vect_inf2.append('')
                        dict_info[key]['MX_DNAME'].extend([vect_inf2])
                    else:
                        dict_info[key]['MX_DNAME'].extend([])
                    if count_mx_dname > max_mx_dname:
                        max_mx_dname = count_mx_dname
                        dom_mx_dname = str(key)
                    if row['response_type'] == 'MXHASH':
                        count_mx_mxhash += 1
                        if len(str(row['mxset_hash_algorithm'])) != 0:
                            vect_inf2.append(str(row['mxset_hash_algorithm']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['mxset_hash'])) != 0:
                            vect_inf2.append(str(row['mxset_hash']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_name'])) != 0:
                            vect_inf2.append(str(row['response_name']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_ttl'])) != 0:
                            vect_inf2.append(str(row['response_ttl']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['worker_id'])) != 0:
                            vect_inf2.append(str(row['worker_id']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rtt'])) != 0:
                            vect_inf2.append(str(row['rtt']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['status_code'])) != 0:
                            vect_inf2.append(str(row['status_code']))
                        else:
                            vect_inf2.append('')
                        dict_info[key]['MX_MXHASH'].extend([vect_inf2])
                    else:
                        dict_info[key]['MX_MXHASH'].extend([])
                    if count_mx_mxhash > max_mxhash:
                        max_mxhash = count_mx_mxhash
                        dom_mx_mxhash = str(key)
                        
                if row['query_type'] == 'TXT':
                    if row['response_type']=='CNAME':
                        count_txt_cname += 1
                        if len(str(row['cname_name'])) != 0:
                            vect_inf2.append(str(row['cname_name']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_name'])) != 0:
                            vect_inf2.append(str(row['response_name']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_ttl'])) != 0:
                            vect_inf2.append(str(row['response_ttl']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['worker_id'])) != 0:
                            vect_inf2.append(str(row['worker_id']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rtt'])) != 0:
                            vect_inf2.append(str(row['rtt']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['status_code'])) != 0:
                            vect_inf2.append(str(row['status_code']))
                        else:
                            vect_inf2.append('')
                        dict_info[key]['TXT_CNAME'].extend([vect_inf2])
                    else:
                        dict_info[key]['TXT_CNAME'].extend([])
                    if count_txt_cname > max_txt_cname:
                        max_txt_cname = count_txt_cname
                        dom_txt_cname = str(key)
                    if row['response_type'] == 'RRSIG':
                        count_txt_rrsig += 1
                        if len(str(row['rrsig_type_covered'])) != 0:
                            vect_inf2.append(str(row['rrsig_type_covered']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_algorithm'])) != 0:
                            vect_inf2.append(str(row['rrsig_algorithm']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_labels'])) != 0:
                            vect_inf2.append(str(row['rrsig_labels']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_original_ttl'])) != 0:
                            vect_inf2.append(str(row['rrsig_original_ttl']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_signature_inception'])) != 0:
                            vect_inf2.append(str(row['rrsig_signature_inception']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_signature_expiration'])) != 0:
                            vect_inf2.append(str(row['rrsig_signature_expiration']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_key_tag'])) != 0:
                            vect_inf2.append(str(row['rrsig_key_tag']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_signer_name'])) != 0:
                            vect_inf2.append(str(row['rrsig_signer_name']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_signature'])) != 0:
                            vect_inf2.append(str(row['rrsig_signature']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_name'])) != 0:
                            vect_inf2.append(str(row['response_name']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_ttl'])) != 0:
                            vect_inf2.append(str(row['response_ttl']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['worker_id'])) != 0:
                            vect_inf2.append(str(row['worker_id']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rtt'])) != 0:
                            vect_inf2.append(str(row['rtt']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['status_code'])) != 0:
                            vect_inf2.append(str(row['status_code']))
                        else:
                            vect_inf2.append('')
                        dict_info[key]['TXT_RRSIG'].extend([vect_inf2])
                    else:
                        dict_info[key]['TXT_RRSIG'].extend([])
                    if count_txt_rrsig > max_txt_rrsig:
                        max_txt_rrsig = count_txt_rrsig
                        dom_txt_rrsig = str(key)
                    if row['response_type'] == 'TXTHASH':
                        count_txt_txthash += 1
                        if len(str(row['txt_hash_algorithm'])) != 0:
                            vect_inf2.append(str(row['txt_hash_algorithm']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['txt_hash'])) != 0:
                            vect_inf2.append(str(row['txt_hash']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_name'])) != 0:
                            vect_inf2.append(str(row['response_name']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_ttl'])) != 0:
                            vect_inf2.append(str(row['response_ttl']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['worker_id'])) != 0:
                            vect_inf2.append(str(row['worker_id']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rtt'])) != 0:
                            vect_inf2.append(str(row['rtt']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['status_code'])) != 0:
                            vect_inf2.append(str(row['status_code']))
                        else:
                            vect_inf2.append('')
                        dict_info[key]['TXT_TXTHASH'].extend([vect_inf2])
                    else:
                        dict_info[key]['TXT_TXTHASH'].extend([])
                    if count_txt_txthash > max_txthash:
                        max_txthash = count_txt_txthash
                        dom_txt_txthash = str(key)
                    if row['response_type']=='DNAME':
                        count_txt_dname += 1
                        if len(str(row['dname_name'])) != 0:
                            vect_inf2.append(str(row['dname_name']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_name'])) != 0:
                            vect_inf2.append(str(row['response_name']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_ttl'])) != 0:
                            vect_inf2.append(str(row['response_ttl']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['worker_id'])) != 0:
                            vect_inf2.append(str(row['worker_id']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rtt'])) != 0:
                            vect_inf2.append(str(row['rtt']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['status_code'])) != 0:
                            vect_inf2.append(str(row['status_code']))
                        else:
                            vect_inf2.append('')
                        dict_info[key]['TXT_DNAME'].extend([vect_inf2])
                    else:
                        dict_info[key]['TXT_DNAME'].extend([])
                    if count_txt_dname > max_txt_dname:
                        max_txt_dname = count_txt_dname
                        dom_txt_dname = str(key)
                        
                if row['query_type'] == 'SOA':
                    if row['response_type'] == 'CNAME':
                        count_soa_cname += 1
                        if len(str(row['cname_name'])) != 0:
                            vect_inf2.append(str(row['cname_name']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_name'])) != 0:
                            vect_inf2.append(str(row['response_name']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_ttl'])) != 0:
                            vect_inf2.append(str(row['response_ttl']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['worker_id'])) != 0:
                            vect_inf2.append(str(row['worker_id']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rtt'])) != 0:
                            vect_inf2.append(str(row['rtt']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['status_code'])) != 0:
                            vect_inf2.append(str(row['status_code']))
                        else:
                            vect_inf2.append('')
                        dict_info[key]['SOA_CNAME'].extend([vect_inf2])
                    else:
                        dict_info[key]['SOA_CNAME'].extend([])
                    if count_soa_cname > max_soa_cname:
                        max_soa_cname = count_soa_cname
                        dom_soa_cname = str(key)
                    if row['response_type'] == 'RRSIG':
                        count_soa_rrsig += 1
                        if len(str(row['rrsig_type_covered'])) != 0:
                            vect_inf2.append(str(row['rrsig_type_covered']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_algorithm'])) != 0:
                            vect_inf2.append(str(row['rrsig_algorithm']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_labels'])) != 0:
                            vect_inf2.append(str(row['rrsig_labels']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_original_ttl'])) != 0:
                            vect_inf2.append(str(row['rrsig_original_ttl']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_signature_inception'])) != 0:
                            vect_inf2.append(str(row['rrsig_signature_inception']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_signature_expiration'])) != 0:
                            vect_inf2.append(str(row['rrsig_signature_expiration']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_key_tag'])) != 0:
                            vect_inf2.append(str(row['rrsig_key_tag']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_signer_name'])) != 0:
                            vect_inf2.append(str(row['rrsig_signer_name']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_signature'])) != 0:
                            vect_inf2.append(str(row['rrsig_signature']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_name'])) != 0:
                            vect_inf2.append(str(row['response_name']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_ttl'])) != 0:
                            vect_inf2.append(str(row['response_ttl']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['worker_id'])) != 0:
                            vect_inf2.append(str(row['worker_id']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rtt'])) != 0:
                            vect_inf2.append(str(row['rtt']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['status_code'])) != 0:
                            vect_inf2.append(str(row['status_code']))
                        else:
                            vect_inf2.append('')
                        dict_info[key]['SOA_RRSIG'].extend([vect_inf2])
                    else:
                        dict_info[key]['SOA_RRSIG'].extend([])
                    if count_soa_rrsig > max_soa_rrsig:
                        max_soa_rrsig = count_soa_rrsig
                        dom_soa_rrsig = str(key)
                    if row['response_type']=='DNAME':
                        count_soa_dname += 1
                        if len(str(row['dname_name'])) != 0:
                            vect_inf2.append(str(row['dname_name']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_name'])) != 0:
                            vect_inf2.append(str(row['response_name']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_ttl'])) != 0:
                            vect_inf2.append(str(row['response_ttl']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['worker_id'])) != 0:
                            vect_inf2.append(str(row['worker_id']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rtt'])) != 0:
                            vect_inf2.append(str(row['rtt']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['status_code'])) != 0:
                            vect_inf2.append(str(row['status_code']))
                        else:
                            vect_inf2.append('')
                        dict_info[key]['SOA_DNAME'].extend([vect_inf2])
                    else:
                        dict_info[key]['SOA_DNAME'].extend([])
                    if count_soa_dname > max_soa_dname:
                        max_soa_dname = count_soa_dname
                        dom_soa_dname = str(key)
                                
                if row['query_type'] == 'DNSKEY':
                    if row['response_type']=='CNAME':
                        count_dnskey_cname += 1
                        if row['response_type']=='CNAME':
                            count_soa_cname += 1
                        if len(str(row['cname_name'])) != 0:
                            vect_inf2.append(str(row['cname_name']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_name'])) != 0:
                            vect_inf2.append(str(row['response_name']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_ttl'])) != 0:
                            vect_inf2.append(str(row['response_ttl']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['worker_id'])) != 0:
                            vect_inf2.append(str(row['worker_id']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rtt'])) != 0:
                            vect_inf2.append(str(row['rtt']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['status_code'])) != 0:
                            vect_inf2.append(str(row['status_code']))
                        else:
                            vect_inf2.append('')
                        dict_info[key]['DNSKEY_CNAME'].extend([vect_inf2])
                    else:
                        dict_info[key]['DNSKEY_CNAME'].extend([])
                    if count_dnskey_cname > max_dnskey_cname:
                        max_dnskey_cname = count_dnskey_cname
                        dom_dnskey_cname = str(key)
                    if row['response_type'] == 'RRSIG':
                        count_dnskey_rrsig += 1
                        if len(str(row['rrsig_type_covered'])) != 0:
                            vect_inf2.append(str(row['rrsig_type_covered']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_algorithm'])) != 0:
                            vect_inf2.append(str(row['rrsig_algorithm']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_labels'])) != 0:
                            vect_inf2.append(str(row['rrsig_labels']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_original_ttl'])) != 0:
                            vect_inf2.append(str(row['rrsig_original_ttl']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_signature_inception'])) != 0:
                            vect_inf2.append(str(row['rrsig_signature_inception']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_signature_expiration'])) != 0:
                            vect_inf2.append(str(row['rrsig_signature_expiration']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_key_tag'])) != 0:
                            vect_inf2.append(str(row['rrsig_key_tag']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_signer_name'])) != 0:
                            vect_inf2.append(str(row['rrsig_signer_name']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_signature'])) != 0:
                            vect_inf2.append(str(row['rrsig_signature']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_name'])) != 0:
                            vect_inf2.append(str(row['response_name']))
                        else:
                            vect_inf2.append('')    
                        if len(str(row['response_ttl'])) != 0:
                            vect_inf2.append(str(row['response_ttl']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['worker_id'])) != 0:
                            vect_inf2.append(str(row['worker_id']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rtt'])) != 0:
                            vect_inf2.append(str(row['rtt']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['status_code'])) != 0:
                            vect_inf2.append(str(row['status_code']))
                        else:
                            vect_inf2.append('')
                        dict_info[key]['DNSKEY_RRSIG'].extend([vect_inf2])
                    else:
                        dict_info[key]['DNSKEY_RRSIG'].extend([])
                    if count_dnskey_rrsig > max_dnskey_rrsig:
                        max_dnskey_rrsig = count_dnskey_rrsig
                        dom_dnskey_rrsig = str(key)
                    if row['response_type']=='DNAME':
                        count_dnskey_dname += 1
                        if len(str(row['dname_name'])) != 0:
                            vect_inf2.append(str(row['dname_name']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_name'])) != 0:
                            vect_inf2.append(str(row['response_name']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_ttl'])) != 0:
                            vect_inf2.append(str(row['response_ttl']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['worker_id'])) != 0:
                            vect_inf2.append(str(row['worker_id']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rtt'])) != 0:
                            vect_inf2.append(str(row['rtt']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['status_code'])) != 0:
                            vect_inf2.append(str(row['status_code']))
                        else:
                            vect_inf2.append('')
                        dict_info[key]['DNSKEY_DNAME'].extend([vect_inf2])
                    else:
                        dict_info[key]['DNSKEY_DNAME'].extend([])
                    if count_dnskey_dname > max_dnskey_dname:
                        max_dnskey_dname = count_dnskey_dname
                        dom_dnskey_dname = str(key)
                        
                if row['query_type'] == 'CDS':
                    if row['response_type'] == 'CNAME':
                        count_cds_cname += 1
                        if len(str(row['cname_name'])) != 0:
                            vect_inf2.append(str(row['cname_name']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_name'])) != 0:
                            vect_inf2.append(str(row['response_name']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_ttl'])) != 0:
                            vect_inf2.append(str(row['response_ttl']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['worker_id'])) != 0:
                            vect_inf2.append(str(row['worker_id']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rtt'])) != 0:
                            vect_inf2.append(str(row['rtt']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['status_code'])) != 0:
                            vect_inf2.append(str(row['status_code']))
                        else:
                            vect_inf2.append('')
                        dict_info[key]['CDS_CNAME'].extend([vect_inf2])
                    else:
                        dict_info[key]['CDS_CNAME'].extend([])
                    if count_cds_cname == 0:
                        max_cds_cname = 0
                        dom_cds_cname = ''
                    elif count_cds_cname > max_cds_cname:
                        max_cds_cname = count_cds_cname
                        dom_cds_cname = str(key)
                    else:
                        print('error cds cname, dom: ' + key)
                        
                    if row['response_type'] == 'RRSIG':
                        count_cds_rrsig += 1
                        if len(str(row['rrsig_type_covered'])) != 0:
                            vect_inf2.append(str(row['rrsig_type_covered']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_algorithm'])) != 0:
                            vect_inf2.append(str(row['rrsig_algorithm']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_labels'])) != 0:
                            vect_inf2.append(str(row['rrsig_labels']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_original_ttl'])) != 0:
                            vect_inf2.append(str(row['rrsig_original_ttl']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_signature_inception'])) != 0:
                            vect_inf2.append(str(row['rrsig_signature_inception']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_signature_expiration'])) != 0:
                            vect_inf2.append(str(row['rrsig_signature_expiration']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_key_tag'])) != 0:
                            vect_inf2.append(str(row['rrsig_key_tag']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_signer_name'])) != 0:
                            vect_inf2.append(str(row['rrsig_signer_name']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_signature'])) != 0:
                            vect_inf2.append(str(row['rrsig_signature']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_name'])) != 0:
                            vect_inf2.append(str(row['response_name']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_ttl'])) != 0:
                            vect_inf2.append(str(row['response_ttl']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['worker_id'])) != 0:
                            vect_inf2.append(str(row['worker_id']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rtt'])) != 0:
                            vect_inf2.append(str(row['rtt']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['status_code'])) != 0:
                            vect_inf2.append(str(row['status_code']))
                        else:
                            vect_inf2.append('')
                        dict_info[key]['CDS_RRSIG'].extend([vect_inf2])
                    else:
                        dict_info[key]['CDS_RRSIG'].extend([])
                    if count_cds_rrsig > max_cds_rrsig:
                        max_cds_rrsig = count_cds_rrsig
                        dom_cds_rrsig = str(key)
                        
                if row['query_type'] == 'CDNSKEY':
                    if row['response_type'] == 'CNAME':
                        count_cdnskey_cname += 1
                        if len(str(row['cname_name'])) != 0:
                            vect_inf2.append(str(row['cname_name']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_name'])) != 0:
                            vect_inf2.append(str(row['response_name']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_ttl'])) != 0:
                            vect_inf2.append(str(row['response_ttl']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['worker_id'])) != 0:
                            vect_inf2.append(str(row['worker_id']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rtt'])) != 0:
                            vect_inf2.append(str(row['rtt']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['status_code'])) != 0:
                            vect_inf2.append(str(row['status_code']))
                        else:
                            vect_inf2.append('')
                        dict_info[key]['CDNSKEY_CNAME'].extend([vect_inf2])
                    else:
                        dict_info[key]['CDNSKEY_CNAME'].extend([])
                    if count_cdnskey_cname > max_cdnskey_cname:
                        max_cdnskey_cname = count_cdnskey_cname
                        dom_cdnskey_cname = str(key)
                    if row['response_type'] == 'RRSIG':
                        count_cdnskey_rrsig += 1
                        if len(str(row['rrsig_type_covered'])) != 0:
                            vect_inf2.append(str(row['rrsig_type_covered']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_algorithm'])) != 0:
                            vect_inf2.append(str(row['rrsig_algorithm']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_labels'])) != 0:
                            vect_inf2.append(str(row['rrsig_labels']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_original_ttl'])) != 0:
                            vect_inf2.append(str(row['rrsig_original_ttl']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_signature_inception'])) != 0:
                            vect_inf2.append(str(row['rrsig_signature_inception']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_signature_expiration'])) != 0:
                            vect_inf2.append(str(row['rrsig_signature_expiration']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_key_tag'])) != 0:
                            vect_inf2.append(str(row['rrsig_key_tag']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_signer_name'])) != 0:
                            vect_inf2.append(str(row['rrsig_signer_name']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_signature'])) != 0:
                            vect_inf2.append(str(row['rrsig_signature']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_name'])) != 0:
                            vect_inf2.append(str(row['response_name']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_ttl'])) != 0:
                            vect_inf2.append(str(row['response_ttl']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['worker_id'])) != 0:
                            vect_inf2.append(str(row['worker_id']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rtt'])) != 0:
                            vect_inf2.append(str(row['rtt']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['status_code'])) != 0:
                            vect_inf2.append(str(row['status_code']))
                        else:
                            vect_inf2.append('')
                        dict_info[key]['CDNSKEY_RRSIG'].extend([vect_inf2])
                    else:
                        dict_info[key]['CDNSKEY_RRSIG'].extend([])
                    if count_cdnskey_rrsig > max_cdnskey_rrsig:
                        max_cdnskey_rrsig = count_cdnskey_rrsig
                        dom_cdnskey_rrsig = str(key)
                        
                if row['query_type'] == 'NS':
                    if row['response_type']=='CNAME':
                        count_ns_cname += 1
                        if len(str(row['cname_name'])) != 0:
                            vect_inf2.append(str(row['cname_name']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_name'])) != 0:
                            vect_inf2.append(str(row['response_name']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_ttl'])) != 0:
                            vect_inf2.append(str(row['response_ttl']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['worker_id'])) != 0:
                            vect_inf2.append(str(row['worker_id']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rtt'])) != 0:
                            vect_inf2.append(str(row['rtt']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['status_code'])) != 0:
                            vect_inf2.append(str(row['status_code']))
                        else:
                            vect_inf2.append('')
                        dict_info[key]['NS_CNAME'].extend([vect_inf2])
                    else:
                        dict_info[key]['NS_CNAME'].extend([])
                    if count_ns_cname > max_ns_cname:
                        max_ns_cname = count_ns_cname
                        dom_ns_cname = str(key)
                    if row['response_type'] == 'NSHASH':
                        count_ns_nshash += 1
                        if len(str(row['nsset_hash_algorithm'])) != 0:
                            vect_inf2.append(str(row['nsset_hash_algorithm']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['nsset_hash'])) != 0:
                            vect_inf2.append(str(row['nsset_hash']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_name'])) != 0:
                            vect_inf2.append(str(row['response_name']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_ttl'])) != 0:
                            vect_inf2.append(str(row['response_ttl']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['worker_id'])) != 0:
                            vect_inf2.append(str(row['worker_id']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rtt'])) != 0:
                            vect_inf2.append(str(row['rtt']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['status_code'])) != 0:
                            vect_inf2.append(str(row['status_code']))
                        else:
                            vect_inf2.append('')
                        dict_info[key]['NS_NSHASH'].extend([vect_inf2])
                    else:
                        dict_info[key]['NS_NSHASH'].extend([])
                    if count_ns_nshash > max_nshash:
                        max_nshash = count_ns_nshash
                        dom_ns_nshash = str(key)
                    if row['query_type'] == 'RRSIG':
                        count_ns_rrsig += 1
                        if len(str(row['rrsig_type_covered'])) != 0:
                            vect_inf2.append(str(row['rrsig_type_covered']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_algorithm'])) != 0:
                            vect_inf2.append(str(row['rrsig_algorithm']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_labels'])) != 0:
                            vect_inf2.append(str(row['rrsig_labels']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_original_ttl'])) != 0:
                            vect_inf2.append(str(row['rrsig_original_ttl']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_signature_inception'])) != 0:
                            vect_inf2.append(str(row['rrsig_signature_inception']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_signature_expiration'])) != 0:
                            vect_inf2.append(str(row['rrsig_signature_expiration']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_key_tag'])) != 0:
                            vect_inf2.append(str(row['rrsig_key_tag']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_signer_name'])) != 0:
                            vect_inf2.append(str(row['rrsig_signer_name']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_signature'])) != 0:
                            vect_inf2.append(str(row['rrsig_signature']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_name'])) != 0:
                            vect_inf2.append(str(row['response_name']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_ttl'])) != 0:
                            vect_inf2.append(str(row['response_ttl']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['worker_id'])) != 0:
                            vect_inf2.append(str(row['worker_id']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rtt'])) != 0:
                            vect_inf2.append(str(row['rtt']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['status_code'])) != 0:
                            vect_inf2.append(str(row['status_code']))
                        else:
                            vect_inf2.append('')
                        dict_info[key]['NS_RRSIG'].extend([vect_inf2])
                    else:
                        dict_info[key]['NS_RRSIG'].extend([])
                    if count_ns_rrsig > max_ns_rrsig:
                        max_ns_rrsig = count_ns_rrsig
                        dom_ns_rrsig = str(key)
                    if row['response_type']=='DNAME':
                        count_ns_dname += 1
                        if len(str(row['dname_name'])) != 0:
                            vect_inf2.append(str(row['dname_name']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_name'])) != 0:
                            vect_inf2.append(str(row['response_name']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_ttl'])) != 0:
                            vect_inf2.append(str(row['response_ttl']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['worker_id'])) != 0:
                            vect_inf2.append(str(row['worker_id']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rtt'])) != 0:
                            vect_inf2.append(str(row['rtt']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['status_code'])) != 0:
                            vect_inf2.append(str(row['status_code']))
                        else:
                            vect_inf2.append('')
                        dict_info[key]['NS_DNAME'].extend([vect_inf2])
                    else:
                        dict_info[key]['NS_DNAME'].extend([])
                    if count_ns_dname > max_ns_dname:
                        max_ns_dname = count_ns_dname
                        dom_ns_dname = str(key)
                        
                if row['query_type'] == 'CAA':
                    if row['response_type']=='CNAME':
                        count_caa_cname += 1
                        if len(str(row['cname_name'])) != 0:
                            vect_inf2.append(str(row['cname_name']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_name'])) != 0:
                            vect_inf2.append(str(row['response_name']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_ttl'])) != 0:
                            vect_inf2.append(str(row['response_ttl']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['worker_id'])) != 0:
                            vect_inf2.append(str(row['worker_id']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rtt'])) != 0:
                            vect_inf2.append(str(row['rtt']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['status_code'])) != 0:
                            vect_inf2.append(str(row['status_code']))
                        else:
                            vect_inf2.append('')
                        dict_info[key]['CAA_CNAME'].extend([vect_inf2])
                    else:
                        dict_info[key]['CAA_CNAME'].extend([])
                    if count_caa_cname > max_caa_cname:
                        max_caa_cname = count_caa_cname
                        dom_caa_cname = str(key)
                    if row['response_type'] == 'RRSIG':
                        count_caa_rrsig += 1
                        if len(str(row['rrsig_type_covered'])) != 0:
                            vect_inf2.append(str(row['rrsig_type_covered']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_algorithm'])) != 0:
                            vect_inf2.append(str(row['rrsig_algorithm']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_labels'])) != 0:
                            vect_inf2.append(str(row['rrsig_labels']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_original_ttl'])) != 0:
                            vect_inf2.append(str(row['rrsig_original_ttl']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_signature_inception'])) != 0:
                            vect_inf2.append(str(row['rrsig_signature_inception']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_signature_expiration'])) != 0:
                            vect_inf2.append(str(row['rrsig_signature_expiration']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_key_tag'])) != 0:
                            vect_inf2.append(str(row['rrsig_key_tag']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_signer_name'])) != 0:
                            vect_inf2.append(str(row['rrsig_signer_name']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_signature'])) != 0:
                            vect_inf2.append(str(row['rrsig_signature']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_name'])) != 0:
                            vect_inf2.append(str(row['response_name']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_ttl'])) != 0:
                            vect_inf2.append(str(row['response_ttl']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['worker_id'])) != 0:
                            vect_inf2.append(str(row['worker_id']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rtt'])) != 0:
                            vect_inf2.append(str(row['rtt']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['status_code'])) != 0:
                            vect_inf2.append(str(row['status_code']))
                        else:
                            vect_inf2.append('')
                        dict_info[key]['CAA_RRSIG'].extend([vect_inf2])
                    else:
                        dict_info[key]['CAA_RRSIG'].extend([])
                    if count_caa_rrsig > max_caa_rrsig:
                        max_caa_rrsig = count_caa_rrsig
                        dom_caa_rrsig = str(key)
                    if row['response_type']=='DNAME':
                        count_caa_dname += 1
                        if len(str(row['dname_name'])) != 0:
                            vect_inf2.append(str(row['dname_name']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_name'])) != 0:
                            vect_inf2.append(str(row['response_name']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_ttl'])) != 0:
                            vect_inf2.append(str(row['response_ttl']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['worker_id'])) != 0:
                            vect_inf2.append(str(row['worker_id']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rtt'])) != 0:
                            vect_inf2.append(str(row['rtt']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['status_code'])) != 0:
                            vect_inf2.append(str(row['status_code']))
                        else:
                            vect_inf2.append('')
                        dict_info[key]['CAA_DNAME'].extend([vect_inf2])
                    else:
                        dict_info[key]['CAA_DNAME'].extend([])
                    if count_caa_dname > max_caa_dname:
                        max_caa_dname = count_caa_dname
                        dom_caa_dname = str(key)
                        
                if row['query_type'] == 'DS':
                    if row['response_type']=='CNAME':
                        count_ds_cname += 1
                        if len(str(row['cname_name'])) != 0:
                            vect_inf2.append(str(row['cname_name']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_name'])) != 0:
                            vect_inf2.append(str(row['response_name']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_ttl'])) != 0:
                            vect_inf2.append(str(row['response_ttl']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['worker_id'])) != 0:
                            vect_inf2.append(str(row['worker_id']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rtt'])) != 0:
                            vect_inf2.append(str(row['rtt']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['status_code'])) != 0:
                            vect_inf2.append(str(row['status_code']))
                        else:
                            vect_inf2.append('')
                        dict_info[key]['DS_CNAME'].extend([vect_inf2])
                    else:
                        dict_info[key]['DS_CNAME'].extend([])
                    if count_ds_cname > max_ds_cname:
                        max_ds_cname = count_ds_cname
                        dom_ds_cname = str(key)
                    if row['response_type'] == 'RRSIG':
                        count_ds_rrsig += 1
                        if len(str(row['rrsig_type_covered'])) != 0:
                            vect_inf2.append(str(row['rrsig_type_covered']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_algorithm'])) != 0:
                            vect_inf2.append(str(row['rrsig_algorithm']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_labels'])) != 0:
                            vect_inf2.append(str(row['rrsig_labels']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_original_ttl'])) != 0:
                            vect_inf2.append(str(row['rrsig_original_ttl']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_signature_inception'])) != 0:
                            vect_inf2.append(str(row['rrsig_signature_inception']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_signature_expiration'])) != 0:
                            vect_inf2.append(str(row['rrsig_signature_expiration']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_key_tag'])) != 0:
                            vect_inf2.append(str(row['rrsig_key_tag']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_signer_name'])) != 0:
                            vect_inf2.append(str(row['rrsig_signer_name']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_signature'])) != 0:
                            vect_inf2.append(str(row['rrsig_signature']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_name'])) != 0:
                            vect_inf2.append(str(row['response_name']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_ttl'])) != 0:
                            vect_inf2.append(str(row['response_ttl']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['worker_id'])) != 0:
                            vect_inf2.append(str(row['worker_id']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rtt'])) != 0:
                            vect_inf2.append(str(row['rtt']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['status_code'])) != 0:
                            vect_inf2.append(str(row['status_code']))
                        else:
                            vect_inf2.append('')
                        dict_info[key]['DS_RRSIG'].extend([vect_inf2])
                    else:
                        dict_info[key]['DS_RRSIG'].extend([])
                    if count_ds_rrsig > max_ds_rrsig:
                        max_ds_rrsig = count_ds_rrsig
                        dom_ds_rrsig = str(key)
                    if row['response_type']=='DNAME':
                        count_ds_dname += 1
                        if len(str(row['dname_name'])) != 0:
                            vect_inf2.append(str(row['dname_name']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_name'])) != 0:
                            vect_inf2.append(str(row['response_name']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_ttl'])) != 0:
                            vect_inf2.append(str(row['response_ttl']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['worker_id'])) != 0:
                            vect_inf2.append(str(row['worker_id']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rtt'])) != 0:
                            vect_inf2.append(str(row['rtt']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['status_code'])) != 0:
                            vect_inf2.append(str(row['status_code']))
                        else:
                            vect_inf2.append('')
                        dict_info[key]['DS_DNAME'].extend([vect_inf2])
                    else:
                        dict_info[key]['DS_DNAME'].extend([])
                    if count_ds_dname > max_ds_dname:
                        max_ds_dname = count_ds_dname
                        dom_ds_dname = str(key)
                        
                if row['query_type'] == 'NSEC3PARAM':
                    if row['response_type'] == 'RRSIG':
                        count_nsec3param_rrsig += 1
                        if len(str(row['rrsig_type_covered'])) != 0:
                            vect_inf2.append(str(row['rrsig_type_covered']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_algorithm'])) != 0:
                            vect_inf2.append(str(row['rrsig_algorithm']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_labels'])) != 0:
                            vect_inf2.append(str(row['rrsig_labels']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_original_ttl'])) != 0:
                            vect_inf2.append(str(row['rrsig_original_ttl']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_signature_inception'])) != 0:
                            vect_inf2.append(str(row['rrsig_signature_inception']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_signature_expiration'])) != 0:
                            vect_inf2.append(str(row['rrsig_signature_expiration']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_key_tag'])) != 0:
                            vect_inf2.append(str(row['rrsig_key_tag']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_signer_name'])) != 0:
                            vect_inf2.append(str(row['rrsig_signer_name']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_signature'])) != 0:
                            vect_inf2.append(str(row['rrsig_signature']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_name'])) != 0:
                            vect_inf2.append(str(row['response_name']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_ttl'])) != 0:
                            vect_inf2.append(str(row['response_ttl']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['worker_id'])) != 0:
                            vect_inf2.append(str(row['worker_id']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rtt'])) != 0:
                            vect_inf2.append(str(row['rtt']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['status_code'])) != 0:
                            vect_inf2.append(str(row['status_code']))
                        else:
                            vect_inf2.append('')
                        dict_info[key]['NSEC3PARAM_RRSIG'].extend([vect_inf2])
                    else:
                        dict_info[key]['NSEC3PARAM_RRSIG'].extend([])
                    if count_nsec3param_rrsig > max_nsec3param_rrsig:
                        max_nsec3param_rrsig = count_nsec3param_rrsig
                        dom_nsec3param_rrsig = str(key)
                if row['query_type'] == 'AFSDB':
                    if row['response_type'] == 'RRSIG':
                        count_afsdb_rrsig += 1
                        if len(str(row['rrsig_type_covered'])) != 0:
                            vect_inf2.append(str(row['rrsig_type_covered']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_algorithm'])) != 0:
                            vect_inf2.append(str(row['rrsig_algorithm']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_labels'])) != 0:
                            vect_inf2.append(str(row['rrsig_labels']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_original_ttl'])) != 0:
                            vect_inf2.append(str(row['rrsig_original_ttl']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_signature_inception'])) != 0:
                            vect_inf2.append(str(row['rrsig_signature_inception']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_signature_expiration'])) != 0:
                            vect_inf2.append(str(row['rrsig_signature_expiration']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_key_tag'])) != 0:
                            vect_inf2.append(str(row['rrsig_key_tag']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_signer_name'])) != 0:
                            vect_inf2.append(str(row['rrsig_signer_name']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rrsig_signature'])) != 0:
                            vect_inf2.append(str(row['rrsig_signature']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_name'])) != 0:
                            vect_inf2.append(str(row['response_name']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['response_ttl'])) != 0:
                            vect_inf2.append(str(row['response_ttl']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['worker_id'])) != 0:
                            vect_inf2.append(str(row['worker_id']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['rtt'])) != 0:
                            vect_inf2.append(str(row['rtt']))
                        else:
                            vect_inf2.append('')
                        if len(str(row['status_code'])) != 0:
                            vect_inf2.append(str(row['status_code']))
                        else:
                            vect_inf2.append('')
                        dict_info[key]['AFSDB_RRSIG'].extend([vect_inf2])
                    else:
                        dict_info[key]['AFSDB_RRSIG'].extend([])
                    if count_afsdb_rrsig > max_afsdb_rrsig:
                        max_afsdb_rrsig = count_afsdb_rrsig
                        dom_afsdb_rrsig = str(key)
                    
        bar_m2.next()
    bar_m2.finish()
    end_c = datetime.now()




    print('\n')
    print('duration {}'.format(end_c - start_c))
    print('A,  cname:' + str(max_a_cname) +',dom :'+dom_a_cname +'  rrsig: ' +str(max_a_rrsig) +', dom:' +dom_a_rrsig+',dname: '+str(max_a_dname)+ ', dom:' + str(dom_a_dname))
    print('AAAA, cname:' + str(max_aaaa_cname)+ ', dom: '+ str(dom_aaaa_cname)+ ', rrsig: '+ str(max_aaaa_rrsig)+', dom:' + str(dom_aaaa_rrsig)+',dname: '+str(max_aaaa_dname)+ ', dom:' + str(dom_aaaa_dname))
    print('MX, cname:' + str(max_mx_cname) + ', dom: ' +str(dom_mx_cname)+  ', rrsig: '+str(max_mx_rrsig)+', dom: '+ str(dom_mx_rrsig)+
        ', mxhash: '+str(max_mxhash) +', dom:' + str(dom_mx_mxhash)+',dname: '+str(max_mx_dname)+ ', dom:' + str(dom_mx_dname))
    print('TXT, cname: '+str(max_txt_cname)+', dom:'+str(dom_txt_cname)+ ',rrsig: '+str(max_txt_rrsig)+', dom: ' + str(dom_txt_rrsig)+',txthash:' +str(max_txthash)+', dom:'
    +str(dom_txt_txthash)+',dname: '+str(max_txt_dname)+ ', dom:' + str(dom_txt_dname))
    print('SOA, cname: '+ str(max_soa_cname)+', dom:' +str(dom_soa_cname)+ ',rrsig:'+str(max_soa_rrsig)+ ',dom: '+ str(dom_soa_rrsig)+',dname: '+str(max_soa_dname)+ ', dom:' + str(dom_soa_dname))
    print('DNSKEY, cname: '+str(max_dnskey_cname)+ ', dom: ' + str(dom_dnskey_cname)+',rrsig: '+str(max_dnskey_rrsig) + ', dom: ' +str(dom_dnskey_rrsig)+',dname: '+str(max_dnskey_dname)+ ', dom:' + str(dom_dnskey_dname))
    print('CDS, cname:' + str(max_cds_cname)+', dom: '+str(dom_cds_cname)+', rrsig: '+str(max_cds_rrsig) + ', dom: ' +str(dom_cds_rrsig))
    print('CDNSKEY, cname:'+str(max_cdnskey_cname)+', rrsig: '+str(max_cdnskey_rrsig)+', dom: ' +str(dom_cdnskey_rrsig))
    print('NS, cname:'+ str(max_ns_cname)+', dom:' + str(dom_ns_cname) +', rrsig: ' + str(max_ns_rrsig)+', dom:'+str(dom_ns_rrsig)+', nshash: '+str(max_nshash)+ ', dom:' + str(dom_ns_nshash)+',dname: '+str(max_ns_dname)+ ', dom:' + str(dom_ns_dname))
    print('CAA, cname: '+str(max_caa_cname)+', dom: ' +str(dom_caa_cname)+ ',rrsig: '+str(max_caa_rrsig) + ', dom: ' +str(dom_caa_rrsig)+',dname: '+str(max_caa_dname)+ ', dom:' + str(dom_caa_dname))
    print('DS, cname: '+ str(max_ds_cname)+', dom: ' +str(dom_ds_cname)+', rrsig: ' +str(max_ds_rrsig)+', dom: '+str(dom_ds_rrsig)+',dname: '+str(max_ds_dname)+ ', dom:' + str(dom_ds_dname))
    print('NSEC3PARAM, rrsig:' +str(max_nsec3param_rrsig)+ ', dom: ' +str(dom_nsec3param_rrsig))
    print('AFSDB, rrsig:' +str(max_afsdb_rrsig)+', dom: '+ str(dom_afsdb_rrsig))







    #############################Feature Extraction############################


    ''''
    if os.path.exists('feature_set.csv'):
        os.remove('feature_set.csv')
        print('old feature_set.csv removed')
    else:
        print('no old feature_set.csv exists')

    RR_feature = ['A','AAAA']
    header_csv_feature = []
    for feature in RR_feature:
        
    with open('feature_set.csv', mode='a') as csv_feature:

        for dom in doms:
    '''      
    if set_out == '0':
        if os.path.exists('dict_info_oi.json'):
            os.remove('dict_info_oi.json')
            print('remove old dict_info_oi.json, ad create a new')
            json.dump(dict_info,open('dict_info_oi.json', 'w'), indent=3)
        else:
            json.dump(dict_info,open('dict_info_oi.json', 'w'), indent=3)
        exit()

    """
    - Section A('x' index that goes from 0 to max-1):
    |x_a_ip4|x_a_response_ttl|x_a_as|x_a_as_full|x_a_country|x_a_ip_prefix|
    create an header -> section-a X max_A-->|0_a_ip4|0_a_response_ttl|0_a_as|0_a_as_full|0_a_country|0_a_ip_prefix|1_a_ip4|1_a_response_ttl|1_a_as|1_a_as_full|1_a_country|1_a_ip_prefix|....

    """

    """ old_header
        header_field = ['domain_name']
        base_A = '_a_'
        base_A_cname = '_a_cname_'
        base_A_rrsig = '_a_rrsig_'

        base_AAAA = '_aaaa_'
        base_AAAA_cname = '_aaaa_cname_'
        base_AAAA_rrsig = '_aaaa_rrsig_'

        base_TXT = '_txt_'
        base_TXT_cname = '_txt_cname_'
        base_TXT_rrsig = '_txt_rrsig_'
        base_TXT_txthash = '_txt_txthash_'

        base_MX = '_mx_'
        base_MX_cname = '_mx_cname_'
        base_MX_rrsig = '_mx_rrsig_'
        base_MX_mxhash = '_mx_mxhash_'

        base_SOA = '_soa_'
        base_SOA_cname = '_soa_cname_'
        base_SOA_rrsig = '_soa_rrsig_'

        base_DS = '_ds_'
        base_DS_cname = '_ds_cname_'
        base_DS_rrsig = '_ds_rrsig_'

        base_NS = '_ns_'
        base_NS_cname = '_ns_cname_'
        base_NS_nshash = '_ns_nshash_'

        base_DNSKEY = '_dnskey_'
        base_DNSKEY_cname = '_dnskey_cname_'
        base_DNSKEY_rrsig = '_dnskey_rrsig_'

        base_CAA = '_caa_'
        base_CAA_cname = '_caa_cname_'
        base_CAA_rrsig = '_caa_rrsig_'

        base_CDS = '_cds_'
        base_CDS_cname = '_cds_cname_'
        base_CDS_rrsig = '_cds_rrsig_'

        base_CDNSKEY = '_cdnskey_'
        base_CDNSKEY_rrsig = '_cdnskey_rrsig_'

        base_NSEC3PARAM = '_nsec3param_'
        base_NSEC3PARAM_rrsig = '_nsec3param_rrsig_'

        base_NSEC3 = '_nsec3_'

        count_dom = 0

        print('----------------------------CSV CREATION----------------------------\n')
        for i in range(0,max_A):
            header_field.extend([str(i)+base_A+'ipv4',str(i)+base_A+'response_ttl',
                                str(i)+base_A+'worker_id',str(i)+base_A+'rtt',str(i)+base_A+'status_code',
                                str(i)+base_A+'as',str(i)+base_A+'as_full',str(i)+base_A+'country',
                                str(i)+base_A+'ip_prefix'])

        for i in range(0,max_a_cname):
            header_field.extend([str(i)+base_A_cname+'cname_name',str(i)+base_A_cname+'response_name',str(i)+base_A_cname+'response_ttl',str(i)+base_A_cname+'worker_id',str(i)+base_A_cname+'rtt',
            str(i)+base_A_cname+'status_code'])
        for i in range(0,max_a_rrsig):
            header_field.extend([str(i)+base_A_rrsig+'type_covered',str(i)+base_A_rrsig+'algorithm',str(i)+base_A_rrsig+'labels',str(i)+base_A_rrsig+'original_ttl',
            str(i)+base_A_rrsig+'signature_inception',str(i)+base_A_rrsig+'signature_expiration',str(i)+base_A_rrsig+'key_tag',str(i)+base_A_rrsig+'signer_name',
            str(i)+base_A_rrsig+'signature', str(i)+base_A_rrsig+'response_name',str(i)+base_A_rrsig+'response_ttl',str(i)+base_A_rrsig+'worker_id',str(i)+base_A_rrsig+'rtt',str(i)+base_A_rrsig+'status_code'])

        for i in range(0,max_AAAA):
            header_field.extend([str(i)+base_AAAA+'ipv6',str(i)+base_AAAA+'response_ttl',
                                str(i)+base_AAAA+'worker_id',str(i)+base_AAAA+'rtt',str(i)+base_AAAA+'status_code',
                                str(i)+base_AAAA+'as',str(i)+base_AAAA+'as_full',str(i)+base_AAAA+'country',
                                str(i)+base_AAAA+'ip_prefix'])

        for i in range(0,max_aaaa_cname):
            header_field.extend([str(i)+base_AAAA_cname+'cname_name',str(i)+base_AAAA_cname+'response_name',str(i)+base_AAAA_cname+'response_ttl',str(i)+base_AAAA_cname+'worker_id',str(i)+base_AAAA_cname+'rtt',
            str(i)+base_AAAA_cname+'status_code'])
        for i in range(0,max_aaaa_rrsig):
            header_field.extend([str(i)+base_AAAA_rrsig+'type_covered',str(i)+base_AAAA_rrsig+'algorithm',str(i)+base_AAAA_rrsig+'labels',str(i)+base_AAAA_rrsig+'original_ttl',
            str(i)+base_AAAA_rrsig+'signature_inception',str(i)+base_AAAA_rrsig+'signature_expiration',str(i)+base_AAAA_rrsig+'key_tag',str(i)+base_AAAA_rrsig+'signer_name',
            str(i)+base_AAAA_rrsig+'signature', str(i)+base_AAAA_rrsig+'response_name',str(i)+base_AAAA_rrsig+'response_ttl',str(i)+base_AAAA_rrsig+'worker_id',str(i)+base_AAAA_rrsig+'rtt',str(i)+base_AAAA_rrsig+'status_code'])

        for i in range(0,max_MX):
            header_field.extend([str(i)+base_MX+'mx_address', str(i)+base_MX+'mx_preference',
                                str(i)+base_MX+'response_ttl',str(i)+base_MX+'worker_id',
                                str(i)+base_MX+'rtt', str(i)+base_MX+'status_code'])
        for i in range(0,max_mx_cname):
            header_field.extend([str(i)+base_MX_cname+'cname_name',str(i)+base_MX_cname+'response_name',str(i)+base_MX_cname+'response_ttl',str(i)+base_MX_cname+'worker_id',str(i)+base_MX_cname+'rtt',
            str(i)+base_MX_cname+'status_code'])
        for i in range(0,max_mx_rrsig):
            header_field.extend([str(i)+base_MX_rrsig+'type_covered',str(i)+base_MX_rrsig+'algorithm',str(i)+base_MX_rrsig+'labels',str(i)+base_MX_rrsig+'original_ttl',
            str(i)+base_MX_rrsig+'signature_inception',str(i)+base_MX_rrsig+'signature_expiration',str(i)+base_MX_rrsig+'key_tag',str(i)+base_MX_rrsig+'signer_name',
            str(i)+base_MX_rrsig+'signature', str(i)+base_MX_rrsig+'response_name',str(i)+base_MX_rrsig+'response_ttl',str(i)+base_MX_rrsig+'worker_id',str(i)+base_MX_rrsig+'rtt',str(i)+base_MX_rrsig+'status_code'])
        for i in range(0,max_mxhash):
            header_field.extend([str(i)+base_MX_mxhash+'mxset_hash_algorithm',str(i)+base_MX_mxhash+'mxset_hash',str(i)+base_MX_mxhash+'response_name',str(i)+base_MX_mxhash+'response_ttl',str(i)+base_MX_mxhash+'worker_id',
            str(i)+base_MX_mxhash+'rtt',str(i)+base_MX_mxhash+'status_code'])

        for i in range(0,max_SOA):
            header_field.extend([str(i)+base_SOA+'soa_mname',str(i)+base_SOA+'soa_rname',str(i)+base_SOA+'soa_serial',
                                str(i)+base_SOA+'soa_refresh',str(i)+base_SOA+'soa_retry',str(i)+base_SOA+'soa_expire',
                                str(i)+base_SOA+'soa_minimum',str(i)+base_SOA+'response_ttl',str(i)+base_SOA+'worker_id',
                                str(i)+base_SOA+'rtt',str(i)+base_SOA+'status_code'])
        for i in range(0,max_soa_cname):
            header_field.extend([str(i)+base_SOA_cname+'cname_name',str(i)+base_SOA_cname+'response_name',str(i)+base_SOA_cname+'response_ttl',str(i)+base_SOA_cname+'worker_id',str(i)+base_SOA_cname+'rtt',
            str(i)+base_SOA_cname+'status_code']) 
        for i in range(0,max_soa_rrsig):
            header_field.extend([str(i)+base_SOA_rrsig+'type_covered',str(i)+base_SOA_rrsig+'algorithm',str(i)+base_SOA_rrsig+'labels',str(i)+base_SOA_rrsig+'original_ttl',
            str(i)+base_SOA_rrsig+'signature_inception',str(i)+base_SOA_rrsig+'signature_expiration',str(i)+base_SOA_rrsig+'key_tag',str(i)+base_SOA_rrsig+'signer_name',
            str(i)+base_SOA_rrsig+'signature', str(i)+base_SOA_rrsig+'response_name',str(i)+base_SOA_rrsig+'response_ttl',str(i)+base_SOA_rrsig+'worker_id',str(i)+base_SOA_rrsig+'rtt',str(i)+base_SOA_rrsig+'status_code'])


        for i in range(0,max_TXT):
            header_field.extend([str(i)+base_TXT+'txt_text',str(i)+base_TXT+'response_ttl',
                                str(i)+base_TXT+'worker_id',str(i)+base_TXT+'rtt',str(i)+base_TXT+'status_code'])
        for i in range(0,max_txt_cname):
            header_field.extend([str(i)+base_TXT_cname+'cname_name',str(i)+base_TXT_cname+'response_name',str(i)+base_TXT_cname+'response_ttl',str(i)+base_TXT_cname+'worker_id',str(i)+base_TXT_cname+'rtt',
            str(i)+base_TXT_cname+'status_code'])
        for i in range(0,max_txt_rrsig):
            header_field.extend([str(i)+base_TXT_rrsig+'type_covered',str(i)+base_TXT_rrsig+'algorithm',str(i)+base_TXT_rrsig+'labels',str(i)+base_TXT_rrsig+'original_ttl',
            str(i)+base_TXT_rrsig+'signature_inception',str(i)+base_TXT_rrsig+'signature_expiration',str(i)+base_TXT_rrsig+'key_tag',str(i)+base_TXT_rrsig+'signer_name',
            str(i)+base_TXT_rrsig+'signature', str(i)+base_TXT_rrsig+'response_name',str(i)+base_TXT_rrsig+'response_ttl',str(i)+base_TXT_rrsig+'worker_id',str(i)+base_TXT_rrsig+'rtt',str(i)+base_TXT_rrsig+'status_code'])
        for i in range(0,max_txthash):
            header_field.extend([str(i)+base_TXT_txthash+'txt_hash_algorithm',str(i)+base_TXT_txthash+'txt_hash',str(i)+base_TXT_txthash+'response_name',str(i)+base_TXT_txthash+'response_ttl',str(i)+base_TXT_txthash+'worker_id',
            str(i)+base_TXT_txthash+'rtt',str(i)+base_TXT_txthash+'status_code'])
        
        for i in range(0,max_NS):
            header_field.extend([str(i)+base_NS+'ns_address',str(i)+base_NS+'response_ttl',str(i)+base_NS+'worker_id',
                                str(i)+base_NS+'rtt',str(i)+base_NS+'status_code'])
        for i in range(0,max_ns_cname):
            header_field.extend([str(i)+base_NS_cname+'cname_name',str(i)+base_NS_cname+'response_name',str(i)+base_NS_cname+'response_ttl',str(i)+base_NS_cname+'worker_id',str(i)+base_NS_cname+'rtt',
            str(i)+base_NS_cname+'status_code'])
        for i in range(0,max_nshash):
            header_field.extend([str(i)+base_NS_nshash+'nsset_hash_algorithm',str(i)+base_NS_nshash+'nsset_hash',str(i)+base_NS_nshash+'response_name',str(i)+base_NS_nshash+'response_ttl',str(i)+base_NS_nshash+'worker_id',
            str(i)+base_NS_nshash+'rtt',str(i)+base_NS_nshash+'status_code'])

        for i in range(0,max_DS):
            header_field.extend([str(i)+base_DS+'ds_key_tag',str(i)+base_DS+'ds_algorithm',str(i)+base_DS+'ds_digest_type',
                                str(i)+base_DS+'ds_digest',str(i)+base_DS+'response_ttl',str(i)+base_DS+'worker_id',
                                str(i)+base_DS+'rtt',str(i)+base_DS+'status_code'])
        for i in range(0,max_ds_cname):
            header_field.extend([str(i)+base_DS_cname+'cname_name',str(i)+base_DS_cname+'response_name',str(i)+base_DS_cname+'response_ttl',str(i)+base_DS_cname+'worker_id',str(i)+base_DS_cname+'rtt',
            str(i)+base_DS_cname+'status_code'])
        for i in range(0,max_ds_rrsig):
            header_field.extend([str(i)+base_DS_rrsig+'rrsig_type_covered',str(i)+base_DS_rrsig+'rrsig_algorithm',str(i)+base_DS_rrsig+'rrsig_labels',str(i)+base_DS_rrsig+'rrsig_original_ttl',
            str(i)+base_DS_rrsig+'rrsig_signature_inception',str(i)+base_DS_rrsig+'rrsig_signature_expiration',str(i)+base_DS_rrsig+'rrsig_key_tag',str(i)+base_DS_rrsig+'rrsig_signer_name',
            str(i)+base_DS_rrsig+'rrsig_signature', str(i)+base_DS_rrsig+'response_name',str(i)+base_DS_rrsig+'response_ttl',str(i)+base_DS_rrsig+'worker_id',str(i)+base_DS_rrsig+'rtt',str(i)+base_DS_rrsig+'status_code'])

        for i in range(0,max_DNSKEY):
            header_field.extend([str(i)+base_DNSKEY+'flags',str(i)+base_DNSKEY+'protocol',str(i)+base_DNSKEY+'algorithm',
                                str(i)+base_DNSKEY+'pk_eccgost_x',str(i)+base_DNSKEY+'response_ttl',str(i)+base_DNSKEY+'worker_id',
                                str(i)+base_DNSKEY+'rtt',str(i)+base_DNSKEY+'status_code'])
        for i in range(0,max_dnskey_cname):
            header_field.extend([str(i)+base_DNSKEY_cname+'cname_name',str(i)+base_DNSKEY_cname+'response_name',str(i)+base_DNSKEY_cname+'response_ttl',str(i)+base_DNSKEY_cname+'worker_id',str(i)+base_DNSKEY_cname+'rtt',
            str(i)+base_DNSKEY_cname+'status_code'])
        for i in range(0,max_dnskey_rrsig):
            header_field.extend([str(i)+base_DNSKEY_rrsig+'rrsig_type_covered',str(i)+base_DNSKEY_rrsig+'rrsig_algorithm',str(i)+base_DNSKEY_rrsig+'rrsig_labels',str(i)+base_DNSKEY_rrsig+'rrsig_original_ttl',
            str(i)+base_DNSKEY_rrsig+'rrsig_signature_inception',str(i)+base_DNSKEY_rrsig+'rrsig_signature_expiration',str(i)+base_DNSKEY_rrsig+'rrsig_key_tag',str(i)+base_DNSKEY_rrsig+'rrsig_signer_name',
            str(i)+base_DNSKEY_rrsig+'rrsig_signature', str(i)+base_DNSKEY_rrsig+'response_name',str(i)+base_DNSKEY_rrsig+'response_ttl',str(i)+base_DNSKEY_rrsig+'worker_id',str(i)+base_DNSKEY_rrsig+'rtt',str(i)+base_DNSKEY_rrsig+'status_code'])  
        
        for i in range(0,max_CAA):
            header_field.extend([str(i)+base_CAA+'flags',str(i)+base_CAA+'tag',str(i)+base_CAA+'value',str(i)+base_CAA+'response_ttl',str(i)+base_CAA+'worker_id',
                                str(i)+base_CAA+'rtt',str(i)+base_CAA+'status_code'])
        for i in range(0,max_caa_cname):
            header_field.extend([str(i)+base_CAA_cname+'cname_name',str(i)+base_CAA_cname+'response_name',str(i)+base_CAA_cname+'response_ttl',str(i)+base_CAA_cname+'worker_id',str(i)+base_CAA_cname+'rtt',
            str(i)+base_CAA_cname+'status_code'])
        for i in range(0,max_caa_rrsig):
            header_field.extend([str(i)+base_CAA_rrsig+'rrsig_type_covered',str(i)+base_CAA_rrsig+'rrsig_algorithm',str(i)+base_CAA_rrsig+'rrsig_labels',str(i)+base_CAA_rrsig+'rrsig_original_ttl',
            str(i)+base_CAA_rrsig+'rrsig_signature_inception',str(i)+base_CAA_rrsig+'rrsig_signature_expiration',str(i)+base_CAA_rrsig+'rrsig_key_tag',str(i)+base_CAA_rrsig+'rrsig_signer_name',
            str(i)+base_CAA_rrsig+'rrsig_signature', str(i)+base_CAA_rrsig+'response_name',str(i)+base_CAA_rrsig+'response_ttl',str(i)+base_CAA_rrsig+'worker_id',str(i)+base_CAA_rrsig+'rtt',str(i)+base_CAA_rrsig+'status_code'])

        for i in range(0,max_CDS):
            header_field.extend([str(i)+base_CDS+'key_tag',str(i)+base_CDS+'algorithm',str(i)+base_CDS+'digest_type',str(i)+base_CDS+'digest',
                                str(i)+base_CDS+'response_ttl',str(i)+base_CDS+'worker_id',str(i)+base_CDS+'rtt',str(i)+base_CDS+'status_code'])
        for i in range(0,max_cds_cname):
            header_field.extend([str(i)+base_CDS_cname+'cname_name', str(i)+base_CDS_cname+'response_name', str(i)+base_CDS_cname+'response_ttl',
                                str(i)+base_CDS_cname+'worker_id', str(i)+base_CDS_cname+'rtt', str(i)+base_CDS_cname+'status_code'])
        for i in range(0,max_cds_rrsig):
            header_field.extend([str(i)+base_CDS_rrsig+'rrsig_type_covered',str(i)+base_CDS_rrsig+'rrsig_algorithm',str(i)+base_CDS_rrsig+'rrsig_labels',str(i)+base_CDS_rrsig+'rrsig_original_ttl',
            str(i)+base_CDS_rrsig+'rrsig_signature_inception',str(i)+base_CDS_rrsig+'rrsig_signature_expiration',str(i)+base_CDS_rrsig+'rrsig_key_tag',str(i)+base_CDS_rrsig+'rrsig_signer_name',
            str(i)+base_CDS_rrsig+'rrsig_signature', str(i)+base_CDS_rrsig+'response_name',str(i)+base_CDS_rrsig+'response_ttl',str(i)+base_CDS_rrsig+'worker_id',str(i)+base_CDS_rrsig+'rtt',str(i)+base_CDS_rrsig+'status_code'])  

        for i in range(0,max_CDNSKEY):
            header_field.extend([str(i)+base_CDNSKEY+'flags',str(i)+base_CDNSKEY+'protocol',str(i)+base_CDNSKEY+'algorithm', str(i)+base_CDNSKEY+'pk_eccgost_x',
                                str(i)+base_CDNSKEY+'response_ttl',str(i)+base_CDNSKEY+'worker_id',str(i)+base_CDNSKEY+'rtt',str(i)+base_CDNSKEY+'status_code'])
        for i in range(0,max_cdnskey_rrsig):
            header_field.extend([str(i)+base_CDNSKEY_rrsig+'rrsig_type_covered',str(i)+base_CDNSKEY_rrsig+'rrsig_algorithm',str(i)+base_CDNSKEY_rrsig+'rrsig_labels',str(i)+base_CDNSKEY_rrsig+'rrsig_original_ttl',
            str(i)+base_CDNSKEY_rrsig+'rrsig_signature_inception',str(i)+base_CDNSKEY_rrsig+'rrsig_signature_expiration',str(i)+base_CDNSKEY_rrsig+'rrsig_key_tag',str(i)+base_CDNSKEY_rrsig+'rrsig_signer_name',
            str(i)+base_CDNSKEY_rrsig+'rrsig_signature', str(i)+base_CDNSKEY_rrsig+'response_name',str(i)+base_CDNSKEY_rrsig+'response_ttl',str(i)+base_CDNSKEY_rrsig+'worker_id',str(i)+base_CDNSKEY_rrsig+'rtt',str(i)+base_CDNSKEY_rrsig+'status_code']) 

        for i in range(0,max_NSEC3PARAM):
            header_field.extend([str(i)+base_NSEC3PARAM+'hash_algorithm',str(i)+base_NSEC3PARAM+'flags',str(i)+base_NSEC3PARAM+'itrations', str(i)+base_NSEC3PARAM+'salt',
                                str(i)+base_NSEC3PARAM+'response_ttl',str(i)+base_NSEC3PARAM+'worker_id',str(i)+base_NSEC3PARAM+'rtt',str(i)+base_NSEC3PARAM+'status_code'])
        for i in range(0,max_nsec3param_rrsig):
            header_field.extend([str(i)+base_NSEC3PARAM_rrsig+'rrsig_type_covered',str(i)+base_NSEC3PARAM_rrsig+'rrsig_algorithm',str(i)+base_NSEC3PARAM_rrsig+'rrsig_labels',str(i)+base_NSEC3PARAM_rrsig+'rrsig_original_ttl',
            str(i)+base_NSEC3PARAM_rrsig+'rrsig_signature_inception',str(i)+base_NSEC3PARAM_rrsig+'rrsig_signature_expiration',str(i)+base_NSEC3PARAM_rrsig+'rrsig_key_tag',str(i)+base_NSEC3PARAM_rrsig+'rrsig_signer_name',
            str(i)+base_NSEC3PARAM_rrsig+'rrsig_signature', str(i)+base_NSEC3PARAM_rrsig+'response_name',str(i)+base_NSEC3PARAM_rrsig+'response_ttl',
            str(i)+base_NSEC3PARAM_rrsig+'worker_id',str(i)+base_NSEC3PARAM_rrsig+'rtt',str(i)+base_NSEC3PARAM_rrsig+'status_code']) 

        for i in range(0,max_NSEC3):
            header_field.extend([str(i)+base_NSEC3+'hash_algorithm',str(i)+base_NSEC3+'flags',str(i)+base_NSEC3+'iterations',str(i)+base_NSEC3+'salt',
                                str(i)+base_NSEC3+'next_domain_name_hash',str(i)+base_NSEC3+'owner_rrset_types',str(i)+base_NSEC3+'response_name',
                                str(i)+base_NSEC3+'response_ttl',str(i)+base_NSEC3+'worker_id', str(i)+base_NSEC3+'rtt', str(i)+base_NSEC3+'status_code'])

    """
    fs_tail = ['response_ttl','worker_id','rtt','status_code']
    cname_fs = ['cname_name','response_name'] + fs_tail
    dname_fs = ['dname_name','response_name'] + fs_tail
    rrsig_fs = ['type_covered','algorithm','labels','original_ttl','signature_inception','signature_expiration',
                'key_tag','signer_name','signature','response_name'] + fs_tail
    mxhash_fs = ['mxset_hash_algorithm','mxset_hash','response_name'] + fs_tail
    txthash_fs = ['hash_algorithm','hash','response_name'] + fs_tail
    nshash_fs = ['nsset_hash_algorithm','nsset_hash','response_name'] + fs_tail
    cdnskey_fs = ['flags','protocol','algorithm','pk_eccgost_x'] + fs_tail
    nsec3param_fs = ['hash_algorithm','flags','iterations','salt'] + fs_tail
    nsec3_fs = ['hash_algorithm','flags','iterations','salt','next_domain_name_hash','owner_rrset_types','response_name'] + fs_tail
    a_fs = ['ip4'] + fs_tail + ['as','as_full','country','ip_prefix']
    aaaa_fs = ['ip6'] + fs_tail + ['as','as_full','country','ip_prefix']
    mx_fs = ['address','preference'] + fs_tail
    txt_fs = ['text'] + fs_tail
    soa_fs = ['mname','rname','serial','refresh','retry','expire','minimum'] + fs_tail
    ns_fs = ['address'] + fs_tail
    ds_fs  = ['key_tag','algorithm','digest_type','digest'] + fs_tail
    dnskey_fs = cdnskey_fs
    caa_fs = ['flags','tag','value'] + fs_tail
    cds_fs = ds_fs
    nsec3param_fs = ['hash_algorithm','flags','iterations','salt'] + fs_tail
    afsdb_fs = rrsig_fs
    header_field = ['domain']


    dict_type_nfield = {
        'A': { 'nf':9,
            'max': max_A,
            'fields': a_fs},
        'A_CNAME': {'nf':6,
                    'max': max_a_cname,
                    'fields': cname_fs},
        'A_RRSIG': {'nf':14,
                    'max': max_a_rrsig,
                    'fields':rrsig_fs},
        'A_DNAME':{'nf':6,
                    'max': max_a_dname,
                'fields':dname_fs},
        'AAAA': {'nf':9,
                'max': max_AAAA,
                'fields':aaaa_fs},
        'AAAA_CNAME': {'nf':6,
                    'max': max_aaaa_cname,
                    'fields':cname_fs},
        'AAAA_RRSIG': {'nf':14,
                    'max': max_aaaa_rrsig,
                    'fields':rrsig_fs},
        'AAAA_DNAME':{'nf':6,
                    'max': max_a_dname,
                'fields':dname_fs},
        'MX': {'nf':6,
            'max':max_MX,
            'fields':mx_fs},
        'MX_CNAME': {'nf':6,
            'max':max_mx_cname,
            'fields':cname_fs},
        'MX_RRSIG': {'nf':14,
            'max':max_mx_rrsig,
            'fields':rrsig_fs},
        'MX_MXHASH': {'nf':7,
            'max':max_mxhash,
            'fields':mxhash_fs},
        'MX_DNAME':{'nf':6,
                    'max': max_mx_dname,
                'fields':dname_fs},
        'SOA': {'nf':11,
            'max':max_SOA,
            'fields':soa_fs},
        'SOA_CNAME': {'nf':6,
            'max':max_soa_cname,
            'fields':cname_fs},
        'SOA_RRSIG': {'nf':14,
            'max':max_soa_rrsig,
            'fields':rrsig_fs},
        'SOA_DNAME':{'nf':6,
                    'max': max_soa_dname,
                'fields':dname_fs},
        'TXT': {'nf':5,
            'max': max_TXT,
            'fields':txt_fs},
        'TXT_CNAME': {'nf':6,
            'max': max_txt_cname,
            'fields':cname_fs},
        'TXT_RRSIG': {'nf': 14,
            'max': max_txt_rrsig,
            'fields':rrsig_fs},
        'TXT_TXTHASH': {'nf': 7,
            'max': max_txthash,
            'fields':txthash_fs},
        'TXT_DNAME':{'nf':6,
                    'max': max_txt_dname,
                'fields':dname_fs},
        'NS': {'nf': 5,
            'max':max_NS,
            'fields':ns_fs},
        'NS_CNAME': {'nf':6,
            'max':max_ns_cname,
            'fields':cname_fs},
        'NS_RRSIG': {'nf':14,
            'max':max_ns_rrsig,
            'fields':rrsig_fs},
        'NS_NSHASH': {'nf': 7,
            'max': max_nshash,
            'fields':nshash_fs},
        'NS_DNAME':{'nf':6,
                    'max': max_ns_dname,
                'fields':dname_fs},
        'DS': {'nf': 8,
            'max': max_DS,
            'fields':ds_fs},
        'DS_CNAME': {'nf':6,
            'max': max_ds_cname,
            'fields':cname_fs},
        'DS_RRSIG': {'nf': 14,
            'max': max_ds_rrsig,
            'fields':rrsig_fs},
        'DS_DNAME':{'nf':6,
                    'max': max_ds_dname,
                'fields':dname_fs},
        'DNSKEY':{'nf': 8,
            'max':max_DNSKEY,
            'fields':dnskey_fs},
        'DNSKEY_CNAME': {'nf':6,
            'max': max_dnskey_cname,
            'fields':cname_fs},
        'DNSKEY_RRSIG': {'nf': 14,
            'max': max_dnskey_rrsig,
            'fields':rrsig_fs},
        'DNSKEY_DNAME':{'nf':6,
                    'max': max_dnskey_dname,
                'fields':dname_fs},
        'CAA': {'nf':7,
            'max': max_CAA,
            'fields':caa_fs},
        'CAA_CNAME': {'nf':6,
            'max': max_caa_cname,
            'fields':cname_fs},
        'CAA_RRSIG': {'nf': 14,
            'max':max_caa_rrsig,
            'fields':rrsig_fs},
        'CAA_DNAME':{'nf':6,
                    'max': max_caa_dname,
                'fields':dname_fs},
        'CDS': {'nf':8,
            'max':max_CDS,
            'fields':cds_fs},
        'CDS_CNAME': {'nf':6,
            'max': max_cdnskey_cname,
            'fields':cname_fs},
        'CDS_RRSIG': {'nf':14,
            'max': max_cds_rrsig,
            'fields':rrsig_fs},
        'CDNSKEY': {'nf':8,
            'max':max_CDNSKEY,
            'fields':cdnskey_fs},
        'CDNSKEY_CNAME': {'nf':6,
            'max':max_cdnskey_cname,
            'fields':cname_fs},
        'CDNSKEY_RRSIG': {'nf':14,
            'max':max_cdnskey_rrsig,
            'fields':rrsig_fs},
        'NSEC3PARAM': {'nf':8,
            'max':max_NSEC3PARAM,
            'fields':nsec3param_fs},
        'NSEC3PARAM_RRSIG': {'nf':14,
            'max':max_nsec3param_rrsig,
            'fields':rrsig_fs},
        'NSEC3': {'nf':11,
            'max':max_NSEC3,
            'fields':nsec3_fs},
        'AFSDB_RRSIG':{'nf':14,
                'max': max_afsdb_rrsig,
                'fields': afsdb_fs    
        }
    }


    for item in dict_type_nfield.items():
        for k in range(0,item[1]['max']):
            header_d = []
            for j in range(len(item[1]['fields'])):
                header_d.append('OI_'+str(k)+'_'+str(item[0])+'_'+ str(item[1]['fields'][j]))
            header_field.extend(header_d)

    bar_csv = Bar('Creation Csv', fill='#', max = len(doms))
    with open('normalization.csv', mode = 'a') as csv_normalization_out:
        writer = csv.writer(csv_normalization_out)
        writer.writerow(header_field)
        for dom in doms:
            dom_dot = dom + '.'
            count_dom += 1
            data = []
            if dom_dot in doms_oi:
                data.append(dom)
                for typeq in dict_type_nfield.keys():
                    for i in range(0,len(dict_info[dom_dot][typeq])):
                        for d in dict_info[dom_dot][typeq][i]:
                            data.append(d)
                    if len(dict_info[dom_dot][typeq]) != int(dict_type_nfield[typeq]['max']):
                        fil = int(dict_type_nfield[typeq]['nf'])
                        diff = int(dict_type_nfield[typeq]['max']) - len(dict_info[dom_dot][typeq])
                        zeros = ['null' for i in range(diff*fil)]
                        for zero in zeros:
                            data.append(zero)
            else:
                data.append(dom)
                offset = 0
                for item in dict_type_nfield.items():
                    offset += item[1]['nf']*item[1]['max']
                zeros =['null' for i in range(offset)]
                for zero in zeros:
                    data.append(zero) 
            writer.writerow(data)
            bar_csv.next()       
    
        


    ''' vecchia(estesa) versione di quello fatto sopra 
                    
        bar = Bar('Creation', fill='$', max =len(doms))
        with open('normalization.csv', mode='a') as csv_normalization:
            writer = csv.writer(csv_normalization)
            writer.writerow(header_field)
            for dom in doms:
                dom_dot = dom + '.'
                dom_www = 'www.' + dom_dot
                count_dom += 1
                data = []
                if dom_dot in doms_oi:
                    data.append(dom_dot)
                    for i in range(0,len(dict_info[dom_dot]['A'])):
                        for d in dict_info[dom_dot]['A'][i]:
                            data.append(d)
                    if len(dict_info[dom_dot]['A']) != max_A:
                        diff = max_A - len(dict_info[dom_dot]['A'])
                        zeros = ['null' for i in range(diff*9)]
                        for zero in zeros:
                            data.append(zero)
                    
                    for i in range(0,len(dict_info[dom_dot]['A_CNAME'])):
                        for d in dict_info[dom_dot]['A_CNAME'][i]:
                            data.append(d)
                    if len(dict_info[dom_dot]['A_CNAME']) != max_a_cname:
                        diff = max_a_cname - len(dict_info[dom_dot]['A_CNAME'])
                        zeros = ['null' for i in range(diff*6)]
                        for zero in zeros:
                            data.append(zero)
                    
                    for i in range(0,len(dict_info[dom_dot]['A_RRSIG'])):
                        for d in dict_info[dom_dot]['A_RRSIG'][i]:
                            data.append(d)
                    if len(dict_info[dom_dot]['A_RRSIG']) != max_a_rrsig:
                        diff = max_a_rrsig - len(dict_info[dom_dot]['A_RRSIG'])
                        zeros = ['null' for i in range(diff*14)]
                        for zero in zeros:
                            data.append(zero)
                        
                    for i in range(0,len(dict_info[dom_dot]['AAAA'])):
                        for d in dict_info[dom_dot]['AAAA'][i]:
                            data.append(d)
                    if len(dict_info[dom_dot]['AAAA']) != max_AAAA:
                        diff = max_AAAA - len(dict_info[dom_dot]['AAAA'])
                        zeros = ['null' for i in range(diff*9)]
                        for zero in zeros:
                            data.append(zero)
                    
                    for i in range(0,len(dict_info[dom_dot]['AAAA_CNAME'])):
                        for d in dict_info[dom_dot]['AAAA_CNAME'][i]:
                            data.append(d)
                    if len(dict_info[dom_dot]['AAAA_CNAME']) != max_aaaa_cname:
                        diff = max_aaaa_cname - len(dict_info[dom_dot]['AAAA_CNAME'])
                        zeros = ['null' for i in range(diff*6)]
                        for zero in zeros:
                            data.append(zero)
                    
                    for i in range(0,len(dict_info[dom_dot]['AAAA_RRSIG'])):
                        for d in dict_info[dom_dot]['AAAA_RRSIG'][i]:
                            data.append(d)
                    if len(dict_info[dom_dot]['AAAA_RRSIG']) != max_aaaa_rrsig:
                        diff = max_aaaa_rrsig - len(dict_info[dom_dot]['AAAA_RRSIG'])
                        zeros = ['null' for i in range(diff*14)]
                        for zero in zeros:
                            data.append(zero)
                    
                    for i in range(0,len(dict_info[dom_dot]['MX'])):
                        for d in dict_info[dom_dot]['MX'][i]:
                            data.append(d)
                    if len(dict_info[dom_dot]['MX']) != max_MX:
                        diff = max_MX - len(dict_info[dom_dot]['MX'])
                        zeros = ['null' for i in range(diff*6)]
                        for zero in zeros:
                            data.append(zero)
                    
                    for i in range(0,len(dict_info[dom_dot]['MX_CNAME'])):
                        for d in dict_info[dom_dot]['MX_CNAME'][i]:
                            data.append(d)
                    if len(dict_info[dom_dot]['MX_CNAME']) != max_mx_cname:
                        diff = max_mx_cname - len(dict_info[dom_dot]['MX_CNAME'])
                        zeros = ['null' for i in range(diff*6)]
                        for zero in zeros:
                            data.append(zero)
                    
                    for i in range(0,len(dict_info[dom_dot]['MX_RRSIG'])):
                        for d in dict_info[dom_dot]['MX_RRSIG'][i]:
                            data.append(d)
                    if len(dict_info[dom_dot]['MX_RRSIG']) != max_mx_rrsig:
                        diff = max_mx_rrsig - len(dict_info[dom_dot]['MX_RRSIG'])
                        zeros = ['null' for i in range(diff*14)]
                        for zero in zeros:
                            data.append(zero)     
                    
                    for i in range(0,len(dict_info[dom_dot]['MX_MXHASH'])):
                        for d in dict_info[dom_dot]['MX_MXHASH'][i]:
                            data.append(d)
                    if len(dict_info[dom_dot]['MX_MXHASH']) != max_mxhash:
                        diff = max_mxhash - len(dict_info[dom_dot]['MX_MXHASH'])
                        zeros = ['null' for i in range(diff*7)]
                        for zero in zeros:
                            data.append(zero)
                    
                    for i in range(0,len(dict_info[dom_dot]['SOA'])):
                        for d in dict_info[dom_dot]['SOA'][i]:
                            data.append(d)
                    if len(dict_info[dom_dot]['SOA']) != max_SOA:
                        diff = max_SOA - len(dict_info[dom_dot]['SOA'])
                        zeros = ['null' for i in range(diff*11)]
                        for zero in zeros:
                            data.append(zero)
                    
                    for i in range(0,len(dict_info[dom_dot]['SOA_CNAME'])):
                        for d in dict_info[dom_dot]['SOA_CNAME'][i]:
                            data.append(d)
                    if len(dict_info[dom_dot]['SOA_CNAME']) != max_soa_cname:
                        diff = max_soa_cname - len(dict_info[dom_dot]['SOA_CNAME'])
                        zeros = ['null' for i in range(diff*6)]
                        for zero in zeros:
                            data.append(zero)
                    
                    for i in range(0,len(dict_info[dom_dot]['SOA_RRSIG'])):
                        for d in dict_info[dom_dot]['SOA_RRSIG'][i]:
                            data.append(d)
                    if len(dict_info[dom_dot]['SOA_RRSIG']) != max_soa_rrsig:
                        diff = max_soa_rrsig - len(dict_info[dom_dot]['SOA_RRSIG'])
                        zeros = ['null' for i in range(diff*14)]
                        for zero in zeros:
                            data.append(zero)    
                    
                    for i in range(0,len(dict_info[dom_dot]['TXT'])):
                        for d in dict_info[dom_dot]['TXT'][i]:
                            data.append(d)
                    if len(dict_info[dom_dot]['TXT']) != max_TXT:
                        diff = max_TXT - len(dict_info[dom_dot]['TXT'])
                        zeros = ['null' for i in range(diff*5)]
                        for zero in zeros:
                            data.append(zero)
                        
                    for i in range(0,len(dict_info[dom_dot]['TXT_CNAME'])):
                        for d in dict_info[dom_dot]['TXT_CNAME'][i]:
                            data.append(d)
                    if len(dict_info[dom_dot]['TXT_CNAME']) != max_txt_cname:
                        diff = max_txt_cname - len(dict_info[dom_dot]['TXT_CNAME'])
                        zeros = ['null' for i in range(diff*6)]
                        for zero in zeros:
                            data.append(zero)
                    
                    for i in range(0,len(dict_info[dom_dot]['TXT_RRSIG'])):
                        for d in dict_info[dom_dot]['TXT_RRSIG'][i]:
                            data.append(d)
                    if len(dict_info[dom_dot]['TXT_RRSIG']) != max_txt_rrsig:
                        diff = max_txt_rrsig - len(dict_info[dom_dot]['TXT_RRSIG'])
                        zeros = ['null' for i in range(diff*14)]
                        for zero in zeros:
                            data.append(zero)     
                
                    for i in range(0,len(dict_info[dom_dot]['TXT_TXTHASH'])):
                        for d in dict_info[dom_dot]['TXT_TXTHASH'][i]:
                            data.append(d)
                    if len(dict_info[dom_dot]['TXT_TXTHASH']) != max_txthash:
                        diff = max_txthash - len(dict_info[dom_dot]['TXT_TXTHASH'])
                        zeros = ['null' for i in range(diff*7)]
                        for zero in zeros:
                            data.append(zero)
                    
                    for i in range(0,len(dict_info[dom_dot]['NS'])):
                        for d in dict_info[dom_dot]['NS'][i]:
                            data.append(d)
                    if len(dict_info[dom_dot]['NS']) != max_NS:
                        diff = max_NS - len(dict_info[dom_dot]['NS'])
                        zeros = ['null' for i in range(diff*5)]
                        for zero in zeros:
                            data.append(zero)
                    
                    for i in range(0,len(dict_info[dom_dot]['NS_CNAME'])):
                        for d in dict_info[dom_dot]['NS_CNAME'][i]:
                            data.append(d)
                    if len(dict_info[dom_dot]['NS_CNAME']) != max_ns_cname:
                        diff = max_ns_cname - len(dict_info[dom_dot]['NS_CNAME'])
                        zeros = ['null' for i in range(diff*6)]
                        for zero in zeros:
                            data.append(zero)
                            
                    for i in range(0,len(dict_info[dom_dot]['NS_NSHASH'])):
                        for d in dict_info[dom_dot]['NS_NSHASH'][i]:
                            data.append(d)
                    if len(dict_info[dom_dot]['NS_NSHASH']) != max_nshash:
                        diff = max_nshash - len(dict_info[dom_dot]['NS_NSHASH'])
                        zeros = ['null' for i in range(diff*7)]
                        for zero in zeros:
                            data.append(zero) 
                        
                    for i in range(0,len(dict_info[dom_dot]['DS'])):
                        for d in dict_info[dom_dot]['DS'][i]:
                            data.append(d)
                    if len(dict_info[dom_dot]['DS']) != max_DS:
                        diff = max_DS - len(dict_info[dom_dot]['DS'])
                        zeros = ['null' for i in range(diff*8)]
                        for zero in zeros:
                            data.append(zero) 
                    
                    for i in range(0,len(dict_info[dom_dot]['DS_CNAME'])):
                        for d in dict_info[dom_dot]['DS_CNAME'][i]:
                            data.append(d)
                    if len(dict_info[dom_dot]['DS_CNAME']) != max_ds_cname:
                        diff = max_ds_cname - len(dict_info[dom_dot]['DS_CNAME'])
                        zeros = ['null' for i in range(diff*6)]
                        for zero in zeros:
                            data.append(zero)
                    
                    for i in range(0,len(dict_info[dom_dot]['DS_RRSIG'])):
                        for d in dict_info[dom_dot]['DS_RRSIG'][i]:
                            data.append(d)
                    if len(dict_info[dom_dot]['DS_RRSIG']) != max_ds_rrsig:
                        diff = max_ds_rrsig - len(dict_info[dom_dot]['DS_RRSIG'])
                        zeros = ['null' for i in range(diff*14)]
                        for zero in zeros:
                            data.append(zero) 
                    
                    for i in range(0,len(dict_info[dom_dot]['DNSKEY'])):
                        for d in dict_info[dom_dot]['DNSKEY'][i]:
                            data.append(d)
                    if len(dict_info[dom_dot]['DNSKEY']) != max_DNSKEY:
                        diff = max_DNSKEY - len(dict_info[dom_dot]['DNSKEY'])
                        zeros = ['null' for i in range(diff*8)]
                        for zero in zeros:
                            data.append(zero)
                    
                    for i in range(0,len(dict_info[dom_dot]['DNSKEY_CNAME'])):
                        for d in dict_info[dom_dot]['DNSKEY_CNAME'][i]:
                            data.append(d)
                    if len(dict_info[dom_dot]['DNSKEY_CNAME']) != max_dnskey_cname:
                        diff = max_dnskey_cname - len(dict_info[dom_dot]['DNSKEY_CNAME'])
                        zeros = ['null' for i in range(diff*6)]
                        for zero in zeros:
                            data.append(zero)
                    
                    for i in range(0,len(dict_info[dom_dot]['DNSKEY_RRSIG'])):
                        for d in dict_info[dom_dot]['DNSKEY_RRSIG'][i]:
                            data.append(d)
                    if len(dict_info[dom_dot]['DNSKEY_RRSIG']) != max_dnskey_rrsig:
                        diff = max_dnskey_rrsig - len(dict_info[dom_dot]['DNSKEY_RRSIG'])
                        zeros = ['null' for i in range(diff*14)]
                        for zero in zeros:
                            data.append(zero)   
                            
                    for i in range(0,len(dict_info[dom_dot]['CAA'])):
                        for d in dict_info[dom_dot]['CAA'][i]:
                            data.append(d)
                    if len(dict_info[dom_dot]['CAA']) != max_CAA:
                        diff = max_CAA - len(dict_info[dom_dot]['CAA'])
                        zeros = ['null' for i in range(diff*7)]
                        for zero in zeros:
                            data.append(zero)
                    
                    for i in range(0,len(dict_info[dom_dot]['CAA_CNAME'])):
                        for d in dict_info[dom_dot]['CAA_CNAME'][i]:
                            data.append(d)
                    if len(dict_info[dom_dot]['CAA_CNAME']) != max_caa_cname:
                        diff = max_caa_cname - len(dict_info[dom_dot]['CAA_CNAME'])
                        zeros = ['null' for i in range(diff*6)]
                        for zero in zeros:
                            data.append(zero)
                    
                    for i in range(0,len(dict_info[dom_dot]['CAA_RRSIG'])):
                        for d in dict_info[dom_dot]['CAA_RRSIG'][i]:
                            data.append(d)
                    if len(dict_info[dom_dot]['CAA_RRSIG']) != max_caa_rrsig:
                        diff = max_caa_rrsig - len(dict_info[dom_dot]['CAA_RRSIG'])
                        zeros = ['null' for i in range(diff*14)]
                        for zero in zeros:
                            data.append(zero)
                            
                    for i in range(0,len(dict_info[dom_dot]['CDS'])):
                        for d in dict_info[dom_dot]['CDS'][i]:
                            data.append(d)
                    if len(dict_info[dom_dot]['CDS']) != max_CDS:
                        diff = max_CDS - len(dict_info[dom_dot]['CDS'])
                        zeros = ['null' for i in range(diff*8)]
                        for zero in zeros:
                            data.append(zero)
                    
                    for i in range(0,len(dict_info[dom_dot]['CDS_CNAME'])):
                        for d in dict_info[dom_dot]['CDS_CNAME'][i]:
                            data.append(d)
                    if len(dict_info[dom_dot]['CDS_CNAME']) != max_cds_cname:
                        diff = max_cds_cname - len(dict_info[dom_dot]['CDS_CNAME'])
                        zeros = ['null' for i in range(diff*6)]
                        for zero in zeros:
                            data.append(zero)
                    
                    for i in range(0,len(dict_info[dom_dot]['CDS_RRSIG'])):
                        for d in dict_info[dom_dot]['CDS_RRSIG'][i]:
                            data.append(d)
                    if len(dict_info[dom_dot]['CDS_RRSIG']) != max_cds_rrsig:
                        diff = max_cds_rrsig - len(dict_info[dom_dot]['CDS_RRSIG'])
                        zeros = ['null' for i in range(diff*14)]
                        for zero in zeros:
                            data.append(zero)
                            
                    for i in range(0,len(dict_info[dom_dot]['CDNSKEY'])):
                        for d in dict_info[dom_dot]['CDNSKEY'][i]:
                            data.append(d)
                    if len(dict_info[dom_dot]['CDNSKEY']) != max_CDNSKEY:
                        diff = max_CDNSKEY - len(dict_info[dom_dot]['CDNSKEY'])
                        zeros = ['null' for i in range(diff*8)]
                        for zero in zeros:
                            data.append(zero)
                    
                    for i in range(0,len(dict_info[dom_dot]['CDNSKEY_RRSIG'])):
                        for d in dict_info[dom_dot]['CDNSKEY_RRSIG'][i]:
                            data.append(d)
                    if len(dict_info[dom_dot]['CDNSKEY_RRSIG']) != max_cdnskey_rrsig:
                        diff = max_cdnskey_rrsig - len(dict_info[dom_dot]['CDNSKEY_RRSIG'])
                        zeros = ['null' for i in range(diff*14)]
                        for zero in zeros:
                            data.append(zero)
                    
                    for i in range(0,len(dict_info[dom_dot]['NSEC3PARAM'])):
                        for d in dict_info[dom_dot]['NSEC3PARAM'][i]:
                            data.append(d)
                    if len(dict_info[dom_dot]['NSEC3PARAM']) != max_NSEC3PARAM:
                        diff = max_NSEC3PARAM - len(dict_info[dom_dot]['NSEC3PARAM'])
                        zeros = ['null' for i in range(diff*8)]
                        for zero in zeros:
                            data.append(zero)
                    
                    for i in range(0,len(dict_info[dom_dot]['NSEC3PARAM_RRSIG'])):
                        for d in dict_info[dom_dot]['NSEC3PARAM_RRSIG'][i]:
                            data.append(d)
                    if len(dict_info[dom_dot]['NSEC3PARAM_RRSIG']) != max_nsec3param_rrsig:
                        diff = max_nsec3param_rrsig -len(dict_info[dom_dot]['NSEC3PARAM_RRSIG'])
                        zeros = ['null' for i in range(diff*14)]
                        for zero in zeros:
                            data.append(zero)
                    
                    for i in range(0,len(dict_info[dom_dot]['NSEC3'])):
                        for d in dict_info[dom_dot]['NSEC3'][i]:
                            data.apppend(d)
                    if len(dict_info[dom_dot]['NSEC3']) != max_NSEC3:
                            diff = max_NSEC3 - len(dict_info[dom_dot]['NSEC3'])
                            zeros = ['null' for i in range(diff*11)]
                            for zero in zeros:
                                data.append(zero)
                    
                        
                else:
                    data.append(dom_dot)
                    zeros = ['null' for i in range(max_A*9 + max_AAAA*9 + max_MX*6 + max_TXT*5 + max_SOA*11 + max_NS*5 + max_DS*8 + max_DNSKEY*8 + max_CAA*7 + max_CDS*8 + max_CDNSKEY*8 + max_NSEC3PARAM*8 + max_NSEC3*11
                                                + max_a_cname*6 + max_a_rrsig*14 + max_aaaa_cname*6 + max_aaaa_rrsig*14 + max_mx_cname*6 + max_mx_rrsig*14 + max_mxhash*7 + max_soa_cname*6 + max_soa_rrsig*14
                                                + max_txt_cname*6 + max_txt_rrsig*14 + max_txthash*7 + max_ns_cname*6 + max_nshash*7 + max_ds_cname*6 + max_ds_rrsig*14 + max_dnskey_cname*6 + max_dnskey_rrsig*14
                                                + max_caa_cname*6 + max_caa_rrsig*14 + max_cds_cname*6 + max_cds_rrsig*14 + max_cdnskey_rrsig*14 + max_nsec3param_rrsig*14
                                                )]
                    for zero in zeros:
                        data.append(zero) 
                """
                add WWWW
                """
                #       
                """
                if dom_www in doms_oi:
                    for i in range(0, len(dict_info[dom_www]['AAAA'])):
                        for d in dict_info[dom_www]['AAAA'][i]:
                            data.append(d)
                    if len(dict_info[dom_www]['AAAA']) != max_AAAA:
                        diff = max_AAAA - len(dict_info[dom_www]['AAAA'])
                        zeros = ['null' for i in range(diff*9)]
                        for zero in zeros:
                            data.append(zero)
                else:
                    zeros = ['null' for i in range(max_AAAA*9)]
                    for zero in zeros:
                        data.append(zero)
                """          
                writer.writerow(data)
                bar.next()
    '''


    bar_csv.finish()        
    end = datetime.now() 
    print('Duration: {}'.format(end-start))     
    print(len(doms))
    print(len(doms_oi))
    print(count)
    print(count_key)
    del df_group_sel
    del df_group
    del df_origin
    gc.collect()

def main():
    #path del csv dei dati ricavati da open intel e in forma sparsa
    path_csv_oi = sys.argv[1]
    #path file csv campus
    path_domain_names = sys.argv[2]
    set_out = sys.argv[3] 
    normalization(path_csv_oi,path_domain_names,set_out)

if __name__ == '__main__':
    main()