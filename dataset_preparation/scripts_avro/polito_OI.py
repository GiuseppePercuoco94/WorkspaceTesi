#!/usr/bin/env python3
import csv
from glob import glob
import pandas as pd
import gc
from datetime import datetime
import os
import sys
'''
#path_prova = '/Volumes/SDPEPPE/polito_py/csv_oi/6_01_20_0.csv'
#path_csv_out = '/Volumes/SDPEPPE/polito_py/csv_prova.csv'
path_csv_out = 'OI_inter1_polito_090120_v2.csv'
path_folder = '/Volumes/SDPEPPE/polito_py/fold_prova'
path_csvOI = '/Volumes/SDPEPPE/polito_py/csv_oi'
path_csv_dom_pol = '/Volumes/SDPEPPE/polito_py/inter1in_polito_OI090120_v2.csv'


################################TLD################################################
path_csv_out_tld =  'OI_inter1_polito_090120_tld.csv'
path_csvOI_tld = '/Volumes/SDPEPPE/polito_py/csv_oi_tld'
path_csv_dom_pol_tld = '/Volumes/SDPEPPE/polito_py/inter1in_polito_OI090120_tld.csv'
###################################################################################
'''
def sparse_info_from_polito(inter1_list_name, folder_csv):
    """
    Args:
        list_name: path of file csv that have the list of names 
        folder_csv: folder that contains the csv of openIntel
        
    Retuns: 
        csv 'info_by_OI_inter1.csv' with information per domain in the list obtained form the csv in the folder passed
    """
    
        
    #path file intersezione1 
    path_inter1 = inter1_list_name
    #path folder  contenente i csv di OpenINtel
    path_csvOI = folder_csv
    
    name = folder_csv.split('/')[-1]
    domains = []
    #extract file in creation time order
    files = glob(path_csvOI + '/*.csv')
    files.sort(key = os.path.getmtime)
    for f in files:
        print(f)

    first = True
    #per il v2 non c'è bisogno dato che non presente ['..']
    '''
    with open(path_csv_dom_pol) as csv_dom:
        lines = csv_dom.readlines()
        for line in lines:
            dom = line[2:-3]
            print(dom)
            domains.append(dom)
    print(len(domains))
    '''
    #extract domain from the list passed
    with open(path_inter1) as csv_dom:
        lines = csv_dom.readlines()
        for line in lines:
            if '\n' in line:
                line = line.strip('\n')
                if 'www8.' in line:
                    line = line.replace('www8.','')
                    domains.append(line)
                elif 'www3.' in line:
                    line = line.replace('www3.','')
                    domains.append(line)
                elif 'www2.' in line:
                    line = line.replace('www2.','')
                    domains.append(line)
                elif 'www.' in line:
                    line = line.replace('www.','')
                    domains.append(line)
                else:
                    #print('new case: ' + str(line))
                    domains.append(line)
            else:
                if 'www8.' in line:
                    line = line.replace('www8.','')
                    domains.append(line)
                elif 'www3.' in line:
                    line = line.replace('www3.','')
                    domains.append(line)
                elif 'www2.' in line:
                    line = line.replace('www2.','')
                    domains.append(line)
                elif 'www.' in line:
                    line = line.replace('www.','')
                    domains.append(line)
                else:
                    #print('new case: ' + str(line))
                    domains.append(line)

    column = ['query_type', 'query_name', 'response_type', 'response_name', 'response_ttl', 'timestamp',
            'rtt', 'worker_id', 'status_code', 'ip4_address', 'ip6_address', 'country', 'as', 'as_full',
            'ip_prefix', 'cname_name', 'dname_name', 'mx_address', 'mx_preference', 'mxset_hash_algorithm'
        , 'mxset_hash', 'ns_address', 'nsset_hash_algorithm', 'nsset_hash', 'txt_text', 'txt_hash_algorithm', 'txt_hash',
            'ds_key_tag',
            'ds_algorithm', 'ds_digest_type', 'ds_digest', 'dnskey_flags', 'dnskey_protocol', 'dnskey_algorithm',
            'dnskey_pk_rsa_n',
            'dnskey_pk_rsa_e', 'dnskey_pk_rsa_bitsize', 'dnskey_pk_eccgost_x', 'dnskey_pk_eccgost_y',
            'dnskey_pk_dsa_t', 'dnskey_pk_dsa_q', 'dnskey_pk_dsa_p', 'dnskey_pk_dsa_g', 'dnskey_pk_dsa_y',
            'dnskey_pk_eddsa_a', 'dnskey_pk_wire', 'nsec_next_domain_name', 'nsec_owner_rrset_types',
            'nsec3_hash_algorithm'
        , 'nsec3_flags', 'nsec3_iterations', 'nsec3_salt', 'nsec3_next_domain_name_hash', 'nsec3_owner_rrset_types',
            'nsec3param_hash_algorithm', 'nsec3param_flags', 'nsec3param_iterations', 'nsec3param_salt',
            'spf_text', 'spf_hash_algorithm', 'spf_hash', 'soa_mname', 'soa_rname', 'soa_serial', 'soa_refresh',
            'soa_retry', 'soa_expire',
            'soa_minimum', 'rrsig_type_covered', 'rrsig_algorithm', 'rrsig_labels', 'rrsig_original_ttl',
            'rrsig_signature_inception', 'rrsig_signature_expiration', 'rrsig_key_tag', 'rrsig_signer_name',
            'rrsig_signature', 'cds_key_tag', 'cds_algorithm', 'cds_digest_type', 'cds_digest', 'cdnskey_flags',
            'cdnskey_protocol', 'cdnskey_algorithm', 'cdnskey_pk_rsa_n', 'cdnskey_pk_rsa_e', 'cdnskey_pk_rsa_bitsize',
            'cdnskey_pk_eccgost_x', 'cdnskey_pk_eccgost_y', 'cdnskey_pk_dsa_t', 'cdnskey_pk_dsa_q', 'cdnskey_pk_dsa_p',
            'cdnskey_pk_dsa_g',
            'cdnskey_pk_dsa_y', 'cdnskey_pk_eddsa_a', 'cdnskey_pk_wire', 'caa_flags', 'caa_tag', 'caa_value',
            'tlsa_usage', 'tlsa_selector', 'tlsa_matchtype', 'tlsa_certdata', 'ptr_name']

    count_dom = 0
    count_file = 0
    
    count_tot = 0
    
    if os.path.exists('info_by_OI_inter1_'+name+'.csv'):
        print('OLD info_by_OI_inter1_'+name+'.csv removed')
        os.remove('info_by_OI_inter1_'+name+'.csv')
    else:
        print('no old info_by_OI_inter1_'+name+'.csv exists')

    test = []
    count_test = 0
    with open('info_by_OI_inter1_'+name+'.csv', mode='a') as csv_out:
        writer = csv.writer(csv_out)
        writer.writerow(column)
        
        #for each csv in the folder search for the presence of domains in the list to retrieve information 
        for file in files:
            print('\n')
            print(file)
            count_file += 1
            df = pd.read_csv(file, delimiter=',', low_memory=False)
            list_dom = df['query_name'].drop_duplicates().tolist()
            print(list_dom[0])
            print(len(list_dom))
            for dom in domains:
                
                #print(str(count_file)+','+str(count_dom) + ',' + dom)
                dot_dom = dom + '.'
                #www_dom = 'www.' + dot_dom
                if dot_dom in list_dom:
                    #print('yes dot')
                    if dot_dom in test:
                        count_test += 1
                    test.append(dot_dom)    
                    count_dom += 1
                    df_sel_dot = df.query("query_name=='" + str(dot_dom) + "'")
                    df_sel_dot.to_csv(csv_out, index=False, header=False)
                    del df_sel_dot
                    
                
                '''
                if www_dom in list_dom:
                    #print('yes www')
                    
                    df_sel_www = df.query("query_name=='" + str(www_dom) + "'")
                    df_sel_www.to_csv(csv_out, index=False, header=False)
                    del df_sel_www
                    
                
                else:
                    print('no www')
                
                '''
            del df
            del list_dom
            gc.collect()
    print(count_dom)
    print(count_test)

   
   
def sparse_info_from_polito2(inter1_list_name, folder_csv):
    """
    Args:
        list_name: path of file csv that have the list of names 
        folder_csv: folder that contains the csv of openIntel
        
    Retuns: 
        csv 'info_by_OI_inter1.csv' with information per domain in the list obtained form the csv in the folder passed
    """
    
        
    #path file intersezione1 
    path_inter1 = inter1_list_name
    #path folder  contenente i csv di OpenINtel
    path_csvOI = folder_csv
    
    name = folder_csv.split('/')[-1]
    domains = []
    #extract file in creation time order
    files = glob(path_csvOI + '/*.csv')
    files.sort(key = os.path.getmtime)
    for f in files:
        print(f)

    first = True
    #per il v2 non c'è bisogno dato che non presente ['..']
    '''
    with open(path_csv_dom_pol) as csv_dom:
        lines = csv_dom.readlines()
        for line in lines:
            dom = line[2:-3]
            print(dom)
            domains.append(dom)
    print(len(domains))
    '''
    #extract domain from the list passed
    with open(path_inter1) as csv_dom:
        lines = csv_dom.readlines()
        for line in lines:
            if '\n' in line:
                line = line.strip('\n')
                if 'www8.' in line:
                    line = line.replace('www8.','')
                    domains.append(line)
                elif 'www4.' in line:
                    line = line.replace('www4.','')
                    domains.append(line)
                elif 'www3.' in line:
                    line = line.replace('www3.','')
                    domains.append(line)
                elif 'www2.' in line:
                    line = line.replace('www2.','')
                    domains.append(line)
                elif 'www.' in line:
                    line = line.replace('www.','')
                    domains.append(line)
                else:
                    #print('new case: ' + str(line))
                    domains.append(line)
            else:
                if 'www8.' in line:
                    line = line.replace('www8.','')
                    domains.append(line)
                elif 'www4.' in line:
                    line = line.replace('www4.','')
                    domains.append(line)
                elif 'www3.' in line:
                    line = line.replace('www3.','')
                    domains.append(line)
                elif 'www2.' in line:
                    line = line.replace('www2.','')
                    domains.append(line)
                elif 'www.' in line:
                    line = line.replace('www.','')
                    domains.append(line)
                else:
                    #print('new case: ' + str(line))
                    domains.append(line)

    column = ['query_type', 'query_name', 'response_type', 'response_name', 'response_ttl', 'timestamp',
            'rtt', 'worker_id', 'status_code', 'ip4_address', 'ip6_address', 'country', 'as', 'as_full',
            'ip_prefix', 'cname_name', 'dname_name', 'mx_address', 'mx_preference', 'mxset_hash_algorithm'
        , 'mxset_hash', 'ns_address', 'nsset_hash_algorithm', 'nsset_hash', 'txt_text', 'txt_hash_algorithm', 'txt_hash',
            'ds_key_tag',
            'ds_algorithm', 'ds_digest_type', 'ds_digest', 'dnskey_flags', 'dnskey_protocol', 'dnskey_algorithm',
            'dnskey_pk_rsa_n',
            'dnskey_pk_rsa_e', 'dnskey_pk_rsa_bitsize', 'dnskey_pk_eccgost_x', 'dnskey_pk_eccgost_y',
            'dnskey_pk_dsa_t', 'dnskey_pk_dsa_q', 'dnskey_pk_dsa_p', 'dnskey_pk_dsa_g', 'dnskey_pk_dsa_y',
            'dnskey_pk_eddsa_a', 'dnskey_pk_wire', 'nsec_next_domain_name', 'nsec_owner_rrset_types',
            'nsec3_hash_algorithm'
        , 'nsec3_flags', 'nsec3_iterations', 'nsec3_salt', 'nsec3_next_domain_name_hash', 'nsec3_owner_rrset_types',
            'nsec3param_hash_algorithm', 'nsec3param_flags', 'nsec3param_iterations', 'nsec3param_salt',
            'spf_text', 'spf_hash_algorithm', 'spf_hash', 'soa_mname', 'soa_rname', 'soa_serial', 'soa_refresh',
            'soa_retry', 'soa_expire',
            'soa_minimum', 'rrsig_type_covered', 'rrsig_algorithm', 'rrsig_labels', 'rrsig_original_ttl',
            'rrsig_signature_inception', 'rrsig_signature_expiration', 'rrsig_key_tag', 'rrsig_signer_name',
            'rrsig_signature', 'cds_key_tag', 'cds_algorithm', 'cds_digest_type', 'cds_digest', 'cdnskey_flags',
            'cdnskey_protocol', 'cdnskey_algorithm', 'cdnskey_pk_rsa_n', 'cdnskey_pk_rsa_e', 'cdnskey_pk_rsa_bitsize',
            'cdnskey_pk_eccgost_x', 'cdnskey_pk_eccgost_y', 'cdnskey_pk_dsa_t', 'cdnskey_pk_dsa_q', 'cdnskey_pk_dsa_p',
            'cdnskey_pk_dsa_g',
            'cdnskey_pk_dsa_y', 'cdnskey_pk_eddsa_a', 'cdnskey_pk_wire', 'caa_flags', 'caa_tag', 'caa_value',
            'tlsa_usage', 'tlsa_selector', 'tlsa_matchtype', 'tlsa_certdata', 'ptr_name']

    count_dom = 0
    count_file = 0
    
    count_tot = 0
    
    if os.path.exists('info_by_OI_inter1_'+name+'.csv'):
        print('OLD info_by_OI_inter1_'+name+'.csv removed')
        os.remove('info_by_OI_inter1_'+name+'.csv')
    else:
        print('no old info_by_OI_inter1_'+name+'.csv exists')

    test = []
    count_test = 0
    with open('info_by_OI_inter1_'+name+'.csv', mode='a') as csv_out:
        writer = csv.writer(csv_out)
        writer.writerow(column)
        
        #for each csv in the folder search for the presence of domains in the list to retrieve information 
        for file in files:
            print('\n')
            print(file)
            count_file += 1
            df = pd.read_csv(file, delimiter=',', low_memory=False)
            for dom in domains:
                
                #print(str(count_file)+','+str(count_dom) + ',' + dom)
                dot_dom = dom + '.'
                df_sel_dot = df.query("query_name=='" + str(dot_dom) + "'")
                if len(df_sel_dot.index) !=0:
                    if dom in test:
                        count_test += 1
                    else:
                        test.append(dom)
                    
                    count_dom += 1
                    df_sel_dot.to_csv(csv_out, index=False, header=False)
                del df_sel_dot
                #www_dom = 'www.' + dot_dom
                '''
                if www_dom in list_dom:
                    #print('yes www')
                    
                    df_sel_www = df.query("query_name=='" + str(www_dom) + "'")
                    df_sel_www.to_csv(csv_out, index=False, header=False)
                    del df_sel_www
                    
                
                else:
                    print('no www')
                
                '''
            del df
            gc.collect()
    
    print(count_dom)
    print(count_test)
    print('effective len intersection :' + str(count_dom-count_test))
       
def main(pi1,pf):
    #path file intersezione1 .. ovvero il file ottenuto da opint_csv intersezione 1 ovvero nomi comuni tra la lsita e i nomi interrogati da OI
    path_inter1 = pi1
    #path folder  contenente i csv di OpenINtel
    path_csvOI = pf
    start = datetime.now()
    sparse_info_from_polito2(path_inter1, path_csvOI)
    end = datetime.now()
    print("Duration: {}".format(end - start))
       
       
       
if __name__ == '__main__':
    #path file intersezione1 .. ovvero il file ottenuto da opint_csv intersezione 1 ovvero nomi comuni tra la lsita e i nomi interrogati da OI
    path_inter1 = sys.argv[1]
    #path folder  contenente i csv di OpenINtel
    path_csvOI = sys.argv[2]
    main(path_inter1,path_csvOI)