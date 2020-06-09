#!/usr/bin/env python3
import os
import sys
import json
import csv
import shutil
import numpy as np 
from collections import Counter
import pycountry 

def check_exists(path):
    if '/' not in path or '.' not in path.split('/')[-1]:
        if os.path.exists(path):
            os.remove(path)
            print('old file ',path,' removed')
        else:
            print('no old file ',path,' exists')
    else:
        if os.path.exists(path):
            shutil.rmtree(path)
            print('old folder ',path,' removed')
        else:
            print('no old folder ',path,' exists')
            print('create folder ',path)
            os.makedirs(path)


def string_to_number(string):
    if("." in string):
        try:
            res = float(string)
        except:
            res = string  
    elif(string.isdigit()):
        res = int(string)
    else:
        res = string
    return(res)         

def id_country(cc_in):
    """
    Create a id for each countries.  It return a id for a given cc passed
    """
    cc = pycountry.countries.get(alpha_2=cc_in)
    numeric_id = cc.numeric 
    str_float = '0.000'+numeric_id
    str_float = string_to_number(str_float)
    return str_float

def top_country(list_country):
    dict_count = Counter(list_country)
    k = list(dict_count.keys())
    max_value = 0
    max_cc = ''
    
    for x in k:
        if max_value < dict_count[x]:
            max_value = dict_count[x]
            max_cc = x
        else:
            continue
    #print('country: '+max_cc)
    id_cc = id_country(max_cc)
    return id_cc
    
    
def feature_extraction(json_info, csv_list_nomdom):
    #load dict form json 
    dict_info = json.load(open(json_info))
    #load domain and remove www, www2, wtc
    doms = remove_www(csv_list_nomdom)

    RR_ = {
        'A':['A_n_ipv4','A_mean_ttl','A_strdev_ttl','A_rtt','A_as_number','A_n_countries','A_top_country'],
        'AAAA': ['AAAA_n_ipv6','AAAA_mean_ttl','AAAA_strdev_ttl','AAAA_rtt','AAAA_as_number','AAAA_n_countries','AAAA_top_country'],
        'A_CNAME': ['A_n_cname'],
        'NS': ['NS_n']
    }
    
    #csv header creation --> ok
    RRkeys = RR_.keys()
    header =[]
    header.append('domain')
    for key in RRkeys:
        for x in RR_[key]:
            header.append(x)
            print(x)
            
    check_exists('feature_set_oi.csv')
    
    with open('feature_set_oi.csv', mode='a') as csv_feature:
        writer = csv.writer(csv_feature)
        writer.writerow(header)
        for dom in doms:
            data = []
            data.append(dom)
            dom_dot = dom + '.'
            if dom_dot in dict_info.keys():
                #print(dom_dot)
                for RRkey in RRkeys:
                    #print(RRkey)
                    if RRkey == 'A' or  RRkey == 'AAAA':
                        #print(' sono in a o in aaaa')
                        vects = dict_info[dom_dot][RRkey]
                        info_ip= []
                        info_coutr=[]
                        info_asn=[]
                        info_ttl = []
                        info_rtt = []
                        if len(vects) != 0:
                            for vect in vects:
                                #wirte function to check if the value is null
                                #print(vect)
                                if vect[0] != 'null':
                                    info_ip.append(vect[0])
                                if string_to_number(vect[1]) != 0:
                                    s = string_to_number(vect[1])
                                    info_ttl.append(s)
                                if vect[5] != 'null' and vect[5] != '--' and vect[5] != '[--]':
                                    info_asn.append(vect[5])
                                if vect[7] != 'null' and vect[7] != '--' and vect[7] != '[--]':
                                    info_coutr.append(vect[7])
                                if string_to_number(vect[3]) != 0:
                                    s = string_to_number(vect[3])
                                    info_rtt.append(s)
                            #print('infottl')
                            #print(info_ttl)
                            #print(info_ip)
                            data.append(len(np.unique(info_ip)))
                            if len(info_ttl) != 0:
                                data.append(np.mean(info_ttl))
                                data.append(np.std(info_ttl))
                            else:
                                data.append(0)
                                data.append(0)
                                
                            if len(info_rtt) != 0:
                                data.append(np.mean(info_rtt))
                            else:
                                data.append(0)
                                data.append(0)
                                
                            data.append(len(np.unique(info_asn)))
                            data.append(len(np.unique(info_coutr)))
                            if len(info_coutr) != 0 and 'null' not in info_coutr:
                                data.append(top_country(info_coutr))
                            else:
                                data.append('null')
                            
                        else:
                            nulls = ['null' for i in range(len(RR_[RRkey]))]
                            for null in nulls:
                                data.append(null)
                    elif RRkey == 'A_CNAME':
                        #print('sono in cname')
                        vects = dict_info[dom_dot][RRkey]
                        #print(vects)
                        info_cname_count = []
                        if len(vects) != 0:
                            for vect in vects:
                                if vect[0] != 'null':
                                    info_cname_count.append(vect[0])
                            data.append(len(info_cname_count))
                        else:
                            nulls = ['null' for i in range(len(RR_[RRkey]))]
                            for null in nulls:
                                data.append(null)
                    elif RRkey == 'NS':
                        vects =  dict_info[dom_dot][RRkey]
                        info_ns = []
                        if len(vects) != 0:
                            for vect in vects:
                                if vect[0] != 'null':
                                    info_ns.append(vect[0])
                            data.append(len(info_ns))
                        else:
                            nulls = ['null' for i in range(len(RR_[RRkey]))]
                            for null in nulls:
                                data.append(null)    
                    else:
                        offset = 0
                        for RRk in RRkeys:
                            offset += len(RR_[RRk])
                        nulls = ['null' for i in range(offset)]
                        for null in nulls:
                            data.append(null)
                writer.writerow(data)
                    
            else:
                offset = 0
                for RRk in RRkeys:
                    offset += len(RR_[RRk])
                nulls = ['null' for i in range(offset)]
                for null in nulls:
                    data.append(null)
                writer.writerow(data)
             
        feature_statistic_dmn(csv_list_nomdom)
                
        
        
def feature_statistic_dmn(domainname):
    print('Calculationg domain name statistics..')
    list_dmn = remove_www(domainname)
    count = 0
    
    header = ['domain','n_letters','n_digits','n_characters']
    
    name_csv = 'feature_statistic_dmn.csv'
    check_exists(name_csv)
    with open(name_csv, mode='a') as csv_out:
        writer = csv.writer(csv_out)
        writer.writerows([header])
        for dom in list_dmn:
            data = []
            data.append(dom)
            
            count_l = 0
            count_d = 0
            count_c = 0
            for s in dom:
                if s.isalpha():
                    count_l += 1
                elif s.isdecimal():
                    count_d += 1
                else:
                    count_c += 1
            data.append(count_l)
            data.append(count_d)
            data.append(count_c)
            writer.writerow(data)
            
    #return name_csv
                    
                    
                    
            
        

def remove_www(path):
    dom_ = []
    with open(path) as csv_in:
        lines = csv_in.readlines()
        for line in lines:
            if '\n' in line:
                line = line.strip('\n')
                if 'www8.' in line:
                    line = line.replace('www8.','')
                    dom_.append(line)
                elif 'www3.' in line:
                    line = line.replace('www3.','')
                    dom_.append(line)
                elif 'www4.' in line:
                    line = line.replace('www4.','')
                    dom_.append(line)
                elif 'www2.' in line:
                    line = line.replace('www2.','')
                    dom_.append(line)
                elif 'www.' in line:
                    line = line.replace('www.','')
                    dom_.append(line)
                else:
                    dom_.append(line)
            else:
                if 'www8.' in line:
                    line = line.replace('www8.','')
                    dom_.append(line)
                elif 'www3.' in line:
                    line = line.replace('www3.','')
                elif 'www4.' in line:
                    line = line.replace('www4.','')
                    dom_.append(line)
                elif 'www2.' in line:
                    line = line.replace('www2.','')
                    dom_.append(line)
                elif 'www.' in line:
                    line = line.replace('www.','')
                    dom_.append(line)
                else:
                    dom_.append(line)
        return dom_


def main(pj,list):
    feature_extraction(pj,list)
    
if __name__ == '__main__':
    path_json = sys.argv[1]
    list_dn = sys.argv[2]
    feature_extraction(path_json,list_dn)