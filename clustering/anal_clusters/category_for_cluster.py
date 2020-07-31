import pandas as pd 
import numpy as np 
import csv
import os
import sys 
import shutil
import gc

def info_categ_by_cluster(path_csv,path_inv):
    '''
    
    path_csv: path of csv of cluster ordered by id cluster label
    path_inv: path of investigate, We keep information for domain by searching on its 'inv_content_cateogries' and 'inv_security_category' (if it is bad)
    
    '''    
    df = pd.read_csv(path_csv,sep=',', low_memory=False)
    df_inv = pd.read_csv(path_inv,sep=';', low_memory=False)
    
    df_inv = df_inv.drop_duplicates(subset='domain')
    df_inv = df_inv.set_index('domain')
    
    df_group = df.groupby(by='cluster')
    
    for key,item in df_group:
        info_ = []
        doms_ = list(set(item['domain']))
        for dom in doms_:
            
            query = df_inv.loc[dom,['INV_content_categories']]
            
            if query[0] == '[]':
                
                continue
                
            else:
                query = query.tolist()[0].replace("['",'').replace("']",'').replace("', '",',')
                
                info_ = info_ + query.split(',')
        
        info_ = set(info_)
        print('Cluster ' + str(key) + ' : ')
       # print(info_.sort())
        print(sorted(info_))
        print('\n')
            
    '''
    
    vect_ = ['Online Trading']
    prova = df_inv.loc['ntp.ubuntu.com',['INV_content_categories']]
    
    prova = prova[0].replace("['",'').replace("']",'').replace("', '",',')
    print(prova)
    print(type(prova))
    prova = prova.split(',')
    vect = ['prova','Software/Technology']
    
    print(vect+prova)
    
    print(list(set(vect+prova)))
    
    prova1 = df_inv.loc['_ldap._tcp',['INV_content_categories']]
    print(prova1)
    print(prova1[0])
    print(type(prova1[0]))
    if (prova1[0]) == '[]':
        print('yes')
    '''
    
    

def main():
    path_csv = sys.argv[1]
    path_inv = sys.argv[2]
    info_categ_by_cluster(path_csv,path_inv)

if __name__ == '__main__':
    main()