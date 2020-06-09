import os
import csv
import sys
import pandas as pd 
import numpy as np 

def folder_exists(name_folder):
    if os.path.exists(name_folder):
        print('folder ',name_folder,' alread exists')
    else:
        print('no old folder ',name_folder,' exists..creating it')
        os.mkdir(name_folder)
        
def file_exists(name_file):
    if os.path.exists(name_file):
        print('file ',name_file,' exists..removing it')
        os.remove(name_file)
    else:
        print('no old ',name_file,' exists')

def vt_column(csv_vt,lim):
    folder_csv_out = 'VT_lim'
    folder_exists(folder_csv_out)
    name_csv_out = os.path.join(folder_csv_out, 'VT_lim_'+str(lim)+'.csv')
    file_exists(name_csv_out)
    
    df = pd.read_csv(csv_vt, low_memory=False,sep=',', index_col=['domain'])
    #fill null values with 0 
    df['VT_positives'] = df['VT_positives'].fillna(0)
    df['VT_total'] = df['VT_total'].fillna(0)
    
    #calc max value of positives
    df['VT_positives'] = df['VT_positives'].astype(float)
    df['VT_total'] = df['VT_total'].astype(int)
    max_ps = df['VT_positives'].max()
    print('Max pos vt: ', str(max_ps),', lim: '+ str(lim))
    
    #column of csv out
    head = ['domain','label']
    bad = 0
    good = 0
    unknown = 0
    
    #compute the csv out
    with open(name_csv_out, mode='a') as csv_out:
        writer = csv.writer(csv_out)
        writer.writerow(head)
        #iterate over pandas dataframe
        for index,row in df.iterrows():
            data = []
            data.append(index)
            if row['VT_total'] == 0:
                unknown += 1
                data.append('untracked')
            else:
                if row['VT_positives'] >= lim:
                    bad += 1
                    data.append('bad')
                elif row['VT_positives'] < lim:
                    good += 1
                    data.append('good')
                else:
                    print('new case'+ str(index))
            writer.writerow(data)
    
    print('good :', str(good),', bad: ',str(bad),', untrucked: ', str(unknown))
                 
def investigate_col(csv_inv):
    folder_csv_out = 'Investigate'
    folder_exists(folder_csv_out)
    name_csv_out = os.path.join(folder_csv_out, 'investigate.csv')
    file_exists(name_csv_out)
    
    df = pd.read_csv(csv_inv, low_memory=False,sep=';', index_col=['domain'])
    
    df['INV_status'] = df['INV_status'].fillna(0)
    
    head = ['domain','label']
    bad = 0
    good = 0 
    unknown = 0
    
    with open(name_csv_out, mode='a') as csv_out:
        wirter = csv.writer(csv_out)
        wirter.writerow(head)
        
        for index,row in df.iterrows():
            data = []
            data.append(index)
            if row['INV_status'] == -1:
                bad += 1
                data.append('bad')
            elif row['INV_status'] == 1:
                good += 1
                data.append('good')
            else:
                unknown += 1
                data.append('untracked')
            wirter.writerow(data)
    
    print('good :', str(good),', bad: ',str(bad),', untracked: ', str(unknown))


def main():
    choose = int(sys.argv[1])
    if choose == 1:
        csv_vt = sys.argv[2]
        limit = int(sys.argv[3])
        vt_column(csv_vt,limit)
    else:
        csv_inv = sys.argv[2]
        investigate_col(csv_inv)
    
    

if __name__ == '__main__':
    main()
    