import pandas as pd 
import numpy as np
import csv
import os
import sys
import shutil




def folder_exists(name_folder):
    if os.path.exists(name_folder):
        print('folder ',name_folder,' already exists')
    else:
        print('no old folder ',name_folder,' exists..creating it')
        os.mkdir(name_folder)
        
def file_exists(name_file):
    if os.path.exists(name_file):
        print('file ',name_file,' exists..removing it')
        os.remove(name_file)
    else:
        print('no old ',name_file,' exists')

def remove_duplicates(df):
    '''
    Remove duplicate tuples removing domain name duplicates
    '''
    df = df.drop_duplicates(subset='domain')
    df = df.set_index('domain')
    return df

def conc_vt_score(csv_vt,csv_score):
    df_vt = pd.read_csv(csv_vt,sep=';',low_memory=False, index_col=['domain'])
    df_score = pd.read_csv(csv_score,sep=';',low_memory=False, index_col=['domain'])
    #df_vt = remove_duplicates(df_vt)
    #df_score = remove_duplicates(df_score)
    
    df_conc = pd.concat([df_vt,df_score],axis=1,join='inner')
    folder_base = os.path.dirname(csv_vt)
    
    folder_exists(folder_base+'/conc_vt_score')
    file_exists(folder_base+'/conc_vt_score/vt_score.csv')
    
    df_conc.to_csv(folder_base+'/conc_vt_score/vt_score.csv')
    

def conc_inv_score(csv_inv,csv_score):
    df_inv = pd.read_csv(csv_inv,sep=';',low_memory=False, index_col=['domain'])
    df_score = pd.read_csv(csv_score,sep=';',low_memory=False, index_col=['domain'])
    #df_inv = remove_duplicates(df_inv)
    #df_score = remove_duplicates(df_score)
    
    df_conc = pd.concat([df_inv,df_score], axis=1, join='inner')
    
    folder_base = os.path.dirname(csv_inv)
    
    folder_exists(folder_base+'/conc_inv_score')
    file_exists(folder_base+'/conc_inv_score/inv_score.csv')
    
    df_conc.to_csv(folder_base+'/conc_inv_score/inv_score.csv')
    
def conc_vt_inv_score(csv_vt,csv_inv,csv_score):
    df_vt = pd.read_csv(csv_vt,sep=';',low_memory=False, index_col=['domain'])
    df_score = pd.read_csv(csv_score,sep=';',low_memory=False, index_col=['domain'])
    df_inv = pd.read_csv(csv_inv,sep=';',low_memory=False, index_col=['domain'])
    #df_vt = remove_duplicates(df_vt)
    #df_inv = remove_duplicates(df_inv)
    #df_score = remove_duplicates(df_score)
    
    df_conc = pd.concat([df_vt,df_inv,df_score], axis=1, join='inner')
    
    folder_base = os.path.dirname(csv_inv)
    
    folder_exists(folder_base+'/conc_vt_inv_score')
    file_exists(folder_base+'/conc_vt_inv_score/vt_inv_score.csv')
    
    df_conc.to_csv(folder_base+'/conc_vt_inv_score/vt_inv_score.csv')


def labeling_vt_score(csv_vt_score,lim):
    base_fodler = os.path.dirname(csv_vt_score)
    folder_out = base_fodler+'/vt_lim_score'
    folder_exists(folder_out)
    path_csv_out = folder_out +'/vt_score_lim'+str(lim)+'.csv'
    file_exists(path_csv_out)
    
    df_vt_score = pd.read_csv(csv_vt_score, sep=';', index_col=['domain'])
    print(df_vt_score)
    #fill null values with 0 
    df_vt_score['VT_positives'] = df_vt_score['VT_positives'].fillna(0)
    df_vt_score['VT_total'] = df_vt_score['VT_total'].fillna(0)
    
    header = ['domain','label']
    bad = 0
    weak_bad = 0
    good = 0
    weak_good = 0
    
    
    with open(path_csv_out, mode='a') as csv_out:
        writer = csv.writer(csv_out)
        writer.writerow(header)
        for index,row in df_vt_score.iterrows():
            data = []
            data.append(index)
            if row['VT_total'] == 0:
                if int(row['INV_score']) >= 70:
                    bad += 1
                    data.append('bad')
                elif int(row['INV_score']) < 70 and int(row['INV_score']) >= 50:
                    weak_bad += 1
                    data.append('weak_bad')
                elif int(row['INV_score']) <= 49 and int(row['INV_score']) > 20:
                    weak_good += 1
                    data.append('weak_good')
                elif int(row['INV_score']) <= 20:
                    good += 1
                    data.append('good')
                else:
                    print('Untracked New case :' + str(index) + ', ' +str(int(row['INV_score'])))
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
    
    print('good :', str(good),', bad: ',str(bad),', weak_good: ', str(weak_good)+ ', weak_bad: '+ str(weak_bad))
    
                
            

def labeling_inv_score(csv_inv_score):
    base_fodler = os.path.dirname(csv_inv_score)
    folder_out = base_fodler+'/inv_score'
    folder_exists(folder_out)
    path_csv_out = folder_out +'/inv_score.csv'
    file_exists(path_csv_out)
    
    df_inv_score = pd.read_csv(csv_inv_score, sep=';', index_col=['domain'])
    print(df_inv_score)
    #fill null values with 0 
    df_inv_score['INV_status'] = df_inv_score['INV_status'].fillna(0)
    
    header = ['domain','label']
    bad = 0
    weak_bad = 0
    good = 0
    weak_good = 0
    
    with open(path_csv_out, mode='a') as csv_out:
        writer = csv.writer(csv_out)
        writer.writerow(header)
        for index,row in df_inv_score.iterrows():
            data = []
            data.append(index)
            if row['INV_status'] == -1:
                bad += 1
                data.append('bad')
            elif int(row['INV_status']) == 1:
                good += 1
                data.append('good')
            elif int(row['INV_status']) == 0:
                if int(row['INV_score']) >= 70:
                    bad += 1
                    data.append('bad')
                elif int(row['INV_score']) < 70 and int(row['INV_score']) >= 50:
                    weak_bad += 1
                    data.append('weak_bad')
                elif int(row['INV_score']) <= 49 and int(row['INV_score']) > 20:
                    weak_good += 1
                    data.append('weak_good')
                elif int(row['INV_score']) <= 20:
                    good += 1
                    data.append('good')
                else:
                    print('Untracked New case :' + str(index) + ', ' +str(int(row['INV_score'])))
            else:
                print('new case'+ str(index))
            writer.writerow(data)
    
    print('good :', str(good),', bad: ',str(bad),', weak_good: ', str(weak_good)+ ', weak_bad: '+ str(weak_bad))

def labeling_vt_inv_score(csv_vt_inv_score,lim):
    exit()
    
def main():
    '''
    choose:
        - 1: vt + score
        - 2: inv + score
        - 3: vt + inv + score
    '''
    choose = int(sys.argv[1])
    if choose == 1:
        path_vt = sys.argv[2]
        path_score = sys.argv[3]
        conc_vt_score(path_vt,path_score)
    elif choose == 2:
        path_inv = sys.argv[2]
        path_score = sys.argv[3]
        conc_inv_score(path_inv,path_score)
    elif choose == 3:
        path_vt = sys.argv[2]
        path_inv = sys.argv[3]
        path_score = sys.argv[4]
        conc_vt_inv_score(path_vt,path_inv,path_score)
    elif choose == 4:
        path_vt_score = sys.argv[2]
        lim = int(sys.argv[3])
        labeling_vt_score(path_vt_score,lim)
    elif choose == 5:
        path_inv_score = sys.argv[2]
        labeling_inv_score(path_inv_score)
    else:
        print('bye')
        
if __name__ == '__main__':
    main()