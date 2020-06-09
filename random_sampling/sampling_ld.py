import random
import csv
import os
import sys
import pandas as pd

def extract_doml(csv):
    """
    ARGS:
        - csv: path of csv to extract the list of doms
    
    RETURNS:
        - dom_ : list of domain extracted
    """
    dom_ = []
    print('extratig list of doms')
    with open(csv) as csv_in:
        lines = csv_in.readlines()
        for line in lines:
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

def sampling_ld(csv,n_samples):
    
    """
    Extract from the list a new list with n_samples domain names.. dont'use it, error 'kill 9 ' too memery usage
    Use df_sampling_ld
    
    ARGS:
        - csv: path of csv where extract the samples
        - n_samples: number of samples to extract
    """
    
    #check the presence of file out
    if os.path.exists('sample_'+str(csv.split('/')[-1].split('.')[0])+'.csv'):
        print('removing old csv')
        os.remove('sample_'+str(csv.split('/')[-1].split('.')[0])+'.csv')
    else:
        print('no old csv exists')
    
    
    list_dom = extract_doml(csv)
    
    print('sampling...')
    new_list = random.sample(list_dom,n_samples)
    
    print('Writing..')
    with open('sample_'+str(csv.split('/')[-1].split('.')[0])+'.csv', mode='a') as csv_out:
        writer = csv.writer(csv_out)
        for x  in new_list:
            writer.writerrow([x])

def df_sampling_ld(csv,n_samples):
    """
    Extract from the list a new list with n_samples domain names
    
    ARGS:
        - csv: path of csv where extract the samples
        - n_samples: number of samples to extract
    """
    
    #check the presence of file out
    if os.path.exists('sample_'+str(csv.split('/')[-1].split('.')[0])+'.csv'):
        print('removing old csv')
        os.remove('sample_'+str(csv.split('/')[-1].split('.')[0])+'.csv')
    else:
        print('no old csv exists')
      
    df = pd.read_csv(csv, low_memory=False,names=['domain'])
    print(df)
    df_out = df.sample(n_samples)
   
    df_out.to_csv('sample_'+str(csv.split('/')[-1].split('.')[0])+'.csv', header=None)
    
    df_final = pd.read_csv('sample_'+str(csv.split('/')[-1].split('.')[0])+'.csv', low_memory=False, names=['index','domain'], index_col=['index'])
    df_final_out = df_final.sort_values(by=['index'])
    print(df_final)
    df_final_out['domain'].to_csv('samplesort_'+str(csv.split('/')[-1].split('.')[0])+'.csv',header=None,index=False)
    

def main():
    
    list = sys.argv[1]
    n = int(sys.argv[2])
    df_sampling_ld(list,n)


if __name__ == '__main__':
    main()