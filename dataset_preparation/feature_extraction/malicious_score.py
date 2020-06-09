import pandas as pd 
import numpy as np
import matplotlib.pyplot as plt 
import csv
import sys
import os


def check_file_exists(name_file):
    if os.path.exists(name_file):
        print('file ',name_file,' exists..removing it')
        os.remove(name_file)
    else:
        print('no old ',name_file,' exists')


def concat_vt_umbrella(csv_vt, csv_investigate,lim):
    """
    ARGS:
        csv_in: file csv with column form VirusTotal and Investigate
        name_csv_out: name of csv file out
    
    RETURNS:
        return a csv file with two colum: 1th column is a list of domain names, 2th a 'score' column
    """
    
    df_vt = pd.read_csv(csv_vt, low_memory=False, sep=',' )
    #print('ok')
    df_inv = pd.read_csv(csv_investigate, low_memory=False, sep=';')
    #print('ok')
    #print(df_inv)
    df_inv = df_inv.drop(['domain'],axis=1)
    #print('drop')
    result = pd.concat([df_inv,df_vt],axis=1, sort=False)
    print('concat')
    result =result.replace(np.nan,'null')
    result = result.set_index(['domain'])
    result.to_csv('concat_vt_umbrella_'+str(lim)+'.csv')
    
    return 'concat_vt_umbrella_'+str(lim)+'.csv'




def score_calc(csv_conc, name_file_out, limit):
    """
    ARGS:
        csv_conc :csv obtained from 'concat_vt_umbrella'
        name_file_out: name of csv to save
        limit: thrashold to VT_positives to say if a domain was malicious or not
        
    """
    check_file_exists(name_file_out+'_'+str(limit)+'.csv')
    
    df = pd.read_csv(csv_conc, low_memory=False, index_col=['domain'])
    #print(df.columns)
    df['VT_positives'] = df['VT_positives'].fillna(0)
    df['INV_status'] = df['INV_status'].fillna(0)
    df['VT_total'] =df['VT_total'].fillna(0)
    #print(df['INV_status'])
    #print(df['VT_positives'])
    
    #calc max value of column VT_positives
    df['INV_status']= df['INV_status'].astype(int)
    df['VT_positives'] = df['VT_positives'].astype(float)
    df['VT_total'] = df['VT_total'].astype(int)
    max_ps = df['VT_positives'].max()
    norm_vt = 0.5/max_ps
    print('Max pos vt: ', str(max_ps),', norm: '+str(norm_vt)+ ', lim: '+ str(limit))
    
    head = ['domain','score','label']
    bad = 0
    good = 0 
    unknown = 0
    with open(name_file_out+'_'+str(limit)+'.csv', mode='a') as csv_out:
        wirter = csv.writer(csv_out)
        wirter.writerow(head)
        #iterate over pandas dataframe
        for index,row in df.iterrows():
            data = []
            score = 0
            data.append(index)
            if row['VT_total'] == 0:
                if row['INV_status'] == -1:
                    bad += 1
                    score = 0.5 
                    data.append(score)
                    data.append('bad')
                elif row['INV_status'] == 1:
                    good += 1
                    score = 0
                    data.append(score)
                    data.append('good')
                elif row['INV_status'] == 0:
                    unknown += 1
                    score = 0
                    data.append(score)
                    data.append('untracked')
            else:
                if row['VT_positives'] >= limit:
                    if row['INV_status'] == -1:
                        bad += 1
                        score = 0.5 + row['VT_positives']*norm_vt
                        data.append(score)
                        data.append('bad')
                    else:
                        bad += 1
                        score = row['VT_positives']*norm_vt
                        data.append(score)
                        data.append('bad')
                elif row['VT_positives'] < limit:
                    if row['INV_status'] == -1:
                        bad += 1
                        score = 0.5
                        data.append(score)
                        data.append('bad')
                    else:
                        good += 1
                        score = 0
                        data.append(score)
                        data.append('good')
                else:
                    print('new case: ',index)
            wirter.writerow(data)          
                    
                    
    print('good :', str(good),', bad: ',str(bad),', untrucked: ', str(unknown))
    return max_ps,good,bad, unknown


def anal_score(csv_score):
    df = pd.read_csv(csv_score)
    good, bad = df['label'].value_counts()
    return good,bad



def plot_score(g_,b_, lim):
    
    fig,axs = plt.subplots()
    
    axs.plot(lim,g_,'-b' ,label = 'good')
    axs.plot(lim,b_,'-y' ,label = 'bad')
    plt.legend()
    plt.show()
    
    
    
            
    
def main():
    csv_vt = sys.argv[1]
    csv_inv = sys.argv[2]
    name = sys.argv[3]
    lim = float(sys.argv[4])
    loop = int(sys.argv[5])
    csv_conc = concat_vt_umbrella(csv_vt, csv_inv,lim)
    max_pos,g,b,u = score_calc(csv_conc, name, lim)
    
    if loop == 1:
        r = range(1,int(max_pos)+1)
        good_ = []
        bad_ = []
        for i in r:
            a,b,c,d = score_calc(csv_conc, name, i)
            good_.append(b)
            bad_.append(c)
        #plot_score(good_,bad_,r)
    else:
        exit()

if __name__ == '__main__':
    main()