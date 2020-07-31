import numpy as np 
import pandas as pd 
import csv
import os
import sys
import shutil
from matplotlib import cm 
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
import hdbscan
from sklearn.preprocessing import MinMaxScaler, StandardScaler
from sklearn import metrics
import seaborn as sns 
from sklearn.metrics import silhouette_samples
from datetime import datetime
import gc

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
        

def clust_hdbscan(csv_in, n_cluster, scale, top_scale):
    '''
    ARGS:
        -csv_in: csv of features
        -n_cluster: 'k' number of clusters
        -scale: 1-> sdandard scaler, 2-> minmax scaler, other no scaler
    '''
    df = pd.read_csv(csv_in, low_memory=False, index_col=['domain'],sep=',')
    #removing duplicates
    #df = df.drop_duplicates(subset='domain')
    #df = df.set_index('domain')

    #remove 'score column' and tuple with no information from OI, and the column score and label
    if 'PC1' not in list(df.columns):
        #remove NaN values filling with 0 values
        df = df.fillna(0)
        #drop domains which not bring information from OpenIntel
        df = df[(df['A_n_ipv4'] != 0) | (df['AAAA_n_ipv6'] != 0)]
    df_copy = df.copy()
    df_copy.reset_index(drop=True, inplace=True)
    #print('Copy: ')
    #print(df_copy)
    if 'score' in list(df.columns):
        df = df.drop(['score'], axis=1)
    else:
        print('no score label')
    if 'label' in list(df.columns):
        df = df.drop(['label'], axis=1)
    else:
        print('no label')
    #df = df.drop(['score','label'], axis=1)
    columns = list(df.columns)
    
    if scale == 1:
        
        scaler = StandardScaler()
        print('Scaling with StandardScaler..')
        #df = scaler.fit_transform(df)
        if top_scale == 1:
            for col in columns:
                df[col] = scaler.fit_transform(df[[col]]) 
        else:
            for col in columns:
                if col == 'A_top_country' or col == 'AAAA_top_country':
                    print(col)
                else:
                    df[col] = scaler.fit_transform(df[[col]])   
        
    elif scale == 2:
        scaler = MinMaxScaler()
        print('Scaling with MinMax..')
        if top_scale == 1:
            for col in columns:
                df[col] = scaler.fit_transform(df[[col]]) 
        else:
            for col in columns:
                if col == 'A_top_country' or col == 'AAAA_top_country':
                    print(col)
                else:
                    df[col] = scaler.fit_transform(df[[col]]) 
    else:
        print('No scaling')
        
    #print(df)
    
    cluster_hdbscan = hdbscan.HDBSCAN(min_cluster_size=n_cluster)
    y_ = cluster_hdbscan.fit_predict(df)
    print('lables:')
    print(cluster_hdbscan.labels_)
    print(len(cluster_hdbscan.labels_))
    #print(type(m_kmeans.labels_))
    sil = metrics.silhouette_score(df, cluster_hdbscan.labels_, metric='euclidean')
    deboul = metrics.davies_bouldin_score(df,cluster_hdbscan.labels_)
    print('unique labels: '+ str(np.unique(cluster_hdbscan.labels_)))
    print('SIL: ' + str(sil))
    print('DB index: '+ str(deboul))
    
    #add column with cluster label
    df['cluster'] = cluster_hdbscan.labels_
    
    #saving csv with data ordered and not ordered by cluster id
    
    folder_exists('csv_hdbscan_not_sorted_by_cluster')
    file_out_hdbscan_not_sorted = os.path.join('csv_hdbscan_not_sorted_by_cluster','cluster_notsorted_'+str(n_cluster)+'.csv')
    file_exists(file_out_hdbscan_not_sorted)
    
    
    folder_exists('csv_hdbscan')
    file_out_hdbscan_csv = os.path.join('csv_hdbscan','cluster_order_'+str(n_cluster)+'.csv')
    file_exists(file_out_hdbscan_csv)
    
    #df['AAAA_top_country'] = df_copy.loc[:,'AAAA_top_country'].values
    
    #df['A_top_country'] = df_copy.loc[:,'A_top_country'].values
    
    if 'label' in list(df.columns):
        df['label'] = df_copy.loc[:,'label'].values
    
    df.to_csv(file_out_hdbscan_not_sorted)
    
    df_order = df.copy()
    df_order = df_order.sort_values(by=['cluster'])
    df_order.to_csv(file_out_hdbscan_csv)
    del df_order
    gc.collect()
    
    if 'label' in list(df.columns):
        df = df.drop(['label'], axis=1)
    
    
    
    return df,sil,deboul

def avgsil_db_index_plot(avgsil,db,minc,maxc):
    '''
    ARGS:
        -sse: vector of sse values
        -avgsil: vector of silh avarage scores 
        -db : vector of davies-bouldin socre
        -minc : minimim number of k
        -maxc : max number of k
    The aim is to plot the curve of values of sse,sil and db to vary of k starting to minc to maxc
    '''
    n = np.arange(minc,maxc+1,1)
    
    
    
    fig,(axs1,axs2) = plt.subplots(2,1)
    fig.tight_layout()
    
    
   
    axs1.plot(n,avgsil)
    axs1.set(xlabel='n_cluster',ylabel='sil')
    axs1.grid()
    axs2.plot(n,db)
    axs2.set(xlabel='n_cluster',ylabel='DB index')
    axs2.grid()
    
    plt.subplots_adjust(hspace=0.7,bottom=0.2,left=0.15)
    
  
    folder_exists('img_sse_avgsil_db')
    path_file_out = os.path.join('img_sse_avgsil_db','sse_avgsil_db_'+str(minc)+'_'+str(maxc)+'.pdf') 
    file_exists(path_file_out)
    plt.savefig(path_file_out)


def save_list_to_file(name_file,list):
    folder_exists('lists_score')
    path = os.path.join('lists_score',name_file+'.txt')
    with open(path,mode='a') as f:
        for x in list:
            f.write(str(x) + '\n')

def analyse_clusters(i):
    exit()      

def comb_pair_2feat(df_,loop):
    exit()

def pair_plot_multi_kmeans(df_,loop):
    exit()

def silh_plot(df_,loop):
    exit()

def main():
    #csv feat conc
    path_csv = sys.argv[1]
    #min value of n sapmple  in a cluster
    n_min = int(sys.argv[2])
    #max vlaue of n  samples in a cluster
    n_max = int(sys.argv[3])
    #1: stadard scaler , 2: min max scaler , other values no scaling
    scale = int(sys.argv[4])
    #sil_pair = int(sys.argv[5])
    # 1 loop over min ot max number of cluster, other values need to analyse 
    loop = int(sys.argv[5])
    #top_scale: 1, during the scaling we also consider A/AAAA_top_country
    top_scale = int(sys.argv[6])
    #save_fig: 1 we save fig 
    save_fig = int(sys.argv[7])
    
    good = []
    bad = []
    mixed = []
    
    if loop == 1:
        r = range(n_min,n_max + 1)
        sse_ = []
        sil_avgs_ = []
        db_index_ = []
        start = datetime.now()
        for i in r:
            data_ = []
            df_,sil,deb = clust_hdbscan(path_csv,i,scale,top_scale)
            
            if save_fig == 1:
                data_ = analyse_clusters(i)
                good.append(data_[0])
                bad.append(data_[1])
                mixed.append(data_[2])
                comb_pair_2feat(df_,loop)
                plt.close('all')
                pair_plot_multi_kmeans(df_,loop)
                silh_plot(df_,loop)
            #sse_.append(sse)
            sil_avgs_.append(sil)
            db_index_.append(deb)
            gc.collect()
        #plot_stat_bm(good,bad,mixed,r)
        avgsil_db_index_plot(sil_avgs_,db_index_,n_min,n_max)
        end = datetime.now()
        print('Duration: {}'.format(end - start))
        print('SIL: ')
        print(sil_avgs_)
        print('DB:')
        print(db_index_)
        
        save_list_to_file('sil',sil_avgs_)
        save_list_to_file('db',db_index_)
        
        os.system("say 'your program is finish hey hey hey hey'")
        gc.collect()


if __name__ == "__main__":
    main()
