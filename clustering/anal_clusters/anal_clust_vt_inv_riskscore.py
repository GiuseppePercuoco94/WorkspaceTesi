import pandas as pd 
import numpy as np
import matplotlib.pyplot as plt
import os
import csv
import glob
import shutil
import gc
import sys


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


def retrieve_df_bad_column_not_sorted(folder_csv_clust,csv_bad):
    folder_sorted = 'sorted_by_cluster'
    folder_not_sorted = 'not_sorted'
    folder_exists(folder_not_sorted)
    folder_exists(folder_sorted)
    
    
    for file in os.listdir(folder_csv_clust):
        print(file)
        if file.endswith('.csv') and file[0] != '.':
            df_clu = pd.read_csv(os.path.join(folder_csv_clust,file), sep=',', low_memory=False, index_col=['domain'])
            df_col_bad = pd.read_csv(csv_bad, sep=',', low_memory=False)
            
            df_col_bad = df_col_bad.drop_duplicates(subset='domain')
            df_col_bad = df_col_bad.set_index('domain')
            #print(df_col_bad)
            
            index_dom_clust_ = []
            for index_,_row in df_clu.iterrows():
                index_dom_clust_.append(index_)
            
            score_ = []
            for dom in index_dom_clust_:
                if type(df_col_bad.loc[dom,'label']) is pd.Series:
                    print(dom)
                    score = df_col_bad.loc[dom,'label'].tolist()[0]
                    score_.append(score)
                else:
                    score = df_col_bad.loc[dom,'label']
                    score_.append(score)
            
            df_clu['label'] = score_
            
            path_sorted = os.path.join(folder_sorted,file)
            path_not_sorted = os.path.join(folder_not_sorted,file)
            file_exists(path_not_sorted)
            file_exists(path_sorted)
            df_clu.to_csv(path_not_sorted)
            df_clu = df_clu.sort_values(by=['cluster'])
            df_clu.to_csv(path_sorted)
            del df_clu
            del df_col_bad
            gc.collect()
    return folder_not_sorted
            
def anal_cluster(folder_csv_not_ordered):
    print('ANAL CLUSTER')
    folder_info_per_cluster = 'info_per_cluster' 
    folder_exists(folder_info_per_cluster)
    
    for file in os.listdir(folder_csv_not_ordered):
        print(file)
        if file.endswith('.csv') and file[0] != '.':
            path_info = os.path.join(folder_info_per_cluster,file)
            file_exists(path_info)
            df = pd.read_csv(os.path.join(folder_csv_not_ordered,file), sep=',',low_memory=False, index_col=['domain'])
            df_group = df.groupby(by='cluster')
            
            with open(path_info,mode='a') as csv_out:
                writer = csv.writer(csv_out)
                header = ['class','good','weak_good','weak_bad','bad','kind']
                writer.writerow(header)
                
                for key,item in df_group:
                    data_ = []
                    value_counts_ = item['label'].value_counts()
                    keys_ = value_counts_.keys()
                    data_.append(key)
            
                    
                    if 'good' in keys_:
                        data_.append(value_counts_['good'])
                    else:
                        data_.append(0)
                    if 'weak_good' in keys_:
                        data_.append(value_counts_['weak_good'])
                    else:
                        data_.append(0) 
                    if 'weak_bad' in keys_:
                        data_.append(value_counts_['weak_bad'])
                    else:
                        data_.append(0)
                    if 'bad' in keys_:
                        data_.append(value_counts_['bad'])
                    else:
                        data_.append(0)
                    
                                    
                    if len(keys_) == 1:
                        if keys_ == 'bad':
                            data_.append('bad')
                        elif keys_ == 'good':
                            data_.append('good')
                        elif keys_ == 'weak_bad':
                            data_.append('weak_bad')
                        else:
                            data_.append('weak_good')
                    elif len(keys_) == 3 or len(keys_) == 4 or len(keys_) == 2:
                        data_.append('mixed')
                    else:
                        print('New Case')
                    writer.writerow(data_)
  
def img_stat(csv_folder_group,kmeans_hdbscan_dbscan):
    if kmeans_hdbscan_dbscan == 1:
        good = []
        weak_good = []
        weak_bad = []
        bad = []   
        mixed = []
        
        for file in os.listdir(csv_folder_group):
            if file.endswith('.csv') and file[0] != '.':
                df = pd.read_csv(os.path.join(csv_folder_group,file),sep=',',low_memory=False)
                occ = df['kind'].value_counts()
                keys_occ = occ.keys()
                
                if 'good' in keys_occ:
                    good.append(occ['good'])
                else:
                    good.append(0)
                if 'bad' in keys_occ:
                    bad.append(occ['bad'])
                else:
                    bad.append(0)
                if 'weak_good' in keys_occ:
                    weak_good.append(occ['weak_good'])
                else:
                    weak_good.append(0)
                if 'weak_bad' in keys_occ:
                    weak_bad.append(occ['weak_bad'])
                else:
                    weak_bad.append(0)
                if 'mixed' in keys_occ:
                    mixed.append(occ['mixed'])
                else:
                    mixed.append(0)
                
        plot_stat_bm(good,weak_good,weak_bad,bad,mixed,range(2,26),'img_stat',kmeans_hdbscan_dbscan)
    elif kmeans_hdbscan_dbscan == 2:
        good = []
        weak_good = []
        weak_bad = []
        bad = []   
        mixed = []
        
        for file in os.listdir(csv_folder_group):
            if file.endswith('.csv') and file[0] != '.':
                df = pd.read_csv(os.path.join(csv_folder_group,file),sep=',',low_memory=False)
                occ = df['kind'].value_counts()
                keys_occ = occ.keys()
                
                if 'good' in keys_occ:
                    good.append(occ['good'])
                else:
                    good.append(0)
                if 'bad' in keys_occ:
                    bad.append(occ['bad'])
                else:
                    bad.append(0)
                if 'weak_good' in keys_occ:
                    weak_good.append(occ['weak_good'])
                else:
                    weak_good.append(0)
                if 'weak_bad' in keys_occ:
                    weak_bad.append(occ['weak_bad'])
                else:
                    weak_bad.append(0)
                if 'mixed' in keys_occ:
                    mixed.append(occ['mixed'])
                else:
                    mixed.append(0)
        plot_stat_bm(good,weak_good,weak_bad,bad,mixed,range(5,26),'img_stat',kmeans_hdbscan_dbscan)
    else:
        range_minc = np.arange(3,6,1)
        range_eps = np.arange(0.1,1.1,0.1)
        
        for i in range_minc:
            good = []
            weak_good = []
            weak_bad = []
            bad = []   
            mixed = []
            for j in range_eps:
                df = pd.read_csv(os.path.join(csv_folder_group,'cluster_notsorted_dbscan_labelorder_'+str(round(j,3))+'-'+str(i)+'.csv'),sep=',',low_memory=False)
                occ = df['kind'].value_counts()
                keys_occ = occ.keys()
                
                if 'good' in keys_occ:
                    good.append(occ['good'])
                else:
                    good.append(0)
                if 'bad' in keys_occ:
                    bad.append(occ['bad'])
                else:
                    bad.append(0)
                if 'weak_good' in keys_occ:
                    weak_good.append(occ['weak_good'])
                else:
                    weak_good.append(0)
                if 'weak_bad' in keys_occ:
                    weak_bad.append(occ['weak_bad'])
                else:
                    weak_bad.append(0)
                if 'mixed' in keys_occ:
                    mixed.append(occ['mixed'])
                else:
                    mixed.append(0)
            
            plot_stat_bm(good,weak_good,weak_bad,bad,mixed,range_eps,'img_stat',0,i)
                
                
def plot_stat_bm(good,weak_good,weak_bad,bad,mixed,range,folder_to_save,kmeans_hdbscan_dbscan, minc=None,vect_clusters=None):
    if kmeans_hdbscan_dbscan == 1:
        folder_exists(folder_to_save)
        min = np.amin(range)
        max = np.amax(range)
        fig, axs = plt.subplots(5,1)
        fig.tight_layout()
        
        axs[0].plot(range,good, label='good')
        axs[0].set_xlabel('k')
        axs[0].set_ylabel('good')
            
        axs[1].plot(range,bad, label='bad')
        axs[1].set_xlabel('k')
        axs[1].set_ylabel('bad')
            
        axs[2].plot(range,mixed, label='mixed')
        axs[2].set_xlabel('k')
        axs[2].set_ylabel('mixed')
        
        axs[3].plot(range,weak_good, label='weak_good')
        axs[3].set_xlabel('k')
        axs[3].set_ylabel('weak_good')

        axs[4].plot(range,weak_bad, label='weak_bad')
        axs[4].set_xlabel('k')
        axs[4].set_ylabel('weak_bad')
        
        path_file = os.path.join(folder_to_save,'kmeans_bgu_'+str(min)+'_'+str(max)+'.pdf')
        file_exists(path_file)
        
        plt.subplots_adjust(hspace=0.4,left=0.2,top=0.9)
        plt.savefig(path_file) 
    elif kmeans_hdbscan_dbscan == 2:
        folder_exists(folder_to_save)
        min = np.amin(range)
        max = np.amax(range)
        fig, axs = plt.subplots(5,1)
        fig.tight_layout()
        
        axs[0].plot(range,good, label='good')
        axs[0].set_xlabel('min_c_size')
        axs[0].set_ylabel('good')
            
        axs[1].plot(range,bad, label='bad')
        axs[1].set_xlabel('min_c_size')
        axs[1].set_ylabel('bad')
            
        axs[2].plot(range,mixed, label='mixed')
        axs[2].set_xlabel('min_c_size')
        axs[2].set_ylabel('mixed')
        
        axs[3].plot(range,weak_good, label='weak_good')
        axs[3].set_xlabel('min_c_size')
        axs[3].set_ylabel('weak_good')

        axs[4].plot(range,weak_bad, label='weak_bad')
        axs[4].set_xlabel('min_c_size')
        axs[4].set_ylabel('weak_bad')
        
        path_file = os.path.join(folder_to_save,'hdbscan_bgu_'+str(min)+'_'+str(max)+'.pdf')
        file_exists(path_file)
        
        plt.subplots_adjust(hspace=0.4,left=0.2,top=0.9)
        plt.savefig(path_file) 
    else:
        folder_exists(folder_to_save)
        fig, axs = plt.subplots(5,1)
        fig.tight_layout()
        
        axs[0].plot(range,good,label='good')
        axs[0].set_xlabel('eps')
        axs[0].set_ylabel('good')
            
        axs[1].plot(range,bad, label='bad')
        axs[1].set_xlabel('eps')
        axs[1].set_ylabel('bad')
            
        axs[2].plot(range,mixed, label='mixed')
        axs[2].set_xlabel('eps')
        axs[2].set_ylabel('mixed')
        
        axs[3].plot(range,weak_good, label='weak_good')
        axs[3].set_xlabel('eps')
        axs[3].set_ylabel('weak_good')

        axs[4].plot(range,weak_bad, label='weak_bad')
        axs[4].set_xlabel('eps')
        axs[4].set_ylabel('weak_bad')

        fig.suptitle('value of min sample in a cluster: '+str(minc))
        
        path_file = os.path.join(folder_to_save,'stat_info_'+str(minc)+'.pdf')
        
    
        plt.subplots_adjust(hspace=0.4,left=0.2,top=0.9)
        plt.savefig(path_file)
        

def main():
    folder = sys.argv[1]
    csv_bad = sys.argv[2]
    '''
    kmeans_hdbscan_dbscan:
        -1 : kmeans
        -2 : hdbscan
        -other : dbscan
    '''
    kmeans_hdbscan_dbscan = int(sys.argv[3])
    folder_not_sorted = retrieve_df_bad_column_not_sorted(folder,csv_bad)
    anal_cluster(folder_not_sorted)
    img_stat('info_per_cluster',kmeans_hdbscan_dbscan)
    
    
if __name__ == '__main__':
    main()