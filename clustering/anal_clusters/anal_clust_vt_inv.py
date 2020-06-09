import pandas as pd 
import numpy as np 
import matplotlib.pyplot as plt
import glob
import csv 
import os 
import sys
import gc
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


def retrieve_df_bad_column_not_sorted(folder_csv_clust,csv_bad, folder_name):
    '''
    Args_in:
        -folder_csv_clust: folder that contain the csv obtained form clustering process (not sorted by cluster)
        -csv_bad: csv obtained from scanning with vt or investigate
        -folder_name: name of folder where save the output..
    The aim is to keep the csv obtained from clustering process and append the column 'label'. The column label depens on the csv_bad 
    and it will be obtained from VT_lim, VT_lim2 etc. The body/Dataframe from clustering don't change, only the column 'label' change
    '''
    print('\n')
    #folder_csv_clust = folder_csv_clust
    
    #files = os.listdir(folder_csv_clust)
    
    folder_ = folder_exists(folder_name)
    folder_sorted = 'sorted_by_cluster'
    folder_exists(folder_sorted)
   
    '''
        for each csv file in the folder we substitute the column 'label' with the column extract form 'csv_bad'
    '''
    for file in os.listdir(folder_csv_clust):
        print(file)
        if file.endswith('.csv') and file[0] != '.':
            #check exitance of file
            #file_path = os.path.join(folder_csv_clust,file)
            
        
            df_clu = pd.read_csv(os.path.join(folder_csv_clust,file), sep=',',low_memory=False, index_col=['domain'])
            df_col_bad = pd.read_csv(csv_bad,sep=',',low_memory=False, index_col=['domain'])
        
            df_clu = df_clu.drop(['label'], axis=1)
        
        #concat csv cluster with csv of bad column -> join = 'inner' only common domain in index
        #df_clu = pd.concat([df_clu,df_col_bad], axis=1, join='inner')
        #df_clu['label'] = df_col_bad
        #print(df_clu)
        
        #retrieve col indexies form csv cluster and retrive vector of Bad decision (bad, good, untrucked)
            index_clu_ = []
            for index_,row in df_clu.iterrows():
                index_clu_.append(index_)
            
            score_ = [] 
            for dom in index_clu_:
                if type(df_col_bad.loc[dom,'label']) is pd.Series:
                    score = df_col_bad.loc[dom,'label'].tolist()[0]
                    score_.append(score)
                else:
                    score = df_col_bad.loc[dom,'label']
                    score_.append(score)
                '''
                    print(df_col_bad.loc[dom,'label'])
                    print(type(df_col_bad.loc[dom,'label']))
                    print(len(df_col_bad.loc[dom,'label']))
                    print(df_col_bad.loc[dom,'label'].tolist()[0])
                score = df_col_bad.loc[dom,'label']
                score_.append(score)
                '''
            #append the new 'label' column .. in this way we can append every column obtain from VTlim1, VTlim2..etc. 
            # the df don't change, but change only the 'label' column
            df_clu['label'] = score_
            
            #create the path where save the csv sorted by cluster and save it
            path_sorted = os.path.join(folder_sorted,file)
            #create the path where save the csv not sorted by cluster and save it
            path_out = os.path.join(folder_name,file) 
            file_exists(path_out)
            df_clu.to_csv(path_out)
            df_clu = df_clu.sort_values(by=['cluster'])
            df_clu.to_csv(path_sorted)
            print('Fine')
            gc.collect()
            
    print('fine')
            
        
        #return df_clu
    
    
    
def anal_cluster(folder_csv_not_ordered,folder_save):
    '''
        ARGS_in:
            -folder_csv_not_ordered: path folder obtained from the 'retrieve_df_bad_column_not_sorted' function
            -folder_save: folder that will contain the csv .. statically assigned with the name 'stat_csv'
        
        In this funcition we keep the csv files in the 'folder_csv_not_ordered' and for each of them we wil compute
        some summary statistics likes for each cluster the number of bad, good and untrucked samples.
        Es:
        domain|good|bad|untrucked|kind
        a.com  1     2   0         mixed
        b.com  3     0   0         good
        c.com  0     3   0         bad
        d.com  0     0   1         mixed
        
    '''
    folder_exists(folder_save)
    
    for file in os.listdir(folder_csv_not_ordered):
        print(file)
        if file.endswith('.csv') and file[0] != '.':
            #fodler_stat = 'stat_csv'
            path_stat  = os.path.join(folder_save,file)
            file_exists(path_stat)
            
            df_in = pd.read_csv(os.path.join(folder_csv_not_ordered,file), sep=',',low_memory=False, index_col=['domain'])
            df_group = df_in.groupby(by='cluster')
            '''
            for each cluster we compute how many samples are labelled as bad,good,untrucked.
            '''
            with open(path_stat,mode='a') as csv_out:
                writer = csv.writer(csv_out)
                header = ['class','good','bad','untracked','kind']
                writer.writerow(header)
                
                for key,item in df_group:
                    data_ = []
                    describe_ = item['label'].describe(include='all')
                    value_counts_ = item['label'].value_counts()
                    keys_ = value_counts_.keys()
                    
                    data_.append(key)
                    
                    if 'good' in keys_:
                        data_.append(value_counts_['good'])
                    else:
                        data_.append(0)
                    
                    if 'bad' in keys_:
                        data_.append(value_counts_['bad'])
                    else:
                        data_.append(0)
                        
                    if 'untracked' in keys_:
                        data_.append(value_counts_['untracked'])
                    else:
                        data_.append(0)
                    
                    '''
                    if the cluster contains only bad it is marked as bad, if it contains only 'good' it is marked as 'good',
                    if it is contain only 'untracked' it is marked as 'untracked', if it contain mixed labels it is marked as 'mixed'
                    '''
                    if len(keys_) == 3 or len(keys_) == 2:
                        data_.append('mixed')
                    elif len(keys_) == 1:
                        if keys_ == 'bad':
                            data_.append('bad')
                        elif keys_ == 'good':
                            data_.append('good')
                        else:
                            data_.append('untracked')
                    else:
                        print('new case in labeling')
                        
                    writer.writerow(data_)
  
    
def img_stat(csv_folder_group,folder_to_save,kmeans_dbscan):
    '''
    ARGS_in:
        -csv_folder_group: folder obtained from 'anal_cluster' .. it is the folder that contain the csv clusters sorted by cluster id
        -folder_to_save: statically assigned as 'img_stat'
        -kmeans_dbscan: 1 if the files are obtained from a kmeans process,  other values for a dbscan process
    
    For each csv we extract info for how many cluster 'bad','good' and 'mixed' there are for plot the beaviour of clustering process when 
    parameters change. 
    '''
    
    if kmeans_dbscan == 1:
        good = []
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
                
                if 'mixed' in keys_occ:
                    mixed.append(occ['mixed'])
                else:
                    mixed.append(0)
        
        plot_stat_bm(good,bad,mixed,range(2,26),folder_to_save,kmeans_dbscan)
    else:
        
        range_minc = np.arange(3,6,1)
        range_eps = np.arange(0.1,1.1,0.1)
        
        for i in range_minc:
            good = []
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
                
                if 'mixed' in keys_occ:
                    mixed.append(occ['mixed'])
                else:
                    mixed.append(0)
            plot_stat_bm(good,bad,mixed,range_eps,folder_to_save,0,i)
        
        

def plot_stat_bm(good,bad,mixed,range,folder_to_save,kmeans_dbscan, minc=None,vect_clusters=None):
    if kmeans_dbscan == 1:
        folder_exists(folder_to_save)
        min = np.amin(range)
        max = np.amax(range)
        fig, axs = plt.subplots(3,1)
        fig.tight_layout()
        
        axs[0].plot(range,good)
        axs[0].set_xlabel('k')
        axs[0].set_ylabel('good')
        
        axs[1].plot(range,bad)
        axs[1].set_xlabel('k')
        axs[1].set_ylabel('bad')
        
        axs[2].plot(range,mixed, label='mixed')
        axs[2].set_xlabel('k')
        axs[2].set_ylabel('mixed')
        

        path_file = os.path.join(folder_to_save,'kmeans_bgu_'+str(min)+'_'+str(max)+'.pdf')
        file_exists(path_file)
        
        plt.subplots_adjust(hspace=0.4,left=0.1,top=0.9)
        plt.savefig(path_file) 
    else:
        
        folder_exists(folder_to_save)
        fig, axs = plt.subplots(3,1)
        fig.tight_layout()
        
        axs[0].plot(range,good)
        axs[0].set_xlabel('eps')
        axs[0].set_ylabel('good')
        
        axs[1].plot(range,bad)
        axs[1].set_xlabel('eps')
        axs[1].set_ylabel('bad')
        
        axs[2].plot(range,mixed, label='mixed')
        axs[2].set_xlabel('eps')
        axs[2].set_ylabel('mixed')
        
        '''
        axs[3].plot(range_x,vect_clusters)
        axs[3].set_xlabel('eps')
        axs[3].set_ylabel('tot clusters')
        '''
        
        fig.suptitle('value of min sample in a cluster: '+str(minc))
        
        path_file = os.path.join(folder_to_save,'stat_info_'+str(minc)+'.pdf')
        
    
        plt.subplots_adjust(hspace=0.4,left=0.1,top=0.9)
        plt.savefig(path_file)
        
    
def main():
    folder_not_sorted = 'csv_not_sorted_by_cluster'
    folder_sorted = 'sorted_by_cluster'
    csv_stat = 'stat_csv'
    img_csv_stat = 'img_stat'
    
    # file csv that contain the column obtained from vt or investigate 
    csv_vt_inv = sys.argv[1]
    #folder in : folder that contain the csv obtained from clustering process
    folder_in = sys.argv[2]
    #list_files = glob.glob(folder_in)
    
    #kmeans_dbscan=1 -> save img kmeans, kmeans_dbscan =0 -> save img dbscan
    kmeans_dbscan = int(sys.argv[3])
    
    
    retrieve_df_bad_column_not_sorted(folder_in,csv_vt_inv,folder_not_sorted)
    anal_cluster(folder_not_sorted,csv_stat)
    img_stat(csv_stat,img_csv_stat,kmeans_dbscan)
    
    vt_inv_folder = csv_vt_inv.split('/')[-1].split('.')[0]
    print(vt_inv_folder)
    folder_in = folder_in.split('/')[-2]
    print(folder_in)
    
    
    #move folders 
    folder_exists(folder_in)
    folder_exists(vt_inv_folder)
    #os.makedirs(folder_in)
    #os.makedirs(vt_inv_folder)
    shutil.move(folder_not_sorted,folder_in)
    shutil.move(folder_sorted,folder_in)
    shutil.move(csv_stat,folder_in)
    shutil.move(img_csv_stat,folder_in)
    shutil.move(folder_in,vt_inv_folder)
    gc.collect()
    
    
    
if __name__ == '__main__':
    main()