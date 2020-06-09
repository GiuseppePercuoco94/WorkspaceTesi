import pandas as pd 
import numpy as np 
import csv 
import shutil 
import os 
import sys 
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import StandardScaler,MinMaxScaler
import gc
import numpy as np 
import matplotlib.pyplot as plt
from sklearn.metrics import silhouette_score,davies_bouldin_score
import seaborn as sns


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
        

def dbscan_mf(csv_in, e, ms,scale):
    """
    ARGS:
        csv_in: csv of feature
        e = eps distance
        ms = minumoum number of samples in a cluster
    """
    df = pd.read_csv(csv_in, low_memory=False, sep=',', index_col=['domain'])
    df = df.drop(['score'], axis=1)
    df = df.fillna(0)
    #df = df[(df['A_n_ipv4'] != 0) | (df['AAAA_n_ipv6'] != 0)]
    #print(df)
    if scale == 1:
        print('Standard Scaler..')
        columns = list(df.columns)
        scaler = StandardScaler()
        df[columns[0:-1]] = scaler.fit_transform(df[columns[0:-1]])
    elif scale == 2:
        print('MIn MAX SCALER')
        columns = list(df.columns)
        scaler = MinMaxScaler()
        df[columns[0:-1]] = scaler.fit_transform(df[columns[0:-1]])
    else:
        print('NO SCALE')
        columns = list(df.columns)
    
    #print(df)
    
    db = DBSCAN(eps=e,min_samples=ms,metric='euclidean')
    y_db = db.fit_predict(df[columns[:-1]])
    labels_ = db.labels_
    
    sil = silhouette_score(df[columns[:-1]],labels_, metric='euclidean')
    deboul = davies_bouldin_score(df[columns[:-1]],labels_)
    #print('labels: ')
    #print(labels_)
    print('len labels: '+ str(len(labels_)))
    n_clusters_ = len(set(labels_)) - (1 if -1 in labels_ else 0)
    n_noise_ = list(labels_).count(-1)   
    print('Estimated number of clusters: %d' % n_clusters_)
    print('Estimated number of noise points: %d' % n_noise_)
    df['cluster'] = labels_
    #print(df)
    df_out = df.sort_values(by=['cluster'])
    folder_name = 'csv_dbscan'
    folder_exists(folder_name)
    eround = round(e,3)
    path_csv_out = os.path.join(folder_name,'dbscan_labelorder_'+str(eround)+'-'+str(ms)+'.csv')
    file_exists(path_csv_out)
    df_out.to_csv(path_csv_out)
    
    
    del df_out
    del df
    gc.collect()
    
    return path_csv_out,n_clusters_,sil,deboul

def analyse_clusters(csv_dbscan):
    e_ms = csv_dbscan.split('/')[-1].split('_')[-1]
    df = pd.read_csv(csv_dbscan, low_memory=False, sep=',', index_col=['domain'])
    df_group = df.groupby(by='cluster')
    name_folder = 'csv_stat'
    folder_exists(name_folder)
    name_file_out = os.path.join(name_folder, 'stat_group_dbscan_'+str(e_ms[:-4])+'.csv')
    file_exists(name_file_out)
    
    with open(name_file_out, mode='a') as csv_out:
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
    
    values = []
    ndf = pd.read_csv(name_file_out, low_memory=False, sep=',')
    occ = ndf['kind'].value_counts()
    keys_occ = occ.keys()
    
    if 'good' in keys_occ:
        values.append(occ['good'])
    else:
        values.append(0)
    
    if 'bad' in keys_occ:
        values.append(occ['bad'])
    else:
        values.append(0) 
    
    if 'mixed' in keys_occ:
        values.append(occ['mixed'])
    else:
        values.append(0)
    
    return values
    
    
            
def plot_stat_bm(good,bad,mixed,range_x,minc,vect_clusters):
    
    fig, axs = plt.subplots(4,1)
    fig.tight_layout()
    
    axs[0].plot(range_x,good)
    axs[0].set_xlabel('eps')
    axs[0].set_ylabel('good')
    
    axs[1].plot(range_x,bad)
    axs[1].set_xlabel('eps')
    axs[1].set_ylabel('bad')
    
    axs[2].plot(range_x,mixed, label='mixed')
    axs[2].set_xlabel('eps')
    axs[2].set_ylabel('mixed')
    
    
    axs[3].plot(range_x,vect_clusters)
    axs[3].set_xlabel('eps')
    axs[3].set_ylabel('tot clusters')
    
    fig.suptitle('value of min sample in a cluster: '+str(minc))
    
    path_file = os.path.join('img_loop_statdbscan','stat_info_'+str(minc)+'.pdf')
    
 
    plt.subplots_adjust(hspace=0.4,left=0.1,top=0.9)
    plt.savefig(path_file)


def plot_stat_silh_db(range_eps,min_c,vct_sil,vct_db):
    print('Making stat..')
    folder_exists('Stat_sil_db_mdbscan')
    emin = np.amin(range_eps)
    emax = np.amax(range_eps)
    path_file_out = os.path.join('Stat_sil_db_mdbscan','stat_minc'+str(min_c)+'_['+str(emin)+'_'+str(emax)+'].png')
    file_exists(path_file_out)
    
    fig,(ax1,ax2) = plt.subplots(2,1)
    fig.tight_layout()
    
    ax1.plot(range_eps,vct_sil)
    ax1.set(xlabel='eps', ylabel='sil')
    ax1.grid() 
    
    ax2.plot(range_eps,vct_db)
    ax2.set(xlabel='eps', ylabel='db')
    ax2.grid() 
    
    plt.subplots_adjust(hspace=0.4, bottom=0.1,left=0.13)
    plt.savefig(path_file_out)
    
    
    
def pair_plot(csv_df_in,save,nmin,eps):
    df_in = pd.read_csv(csv_df_in, low_memory=False, sep=',', index_col=['domain'])
    df_in = df_in.drop(['label'],axis=1)
    cluster_labels = np.unique(df_in.loc[:,['cluster']])
    n_cluster = cluster_labels.shape[0]
    color = sns.color_palette(None, n_cluster-1)
    color = [[0,0,0,1]] + color
    columns = df_in.columns
    
    eps = round(eps,3)
    
    #print('Plotting')
    #print(df_kmns[columns[:-1]])
    g = sns.pairplot(df_in,height=1.5,hue='cluster',vars=df_in[columns[:-1]],diag_kind="hist", markers='o',palette=color)
    
    if save == 1:
        print('Saving pair plot n:' +str(n_cluster)+'..')
        folder_exists('Cluster_'+str(nmin)+'_'+str(eps))
        
        img_multi_png = os.path.join('Cluster_'+str(nmin)+'_'+str(eps),'pariplot_cluster_'+str(nmin)+'_'+str(eps)+'.png')
        #img_multi = os.path.join('Cluster_'+str(nmin)+'_'+str(eps),'pariplot_cluster_'+str(nmin)+'_'+str(eps)+'.pdf')
        
        #file_exists(img_multi)
        file_exists(img_multi_png)
        
        plt.savefig(img_multi_png)
        #plt.savefig(img_multi)
        
    else:
        plt.show()


  
def scatter_2f_cluster(df_in,f1_in,f2_in,nmin,eps, save):
    cluster_labels = np.unique(df_in.loc[:,['cluster']])
    #number of cluster
    n_cluster = cluster_labels.shape[0]
    #palette
    colors = sns.color_palette(None, n_cluster)
    
    fig = plt.figure(figsize=(8,8))
    ax = fig.add_subplot(1,1,1)
    ax.set_xlabel(f1_in, fontsize=10)
    ax.set_ylabel(f2_in, fontsize=10)
    
    for label,color in zip(cluster_labels,colors):
        if label == -1:
            #black color for noise point
            color = [[0, 0, 0, 1]]
        index_ = df_in['cluster'] == label
        ax.scatter(df_in.loc[index_,f1_in],df_in.loc[index_,f2_in], c=color, s=25)
    '''   
    legend = []
    for x in cluster_labels:
        legend.append('cluster '+ str(x))
    ax.legend(legend)
    '''
    
    if save == 1:
        folder_cluster = 'Cluster_'+str(nmin)+'_'+str(eps)
        folder_exists(folder_cluster)
        folder_scatter_2feats = os.path.join(folder_cluster,'scatter_2feats')
        folder_exists(folder_scatter_2feats)
        
        scatter_2feats_png = os.path.join(folder_scatter_2feats,'scatter_'+f1_in+'_'+f2_in+'.png')
        scatter_2feats_pdf = os.path.join(folder_scatter_2feats,'scatter_'+f1_in+'_'+f2_in+'.pdf')
        file_exists(scatter_2feats_png)
        file_exists(scatter_2feats_pdf)
        
        plt.savefig(scatter_2feats_png)
        plt.savefig(scatter_2feats_pdf)
        
    else:
        plt.show()
        


def comb_pair_2feat(csv_df_in,nmin,eps,loop):
    df_in = pd.read_csv(csv_df_in, low_memory=False, sep=',', index_col=['domain'])
    df_in = df_in.drop(['label'],axis=1)
    cluster_labels = np.unique(df_in.loc[:,['cluster']])
    #number of cluster
    n_cluster = cluster_labels.shape[0]
    #palette
    colors = sns.color_palette(None, n_cluster)
    columns = list(df_in.columns)
    columns = columns[:-1]
    print(columns)
    eps = round(eps,3)
    
    l = len(columns)
    for i in range(0,l-1):
        print('iter: '+str(i))
        if i < l-1:
            for j in range (i+1,l):
                scatter_2f_cluster(df_in,columns[i],columns[j],nmin,eps,loop)
                gc.collect()
        else:
            print('Finish scatter comb 2feat')
    
    

def main_loop():
    csv_feat = sys.argv[1]
    max_eps = float(sys.argv[2])
    max_min_clust = int(sys.argv[3])
    scale = int(sys.argv[4])
    
    range_eps = np.arange(0.1,max_eps,0.1)
    range_min_clust = range(3, max_min_clust +1)
    
    print(range_eps)
    print(range_min_clust)
    
    info_to_plot_ =[]
    n_clusters = []
    #sub_s_name_stat = '[0.1_'+str(max_eps)+''
    folder_exists('img_loop_statdbscan')
    
    for i in range_min_clust:
        good_ = []
        bad_ = []
        mixed_ = []
        clus_= []
        sil_ = []
        db_ = []
        for j in range_eps:
            data_ = []
            csv_dbscn,nc,sil,deb = dbscan_mf(csv_feat,j,i,scale)
            clus_.append(nc)
            data_ = analyse_clusters(csv_dbscn)
            print(data_)
            good_.append(data_[0])
            bad_.append(data_[1])
            mixed_.append(data_[2])
            sil_.append(sil)
            db_.append(deb)
            comb_pair_2feat(csv_dbscn,i,j,1)
            pair_plot(csv_dbscn,1,i,j)
            plt.close('all')
            gc.collect()
        print('param sil deb')
        print(sil_)
        print(db_)
        plot_stat_bm(good_,bad_,mixed_,range_eps,i,clus_)
        plot_stat_silh_db(range_eps,i,sil_,db_)
        #info_to_plot_.append(data_)
        #n_clusters.append(clus_)
            
    #print(info_to_plot_)
    #print(n_clusters)
    
    #plot_stat(info_to_plot_, n_clusters)
    gc.collect()
    
 
    
    
def main():
    csv_feat = sys.argv[1]
    eps = float(sys.argv[2])
    minc = int(sys.argv[3])
    csv_dbscan,nclu = dbscan_mf(csv_feat, eps, minc)
    analyse_clusters(csv_dbscan)

if __name__ == '__main__':
    #main()
    main_loop()