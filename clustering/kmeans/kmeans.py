import numpy as np 
import pandas as pd 
import csv
import os
import sys
import shutil
from matplotlib import cm 
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
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
        

def multi_kmeans(csv_in, n_cluster, scale, top_scale):
    '''
    ARGS:
        -csv_in: csv of features
        -n_cluster: 'k' number of clusters
        -scale: 1-> sdandard scaler, 2-> minmax scaler, other no scaler
    '''
    df = pd.read_csv(csv_in, low_memory=False)
    df = df.drop_duplicates(subset='domain')
    df = df.set_index('domain')

    #remove 'score column' and tuple with no information from OI, and the column score and label
    if 'PC1' not in list(df.columns):
        df = df.fillna(0)
        df = df[(df['A_n_ipv4'] != 0) | (df['AAAA_n_ipv6'] != 0)]
    print(df)
    if 'label' in list(df.columns):
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
    
    m_kmeans = KMeans(n_clusters=int(n_cluster))
    y_ = m_kmeans.fit_predict(df)
    centroids = m_kmeans.cluster_centers_
    print('Centroids: ')
    print(centroids)
    print('lables:')
    print(m_kmeans.labels_)
    print(len(m_kmeans.labels_))
    #print(type(m_kmeans.labels_))
    sil = metrics.silhouette_score(df, m_kmeans.labels_, metric='euclidean')
    deboul = metrics.davies_bouldin_score(df,m_kmeans.labels_)
    print('unique labels: '+ str(np.unique(m_kmeans.labels_)))
    print('SSE: '+ str(m_kmeans.inertia_))
    print('SIL: ' + str(sil))
    print('DB index: '+ str(deboul))
    
    #add column with cluster label
    df['cluster'] = m_kmeans.labels_
    
    #saving csv with data ordered and not ordered by cluster id
    
    folder_exists('csv_kmeans_not_sorted_by_cluster')
    file_out_kmeans_not_sorted = os.path.join('csv_kmeans_not_sorted_by_cluster','cluster_notsorted_'+str(n_cluster)+'.csv')
    file_exists(file_out_kmeans_not_sorted)
    
    
    folder_exists('csv_kmeans')
    file_out_kmeans_csv = os.path.join('csv_kmeans','cluster_order_'+str(n_cluster)+'.csv')
    file_exists(file_out_kmeans_csv)
    
    #df['AAAA_top_country'] = df_copy.loc[:,'AAAA_top_country'].values
    
    #df['A_top_country'] = df_copy.loc[:,'A_top_country'].values
    
    if 'label' in list(df.columns):
        df['label'] = df_copy.loc[:,'label'].values
    
    df.to_csv(file_out_kmeans_not_sorted)
    
    df_order = df.copy()
    df_order = df_order.sort_values(by=['cluster'])
    df_order.to_csv(file_out_kmeans_csv)
    del df_order
    gc.collect()
    
    if 'label' in list(df.columns):
        df = df.drop(['label'], axis=1)
    
    
    
    return df,m_kmeans.inertia_,sil,deboul


def analyse_clusters(n_cluster):
    '''
    ARGS:
        -n_cluster: number of 'k' of kmeans
    
    The aim is to evaluate how many cluster are 'bad','good' or 'mixed' according to how the cluster is composed 
    (only 'bad', 'good','untracked' or a combination of them('mixed') )
    
    '''
    df_in = pd.read_csv('csv_kmeans/cluster_order_'+str(n_cluster)+'.csv',index_col=['domain'],low_memory=False,sep=',')

    folder_exists('csv_stat')
    name_file_out = os.path.join('csv_stat','stat_cluster_'+str(n_cluster)+'.csv')
    file_exists(name_file_out)
    
    df_group = df_in.groupby(by='cluster')
    with open(name_file_out,mode='a') as csv_out:
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

    
    

def silh_plot(df_kmean,save):
    '''
     ARGS:
        -df_kmean: dataframe of kmeans process
        -save: 1 save silh plot, else show the plot
    
    With this funcition we construc an orizontal plot: on the y axis we have each cluster with the relative samples, and the orizontal
    bar represent the value of silh for each saples. In addition we plot vertically a line that means the avarage score of silh values
    '''
    fig = plt.figure(figsize=(8,8))
    ax = fig.add_subplot(1,1,1)
    y = df_kmean.loc[:,['cluster']].values.ravel()
    df_kmean = df_kmean.drop(['cluster'], axis=1)
    cluster_labels = np.unique(y)
    n_cluster = cluster_labels.shape[0]
    
    print('Silh  plot '+str(n_cluster)+'..')
    silhouette_vals = silhouette_samples(df_kmean,y, metric='euclidean')
    color = sns.color_palette(None,n_cluster)
    y_ax_lower, y_ax_upper = 0, 0
    yticks = []
    
    for i,c in enumerate(cluster_labels):
        c_silhouette_vals = silhouette_vals[y == c]
        c_silhouette_vals.sort()
        y_ax_upper += len(c_silhouette_vals)
        c= color[i]
        ax.barh(range(y_ax_lower,y_ax_upper), c_silhouette_vals, height=1.0, edgecolor='none',color=c)
        yticks.append((y_ax_lower + y_ax_upper) / 2.)
        y_ax_lower += len(c_silhouette_vals)
        
    silhouette_avg = np.mean(silhouette_vals)
    ax.axvline(silhouette_avg, color = 'red', linestyle="--")    
    ax.set_yticks(yticks)
    ax.set_yticklabels(cluster_labels+1)
    ax.set_ylabel('Cluster')
    ax.set_xlabel('Silhouette coefficient')
    ax.set_title('N Cluster ' + str(n_cluster))
    
    if save == 1:
        folder_cluster = 'Cluster_'+str(n_cluster)
        folder_exists(folder_cluster)
        folder_sil = os.path.join(folder_cluster,'sil_plot')
        folder_exists(folder_sil)
        path_img_shil_png = os.path.join(folder_sil, 'shil_n_'+str(n_cluster)+'.png')
        path_img_shil_pdf = os.path.join(folder_sil, 'shil_n_'+str(n_cluster)+'.pdf')
        file_exists(path_img_shil_png)
        file_exists(path_img_shil_pdf)
        
        
        plt.savefig(path_img_shil_png)
        plt.savefig(path_img_shil_pdf)
        
    else:
        plt.show()
        

def sse_avgsil_db_index_plot(sse,avgsil,db,minc,maxc):
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
    
    
    
    fig,(axs1,axs2,axs3) = plt.subplots(3,1)
    fig.tight_layout()
    
    
    axs1.plot(n,sse)
    axs1.set(xlabel='n_cluster',ylabel='SSE')
    axs1.grid()
    axs2.plot(n,avgsil)
    axs2.set(xlabel='n_cluster',ylabel='sil')
    axs2.grid()
    axs3.plot(n,db)
    axs3.set(xlabel='n_cluster',ylabel='DB index')
    axs3.grid()
    
    plt.subplots_adjust(hspace=0.7,bottom=0.2,left=0.15)
    
  
    folder_exists('img_sse_avgsil_db')
    path_file_out = os.path.join('img_sse_avgsil_db','sse_avgsil_db_'+str(minc)+'_'+str(maxc)+'.png') 
    file_exists(path_file_out)
    plt.savefig(path_file_out)
    

def pair_plot_multi_kmeans(df_kmns,save):
    cluster_labels = np.unique(df_kmns.loc[:,['cluster']])
    n_cluster = cluster_labels.shape[0]
    color = sns.color_palette('hls', n_cluster)
    columns = df_kmns.columns
    
    #print('Plotting')
    #print(df_kmns[columns[:-1]])
    g = sns.pairplot(df_kmns,height=1.5,hue='cluster',vars=df_kmns[columns[:-1]],diag_kind="hist", markers='o',palette=color)
    
    if save == 1:
        print('Saving pair plot n:' +str(n_cluster)+'..')
        folder_exists('Cluster_'+str(n_cluster))
        
        img_multi = os.path.join('Cluster_'+str(n_cluster),'pariplot_cluster_'+str(n_cluster)+'.pdf')
        img_multi_png = os.path.join('Cluster_'+str(n_cluster),'pariplot_cluster_'+str(n_cluster)+'.png')
        file_exists(img_multi)
        file_exists(img_multi_png)
        
        plt.savefig(img_multi_png)
        plt.savefig(img_multi)
        
    else:
        plt.show()
        

def scatter_2f_cluster(df_in,f1_in,f2_in, save):
    '''
        ARGS:
            -df_in: dataframe with features
            -f1_in: first feature
            -f2_in: second feature
            -save: 1-> save polot, else show it
    '''
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
        index_ = df_in['cluster'] == label
        ax.scatter(df_in.loc[index_,f1_in],df_in.loc[index_,f2_in], c=color, s=25)
        
    legend = []
    for x in cluster_labels:
        legend.append('cluster '+ str(x))
    ax.legend(legend)
    
    if save == 1:
        folder_cluster = 'Cluster_'+str(n_cluster)
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
        


def comb_pair_2feat(df_in,loop):
    '''
    ARGS:
        -df_in: dataframe from kmeans process
        -loop: 1-> save, 0 show plots
    '''
    cluster_labels = np.unique(df_in.loc[:,['cluster']])
    #number of cluster
    n_cluster = cluster_labels.shape[0]
    #palette
    colors = sns.color_palette(None, n_cluster)
    columns = list(df_in.columns)
    columns = columns[:-1]
    print(columns)
    
    l = len(columns)
    for i in range(0,l-1):
        print('iter: '+str(i))
        if i < l-1:
            for j in range (i+1,l):
                scatter_2f_cluster(df_in,columns[i],columns[j],loop)
                gc.collect()
        else:
            print('Finish scatter comb 2feat')
        
    
def plot_stat_bm(good,bad,mixed,range):
    '''
    ARGS:
        -good: vector 
        -bad: vecotr
        -mixed: vector
        -range: range of x axies
        
    The aim is to plot the curve of values of 'bad', 'good', 'mixed' to vary of eps values and the minc value is fixed
        
    '''
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
    
    
    folder_exists('img_kmeans_bgu')
    path_file = os.path.join('img_kmeans_bgu','kmeans_bgu_'+str(min)+'_'+str(max)+'.pdf')
    file_exists(path_file)
    
    plt.subplots_adjust(hspace=0.4,left=0.1,top=0.9)
    plt.savefig(path_file) 


def save_list_to_file(name_file,list):
    folder_exists('lists_score')
    path = os.path.join('lists_score',name_file+'.txt')
    with open(path,mode='w') as f:
        for x in list:
            f.write(str(x) + '\n')
  

def main():
    #csv feat conc
    path_csv = sys.argv[1]
    #min value of n cluster
    n_min = int(sys.argv[2])
    #max vlaue of n cluster
    n_max = int(sys.argv[3])
    #1: stadard scaler , 2: min max scaler , other values no scaling
    scale = int(sys.argv[4])
    #sil_pair = int(sys.argv[5])
    # 1 loop over min ot max number of cluster, other values need to analyse 
    loop = int(sys.argv[5])
    #top_scale: 1, during the scaling we also consider A/AAAA_top_country
    top_scale = int(sys.argv[6])
    #save_fig: 1 save fig 
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
            #data_ = []
            df_,sse,sil,deb = multi_kmeans(path_csv,i,scale,top_scale)
            if save_fig == 1:
                #data_ = analyse_clusters(i)
                #good.append(data_[0])
                #bad.append(data_[1])
                #mixed.append(data_[2])
                comb_pair_2feat(df_,loop)
                plt.close('all')
                pair_plot_multi_kmeans(df_,loop)
                silh_plot(df_,loop)
            sse_.append(sse)
            sil_avgs_.append(sil)
            db_index_.append(deb)
            gc.collect()
        #plot_stat_bm(good,bad,mixed,r)
        sse_avgsil_db_index_plot(sse_,sil_avgs_,db_index_,n_min,n_max)
        end = datetime.now()
        print('Duration: {}'.format(end - start))
        print('SSE: ')
        print(sse_)
        print('SIL: ')
        print(sil_avgs_)
        print('DB:')
        print(db_index_)
        
        save_list_to_file('sse',sse_)
        save_list_to_file('sil',sil_avgs_)
        save_list_to_file('db',db_index_)
        
        os.system("say 'your program is finish hey hey hey hey'")
        gc.collect()
    else:
        print("Insert first feature: ")
        f1 = input()
        print("Insert second feature: ")
        f2 = input()
        
        if n_min != n_max:
            print('Check the value of n_min and n_man. In this section thay must be the same.')
            exit()
        df_,sse,sil,deb = multi_kmeans(path_csv,n_min,scale)
        silh_plot(df_,loop)
        scatter_2f_cluster(df_,f1,f2,loop)
        

if __name__ == "__main__":
    main()