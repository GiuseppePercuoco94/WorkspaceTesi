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


def scatter_plot_clustering(name_polot,features1,features2,centroids,labels):
    """
    ARGS:
        - name_plot: name of plot to save
        - features: list of features 
        - centroids: coordinates of centroids
    """
    #check the presence of img folder
    folder_exists('img')
    #creating the path of file out
    path_file_out = os.path.join('img',name_polot)
    #check if it already exists
    file_exists(path_file_out)
    #extract the name of the features
    name_f1 = name_polot.split('-')[0]
    name_f2 = name_polot.split('-')[1]
    #unique values of label returned by clustering
    unique_labels = np.unique(labels)
    
    fig,axs = plt.subplots()
    
    #fig.tight_layout()
   
    #scatter plot of features
    axs.scatter(features1,features2, c=labels.astype(float), s=10,alpha=1)
    #add plot of centroids
    axs.scatter(centroids[:,0],centroids[:,1],marker='*', c='red')
    plt.xlabel(name_f1)
    plt.ylabel(name_f2)
    plt.title('n cluster '+str(len(unique_labels)))
    
    plt.savefig(path_file_out)
    

def plot_sse(met,n_clusters, name):
    
    sse_value = met[0]
    sil_value = met[1]
    dabol = met[2]
    folder_exists('img')
    path_file_out = os.path.join('img',name)
    file_exists(path_file_out)
    
    fig,(axs1,axs2,axs3) = plt.subplots(1,3,)
    fig.tight_layout()
    
    #fig.tight_layout()
    axs1.plot(n_clusters,sse_value)
    axs1.set(xlabel='n_cluster',ylabel='SSE')
    axs1.grid()
    axs2.plot(n_clusters,sil_value)
    axs2.set(xlabel='n_cluster',ylabel='sil')
    axs2.grid()
    axs3.plot(n_clusters,dabol)
    axs3.set(xlabel='n_cluster',ylabel='DB index')
    axs3.grid()
    
    
    #plt.xlabel('n_cluster')
    #plt.ylabel('Sum of squared error')
    plt.legend()
    plt.subplots_adjust(wspace=0.5,bottom=0.2,left=0.15)
    #plt.grid(True)
    folder_exists('img')
    path_file_out = os.path.join('img',name)
    file_exists(path_file_out)
    
    plt.savefig(path_file_out)
    
    
    
def kmeans(csv_in,feature_1, feature_2, n_max_cluster,minmax_scale):
    """
    ARGS:
        - csv_in : path of csv
        - feature_1: name of column 
        - feature_2: name of column 
        - n_max_maxcluster: number of cluster
        - minmax_scale: act scale (1) or no (0) 
        
    """
    df = pd.read_csv(csv_in, low_memory=False, sep=',', index_col=['domain'])
    '''removing only tuples with all null values
       Es:
       domx->null,null,null,null -> it will be removed due to it doesn't have information
       domy->null,null,1,0-> it don't will be removed->after we transform the null values in 0 vlaues
    
    '''
    df = df.dropna(how='all')
    #remove NaN replacing it with 0
    df = df.fillna(0)
    #now we consider only the two column idicated in feature_1/2
    df = df[[feature_1,feature_2]]
    
    #scaling
    if minmax_scale == 1:
        print('- Scaling MinMax')
        scaler = MinMaxScaler()
        scaler.fit(df[[feature_1]])
        df[feature_1] = scaler.transform(df[[feature_1]])
        scaler.fit(df[[feature_2]])
        df[feature_2] = scaler.transform(df[[feature_2]])
    else:
        print('no minmax scaling')
    #print(df)
    feat1 = df[feature_1].tolist()
    feat2 = df[feature_2].tolist()
    matr_feat = np.column_stack((feat1,feat2))
    print(len(matr_feat))
    kmeans = KMeans(n_clusters=int(n_max_cluster))
    y_ = kmeans.fit_predict(df)
    centroids = kmeans.cluster_centers_
    print(centroids)
    print(kmeans.labels_)
    print(type(kmeans.labels_))
    print('unique labels: '+ str(np.unique(kmeans.labels_)))
    print('SSE: '+ str(kmeans.inertia_))
    name_plot = str(feature_1)+'-'+feature_2+'-'+str(n_max_cluster)+'.png'
    sil = metrics.silhouette_score(df, kmeans.labels_, metric='euclidean')
    deboul = metrics.davies_bouldin_score(df,kmeans.labels_)
    print('SIL score: ',sil)
    print('DV index :', deboul)
    metrics_  = []
    metrics_.append([kmeans.inertia_])
    metrics_.append(sil)
    metrics_.append(deboul)
    scatter_plot_clustering(name_plot,df[feature_1],df[feature_2],centroids,kmeans.labels_)
    
    return metrics_
    
################################################################################################################################################################

def loop_kmeans(csv_in,feature_1, feature_2, n_max_cluster,minmax_scale):
    SSE_ = []
    sil_ = []
    deboul_ = []
    metrics_ = []
    n_cluster_ = []
    name_plot_sse = 'SSE-'+str(feature_1)+'-'+feature_2+'-'+str(n_max_cluster)+'.png'
    rng = range(2,n_max_cluster+1)
    for i in rng:
        print('\n')
        n_cluster_.append(i)
        metrics_ = kmeans(csv_in,feature_1,feature_2,i,minmax_scale)
        SSE_.append(metrics_[0])
        sil_.append(metrics_[1])
        deboul_.append(metrics_[2])
        metrics_ = []
    metrics_.append(SSE_)
    metrics_.append(sil_)
    metrics_.append(deboul_)
    plot_sse(metrics_,n_cluster_,name_plot_sse)
        
    
def multi_kmeans(csv_in, n_cluster, scale,sil_pair):
    df = pd.read_csv(csv_in, low_memory=False, index_col=['domain'])

    #remove 'score column' and tuple with no information from OI, and the column score and label
    df = df.fillna(0)
    df = df[(df['A_n_ipv4'] != 0) | (df['AAAA_n_ipv6'] != 0)]
    df_copy = df.copy()
    df_copy.reset_index(drop=True, inplace=True)
    #print('Copy: ')
    #print(df_copy)
    df = df.drop(['score','label'], axis=1)
    columns = list(df.columns)
    
    if scale == 1:
        scaler = StandardScaler()
        print('Scaling with StandardScaler..')
        #df = scaler.fit_transform(df)
        for col in columns:
            df[col] = scaler.fit_transform(df[[col]])
    elif scale == 2:
        scaler = MinMaxScaler()
        print('Scaling with MinMax..')
        for col in columns:
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
    
    color = sns.color_palette(None,len(np.unique(m_kmeans.labels_)))
    print('Palette..')
    print(color)
    #print(df)
    if sil_pair == 1:
        silh_plot(df,m_kmeans.labels_, color)
        pair_plot_multi_kmeans(df,color)
        
    return df,m_kmeans.inertia_,sil,deboul,color

    #plt.scatter(df['A_n_ipv4'], df['A_as_number'],df['A_n_countries'],df['NS_n'],alpha=1)
    #plt.scatter(centroids[:,0],centroids[:,1],centroids[:,2],centroids[:,3], marker='*')
    #plt.show()
    

def plot_from_mutili_to_two(df_kmean, f1, f2,c):
    print('Plotting...')
    print(df_kmean)
    labels_ = list(df_kmean.loc[:,['cluster']].values)
    unique_labels_ = np.unique(labels_)
    colors = c
    
    fig = plt.figure(figsize=(8,8))
    ax = fig.add_subplot(1,1,1)
    ax.set_xlabel(f1, fontsize=10)
    ax.set_ylabel(f2, fontsize=10)
    ax.set_title('N cluster '+ str(len(unique_labels_)), fontsize=15)
    
        
    for label,color in zip(unique_labels_,colors):
        index_ = df_kmean['cluster'] == label
        ax.scatter(df_kmean.loc[index_,f1],df_kmean.loc[index_,f2], c=color, s=25)
                
    legend = []
    for x in unique_labels_:
        legend.append('cluster '+ str(x))
        
    ax.legend(legend)
    plt.show()
    
def pair_plot_multi_kmeans(df_kmns,color):
    
    cluster_labels = df_kmns['cluster'].unique()
    n_cluster = cluster_labels.shape[0]
    columns = df_kmns.columns
    folder_exists('folder_pairplot_multi')
    img_multi = os.path.join('folder_pairplot_multi','pariplot_cluster_'+str(n_cluster)+'.pdf')
    img_multi_png = os.path.join('folder_pairplot_multi','pariplot_cluster_'+str(n_cluster)+'.png')
    file_exists(img_multi)
    file_exists(img_multi_png)
    #print('Plotting')
    #print(df_kmns[columns[:-1]])
    g = sns.pairplot(df_kmns,height=1.5,hue='cluster',vars=df_kmns[columns[:-1]],diag_kind="hist", markers='o',palette=color)
    print('Saving pair plot n:' +str(n_cluster)+'..')
    
    plt.savefig(img_multi_png)
    plt.savefig(img_multi)
    

def silh_plot(df_kmean,y,color):
    """
    ARGS:
        - df_kmean: dataframe from kmeans-> dataframe + column of cluster label
    """
    print('Silh saving plot..')
    fig = plt.figure(figsize=(8,8))
    ax = fig.add_subplot(1,1,1)
    #y_km = df_kmean.loc[:,['cluster']].values
    df_kmean = df_kmean.drop(['cluster'], axis=1)
    cluster_labels = np.unique(y)
    n_cluster = cluster_labels.shape[0]
    print('Silh saving plot '+str(n_cluster)+'..')
    silhouette_vals = silhouette_samples(df_kmean,y, metric='euclidean')
    
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
    #plt.show()
    folder_sil = 'sil_plot'
    folder_exists(folder_sil)
    path_img_shil = os.path.join(folder_sil, 'shil_n_'+str(n_cluster)+'.png')
    
    file_exists(path_img_shil)
    #plt.legend(cluster_labels)
    plt.savefig(path_img_shil)
    
    
def sse_avgsil_db_index_plot(sse,avgsil,db,minc,maxc):
    
    n = np.arange(minc,maxc+1,1)
    print('in plot ssesil..')
    
    print(n)
    folder_exists('img_sse_avgsil_db')
    path_file_out = os.path.join('img_sse_avgsil_db','sse_avgsil_db_'+str(minc)+'_'+str(maxc)+'.png') 
    file_exists(path_file_out)
    
    fig,(axs1,axs2,axs3) = plt.subplots(3,1)
    fig.tight_layout()
    
    #fig.tight_layout()
    axs1.plot(n,sse)
    axs1.set(xlabel='n_cluster',ylabel='SSE')
    axs1.grid()
    axs2.plot(n,avgsil)
    axs2.set(xlabel='n_cluster',ylabel='sil')
    axs2.grid()
    axs3.plot(n,db)
    axs3.set(xlabel='n_cluster',ylabel='DB index')
    axs3.grid()
    
    #plt.xlabel('n_cluster')
    #plt.ylabel('Sum of squared error')
   #plt.legend()
    plt.subplots_adjust(hspace=0.7,bottom=0.2,left=0.15)
    #plt.grid(True)
    
    plt.savefig(path_file_out)
    
    
def loop_multiplefeat_kmeans():
    path_csv = sys.argv[1]
    n_min = int(sys.argv[2])
    n_max = int(sys.argv[3])
    scale = int(sys.argv[4])
    sil_pair = int(sys.argv[5])
    r = range(n_min,n_max + 1)
    sse_ = []
    sil_avgs_ = []
    db_index_ = []
    start = datetime.now()
    for i in r:
        df_,sse,sil,deb,c = multi_kmeans(path_csv,i,scale,sil_pair)
        sse_.append(sse)
        sil_avgs_.append(sil)
        db_index_.append(deb)
    
    print(sse_)
    print(len(sse_))
    sse_avgsil_db_index_plot(sse_,sil_avgs_,db_index_,n_min,n_max)
    end = datetime.now()
    print('Duration: {}'.format(end - start))

#def combination_scatter()

def main2():
    #path of feature csv
    path_csv = sys.argv[1]
    #number of cluster
    n = int(sys.argv[2])
    scale = int(sys.argv[3])
    feat_1 = sys.argv[4]
    feat_2 = sys.argv[5]
    sil_pair = int(sys.argv[6])
    
    df_,sse,sil,db,c = multi_kmeans(path_csv,n,scale,sil_pair)
    #silh_plot(df_,y)
    #pair_plot_multi_kmeans(df_)
    plot_from_mutili_to_two(df_,feat_1,feat_2,c)
    #silh_plot(df_,df_.loc[:,['cluster']].values,c)
    
    
    
def main():
    #path of feature csv
    path_csv = sys.argv[1]
    #feature on x
    f1 = sys.argv[2]
    #feature on y
    f2 = sys.argv[3]
    #number of cluster
    n = int(sys.argv[4])
    #minmax scale : 1 yes, 0 no
    minmax_scale = int(sys.argv[5])
    #loop: 0 clustering with n cluster, 1 clustering from 0 to n
    loop = int(sys.argv[6])
    if loop == 0:
        kmeans(path_csv,f1,f2,n,minmax_scale)   
    else:
        loop_kmeans(path_csv,f1,f2,n,minmax_scale)


if __name__ == '__main__':
    loop_multiplefeat_kmeans()
    #main2()
    