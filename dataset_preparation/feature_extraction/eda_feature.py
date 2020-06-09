#!/usr/bin/env python3
import pandas as pd 
import numpy as np
import matplotlib.pyplot as plt
import sys
from sklearn.preprocessing import MinMaxScaler,StandardScaler
import os

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


def box(csv_features):
    """PLOT OF box FOR FEATURES
    
    Args
        - csv_features: csv features
    """
    
    df = pd.read_csv(csv_features,sep=',',low_memory=False,index_col=['domain'])
    df = df.dropna(how='all')
    
  
    x1 = df['A_n_ipv4'].dropna()
    x2 = df['AAAA_mean_ttl'].dropna()
    x3 = df['A_strdev_ttl'].fillna(0)
    x4 = df['A_as_number'].fillna(0)
    x5 = df['A_n_countries'].fillna(0)
    
    x6 = df['AAAA_n_ipv6'].fillna(0)
    x7 = df['AAAA_mean_ttl'].fillna(0)
    x8 = df['AAAA_strdev_ttl'].fillna(0)
    x9 = df['AAAA_as_number'].fillna(0)
    x10 = df['AAAA_n_countries'].fillna(0)
    data = [x1,x2,x3,x4,x5,x6,x7,x8,x9,x10]
    
    label = [[''],['A_n_ipv4'],['A_mean_ttl'],['A_strdev_ttl'],['A_as_number'],['A_n_countires'],
             ['AAAA_n_ipv6'],['AAAA_mean_ttl'],['AAAA_strdev_ttl'],['AAAA_as_number'],['AAAA_n_countires']]
    
    fig1, ((ax1,ax2,ax3,ax4,ax5),(ax6,ax7,ax8,ax9,ax10)) = plt.subplots(2,5)
    vax = [ax1,ax2,ax3,ax4,ax5,ax6,ax7,ax8,ax9,ax10]
    count = 1
    for x,y in zip(data,vax):
        print(label[count])
        y.boxplot(x,vert=True,labels=label[count],patch_artist=True)
        count += 1
        
    for ax in vax:
        ax.yaxis.grid(True)
    plt.show()
    


def hist(csv_features):
    """PLOT OF HISTROGRAM FOR FEATURES
    
    Args
        - csv_features: csv features
    """
    df = pd.read_csv(csv_features,sep=',',low_memory=False,index_col=['domain'])
    df = df.dropna(how='all')
    for col in df.columns:
        print(col)
    
    x1 = df['A_n_ipv4'].dropna()
    x2 = df['AAAA_n_ipv6'].dropna()
    
    b1 = int(np.max(x1))
    print(b1)
    print(np.unique(x1))
    
    b2 = int(np.max(x2))
    print(b2)
    print(np.unique(x2))
    
   
    
    label = ['A_n_ipv4']
    fig, (ax1, ax2) = plt.subplots(1,2)
    
    ax1.hist(x1, bins=b1, histtype='step', fill=False)
    
    ax2.hist(x2, bins=b2, histtype='step', fill=False)

    fig.tight_layout()
    plt.show()
    
    

def scatter(csv):
    """
    ARGS
        - csv: csv with features
    """
    #scatter of combination of columns two by two: number of combination: n!/(n-k)!k!.. for now exlude A_n_cname
    df = pd.read_csv(csv,sep=',',index_col='domain',low_memory=False).drop(['A_n_cname','NS_n'],axis=1)
    cols = ['domain']
    for col in df.columns:
        cols.append(col)
    print(type(cols))
    print(len(cols))
    for col in cols:
        print(col)
        
    #print(df[cols[1]].dropna())
    fig,axs = plt.subplots(5,9, figsize=(8,6))
    
    fig.tight_layout()
    axs = axs.ravel()
    count = 0
 
    for i in range(1,len(cols)-1):
        j = i
        while j < len(cols)-1:
            j += 1
            print(i,j)
            axs[count].scatter(df[cols[i]],df[cols[j]], marker='.',s=4.5)
            #axs[count].set_title(str(cols[i])+' - '+ str(cols[j]))
            axs[count].set(xlabel=cols[i], ylabel=cols[j])
            
            if(np.max(df[cols[j]]) > 5000):
                axs[count].set_yscale('log')

            if(np.max(df[cols[i]]) > 1000):
                axs[count].set_xscale('log')
               
            print(cols[i],cols[j])
            count += 1
    #plt.rcParams['font.size']='3'
    plt.xticks(rotation=45)
    plt.show()
    
def scatter_selecting_feat(csv,feature1,feature2):
    df = pd.read_csv(csv, low_memory=False, index_col=['domain'],sep=',')
    '''removing only tuples with all null values
       Es:
       domx->null,null,null,null -> it will be removed due to it doesn't have information
       domy->null,null,1,0-> it don't will be removed->after we transform the null values in 0 vlaues
    
    '''
    df = df.dropna(how='all')
    #remove NaN replacing it with 0
    df = df.fillna(0)
    
    feat1 = df[feature1].tolist()
    feat2 = df[feature2].tolist()
    
    fig,ax = plt.subplots(1,1)
    ax.scatter(feat1,feat2,s=4,marker='o')
    ax.set(xlabel=feature1,ylabel=feature2)
    #ax.grid()
    plt.show()

def box_for_feat(csv_in,scale):
    df = pd.read_csv(csv_in, sep=',',low_memory=False, index_col=['domain'])
    
    df = df.fillna(0)
    df = df[(df['A_n_ipv4'] != 0) | (df['AAAA_n_ipv6'] != 0)]
    df = df.drop(['score','label'], axis=1)
    columns = list(df.columns)
    print(columns)
    
    folder_exists('boxplot_4_feat')
                  
    fig, ax = plt.subplots(1,1)
    fig.tight_layout()
    
   
    
    s = ''
    path_s = ''
    path_s_png = ''
    if scale == 1:
        ss = 'SS'
        scaler = StandardScaler()
        print('Scaling with StandardScaler..')
        path_s = 'boxplot_4feat_SS.pdf'
        path_s_png = 'boxplot_4feat_SS.png'
        #df = scaler.fit_transform(df)
        for col in columns:
            df[col] = scaler.fit_transform(df[[col]])
    elif scale == 2:
        ss = 'MM'
        scaler = MinMaxScaler()
        path_s = 'boxplot_4feat_MM.pdf'
        path_s_png = 'boxplot_4feat_MM.png'
        print('Scaling with MinMax..')
        for col in columns:
            if col == 'A_top_country' or col == 'AAAA_top_country':
                 print(col)
            else:
                df[col] = scaler.fit_transform(df[[col]])
    else:
        ss = 'NO'
        path_s = 'boxplot_4feat_origin.pdf'
        path_s_png = 'boxplot_4feat_origin.png'
        print('No scaling')
        
    ax = df.boxplot(rot=45, fontsize=5)
    ax.set_title('Box '+ss+' Scaler')
    plt.subplots_adjust(bottom=0.2)
    path_to_save = os.path.join('boxplot_4_feat',path_s)
    path_to_save_png = os.path.join('boxplot_4_feat',path_s_png)
    file_exists(path_to_save)
    file_exists(path_to_save_png)
    plt.savefig(path_to_save)
    plt.savefig(path_to_save_png)
    
    
def main():
    csv_features = sys.argv[1]
    choose = int(sys.argv[2])
    if choose == 1:
        box(csv_features)
    elif choose == 2:
        hist(csv_features)
    elif choose == 3:
        scatter(csv_features)
    elif choose == 4:
        feat1 = sys.argv[3]
        feat2 = sys.argv[4]
        scatter_selecting_feat(csv_features,feat1,feat2)
    elif choose == 5:
        print('Enter scaler:')
        scaler = int(input())
        box_for_feat(csv_features,scaler)
    else:
        exit()
    


if __name__ == '__main__':
    main()