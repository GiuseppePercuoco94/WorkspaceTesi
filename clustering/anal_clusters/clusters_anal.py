import csv
import os 
import sys
import pandas as pd 
import numpy as np 
from sklearn import metrics
from sklearn.preprocessing import StandardScaler,MinMaxScaler

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



def centroids_and_similarity(csv_df):
    '''
    Args_in:
        -csv_df: csv obtained from clustering process
    '''
    df = pd.read_csv(csv_df,sep=',',low_memory=False,index_col=['domain'])
    columns = list(df.columns)
    print(columns)
    print(columns[0:-2])
    clusters = np.unique(df.loc[:,'cluster'].values)
    print(clusters)
    
    #creation of folder and file name to save
    
    #extract the base folder
    dir_name = os.path.dirname(csv_df).split('/')[-2]
    #extract the file name
    base_name = os.path.basename(csv_df).split('.')[0]
    print(dir_name)
    print(base_name)
    #folder that contain the csv with centroids
    folder_centroid = 'centroid'
    folder_exists(folder_centroid)
    csv_out = os.path.join(folder_centroid,base_name + '_' + dir_name + '.csv')
    #creation of header for csv out.'cluster' will contian the id of cluster, and the other column will be the original feature columns
    headers = ['cluster'] + columns[0:-2]
    print(headers)
    file_exists(csv_out)
    
    with open(csv_out,mode='a') as csv_o:
        writer = csv.writer(csv_o)
        writer.writerow(headers)
        for cluster in clusters:
            data = []
            data.append(cluster)
            #select tules for each cluster id 
            df_c = df.loc[df['cluster']==cluster]
            #print(df_c)
            #calculate the mean of each features --> definition of centroid
            print(df_c.mean())
            for x in list(df_c.mean())[0:-1]:
                data.append(x)
            writer.writerow(data)
            print('\n')
            print(cluster)
            nearest_point_centroid(df_c,columns[0:-2],list(df_c.mean())[0:-1])
        #df_group = df.groupby(by='cluster')


def nearest_point_centroid(df,columns,centroid):
    print('\n NEAREST POINT')
    #print(df)
    #print(columns)
    #print(centroid)
    '''
        calc the distance of each samples for cluster to their centroids.. the samples with the minimum distance is the 
        nearest to the centroid
    '''
    near = (df[columns] - np.array(centroid)).pow(2).sum(1).pow(0.5)
    print(near.sort_values(ascending=False))

def retrieve_sil_db(csv_in):
    #csv_in = csv in the 'csv_kmeans' folder 
  
    df = pd.read_csv(csv_in, sep=',',low_memory=False,index_col=['domain'])  
    print(df)  
    columns = df.columns
    print(columns[0:-2])
    sil = metrics.silhouette_score(df[columns[0:-2]],df.loc[:,'cluster'],metric='euclidean')
    deboul = metrics.davies_bouldin_score(df[columns[0:-2]],df.loc[:,'cluster'])
    print('SIL: ' + str(sil))
    print('DB index: '+ str(deboul))
    

def unscaled_data(original_csv, csv_scaled_in,s):
    '''
        original_csv: csv that contain orignal data not yet scaled 
        csv_scaled_in: csv thath contain data scaled 
        s : type of scaler, 1 standard scaler, 2 minmax
    '''
    
    if s == 1:
        print('StandardScaler')
        scaler = StandardScaler()
    elif s == 2:
        print('MinMaxScaler')
        scaler = MinMaxScaler()
    else:
        print('No Scaler')
    
    df_original = pd.read_csv(original_csv, sep=',',low_memory=False, index_col=['domain'])
    columns = df_original.columns
    #fit model scaler from original data
    scaler.fit(df_original[columns[0:-2]])
    
    df_scaled =  pd.read_csv(csv_scaled_in, sep=',',low_memory=False, index_col=['domain'])
    df_scaled[columns[0:-2]] = scaler.inverse_transform(df_scaled[columns[0:-2]])
    

def main():
    csv_in = sys.argv[1]
    centroids_and_similarity(csv_in)
    retrieve_sil_db(csv_in)
    
if __name__ == '__main__':
    main()