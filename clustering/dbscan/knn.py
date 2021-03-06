import pandas as pd 
import csv 
import sys
from sklearn.neighbors import NearestNeighbors
import numpy as np 
import matplotlib.pyplot as plt 
from sklearn.preprocessing import MinMaxScaler,StandardScaler

def knn(csv_in, scale):
    df = pd.read_csv(csv_in, sep=',',low_memory=False, index_col=['domain'])
    
    
    #remove 'score column' and tuple with no information from OI, and the column score and label
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
    if scale == 1:
        print('Standard Scaler..')
        columns = list(df.columns)
        scaler = StandardScaler()
        
        df[columns[0:]] = scaler.fit_transform(df[columns[0:]])
    elif scale == 2:
        print('MIN MAX SCALER')
        columns = list(df.columns)
        scaler = MinMaxScaler()
        df[columns[0:]] = scaler.fit_transform(df[columns[0:]])
        print(df)
    else:
        print('NO SCALE')
        columns = list(df.columns)
    neigh = NearestNeighbors(n_neighbors=2)
    nbrs = neigh.fit(df[columns[0:-1]])
    
    distances, indices = nbrs.kneighbors(df[columns[0:-1]])
    print(distances)
    distances = np.sort(distances,axis=0)
    print(distances)
    distances = distances[:,1]
    plt.plot(distances)
    
    plt.show()
    

def main():
    csv_in = sys.argv[1]
    scale = int(sys.argv[2])
    
    knn(csv_in,scale)

if __name__ == '__main__':
    main()
    
