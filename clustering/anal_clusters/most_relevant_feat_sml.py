import csv
import os 
import sys 
import pandas as pd 
import numpy as np 
from sklearn.ensemble import RandomForestClassifier



def most_relevant_feat_rfc(csv_in):
    '''
        Args_in:
            - csv_in: csv that contain the column of domain name ('domain'), the columns of features and the column 'cluster' that
                contains the id of cluster which samples belongs... simply is the csv that we obtain after clustering procedure.
        
        The aim is to retrieve the most relevant features which influence the cluster labeling process.
        We train a RandomForest classifier with the csv passed, where the training data are the features values and the 
        values to predict are the cluster label
    '''
    df = pd.read_csv(csv_in, low_memory=False,sep=',',index_col=['domain'])
    df_c = df.copy()
    
    val_to_predict = np.array(df['cluster'])
    labels = list(df['label'].values)
    print(val_to_predict)
    #remove the 'cluster' and 'label' columns
    df = df.drop(['cluster','label'],axis=1)
    print(df)
    #print(labels)
    
    columns = list(df.columns)
    print(columns)
    df = np.array(df)
  
    clssif = RandomForestClassifier(n_estimators=100,random_state=10)
    clssif.fit(df,val_to_predict)
    print('Relevant feature')
    print(clssif.feature_importances_)
    
    #zip(* ziplist)->unzip the list
    importance, features = zip(*sorted(zip(clssif.feature_importances_, columns), reverse = True))
    print(importance,features)

    #df_c = df_c.drop(['label'],axis=1)
    #print(df_c)
    
    '''
    for c in np.unique(val_to_predict):
       
        print('* Cluster :' + str(c)+ ' *')
        df_selc = df_c.loc[df_c['cluster']==c]
        print(df_selc)
        cluster_label =list(df_selc['cluster'].values)
        
        df_selc = df_selc.drop(['cluster'],axis=1)
        clsf = RandomForestClassifier(n_estimators=100,random_state=10)
        clsf.fit(df_selc,cluster_label)
        print('Revelant Features: ')
        importance_c, features_c = zip(*sorted(zip(clsf.feature_importances_, columns), reverse = True))
        print(importance_c,features_c)
        print('\n')
    '''
        
    

def main():
    csv_in = sys.argv[1]
    most_relevant_feat_rfc(csv_in)
    os.system("say 'your program is finish hey hey hey hey'") 

if __name__ == '__main__':
    main()