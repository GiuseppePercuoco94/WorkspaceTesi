import pandas as pd 
import sys
from glob import glob
import os
import numpy as np




def merge_csv(path_folder, name):
     """
     
     Args:
          path_fodler: path folder that contains csv to merge -> for each csv there must be st least one column in common ( in this particular case 'domain' column)-> 
          and this common column must have the same data and the data must be sorted in the same way
          name: name of file to save
     Reurns:
          'merge_csv.csv' file 

     """
     #fodler that contains csv to merge
     folder_csv = path_folder

     df_l = []
     name_csv = name + '.csv'
     #check if merged csv already exists
     if os.path.isfile(name_csv):
          os.remove(name_csv)
          print("old file out removed")
     else:
          print("no ",name_csv," exists")

     #check print of filename in the folder
     print('Reading starts')
     files = sorted(glob(folder_csv + '/*.csv'))
     countf = 0
     for file in files:
          print(file)
          
     #appending dataframes of csv in the folder   
     for file in files:
          if file.split('/')[-1] == 'combined_umbrella_copia.csv':
               countf += 1
               df = pd.read_csv(file, low_memory=False,  sep=';')
               df_l.append(df) 
               print('file: '+ str(file) + ', ' + str(countf))
               print('\n')
          else:
               countf += 1
               df = pd.read_csv(file, low_memory=False, sep=',')
               df_l.append(df) 
               print('file: '+ str(file) + ', ' + str(countf))
               print('\n')
     print('concatenation starts')
     '''
     result = pd.concat(df_l, axis=1,sort=False)
     result.set_index(['domain'])
     '''

     #concatenation
     first = True
     result = pd.DataFrame
     for dfi in df_l:
          if first:
               result = dfi.copy()
               first = False
          else:
               #deleting 'domain' column due to the firt csv uploaded already had it, in the final csv we would like to have only one 'domain' colum that is used as index
               dfi = dfi.drop(['domain'],axis=1)
               result = pd.concat([result,dfi],axis=1, sort=False)

     #replacement of empty fileds with a 'null' string        
     result =result.replace(np.nan,'null')
     result = result.set_index(['domain'])
     #export to csv
     name_csv = name + '.csv'
     result.to_csv(name_csv)

          
if __name__ == '__main__':
     path_folder = sys.argv[1]
     name_file_out = sys.argv[2]
     merge_csv(path_folder, name_file_out)
     