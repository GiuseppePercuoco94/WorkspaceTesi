import csv
import pandas as pd
import seaborn
import sys
from glob import glob
import seaborn as sns
import matplotlib.pyplot as plt

folder_path = sys.argv[1]
#csv_path = sys.argv[1]



header = True
files = sorted(glob(folder_path + '/*.csv'))
list_files = []
for file in files:
    print(file)
    temp = pd.read_csv(file, delimiter=',')
    #print(temp['status'].value_counts())
    list_files.append(temp)

csv_conc = pd.concat(list_files, ignore_index=True, names=['index','status'])
csv_conc_group = csv_conc.sort_values(by=['status'])
csv_conc_group.to_csv('cisco_umbrella_order.csv', index = False)
csv_conc.to_csv('combined_umbrella.csv', index=False)
print(csv_conc['status'].value_counts().reset_index(name='count_occurences'))
#fig = sns.barplot(x='index',y='count_occurences',data=csv_conc['status'].value_counts().reset_index(name='count_occurences'))
#plt.show()



