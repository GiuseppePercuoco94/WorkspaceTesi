import pandas as pd 
import numpy as np 
import seaborn as sns
import matplotlib.pyplot as plt
import csv
import os
import shutil
import sys

def file_exists(name_file):
    if os.path.exists(name_file):
        print('file ',name_file,' exists..removing it')
        os.remove(name_file)
    else:
        print('no old ',name_file,' exists')



def bad_extr(csv_in):
    df = pd.read_csv(csv_in, low_memory=False, sep=',', index_col=['domain'])
    df_bad_group = df[df['label'] == 'bad']
    df_bad_group = df_bad_group.drop(['score','label'],axis=1)
    df_bad_group = df_bad_group.fillna(0)
    df_bad_group = df_bad_group[(df_bad_group['A_n_ipv4'] != 0) | (df_bad_group['AAAA_n_ipv6'] != 0)]
    print(df_bad_group)
    file_exists('bad_feat.csv')
    df_bad_group.to_csv('bad_feat.csv')
    g = sns.pairplot(df_bad_group,height=2,plot_kws={"s": 9}, markers=["o"])
    g.fig.set_figwidth(6)
    g.fig.set_figheight(11.7)
    plt.title('Matrix bad')
    name_img_bad_pdf = 'bad_matrix.pdf'
    name_img_bad_png = 'bad_matrix.png'
    file_exists(name_img_bad_pdf)
    file_exists(name_img_bad_png)
    plt.savefig(name_img_bad_pdf)
    plt.savefig(name_img_bad_png)
    plt.show()

def good_extr(csv_in):
    df = pd.read_csv(csv_in, low_memory=False, sep=',', index_col=['domain'])
    df_good_group = df[df['label'] == 'good']
    df_good_group = df_good_group.drop(['score','label'],axis=1)
    df_good_group = df_good_group.fillna(0)
    df_good_group = df_good_group[(df_good_group['A_n_ipv4'] != 0) | (df_good_group['AAAA_n_ipv6'] != 0)]
    print(df_good_group)
    file_exists('good_feat.csv')
    df_good_group.to_csv('good_feat.csv')
    g = sns.pairplot(df_good_group,height=2,plot_kws={"s": 9}, markers=["o"])
    g.fig.set_figwidth(6)
    g.fig.set_figheight(11.7)
    plt.title('Matrix good')
    name_img_good_pdf = 'good_matrix.pdf'
    name_img_good_png = 'good_matrix.png'
    file_exists(name_img_good_pdf)
    file_exists(name_img_good_png)
    plt.savefig(name_img_good_pdf)
    plt.savefig(name_img_good_png)
    plt.show()
    
def bad_and_good_pairplt(csv_in):
    df = pd.read_csv(csv_in, low_memory=False, sep=',', index_col=['domain'])
    df = df.drop(['score'],axis=1)
    df = df.fillna(0)
    #df = df['A_n_ipv4'].astype(int)
    #df = df['AAAA_n_ipv6'].astype(int)    
    #df = df[(df['A_n_ipv4'] != 0) | (df['AAAA_n_ipv6'] != 0)]
    print(df)
    print(df.dtypes)
    #sns.set(style="ticks", color_codes=True)
    g = sns.pairplot(df,height=2, hue="label", vars=df.columns[:-1],diag_kind="hist",markers=["s", "+", "d",],)
    
    #g.fig.set_figwidth(5)
    #g.fig.set_figheight(10)
    plt.title('Matrix bag&good')
    name_img_bad_and_good_pdf = 'bag_and_good_matrix.pdf'
    name_img_bad_and_good_png = 'bag_and-good_matrix.png'
    file_exists(name_img_bad_and_good_pdf)
    file_exists(name_img_bad_and_good_png)
    plt.savefig(name_img_bad_and_good_pdf)
    plt.savefig(name_img_bad_and_good_png)
    #plt.figure(figsize=(7,7))
    #plt.tight_layout()
    plt.show()

def heat_corr(csv_in):
    df = pd.read_csv(csv_in, low_memory=False, sep=',', index_col=['domain'])
    df = df.drop(['score','label'],axis=1)
    df = df.fillna(0)
    
    sns.heatmap(df.corr(), annot=True, fmt='.1g')
    plt.figure(figsize=(8,8))
    plt.show()
    

def main():
    csv_feat = sys.argv[1]
    heat_corr(csv_feat)
    #bad_and_good_pairplt(csv_feat)
    #bad_extr(csv_feat)
    #good_extr(csv_feat)
    



if __name__ == '__main__':
    main()