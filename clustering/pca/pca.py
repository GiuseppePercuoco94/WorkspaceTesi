import pandas as pd 
import numpy as np 
import matplotlib.pyplot as plt 
from mpl_toolkits.mplot3d import Axes3D
import os 
import sys 
from sklearn.preprocessing import StandardScaler,MinMaxScaler
from sklearn.decomposition import PCA
from sklearn.feature_selection import VarianceThreshold
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


def gen_pcx_string(n):
    #gen string of column name for pca, depend on the size of number of component
    #ES : n_component= 4 -> ['PC1','PC2','PC3','PC4']
    column_names_ = []
    for i in range(1,n+1):
        column_names_.append('PC'+str(i))
        
    return column_names_


def pca(csv_feat, ncomponents, flag_scale, name,plot,save,top_scale):
    df = pd.read_csv(csv_feat, low_memory=False, sep=',')
    df = df.drop_duplicates(subset='domain')
    df = df.set_index('domain')
    
    #df_copy = pd.read_csv(csv_feat, low_memory=False, sep=',')
    columns = list(df.columns)
    
    #remove 'score column' and tuple with no information from OI, and the column score and label
    df = df.fillna(0)
    df = df[(df['A_n_ipv4'] != 0) | (df['AAAA_n_ipv6'] != 0)]
    df_copy = df.copy()
    df_copy_2 = df.copy()
    df_copy_2 = df_copy_2.reset_index()
    #print(df_copy_2)
    df_copy.reset_index(drop=True, inplace=True)
    #print('Copy: ')
    score_label = False
    if 'score' in list(df.columns) and 'label' in list(df.columns):
        df = df.drop(['score','label'], axis=1)
        score_label = False
    columns = list(df.columns)
    #index_df = pd.DataFrame({'domain':list(df.index.values)}).
    #index_df = list(df.index.values)
    
    #print(index_df)
    
    print("***** VAR AND CORR BEFORE MINMAX SCALER ********")
    var_calc(df)
    corr_matrix(df,'CORR MATRIX BEFORE MIXMAX SCALER')
    plt.close('all')
    
    if flag_scale == 1:
        #Scaling
        scaler = StandardScaler()
        print('STANDARD SCALER')
        if top_scale == 1:
            for col in columns:
                print('Include TOP')
                df[col] = scaler.fit_transform(df[[col]]) 
        else:
            for col in columns:
                if col == 'A_top_country' or col == 'AAAA_top_country':
                    print(col)
                else:
                    df[col] = scaler.fit_transform(df[[col]])
    elif flag_scale == 2:
        print('MINMAX SCALER')
        scaler = MinMaxScaler()
        if top_scale == 1:
            for col in columns:
                print('Include TOP')
                df[col] = scaler.fit_transform(df[[col]]) 
        else:
            for col in columns:
                if col == 'A_top_country' or col == 'AAAA_top_country':
                    print(col)
                else:
                    df[col] = scaler.fit_transform(df[[col]])
    else:
        print('No scale..')
        
    
    
    #PCA
    colname = gen_pcx_string(ncomponents)
    pca = PCA(n_components=ncomponents)
    x_pca = pca.fit_transform(df)
    pca_df = pd.DataFrame(data=x_pca, columns = colname)
    
   
    
    print("***** VAR AFTER PCA ********")
    var_calc(pca_df)
    
    '''
    print('PCA: ')
    print(pca_df)
    print('Originally data hava a shape of : '+ str(df.shape)+', after the pca we have: '+ str(x_pca.shape))
    '''
    
    #add column of label 
    if score_label:
        pca_df = pd.concat([pca_df,df_copy[['score','label']]], axis=1)
    pca_df['domain'] = df_copy_2['domain']
    pca_df = pca_df.set_index(['domain'])
    #print(pca_df)
    
    if save == 1:
        folder_exists('pca_csv')
        path_to_save = os.path.join('pca_csv',name+'.csv')
        pca_df.to_csv(path_to_save)
    
    #explained variance..
    print('Explained variance for n '+str(ncomponents)+' components: ')
    print(pca.explained_variance_ratio_)
    
    if ncomponents == 2 and plot:
        plot_pca(pca_df)
    elif ncomponents == 3 and plot:
        plot3d_3pc(pca_df)
    elif plot:
        plot_bar_pca(pca,colname)
    else:
        print('FINISH')

def plot3d_3pc(pca_in):
    
    fig = plt.figure()
    ax = Axes3D(fig)
    
    targets = ['good','bad','untracked']
    #colors = sns.color_palette('bright',3)
    colors= ['r','g','b']
    
    for target,color in zip(targets,colors):
        index_to_keep = pca_in['label'] == target
        ax.scatter(pca_in.loc[index_to_keep,'PC1'],pca_in.loc[index_to_keep,'PC2'],pca_in.loc[index_to_keep,'PC3'],s=15,c=color)
    
    ax.set_xlabel('PC1')
    ax.set_ylabel('PC2')
    ax.set_zlabel('PC3')  
    
    plt.legend(targets)
    #plt.show()
    

def plot_bar_pca(pca_in,x_names):
    per_var = np.round(pca_in.explained_variance_ratio_*100, decimals=2)
    
    fig = plt.figure(figsize=(8,8))
    ax = fig.add_subplot(1,1,1)
    ax.set_xlabel('components', fontsize=10)
    ax.set_ylabel('Variance %', fontsize=10)
    ax.set_title('Scree Plot', fontsize=15)
    
    rects = ax.bar(x=range(1,len(per_var)+1), height=per_var, tick_label=x_names)
    
    
    def autolabel(rects):
        """Attach a text label above each bar in *rects*, displaying its height."""
        for rect in rects:
            height = rect.get_height()
            ax.annotate('{}'.format(height),
                        xy=(rect.get_x() + rect.get_width() / 2, height),
                        xytext=(0, 3),  # 3 points vertical offset
                        textcoords="offset points",
                        ha='center', va='bottom')
    
    autolabel(rects)
    folder_exists('img')
    path_file_to_save = os.path.join('img','pca_n_'+str(len(per_var))+'.png')
    file_exists(path_file_to_save)
    plt.savefig(path_file_to_save)
    plt.show()
    

def plot_pca(df_pca):
    fig = plt.figure(figsize=(8,8))
    ax = fig.add_subplot(1,1,1)
    ax.set_xlabel('pc1', fontsize=10)
    ax.set_ylabel('pc2', fontsize=10)
    ax.set_title('PCA', fontsize=15)
    
    targets = ['good','bad','untracked']
    colors = sns.color_palette('bright',3)
    for target,color in zip(targets,colors):
        index_to_keep = df_pca['label'] == target
        ax.scatter(df_pca.loc[index_to_keep,'PC1'],df_pca.loc[index_to_keep,'PC2'], c=color, s=5)
    ax.legend(targets)
    ax.grid()
    plt.show()    

def var_calc(df_in):
    '''
    columns = list(df_in.columns)
    index = range(1, len(columns)+ 1)
    
    for i,col in zip(index,columns):
        print(''+str(i)+', '+ str(col)+', var : ' + str(df_in[col].var))
    '''
    variance = VarianceThreshold()
    variance.fit_transform(df_in)
    print('Variance..')
    print(variance.variances_)
    
    tot = 0
    for v in variance.variances_:
        tot += v
    
    print('TOT: '+ str(tot))

def corr_matrix(df_in, title):
    corr = df_in.corr()
    sns.set(font_scale=0.3)
    ax = sns.heatmap(corr,vmin=-1,vmax=1,linewidths=0.5,fmt='.2g',cmap="coolwarm",annot=True)
    ax.set_xticklabels(ax.get_xticklabels(),rotation=45,horizontalalignment='right',fontsize=4)
    ax.set_yticklabels(ax.get_yticklabels(),horizontalalignment='right',fontsize=4)
    ax.set_title(title)

    folder_exists('img_corr_matr')
    path_file_out = os.path.join('img_corr_matr',title+'.pdf')
    plt.savefig(path_file_out)
    
        
    
def main():
    #feature set
    csv_in = sys.argv[1]
    #number of pca's components
    components = int(sys.argv[2])
    #1-> standard scaler, 2->minmax scaler, other-> no scaler
    scale = int(sys.argv[3])
    #name of csv_out
    name = sys.argv[4]

    plotting = int(sys.argv[5])
    saving = int(sys.argv[6])
    #top_scale 1-> include A/AAAA in scaling, 0 not include in the scaling
    top_scale = int(sys.argv[7])
    pca(csv_in,components,scale,name,plotting,saving,top_scale)
    

if __name__ == '__main__':
    main()