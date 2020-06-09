import pandas as pd 
import sys 

def var_influ(csv_in):
    df = pd.read_csv(csv_in,sep=',',low_memory=False, index_col=['cluster'])
    var_influence = df.describe()
    sort = var_influence.sort_values(axis=1, by='std', ascending = False)
    print(sort)
    print(sort.columns)
    print(pd.Series(var_influence.loc['max']-var_influence.loc['min']).sort_values(ascending=False))
    
def main():
    csv_in = sys.argv[1]
    var_influ(csv_in)

if __name__ == '__main__':
    main()