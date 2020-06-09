import pandas as pd 
import csv
import sys

def count_occ(csv_in):
    df = pd.read_csv(csv_in, sep=',',low_memory=False, index_col=['domain'])
    columns = list(df.columns)
    #remove 'score column' and tuple with no information from OI, and the column score and label
    df = df.fillna(0)
    df = df[(df['A_n_ipv4'] != 0) | (df['AAAA_n_ipv6'] != 0)]
    print(df.shape)
    value_counts_ = df['label'].value_counts()
    print(value_counts_)


def main():
    csv_in = sys.argv[1]
    count_occ(csv_in)


if __name__ == '__main__':
    main()