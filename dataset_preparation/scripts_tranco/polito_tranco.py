#!/usr/bin/env python3

import csv
import os
import pandas as pd
import sys
from progress.bar import Bar


def trunco_check(path_tranco_list, path_names):
    """
    Args:
        path_tranco_list: top1m from tranco
        path_names = list of dom names
    """
    #path tranco list
    path_tranco = path_tranco_list
    #path lista nomi dom campus
    path_lnd_campus = path_names

    #extract domain names from tranco list
    tranco_dom = []
    with open(path_tranco) as tranco_csv:
        lines = tranco_csv.readlines()
        for line in lines:
            tranco_dom.append(line)

    campus_dom = []
    #extract domain from campus list, removing 'www.', 'www8.','www2.' at beginning of domain names
    with open(path_lnd_campus) as campus_txt:
        lines = campus_txt.readlines()
        for line in lines:
            if '\n' in line:
                    #list_domain_name.append(line.strip('\n'))
                    line = line.strip('\n')
                    if 'www8.' in line:
                        line = line.replace('www8.','')
                        campus_dom.append(line)
                    elif 'www2.' in line:
                        line = line.replace('www2.','')
                        campus_dom.append(line)
                        #aggiungere gestione che toglie il 'www.', 'www2.', 'www8.'.. anche sotto
                    elif 'www.' in line:
                        line = line.replace('www.','')
                        campus_dom.append(line)
                    else:
                        print('[debug]probably no new case')
                        campus_dom.append(line)
            else:
                    if 'www8.' in line:
                        line = line.replace('www8.','')
                        campus_dom.append(line)
                    elif 'www2.' in line:
                        line = line.replace('www2.','')
                        campus_dom.append(line)
                        #aggiungere gestione che toglie il 'www.', 'www2.', 'www8.'.. anche sotto
                    elif 'www.' in line:
                        line = line.replace('www.','')
                        campus_dom.append(line)
                    else:
                        print('[debug]2 probably no new case ')
                        campus_dom.append(line)

    if os.path.exists('tranco_inter_campus.csv'):
        print('Removingn old csv')
        os.remove('tranco_inter_campus.csv')
    else:
        print('No old csv exists')
        
    header = ['domain','tranco']
    print('CSV Creation')
    
    bar_csv = Bar('Csv creation', max=len(campus_dom), fill='~')
    with open('tranco_inter_campus.csv', mode ='a') as csv_out:
        writer = csv.writer(csv_out)
        writer.writerow(header)
        for x in campus_dom:
            data = []
            if x in tranco_dom:
                data.append(str(x))
                data.append('1')
                writer.writerow(data)
            else:
                data.append(str(x))
                data.append('0')
                writer.writerow(data)
            bar_csv.next()
    bar_csv.finish()


if __name__ == '__main__':
    #path tranco list
    path_tranco = sys.argv[1]
    #path lista nomi dom campus
    path_lnd_campus = sys.argv[2]
    trunco_check(path_tranco, path_lnd_campus)