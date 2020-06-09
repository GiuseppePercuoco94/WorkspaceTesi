import csv
import json
import requests
from datetime import datetime
import sys
import time
import os
import re


def list_dmn_norep(csv_list):
    re_www = re.compile(r'www.')
    re_wwwn= re.compile(r"www\w+.")
    list_domain_name = []
    count_null = 0
    with open(csv_list) as list_in:
        lines = list_in.readlines()
        for line in lines:
            if '\n' in line:
                #list_domain_name.append(line.strip('\n'))
                line = line.strip('\n')
                if len(line) == 0:
                    count_null += 1
                    #continue
                elif re.match(re_wwwn,line):
                    line = re_wwwn.sub('',line)
                elif re.match(re_www,line):
                    line = re_www.sub('',line)
                else:
                    print('[debug]probably no new case ')
                list_domain_name.append(line)
            else:
                if len(line) == 0:
                    count_null += 1
                    continue
                elif re.match(re_wwwn,line):
                    line = re_wwwn.sub('',line)
                elif re.match(re_www,line):
                    line = re_www.sub('',line)
                else:
                    #continue
                    print('[debug]probably no new case ')
                list_domain_name.append(line)
    print(len(list_domain_name))
    print(count_null)
    dict_list = list(dict.fromkeys(list_domain_name)) 
    
    with open('list_norep.csv', mode='a') as csv_out:
        writer = csv.writer(csv_out)
        for dom in dict_list:
            writer.writerow([dom])
    print(len(dict_list))


if __name__ == '__main__':
    csv_list = sys.argv[1]
    list_dmn_norep(csv_list)