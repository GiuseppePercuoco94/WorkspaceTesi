#!/usr/bin/env python3
import avro_to_csv
import opint_csv
import polito_OI
import normalization_csvOI
import sys
import os




#data for avro_to_csv
start_date = sys.argv[1] 
end_date = sys.argv[2]
type_data = sys.argv[3]
#data for opint_csv

#path csv
path_csv_l = sys.argv[4]
#path name folder intersect
name_folder_intersect = sys.argv[5]
#info for normalization, set for json
json_set = sys.argv[6]
avro_to_csv.main(start_date,end_date,type_data)
folder_csv = 'OI_folder/openintel-'+type_data+'-'+start_date
opint_csv.main(folder_csv,path_csv_l,name_folder_intersect)
path_intersect = name_folder_intersect+'/'+'inter1_ldn_oi_openintel-'+type_data+'-'+start_date+'.csv'
polito_OI.main(path_intersect,folder_csv)
path_info_by_oi = 'info_by_OI_inter1_openintel-'+type_data+'-'+start_date+'.csv'
normalization_csvOI.main(path_info_by_oi,path_csv_l,json_set)









