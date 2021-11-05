import sys
sys.path.insert(0,'C:\\Users\\Shivank Kashyap\\pythonproject\\Data_copier\\retail_db_json')
#sys.path.insert(1,'C:\Users\Shivank Kashyap\pythonproject\Data_copier')


import pandas as pd
import os
import db_credentails as dc


#from retail_db_json import *

def fetching_tbname():
    base_dir = dc.credentials['BASE_DIR']
    #content_list = os.listdir(f'{base_dir}')
    dir_name = [ name for name in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, name)) ]
    return [dr_nm for dr_nm in dir_name if not dr_nm.startswith('.')]
    #return dir_name



def json_reader(base_dir,table_name,chunk_size=1000):
    file_name = os.listdir(f'{base_dir}\{table_name}')[0]
    fp = f'{base_dir}\{table_name}\{file_name}'
    return pd.read_json(fp,lines=True,chunksize=chunk_size)

if __name__ == '__main__':
    tab_names=fetching_tbname()
    print(tab_names,' ',type(tab_names))
    reader = json_reader(dc.credentials['BASE_DIR'],tab_names[1],chunk_size=1000)
    for rfp in reader:
        min_key = rfp[rfp.columns[0]].min()
        max_key = rfp[rfp.columns[0]].max()
        #print(rfp.count)
        print(f'processed {tab_names[1]} with in range {min_key} and {max_key}')
