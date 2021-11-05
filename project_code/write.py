import read
import pandas as pd
import db_credentails as dc


conn = f"postgresql://{dc.credentials['USER']}:{dc.credentials['PASSWD']}@{dc.credentials['HOST_NAME']}:{dc.credentials['PORT_NO']}/{dc.credentials['DB_NAME']}"

def write_to_db(df,conn,table_name,key):
    min_key = df[key].min()
    max_key = df[key].max()
    df.to_sql(table_name,conn,if_exists='append',index=False)
    print(f'Loaded data for {table_name} with in range {min_key} and {max_key}')

if __name__ == '__main__':
    tab_names=read.fetching_tbname()
    reader_dept = read.json_reader(dc.credentials['BASE_DIR'],tab_names[2],chunk_size=1000)
    for rdf in reader_dept:
        write_to_db(rdf,conn,tab_names[2],rdf.columns[0])

