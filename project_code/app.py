import read
import write
import db_credentails as dc
 
import pandas as pd


def main():
    tb_names = read.fetching_tbname()
    for tab in tb_names:
        reader = read.json_reader(dc.credentials['BASE_DIR'],tab,chunk_size=1000)
        for rdf in reader:
            write.write_to_db(rdf,write.conn,tab,rdf.columns[0])


    print("Data from the JSON files has been loaded into tables.")   


if __name__=='__main__':
    main()     