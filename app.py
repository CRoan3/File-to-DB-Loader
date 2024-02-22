import sys
import glob
import os
import json
import re
import pandas as pd

#getting meta data (schema info)
def get_column_names(schemas, ds_name, sorting_key = 'column_position'):
    column_details = schemas[ds_name] #ds_name = dataset name
    columns = sorted(column_details, key = lambda col: col[sorting_key]) 	#sorting columns by specified sorting_key
    return [col['column_name'] for col in columns] #gives column names

#reading data from source files into dataframes
def read_csv(file, schemas):
    file_path_list = re.split('[/\\\]', file) 
    ds_name = file_path_list[-2] 	#getting dataset name
    columns = get_column_names(schemas, ds_name) 	#getting column names from schema for respective dataset
    df_reader = pd.read_csv(file, names=columns, chunksize=10000) 	#instead of creating dataframe by reading CSV file assigning column names, we are getting df reader 
    return df_reader

def to_sql(df, db_conn_url, ds_name):
    df.to_sql(
        ds_name, 
        db_conn_url, 
        if_exists='append', #append vs replace - replace: drop table before inserting new values. Append: insert new values into existing table
        index=False  
)
    
def db_loader(src_base_dir, db_conn_url, ds_name):
    schemas = json.load(open(f'{src_base_dir}/schemas.json')) #creates dict 
    files = glob.glob(f'{src_base_dir}/{ds_name}/part-*') #returns list of file names
    if len(files) == 0:
        raise NameError(f'No files found for {ds_name}')
    
    for files in files:
        df_reader = read_csv(file, schemas)   #data from file into df reader
        for idx, df in enumerate(df_reader):   #iterating through df reader, returning a df based on chunk size
            print(f'Populating chunk {idx} of {ds_name}')
            to_sql(df, db_conn_url, ds_name)   #writing dataframe to table  

def process_files(ds_names=None): #not passing an argument will attempt to process all files
    src_base_dir = os.environ.get('SRC_BASE_DIR')
    db_host = os.environ.get('DB_HOST')
    db_port = os.environ.get('DB_PORT')
    db_name = os.environ.get('DB_NAME')
    db_user = os.environ.get('DB_USER')
    db_pass = os.environ.get('DB_PASS')
    db_conn_url = f'postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}'
    schemas = json.load(open(f'{src_base_dir}/schemas.json'))
    if not ds_names: #if no ds_names given, it processes all datasets based on keys available as part of schemas
        ds_names = schemas.keys()
    for ds_name in ds_names:
        try:
            print(f'Processing {ds_name}')
            db_loader(src_base_dir, db_conn_url, ds_name) #reading data to df in chunks, then they are returned to respective table in db
        except NameError as ne:
            print(ne)
            pass
        except Exception as e:
            print(e)
            pass
        finally:
            print(f'Error Processing {ds_name}')


if __name__ == '__main__':
    if len(sys.argv) == 2:
        ds_names = json.load(sys.argv[1])
        process_files(ds_names)
    else:
        process_files()