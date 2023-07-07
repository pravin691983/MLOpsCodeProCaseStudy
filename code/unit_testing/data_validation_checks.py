"""
Import necessary modules
############################################################################## 
"""

import pandas as pd
from constants import *
from schema import *
import sqlite3
from sqlite3 import Error

###############################################################################
# Define function to validate raw data's schema
# ############################################################################## 

def raw_data_schema_check():
    '''
    This function check if all the columns mentioned in schema.py are present in
    leadscoring.csv file or not.

   
    INPUTS
        db_file_name : Name of the database file
        db_path : path where the db file should be   
        raw_data_schema : schema of raw data in the form oa list/tuple as present 
                          in 'schema.py'

    OUTPUT
        If the schema is in line then prints 
        'Raw datas schema is in line with the schema present in schema.py' 
        else prints
        'Raw datas schema is NOT in line with the schema present in schema.py'

    
    SAMPLE USAGE
        raw_data_schema_check
    '''
    try:
        df = pd.read_csv(LEADSCORING_CSV_PATH, index_col=[0])    
        if sorted(raw_data_schema) == sorted(df.columns) :
            print('Schema is inline with schema.py')
        else :
            print('Schema is not inline with schema.py')
    except Exception as e:
        print (f'Error while running raw_data_schema_check : {e}')
   

##############################################################################
# Define function to validate model's input schema
# ############################################################################## 

def model_input_schema_check():
    '''
    This function check if all the columns mentioned in model_input_schema in 
    schema.py are present in table named in 'model_input' in db file.

   
    INPUTS
        db_file_name : Name of the database file
        db_path : path where the db file should be   
        raw_data_schema : schema of models input data in the form oa list/tuple
                          present as in 'schema.py'

    OUTPUT
        If the schema is in line then prints 
        'Models input schema is in line with the schema present in schema.py'
        else prints
        'Models input schema is NOT in line with the schema present in schema.py'
    
    SAMPLE USAGE
        raw_data_schema_check
    '''
    try:
        connection = sqlite3.connect(DB_PATH + DB_FILE_NAME)
        df = pd.read_sql('select * from model_input', connection)          
        connection.close()
        if sorted(model_input_schema) == sorted(df.columns) :
            print('Models input schema is inline with schema.py')
        else :
            print( 'Models input schema is NOT inline with schema.py')
    except Exception as e:
        print (f'Error while running model_input_schema_check')
    
