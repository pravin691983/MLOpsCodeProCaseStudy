##############################################################################
# Import the necessary modules
# #############################################################################

import pytest
from utils import *
from constants import *
import sqlite3

###############################################################################
# Write test cases for load_data_into_db() function
# ##############################################################################

def test_load_data_into_db():
    """_summary_
    This function checks if the load_data_into_db function is working properly by
    comparing its output with test cases provided in the db in a table named
    'loaded_data_test_case'


    SAMPLE USAGE
        output=test_get_data()

    """
    build_dbs()
    load_data_into_db()
    connection = sqlite3.connect(DB_PATH + DB_FILE_NAME)
    df = pd.read_sql('select * from loaded_data', connection)          
    connection.close()
    connection = sqlite3.connect(DB_PATH + UNIT_TEST_DB_FILE_NAME)
    ref_df = pd.read_sql('select * from loaded_data_test_case', connection)          
    connection.close()
    assert df.equals(ref_df), "loaded_data does not match loaded_data_test_case"
    
    

###############################################################################
# Write test cases for map_city_tier() function
# ##############################################################################
def test_map_city_tier():
    """_summary_
    This function checks if map_city_tier function is working properly by
    comparing its output with test cases provided in the db in a table named
    'city_tier_mapped_test_case'


    SAMPLE USAGE
        output=test_map_city_tier()

    """
    build_dbs()
    load_data_into_db()
    map_city_tier()
    connection = sqlite3.connect(DB_PATH + DB_FILE_NAME)
    df = pd.read_sql('select * from city_tier_mapped', connection)          
    connection.close()
    connection = sqlite3.connect(DB_PATH + UNIT_TEST_DB_FILE_NAME)
    ref_df = pd.read_sql('select * from city_tier_mapped_test_case', connection)          
    connection.close()
    assert df.equals(ref_df), "city_tier_mapped does not match with city_tier_mapped_test_case"

###############################################################################
# Write test cases for map_categorical_vars() function
# ##############################################################################    
def test_map_categorical_vars():
    """_summary_
    This function checks if map_cat_vars function is working properly by
    comparing its output with test cases provided in the db in a table named
    'categorical_variables_mapped_test_case'


    SAMPLE USAGE
        output=test_map_cat_vars()

    """
    build_dbs()
    load_data_into_db()
    map_city_tier()
    map_categorical_vars()
    connection = sqlite3.connect(DB_PATH + DB_FILE_NAME)
    df = pd.read_sql('select * from categorical_variables_mapped', connection)          
    connection.close()
    connection = sqlite3.connect(DB_PATH + UNIT_TEST_DB_FILE_NAME)
    ref_df = pd.read_sql('select * from categorical_variables_mapped_test_case', connection)          
    connection.close()
    assert df.equals(ref_df), "categorical_variables_mapped does not match with categorical_variables_mapped_test_case"
    

###############################################################################
# Write test cases for interactions_mapping() function
# ##############################################################################    
def test_interactions_mapping():
    """_summary_
    This function checks if test_column_mapping function is working properly by
    comparing its output with test cases provided in the db in a table named
    'interactions_mapped_test_case'


    SAMPLE USAGE
        output=test_column_mapping()

    """
    build_dbs()
    load_data_into_db()
    map_city_tier()
    map_categorical_vars()
    interactions_mapping()
    connection = sqlite3.connect(DB_PATH + DB_FILE_NAME)
    df = pd.read_sql('select * from interactions_mapped', connection)          
    connection.close()
    connection = sqlite3.connect(DB_PATH + UNIT_TEST_DB_FILE_NAME)
    ref_df = pd.read_sql('select * from interactions_mapped_test_case', connection)          
    connection.close()
    assert df.equals(ref_df), "interactions_mapped does not match with interactions_mapped_test_case"
