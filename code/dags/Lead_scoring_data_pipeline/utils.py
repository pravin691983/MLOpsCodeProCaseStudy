##############################################################################
# Import necessary modules and files
##############################################################################


import pandas as pd
import os
import sqlite3
from sqlite3 import Error
from Lead_scoring_data_pipeline.constants import *
from Lead_scoring_data_pipeline.mapping.city_tier_mapping import city_tier_mapping
from Lead_scoring_data_pipeline.mapping.significant_categorical_level import *
from Lead_scoring_data_pipeline.data_validation_checks import *


def load_data(file_path):
    """
    Description: Load the data
    input: file path
    output : Dataframe
    """
    if "test" in file_path:
        return pd.read_csv(file_path)
    return pd.read_csv(file_path, index_col=[0])


def check_if_table_has_value(cnx, table_name):
    """
    Description: Checks whether the table in the database connection has any values
    input: database connection and table name
    output: boolean
    """
    # cnx = sqlite3.connect(db_path+db_file_name)
    check_table = pd.read_sql(
        f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';",
        cnx,
    ).shape[0]
    return check_table == 1


###############################################################################
# Define the function to build database
###############################################################################


def build_dbs():
    """
    This function checks if the db file with specified name is present
    in the /Assignment/01_data_pipeline/scripts folder. If it is not present it creates
    the db file with the given name at the given path.


    INPUTS
        db_file_name : Name of the database file 'utils_output.db'
        db_path : path where the db file should be '


    OUTPUT
    The function returns the following under the conditions:
        1. If the file exsists at the specified path
                prints 'DB Already Exsists' and returns 'DB Exsists'

        2. If the db file is not present at the specified loction
                prints 'Creating Database' and creates the sqlite db
                file at the specified path with the specified name and
                once the db file is created prints 'New DB Created' and
                returns 'DB created'


    SAMPLE USAGE
        build_dbs()
    """
    if os.path.isfile(DB_PATH + DB_FILE_NAME):
        print("DB Already Exsist")
        print(os.getcwd())
        return "DB Exsist"
    else:
        print("Creating Database")
        """ create a database connection to a SQLite database """
        conn = None
        try:
            conn = sqlite3.connect(DB_PATH + DB_FILE_NAME)
            print("New DB Created")
            return "DB created"
        except Error as e:
            print(e)
            return "Error creating DB " + DB_PATH + DB_FILE_NAME
        # closing the connection once the database is created
        finally:
            if conn:
                conn.close()


###############################################################################
# Define function to load the csv file to the database
###############################################################################


def load_data_into_db():
    """
    Thie function loads the data present in datadirectiry into the db
    which was created previously.
    It also replaces any null values present in 'toal_leads_dropped' and
    'referred_lead' with 0.


    INPUTS
        db_file_name : Name of the database file
        db_path : path where the db file should be
        data_directory : path of the directory where 'leadscoring.csv'
                        file is present


    OUTPUT
        Saves the processed dataframe in the db in a table named 'loaded_data'.
        If the table with the same name already exsists then the function
        replaces it.


    SAMPLE USAGE
        load_data_into_db()
    """
    try:
        conn = sqlite3.connect(DB_PATH + DB_FILE_NAME)

        if not check_if_table_has_value(conn, "loaded_data"):
            _extracted_from_load_data_into_db_29(conn)
        else:
            print("loaded_data already populated")
    except Exception as e:
        print(f"Exception thrown in load_data_into_db : {e}")
    finally:
        if conn:
            conn.close()


def _extracted_from_load_data_into_db_29(conn):
    print("Loading data from " + LEADSCORING_CSV_PATH)
    df = load_data(LEADSCORING_CSV_PATH)

    print("Processing total_leads_droppped and referred_lead columns")
    df["total_leads_droppped"] = df["total_leads_droppped"].fillna(0)
    df["referred_lead"] = df["referred_lead"].fillna(0)

    print("Storing processed df to loaded_data table")
    df.to_sql(name="loaded_data", con=conn, if_exists="replace", index=False)


###############################################################################
# Define function to map cities to their respective tiers
###############################################################################


def map_city_tier():
    """
    This function maps all the cities to their respective tier as per the
    mappings provided in /mappings/city_tier_mapping.py file. If a
    particular city's tier isn't mapped in the city_tier_mapping.py then
    the function maps that particular city to 3.0 which represents
    tier-3.


    INPUTS
        db_file_name : Name of the database file
        db_path : path where the db file should be
        city_tier_mapping : a dictionary that maps the cities to their tier


    OUTPUT
        Saves the processed dataframe in the db in a table named
        'city_tier_mapped'. If the table with the same name already
        exsists then the function replaces it.


    SAMPLE USAGE
        map_city_tier()

    """
    try:
        conn = sqlite3.connect(DB_PATH + DB_FILE_NAME)
        if not check_if_table_has_value(conn, "city_tier_mapped"):
            _extracted_from_map_city_tier_29(conn)
        else:
            print("city_tier_mapped already populated")
    except Exception as e:
        print(f"Exception thrown in map_city_tier : {e}")
    finally:
        if conn:
            conn.close()


def _extracted_from_map_city_tier_29(conn):
    print("Loading loaded_data table")
    df = pd.read_sql("select * from loaded_data", conn)

    print("Mapping city_mapped to tiers")
    df["city_tier"] = df["city_mapped"].map(city_tier_mapping)
    df["city_tier"] = df["city_tier"].fillna(3.0)

    # we do not need city_mapped later
    df = df.drop(["city_mapped"], axis=1)

    print("Storing mapped df to table city_tier_mapped")
    df.to_sql(name="city_tier_mapped", con=conn, if_exists="replace", index=False)


###############################################################################
# Define function to map insignificant categorial variables to "others"
###############################################################################


def map_categorical_vars():
    """
    This function maps all the unsugnificant variables present in 'first_platform_c'
    'first_utm_medium_c' and 'first_utm_source_c'. The list of significant variables
    should be stored in a python file in the 'significant_categorical_level.py'
    so that it can be imported as a variable in utils file.


    INPUTS
        db_file_name : Name of the database file
        db_path : path where the db file should be
        list_platform : list of all the significant platform.
        list_medium : list of all the significat medium
        list_source : list of all rhe significant source

        **NOTE : list_platform, list_medium & list_source are all constants and
                 must be stored in 'significant_categorical_level.py'
                 file. The significant levels are calculated by taking top 90
                 percentils of all the levels. For more information refer
                 'data_cleaning.ipynb' notebook.


    OUTPUT
        Saves the processed dataframe in the db in a table named
        'categorical_variables_mapped'. If the table with the same name already
        exsists then the function replaces it.


    SAMPLE USAGE
        map_categorical_vars()
    """
    try:
        conn = sqlite3.connect(DB_PATH + DB_FILE_NAME)
        if not check_if_table_has_value(conn, "categorical_variables_mapped"):
            _extracted_from_map_categorical_vars_35(conn)
        else:
            print("categorical_variables_mapped already Stored")
    except Exception as e:
        print(f"Error while running map_categorical_vars : {e}")
    finally:
        if conn:
            conn.close()


def _extracted_from_map_categorical_vars_35(conn):
    df = pd.read_sql("select * from city_tier_mapped", conn)
    print("Reading of city_tier_mapped data completed")

    # all the levels below 90 percentage are assgined to a single level called others
    new_df = df[
        ~df["first_platform_c"].isin(list_platform)
    ]  # get rows for levels which are not present in list_platform
    new_df["first_platform_c"] = "others"  # replace the value of these levels to others
    old_df = df[
        df["first_platform_c"].isin(list_platform)
    ]  # get rows for levels which are present in list_platform
    df = pd.concat(
        [new_df, old_df]
    )  # concatenate new_df and old_df to get the final dataframe

    # all the levels below 90 percentage are assgined to a single level called others
    new_df = df[
        ~df["first_utm_medium_c"].isin(list_medium)
    ]  # get rows for levels which are not present in list_medium
    new_df[
        "first_utm_medium_c"
    ] = "others"  # replace the value of these levels to others
    old_df = df[
        df["first_utm_medium_c"].isin(list_medium)
    ]  # get rows for levels which are present in list_medium
    df = pd.concat(
        [new_df, old_df]
    )  # concatenate new_df and old_df to get the final dataframe

    # all the levels below 90 percentage are assgined to a single level called others
    new_df = df[
        ~df["first_utm_source_c"].isin(list_source)
    ]  # get rows for levels which are not present in list_source
    new_df[
        "first_utm_source_c"
    ] = "others"  # replace the value of these levels to others
    old_df = df[
        df["first_utm_source_c"].isin(list_source)
    ]  # get rows for levels which are present in list_source
    df = pd.concat(
        [new_df, old_df]
    )  # concatenate new_df and old_df to get the final dataframe

    df = df.drop_duplicates()
    print("Storing mapped df to table categorical_variables_mapped")
    df.to_sql(
        name="categorical_variables_mapped",
        con=conn,
        if_exists="replace",
        index=False,
    )


##############################################################################
# Define function that maps interaction columns into 4 types of interactions
##############################################################################
def interactions_mapping():
    """
    This function maps the interaction columns into 4 unique interaction columns
    These mappings are present in 'interaction_mapping.csv' file.


    INPUTS
        db_file_name : Name of the database file
        db_path : path where the db file should be
        interaction_mapping_file : path to the csv file containing interaction's
                                   mappings
        index_columns : list of columns to be used as index while pivoting and
                        unpivoting
        NOTE : Since while inference we will not have 'app_complete_flag' which is
        our label, we will have to exculde it from our index_columns. It is recommended
        that you use an if loop and check if 'app_complete_flag' is present in
        'categorical_variables_mapped' table and if it is present pass a list with
        'app_complete_flag' in it as index_column else pass a list without 'app_complete_flag'
        in it.


    OUTPUT
        Saves the processed dataframe in the db in a table named
        'interactions_mapped'. If the table with the same name already exsists then
        the function replaces it.

        It also drops all the features that are not requried for training model and
        writes it in a table named 'model_input'


    SAMPLE USAGE
        interactions_mapping()
    """
    try:
        conn = sqlite3.connect(DB_PATH + DB_FILE_NAME)
        if not check_if_table_has_value(
            conn, "model_input"
        ) or not check_if_table_has_value(conn, "interactions_mapped"):
            _extracted_from_interactions_mapping_39(conn)
        else:
            print("model_input and interactions_mapped already stored")
    except Exception as e:
        print(f"Error while running interactions_mapping : {e}")
    finally:
        if conn:
            conn.close()


def _extracted_from_interactions_mapping_39(conn):
    df = pd.read_sql("select * from categorical_variables_mapped", conn)
    print("categorical_variables_mapped table loaded")

    # reading interaction mapping file
    df_event_mapping = pd.read_csv(INTERACTION_MAPPING, index_col=[0])

    # unpivot the interaction columns
    id_vars = INDEX_COLUMNS
    if "app_complete_flag" not in df.columns:
        id_vars.remove("app_complete_flag")

    df_unpivot = pd.melt(
        df,
        id_vars=id_vars,
        var_name="interaction_type",
        value_name="interaction_value",
    )

    # handling null value
    df_unpivot["interaction_value"] = df_unpivot["interaction_value"].fillna(0)

    # map interaction type column with the mapping file to get interaction mapping
    df = pd.merge(df_unpivot, df_event_mapping, on="interaction_type", how="left")

    # dropping the interaction type column as it is not needed
    df = df.drop(["interaction_type"], axis=1)

    # pivot interaction mapping column values
    df_pivot = df.pivot_table(
        values="interaction_value",
        index=id_vars,
        columns="interaction_mapping",
        aggfunc="sum",
    )
    df_pivot = df_pivot.reset_index()

    df_pivot.to_sql(
        name="interactions_mapped", con=conn, if_exists="replace", index=False
    )

    # Selecting a subset of columns for model traning part, excluding created_date
    trimmed_dataset = df_pivot[INDEX_COLUMNS[1:]]

    print("Storing trimmed df to table model_input")
    trimmed_dataset.to_sql(
        name="model_input", con=conn, if_exists="replace", index=False
    )
