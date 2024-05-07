from datetime import datetime, timedelta
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
from datetime import datetime
import pandas as pd

from utils.database import insert_update_condition_sql, truncate_table_sql,insert_sql

def etl_agegroup_dds(LSET,CURRENT_TIME):
    mssql_hook_nds = MsSqlHook(mssql_conn_id="covid_nds")
    df = mssql_hook_nds.get_pandas_df(
        sql=f"""SELECT *
                FROM AgeGroup
                where (CreatedDate > '{LSET}' and CreatedDate <= '{CURRENT_TIME}') 
                or (ModifiedDate > '{LSET}' and ModifiedDate <= '{CURRENT_TIME}')"""
    )

    functionname = "etl_agegroup_dds"
    print("\n************\n"+ functionname + " update size ::   ", df.shape, "\n*****************")
    if df.shape[0] == 0: return 

    # Transform data
    df.drop(columns=['CreatedDate','ModifiedDate'], inplace=True)
    
    df_clean = df 

    # Load data
    mssql_hook_stage = MsSqlHook(mssql_conn_id="covid_dds")
   
    sql_script_list = insert_update_condition_sql(df_clean, 'AgeGroup', indentity_insert=True)
    for sql_script in sql_script_list:
        mssql_conn_stage = mssql_hook_stage.run(sql_script)

def etl_city_dds(LSET,CURRENT_TIME):
    # query data from stage
    mssql_hook_nds = MsSqlHook(mssql_conn_id="covid_nds")
    df = mssql_hook_nds.get_pandas_df(
        sql=f"""SELECT *
                FROM PHUcity
                where (CreatedDate > '{LSET}' and CreatedDate <= '{CURRENT_TIME}') 
                or (ModifiedDate > '{LSET}' and ModifiedDate <= '{CURRENT_TIME}')"""
    )

    functionname = "etl_city_dds"
    print("\n************\n"+ functionname + " update size ::   ", df.shape, "\n*****************")
    if df.shape[0] == 0: return 

    # Transform data
    df.drop(columns=['CreatedDate','ModifiedDate'], inplace=True)
    df.rename(columns={"PHUCityID": "CityID",
                        "PHUCityName": "CityName"}, inplace = True)
    df_clean = df 

    # Load data
    mssql_hook_stage = MsSqlHook(mssql_conn_id="covid_dds")
   
    sql_script_list = insert_update_condition_sql(df_clean, 'city', indentity_insert=True)
    for sql_script in sql_script_list:
        mssql_conn_stage = mssql_hook_stage.run(sql_script)


def etl_phugroup_dds(LSET,CURRENT_TIME):
    # query data from stage
    mssql_hook_nds = MsSqlHook(mssql_conn_id="covid_nds")
    df = mssql_hook_nds.get_pandas_df(
        sql=f"""SELECT *
                FROM PHUGroup
                where (CreatedDate > '{LSET}' and CreatedDate <= '{CURRENT_TIME}') 
                or (ModifiedDate > '{LSET}' and ModifiedDate <= '{CURRENT_TIME}')"""
    )

    functionname = "etl_phugroup_dds"
    print("\n************\n"+ functionname + " update size ::   ", df.shape, "\n*****************")
    if df.shape[0] == 0: return 

    # Transform data
    df.drop(columns=['CreatedDate','ModifiedDate'], inplace=True)
    
    df_clean = df 

    # Load data
    mssql_hook_stage = MsSqlHook(mssql_conn_id="covid_dds")
   
    sql_script_list = insert_update_condition_sql(df_clean, 'PHUGroup', indentity_insert=True)
    for sql_script in sql_script_list:
        mssql_conn_stage = mssql_hook_stage.run(sql_script)

def etl_phu_dds(LSET,CURRENT_TIME):
    # query data from stage
    mssql_hook_nds = MsSqlHook(mssql_conn_id="covid_nds")
    df = mssql_hook_nds.get_pandas_df(
        sql=f"""SELECT *
                FROM PHU
                where (CreatedDate > '{LSET}' and CreatedDate <= '{CURRENT_TIME}') 
                or (ModifiedDate > '{LSET}' and ModifiedDate <= '{CURRENT_TIME}')"""
    )

    functionname = "etl_phugroup_dds"
    print("\n************\n"+ functionname + " update size ::   ", df.shape, "\n*****************")
    if df.shape[0] == 0: return 

    # Transform data
    df.drop(columns=['CreatedDate','ModifiedDate'], inplace=True)
    df.rename(columns={"PHUCityID": "CityID"}, inplace = True)
    df_clean = df 

    # Load data
    mssql_hook_stage = MsSqlHook(mssql_conn_id="covid_dds")
   
    sql_script_list = insert_update_condition_sql(df_clean, 'PHU', indentity_insert=True)
    for sql_script in sql_script_list:
        mssql_conn_stage = mssql_hook_stage.run(sql_script)

def etl_outbreakgroup_dds(LSET,CURRENT_TIME):
    # query data from stage
    mssql_hook_nds = MsSqlHook(mssql_conn_id="covid_nds")
    df = mssql_hook_nds.get_pandas_df(
        sql=f"""SELECT *
                FROM OutbreakGroup
                where (CreatedDate > '{LSET}' and CreatedDate <= '{CURRENT_TIME}') 
                or (ModifiedDate > '{LSET}' and ModifiedDate <= '{CURRENT_TIME}')"""
    )

    functionname = "etl_outbreakgroup_dds"
    print("\n************\n"+ functionname + " update size ::   ", df.shape, "\n*****************")
    if df.shape[0] == 0: return 

    # Transform data
    df.drop(columns=['CreatedDate','ModifiedDate'], inplace=True)
    
    df_clean = df 

    # Load data
    mssql_hook_stage = MsSqlHook(mssql_conn_id="covid_dds")
   
    sql_script_list = insert_update_condition_sql(df_clean, 'OutbreakGroup',indentity_insert=True, update=False)
    for sql_script in sql_script_list:
        mssql_conn_stage = mssql_hook_stage.run(sql_script)

def etl_gender_dds(LSET,CURRENT_TIME):
    mssql_hook_stage = MsSqlHook(mssql_conn_id="covid_nds")
    df = mssql_hook_stage.get_pandas_df(
        sql=f"""SELECT distinct(upper(Gender)) as gendername
                FROM casereport
                where (CreatedDate > '{LSET}' and CreatedDate <= '{CURRENT_TIME}') 
                or (ModifiedDate > '{LSET}' and ModifiedDate <= '{CURRENT_TIME}') 
            """
    )
    functionname = "etl_age_group_casedetail_nds"
    print("\n************\n"+ functionname + " update size ::   ", df.shape, "\n*****************")
    if df.shape[0] == 0: return
    # Transform data
    df_clean = df

     # Load data
    mssql_hook_nds = MsSqlHook(mssql_conn_id="covid_dds")
    colCondition = ['gendername']

    # insert with look up
    sql_script_list = insert_update_condition_sql(df_clean,'gender', lookup_col = colCondition,update=False)
    for sql_script in sql_script_list:
        mssql_conn_nds = mssql_hook_nds.run(sql_script)

def etl_date_dds(LSET,CURRENT_TIME):
    mssql_hook_stage = MsSqlHook(mssql_conn_id="covid_nds")
    df = mssql_hook_stage.get_pandas_df(
        sql=f"""SELECT distinct(casereporteddate) as casereporteddate
                FROM casereport
                where (CreatedDate > '{LSET}' and CreatedDate <= '{CURRENT_TIME}') 
                or (ModifiedDate > '{LSET}' and ModifiedDate <= '{CURRENT_TIME}')
                union 
                SELECT distinct(OutbreakDate) as casereporteddate
                FROM outbreakPHU
                where (CreatedDate > '{LSET}' and CreatedDate <= '{CURRENT_TIME}') 
                or (ModifiedDate > '{LSET}' and ModifiedDate <= '{CURRENT_TIME}') 
                union 
                SELECT distinct([Date]) as casereporteddate
                FROM Vaccinate
                where (CreatedDate > '{LSET}' and CreatedDate <= '{CURRENT_TIME}') 
                or (ModifiedDate > '{LSET}' and ModifiedDate <= '{CURRENT_TIME}')
            """
    )
    functionname = "etl_date_dds"
    print("\n************\n"+ functionname + " update size ::   ", df.shape, "\n*****************")
    if df.shape[0] == 0: return
    # Transform data
    df['casereporteddate'] = pd.to_datetime(df['casereporteddate'])
    df['Year'] = df['casereporteddate'].dt.strftime('%Y')
    df['QuarterOfYear'] = df['casereporteddate'].dt.to_period('Q').dt.strftime('%q')
    df['MonthOfYear'] = df['casereporteddate'].dt.strftime('%m')
    df['DayOfMonth'] = df['casereporteddate'].dt.strftime('%d')
    df_clean = df[['Year','QuarterOfYear','MonthOfYear','DayOfMonth']]
    functionname = "etl_date_dds"
    print("\n************\n"+ functionname + " update size ::   ", df.shape, "\n*****************")
    if df.shape[0] == 0: return
    # Transform data
    df['casereporteddate'] = pd.to_datetime(df['casereporteddate'])
    df['Year'] = df['casereporteddate'].dt.strftime('%Y')
    df['QuarterOfYear'] = df['casereporteddate'].dt.to_period('Q').dt.strftime('%q')
    df['MonthOfYear'] = df['casereporteddate'].dt.strftime('%m')
    df['DayOfMonth'] = df['casereporteddate'].dt.strftime('%d')
    df_clean = df[['Year','QuarterOfYear','MonthOfYear','DayOfMonth']]
    df_clean.drop_duplicates(inplace=True)

    # Load data
    mssql_hook_nds = MsSqlHook(mssql_conn_id="covid_dds")
    colCondition = ['Year','QuarterOfYear','MonthOfYear','DayOfMonth']

    # insert with look up
    sql_script_list = insert_update_condition_sql(df_clean,'date', lookup_col = colCondition,update=False)
    for sql_script in sql_script_list:
        mssql_conn_nds = mssql_hook_nds.run(sql_script)