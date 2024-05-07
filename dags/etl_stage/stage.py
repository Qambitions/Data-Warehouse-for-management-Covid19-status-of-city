from datetime import datetime, timedelta
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
from datetime import datetime
import pandas as pd

from utils.database import insert_update_condition_sql, truncate_table_sql,insert_sql

def etl_casereport_stage(path,conn_id,table,type='csv'):
    try:
        if type == 'csv':
            df = pd.read_csv(path)
        else: 
            df = pd.read_excel(path) 
    except:
        mssql_hook_stage = MsSqlHook(mssql_conn_id=conn_id)
        mssql_hook_stage.run(truncate_table_sql(table))
        return
    
    # truncate table
    mssql_hook_stage = MsSqlHook(mssql_conn_id=conn_id)
    mssql_hook_stage.run(truncate_table_sql(table))

    functionname = "etl " + path
    print("\n************\n"+ functionname + " update size ::   ", df.shape, "\n*****************")
    
    if df.shape[0] == 0: return

    # Transform data
    df.columns = df.columns.str.replace(' |_', '')
    # df.drop(columns=['CaseReportID'], inplace=True)
 
    # Load data
    sql_script_list = insert_sql(df, table,chunk_size = 1000)
    for sql_script in sql_script_list:
        mssql_conn_stage = mssql_hook_stage.run(sql_script)


def etl_casedetailscanada_stage(path,conn_id,table,type='csv'):
    try:
        if type == 'csv':
            df = pd.read_csv(path)
        else: 
            df = pd.read_excel(path) 
    except:
        mssql_hook_stage = MsSqlHook(mssql_conn_id=conn_id)
        mssql_hook_stage.run(truncate_table_sql(table))
        return
    
    # truncate table
    mssql_hook_stage = MsSqlHook(mssql_conn_id=conn_id)
    mssql_hook_stage.run(truncate_table_sql(table))

    functionname = "etl " + path
    print("\n************\n"+ functionname + " update size ::   ", df.shape, "\n*****************")

    if df.shape[0] == 0: return

    # Transform data
    # df.drop(columns=['CaseDetailsCanadaStageID'], inplace=True)
    df.columns = df.columns.str.replace(' |_', '')
    df['datereported'] = pd.to_datetime(df.datereported.astype(str))
    df['datereported'] = df['datereported'].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    # Load data
    sql_script_list = insert_sql(df, table,chunk_size=1000)
    for sql_script in sql_script_list:
        mssql_conn_stage = mssql_hook_stage.run(sql_script)

def etl_outbreak_stage(path,conn_id,table,type='csv'):
    try:
        if type == 'csv':
            df = pd.read_csv(path)
        else: 
            df = pd.read_excel(path)
    except:
        mssql_hook_stage = MsSqlHook(mssql_conn_id=conn_id)
        mssql_hook_stage.run(truncate_table_sql(table))
        return 
        
    # truncate table
    mssql_hook_stage = MsSqlHook(mssql_conn_id=conn_id)
    mssql_hook_stage.run(truncate_table_sql(table))
    functionname = "etl " + path
    print("\n************\n"+ functionname + " update size ::   ", df.shape, "\n*****************")

    if df.shape[0] == 0: return

    # Transform data
    df.columns = df.columns.str.replace(' ', '')
    df.columns = df.columns.str.replace('_', '')
    
    # Load data
    sql_script_list = insert_sql(df, table,chunk_size=1000)
    for sql_script in sql_script_list:
        mssql_conn_stage = mssql_hook_stage.run(sql_script)

def etl_vaccinate_stage(path,conn_id,table,type='csv'):
    try:
        if type == 'csv':
            df = pd.read_csv(path)
        else: 
            df = pd.read_excel(path)
    except:
        mssql_hook_stage = MsSqlHook(mssql_conn_id=conn_id)
        mssql_hook_stage.run(truncate_table_sql(table))

        return 
    
    # truncate table
    mssql_hook_stage = MsSqlHook(mssql_conn_id=conn_id)
    mssql_hook_stage.run(truncate_table_sql(table))
    functionname = "etl " + path
    print("\n************\n"+ functionname + " update size ::   ", df.shape, "\n*****************")

    if df.shape[0] == 0: return

    # Transform data
    df.columns = df.columns.str.replace(' ', '')
    df.columns = df.columns.str.replace('_', '')
   
    # Load data
    sql_script_list = insert_sql(df, table,chunk_size=1000)
    for sql_script in sql_script_list:
        mssql_conn_stage = mssql_hook_stage.run(sql_script)

def etl_phugroup_stage(path,conn_id,table,type='csv'):
    try:
        if type == 'csv':
            df = pd.read_csv(path)
        else: 
            df = pd.read_excel(path) 
    except:
        mssql_hook_stage = MsSqlHook(mssql_conn_id=conn_id)
        mssql_hook_stage.run(truncate_table_sql(table))

        return
    
    functionname = "etl " + path
    print("\n************\n"+ functionname + " update size ::   ", df.shape, "\n*****************")
    # truncate table
    mssql_hook_stage = MsSqlHook(mssql_conn_id=conn_id)
    mssql_hook_stage.run(truncate_table_sql(table))

    if df.shape[0] == 0: return

    # Transform data
    df.columns = df.columns.str.replace(' ', '')
    df.columns = df.columns.str.replace('_', '')
   
    # Load data
    sql_script_list = insert_sql(df, table,chunk_size=1000)
    for sql_script in sql_script_list:
        mssql_conn_stage = mssql_hook_stage.run(sql_script)

def etl_phu_stage(path,conn_id,table,type='csv'):
    try:
        if type == 'csv':
            df = pd.read_csv(path)
        else: 
            df = pd.read_excel(path)
    except:
        mssql_hook_stage = MsSqlHook(mssql_conn_id=conn_id)
        mssql_hook_stage.run(truncate_table_sql(table))
        return 
    
    functionname = "etl " + path
    print("\n************\n"+ functionname + " update size ::   ", df.shape, "\n*****************")
    # truncate table
    mssql_hook_stage = MsSqlHook(mssql_conn_id=conn_id)
    mssql_hook_stage.run(truncate_table_sql(table))

    if df.shape[0] == 0: return

    # Transform data
    df.columns = df.columns.str.replace(' ', '')
    df.columns = df.columns.str.replace('_', '')
    
    # Load data
    sql_script_list = insert_sql(df, table,chunk_size=1000)
    for sql_script in sql_script_list:
        mssql_conn_stage = mssql_hook_stage.run(sql_script)
