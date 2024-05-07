from datetime import datetime, timedelta
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
from datetime import datetime
import pandas as pd
import numpy as np

from utils.database import insert_update_condition_sql, truncate_table_sql,insert_sql

def etl_fact_case_acquisition_table(LSET,CURRENT_TIME):
    mssql_hook_nds = MsSqlHook(mssql_conn_id="covid_nds")
    mssql_hook_dds = MsSqlHook(mssql_conn_id="covid_dds")
    df = mssql_hook_nds.get_pandas_df(
        sql=f"""SELECT *
                FROM CaseReport
                where (CreatedDate > '{LSET}' and CreatedDate <= '{CURRENT_TIME}') 
                or (ModifiedDate > '{LSET}' and ModifiedDate <= '{CURRENT_TIME}')
            """
    )
    functionname = "etl_fact_case_acquisition_table"
    print("\n************\n"+ functionname + " update size ::   ", df.shape, "\n*****************")
    if df.shape[0] == 0: return 
    # df.to_csv("/home/diastic/airflow/data/111testttt.csv")
    df.drop(columns=['CreatedDate','ModifiedDate','CaseReportID'], inplace=True)
    ############### take gender ############
    df_gender = mssql_hook_nds.get_pandas_df(
        sql=f"""SELECT distinct(upper(gender)) as Gender
                FROM CaseReport
                where (CreatedDate > '{LSET}' and CreatedDate <= '{CURRENT_TIME}') 
                or (ModifiedDate > '{LSET}' and ModifiedDate <= '{CURRENT_TIME}')
            """
    )
    df_gender_look_up = mssql_hook_dds.get_pandas_df(
        sql=f"""SELECT GenderID, upper(genderName) as Gender
                FROM Gender
            """
    )
    df_gender = df_gender.merge(df_gender_look_up, how = 'left', on = ['Gender'])
    df_gender = df_gender[['GenderID','Gender']]
    df['Gender'] = df['Gender'].str.upper()
    df_gender['Gender'] = df_gender['Gender'].str.upper()
    df = df.merge(df_gender, how = 'left', on = 'Gender')
    df.drop(columns=['Gender'],inplace=True)
    # df.to_csv("/home/diastic/airflow/data/2.csv")
    ############### take date ############
    df_date = mssql_hook_nds.get_pandas_df(
        sql=f"""SELECT distinct(casereporteddate) as CaseReportedDate
                FROM casereport
                where (CreatedDate > '{LSET}' and CreatedDate <= '{CURRENT_TIME}') 
                or (ModifiedDate > '{LSET}' and ModifiedDate <= '{CURRENT_TIME}')
            """
    )
    df_date['CaseReportedDate'] = pd.to_datetime(df_date['CaseReportedDate'])
    df_date['Year']          = df_date['CaseReportedDate'].dt.strftime('%Y').astype(int)
    df_date['QuarterOfYear'] = df_date['CaseReportedDate'].dt.to_period('Q').dt.strftime('%q').astype(int)
    df_date['MonthOfYear']   = df_date['CaseReportedDate'].dt.strftime('%m').astype(int)
    df_date['DayOfMonth']    = df_date['CaseReportedDate'].dt.strftime('%d').astype(int)
    # df_date = df_date[['CaseReportedDate','Year','QuarterOfYear','MonthOfYear','DayOfMonth']]
    mssql_hook_nds = MsSqlHook(mssql_conn_id="covid_nds")
    df_date_look_up = mssql_hook_dds.get_pandas_df(
        sql=f"""SELECT *
                FROM Date
            """
    )
    df_date = df_date.merge(df_date_look_up, how = 'left', on = ['Year','QuarterOfYear','MonthOfYear','DayOfMonth'])
    df_date = df_date[['DateID','CaseReportedDate']]
    df['CaseReportedDate'] = pd.to_datetime(df['CaseReportedDate'])
    df = df.merge(df_date, how = 'left', on = 'CaseReportedDate')
    df.drop(columns=['CaseReportedDate'],inplace=True)

    df['TotalDeath'] = np.where(
        (df['CaseStatus'] == 'Deceased') | (df['CaseStatus'] == 'Fatal'),1,0)

    df['TotalRecovery'] = np.where(
        (df['CaseStatus'] == 'Recovered') | (df['CaseStatus'] == 'Resolved'),1,0)

    df['TotalInfection'] = np.where(
        (df['CaseStatus'] == 'Active') | (df['CaseStatus'] == 'Not Resolved'),1,0)
    df['TotalCase'] = 1
    groupby_object = df.groupby(by=['DateID','GenderID','PHUID','AgeGroupID'])
    df = groupby_object.agg({'TotalDeath': 'sum',
                    'TotalRecovery': 'sum',
                    'TotalInfection': 'sum',
                    'TotalCase': 'sum',})
    df.reset_index(inplace = True)
    
    ###### merge with old data####################################
    final_df = df
    if df.shape[0] < 1000:
        df['tmp'] = df['DateID'].astype(str) + "|" + df['GenderID'].astype(str) + "|" + df['PHUID'].astype(str) + "|" + df['AgeGroupID'].astype(str)
        tuple_tmpid_distinct = tuple((df["tmp"].unique()))
        if len(tuple_tmpid_distinct) == 1: tuple_tmpid_distinct = f""" ('{tuple_tmpid_distinct[0]}') """
        df.drop(columns=['tmp'], inplace=True)
        df_look_up = mssql_hook_dds.get_pandas_df(
            sql=f"""SELECT *
                    FROM StatisticCaseAcquisition
                    Where CONCAT(DateID,'|',GenderID,'|',PHUID,'|',AgeGroupID) in { tuple_tmpid_distinct }
                """
        )
        if df_look_up.shape[0] > 0:
            final_df = pd.concat([df_look_up,df])
            final_df.reset_index(drop=True,inplace=True)
            groupby_object = final_df.groupby(by=['DateID','GenderID','PHUID','AgeGroupID'])
            final_df = groupby_object.agg({'TotalDeath': 'sum',
                            'TotalRecovery': 'sum',
                            'TotalInfection': 'sum',
                            'TotalCase': 'sum',})
            final_df.reset_index(inplace = True)
  
    #########################################
    df_clean = final_df
    
    # Load data
    mssql_hook_nds = MsSqlHook(mssql_conn_id="covid_dds")
    colCondition = ['DateID','GenderID','PHUID','AgeGroupID']

    # insert with look up
    sql_script_list = insert_update_condition_sql(df_clean,'StatisticCaseAcquisition', lookup_col = colCondition)
    for sql_script in sql_script_list:
        mssql_conn_nds = mssql_hook_nds.run(sql_script)

def etl_fact_out_break_table(LSET,CURRENT_TIME):
    mssql_hook_nds = MsSqlHook(mssql_conn_id="covid_nds")
    mssql_hook_dds = MsSqlHook(mssql_conn_id="covid_dds")
    df = mssql_hook_nds.get_pandas_df(
        sql=f"""SELECT *
                FROM OutbreakPHU
                where (CreatedDate > '{LSET}' and CreatedDate <= '{CURRENT_TIME}') 
                or (ModifiedDate > '{LSET}' and ModifiedDate <= '{CURRENT_TIME}')
            """
    )
    functionname = "etl_fact_out_break_table"
    print("\n************\n"+ functionname + " update size ::   ", df.shape, "\n*****************")
    if df.shape[0] == 0: return 
    # df.to_csv("/home/diastic/airflow/data/111testttt.csv")
    df.drop(columns=['CreatedDate','ModifiedDate'], inplace=True)

     ############### take date ############
    df_date = mssql_hook_nds.get_pandas_df(
        sql=f"""SELECT distinct(OutbreakDate) as OutbreakDate
                FROM outbreakPHU
                where (CreatedDate > '{LSET}' and CreatedDate <= '{CURRENT_TIME}') 
                or (ModifiedDate > '{LSET}' and ModifiedDate <= '{CURRENT_TIME}') 
            """
    )
    df_date['OutbreakDate'] = pd.to_datetime(df_date['OutbreakDate'])
    df_date['Year']          = df_date['OutbreakDate'].dt.strftime('%Y').astype(int)
    df_date['QuarterOfYear'] = df_date['OutbreakDate'].dt.to_period('Q').dt.strftime('%q').astype(int)
    df_date['MonthOfYear']   = df_date['OutbreakDate'].dt.strftime('%m').astype(int)
    df_date['DayOfMonth']    = df_date['OutbreakDate'].dt.strftime('%d').astype(int)
    # df_date = df_date[['CaseReportedDate','Year','QuarterOfYear','MonthOfYear','DayOfMonth']]
    mssql_hook_nds = MsSqlHook(mssql_conn_id="covid_nds")
    df_date_look_up = mssql_hook_dds.get_pandas_df(
        sql=f"""SELECT *
                FROM Date
            """
    )
    df_date = df_date.merge(df_date_look_up, how = 'left', on = ['Year','QuarterOfYear','MonthOfYear','DayOfMonth'])
    df_date = df_date[['DateID','OutbreakDate']]
    df['OutbreakDate'] = pd.to_datetime(df['OutbreakDate'])
    df = df.merge(df_date, how = 'left', on = 'OutbreakDate')
    df.drop(columns=['OutbreakDate'],inplace=True)
    # print(df.columns)
    groupby_object = df.groupby(by=['PHUID','DateID','OutbreakGroupID'])
    df = groupby_object.agg({'NumberOnGoingOutbreaks': 'sum'})
    df.reset_index(inplace = True)
    df.rename(columns={"NumberOnGoingOutbreaks": "TotalOnGoingOutbreaks"}, inplace = True)

    ###### merge with old data####################################
    final_df = df
    if df.shape[0] < 1000:
        df['tmp'] = df['PHUID'].astype(str) + "|" + df['DateID'].astype(str) + "|" + df['OutbreakGroupID'].astype(str) 
        tuple_tmpid_distinct = tuple((df["tmp"].unique()))
        if len(tuple_tmpid_distinct) == 1: tuple_tmpid_distinct = f""" ('{tuple_tmpid_distinct[0]}') """
        df.drop(columns=['tmp'], inplace=True)
        df_look_up = mssql_hook_dds.get_pandas_df(
            sql=f"""SELECT *
                    FROM StatisticOutbreak
                    Where CONCAT(PHUID,'|',DateID,'|',OutbreakGroupID) in { tuple_tmpid_distinct }
                """
        )
        if df_look_up.shape[0] > 0: 
            final_df = pd.concat([df_look_up,df])
            final_df.reset_index(drop=True,inplace=True)
            groupby_object = final_df.groupby(by=['PHUID','DateID','OutbreakGroupID'])
            final_df = groupby_object.agg({'TotalOnGoingOutbreaks': 'sum'})
            final_df.reset_index(inplace = True)
    ########################################
    
    df_clean = final_df
    print(df_clean)

    # Load data
    mssql_hook_nds = MsSqlHook(mssql_conn_id="covid_dds")
    colCondition = ['PHUID','DateID','OutbreakGroupID']

    # insert with look up
    sql_script_list = insert_update_condition_sql(df_clean,'StatisticOutbreak', lookup_col = colCondition)
    for sql_script in sql_script_list:
        mssql_conn_nds = mssql_hook_nds.run(sql_script)


def etl_fact_statistic_vaccinate_table(LSET,CURRENT_TIME):
    mssql_hook_nds = MsSqlHook(mssql_conn_id="covid_nds")
    mssql_hook_dds = MsSqlHook(mssql_conn_id="covid_dds")
    df = mssql_hook_nds.get_pandas_df(
        sql=f"""SELECT *
                FROM Vaccinate
                where (CreatedDate > '{LSET}' and CreatedDate <= '{CURRENT_TIME}') 
                or (ModifiedDate > '{LSET}' and ModifiedDate <= '{CURRENT_TIME}')
            """
    )
    functionname = "etl_fact_case_acquisition_table"
    print("\n************\n"+ functionname + " update size ::   ", df.shape, "\n*****************")
    if df.shape[0] == 0: return 
    df.drop(columns=['CreatedDate','ModifiedDate'], inplace=True)
    ############### take date ############
    df_date = mssql_hook_nds.get_pandas_df(
        sql=f"""SELECT distinct([date]) as CaseReportedDate
                FROM Vaccinate
                where (CreatedDate > '{LSET}' and CreatedDate <= '{CURRENT_TIME}') 
                or (ModifiedDate > '{LSET}' and ModifiedDate <= '{CURRENT_TIME}')
            """
    )
    df_date['CaseReportedDate'] = pd.to_datetime(df_date['CaseReportedDate'])
    df_date['Year']          = df_date['CaseReportedDate'].dt.strftime('%Y').astype(int)
    df_date['QuarterOfYear'] = df_date['CaseReportedDate'].dt.to_period('Q').dt.strftime('%q').astype(int)
    df_date['MonthOfYear']   = df_date['CaseReportedDate'].dt.strftime('%m').astype(int)
    df_date['DayOfMonth']    = df_date['CaseReportedDate'].dt.strftime('%d').astype(int)
    mssql_hook_nds = MsSqlHook(mssql_conn_id="covid_nds")
    df_date_look_up = mssql_hook_dds.get_pandas_df(
        sql=f"""SELECT *
                FROM Date
            """
    )
    df_date = df_date.merge(df_date_look_up, how = 'left', on = ['Year','QuarterOfYear','MonthOfYear','DayOfMonth'])
    df.rename(columns={"Date": "CaseReportedDate"}, inplace = True)
    df_date = df_date[['DateID','CaseReportedDate']]
    df['CaseReportedDate'] = pd.to_datetime(df['CaseReportedDate'])
    df = df.merge(df_date, how = 'left', on = 'CaseReportedDate')
    df.drop(columns=['CaseReportedDate'],inplace=True)
    temp_cols=df.columns.tolist()
    new_cols=temp_cols[-1:] + temp_cols[:-1]
    df_clean = df[new_cols]
    df_clean.fillna(0,inplace=True)
    
    # Load data
    mssql_hook_nds = MsSqlHook(mssql_conn_id="covid_dds")
    colCondition = ['DateID','PHUID','AgeGroupID']

    # insert with look up
    sql_script_list = insert_update_condition_sql(df_clean,'StatisticVaccinate', lookup_col = colCondition)
    for sql_script in sql_script_list:
        mssql_conn_nds = mssql_hook_nds.run(sql_script)

