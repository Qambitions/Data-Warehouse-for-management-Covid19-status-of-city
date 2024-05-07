from datetime import datetime, timedelta
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
from datetime import datetime

from utils.database import insert_update_condition_sql, truncate_table_sql

def etl_phu_group_and_city_nds(CURRENT_TIME):
    mssql_hook_stage = MsSqlHook(mssql_conn_id="covid_stage")
    df = mssql_hook_stage.get_pandas_df(
        sql=f"""SELECT *
                FROM PHUGroupStage
            """
    )
    functionname = "etl_phu_group_and_city_nds"
    print("\n************\n"+ functionname + " update size ::   ", df.shape, "\n*****************")
    if df.shape[0] == 0: return  
    ############################### phu group ########################
    df_phugroup = df[['PHUGroup']]
    df_phugroup.drop_duplicates(inplace = True)

    # Transform data
    df_phugroup.rename(columns={"PHUGroup": "PHUGroupName"}, inplace = True)
    df_phugroup['CreatedDate']  = CURRENT_TIME
    df_phugroup['ModifiedDate'] = CURRENT_TIME
    df_clean = df_phugroup

    # Load data
    mssql_hook_nds = MsSqlHook(mssql_conn_id="covid_nds")
    colCondition =['PHUGroupName']

    # insert with look up
    sql_script_list = insert_update_condition_sql(df_clean,'PHUGroup', lookup_col = colCondition)
    for sql_script in sql_script_list:
        mssql_conn_nds = mssql_hook_nds.run(sql_script)

    #############################  city   ################# 
    #look up phugroup
    tuple_phugroup_distinct = tuple((df["PHUGroup"].unique()))
    if len(tuple_phugroup_distinct) == 1: tuple_phugroup_distinct = f""" ('{tuple_phugroup_distinct[0]}') """

    df_look_up_phugroup = mssql_hook_nds.get_pandas_df(
        sql=f"""SELECT PHUGroupID, PHUGroupName
                FROM PHUGroup
                Where PHUGroupName in { tuple_phugroup_distinct }
            """
    )
    # Transform data
    # print('bbb',df.columns, 'aaa',df_look_up_phugroup.columns)
    df.rename(columns={"PHUCity"   : "PHUCityName",
                       "PHUGroup"  : "PHUGroupName",
                       "PHURegion" : "PHUName"}, inplace = True)
    df.drop(columns=['PHUGroupID'],inplace=True)
    df = df.merge(df_look_up_phugroup, how = 'left', on = ['PHUGroupName'])
    
    df_city = df[['PHUGroupID','PHUCityName']]
    df_city.drop_duplicates(inplace = True)

    df_city['CreatedDate']  = CURRENT_TIME
    df_city['ModifiedDate'] = CURRENT_TIME
    df_clean = df_city

    # Load data
    # mssql_hook_nds = MsSqlHook(mssql_conn_id="covid_nds")
    colCondition =['PHUGroupID','PHUCityName']

    # insert with look up
    sql_script_list = insert_update_condition_sql(df_clean,'PHUCity', lookup_col = colCondition)
    for sql_script in sql_script_list:
        mssql_conn_nds = mssql_hook_nds.run(sql_script)


def etl_outbreak_group_nds(CURRENT_TIME):
    mssql_hook_stage = MsSqlHook(mssql_conn_id="covid_stage")
    df = mssql_hook_stage.get_pandas_df(
        sql=f"""SELECT distinct(outbreakgroup)
                FROM OutbreakStage
            """
    )
    functionname = "etl_outbreak_group_nds"
    print("\n************\n"+ functionname + " update size ::   ", df.shape, "\n*****************")
    if df.shape[0] == 0: return  
    ############################### outbreak group ########################

    # Transform data
    df.rename(columns={"outbreakgroup": "outbreakgroupname"}, inplace = True)
    df['CreatedDate']  = CURRENT_TIME
    df['ModifiedDate'] = CURRENT_TIME
    df_clean = df

    # Load data
    mssql_hook_nds = MsSqlHook(mssql_conn_id="covid_nds")
    colCondition = ['OutbreakGroupName']

    # insert with look up
    sql_script_list = insert_update_condition_sql(df_clean,'OutbreakGroup', lookup_col = colCondition)
    for sql_script in sql_script_list:
        mssql_conn_nds = mssql_hook_nds.run(sql_script)


def etl_case_acquisition_group_caserp_nds(CURRENT_TIME):
    mssql_hook_stage = MsSqlHook(mssql_conn_id="covid_stage")
    df = mssql_hook_stage.get_pandas_df(
        sql=f"""SELECT distinct(CaseAcquisitionInfo) as CaseAcquisitionInfo
                FROM CaseReportStage
            """
    )
    functionname = "etl_case_acquisition_group_caserp_nds"
    print("\n************\n"+ functionname + " update size ::   ", df.shape, "\n*****************")
    if df.shape[0] == 0: return  
    ############################### outbreak group ########################

    # Transform data
    # df['SourceName']   = 'CaseReport'
    df['CreatedDate']  = CURRENT_TIME
    df['ModifiedDate'] = CURRENT_TIME
    df_clean = df

    # Load data
    mssql_hook_nds = MsSqlHook(mssql_conn_id="covid_nds")
    colCondition = ['CaseAcquisitionInfo']

    # insert with look up
    sql_script_list = insert_update_condition_sql(df_clean,'CaseAcquisition', lookup_col = colCondition)
    for sql_script in sql_script_list:
        mssql_conn_nds = mssql_hook_nds.run(sql_script)

def etl_case_acquisition_group_case_details_nds(CURRENT_TIME):
    mssql_hook_stage = MsSqlHook(mssql_conn_id="covid_stage")
    df = mssql_hook_stage.get_pandas_df(
        sql=f"""SELECT distinct(Exposure) as CaseAcquisitionInfo
                FROM CaseDetailsCanadaStage
            """
    )
    functionname = "etl_case_acquisition_group_case_details_nds"
    print("\n************\n"+ functionname + " update size ::   ", df.shape, "\n*****************")
    if df.shape[0] == 0: return  
    ############################### outbreak group ########################

    # Transform data
    # df['SourceName']   = 'CaseDetailsCanadaStage'
    df['CreatedDate']  = CURRENT_TIME
    df['ModifiedDate'] = CURRENT_TIME
    df_clean = df

    # Load data
    mssql_hook_nds = MsSqlHook(mssql_conn_id="covid_nds")
    colCondition = ['CaseAcquisitionInfo']

    # insert with look up
    sql_script_list = insert_update_condition_sql(df_clean,'CaseAcquisition', lookup_col = colCondition)
    for sql_script in sql_script_list:
        mssql_conn_nds = mssql_hook_nds.run(sql_script)

####################################################
def func_age_case_details(x):
    start_age = 0
    end_age   = 1000
    if x.AgeGroup[0].isnumeric() == True:
        start_age = int(x.AgeGroup[0]) * 10
    elif x.AgeGroup[0] == '<':
        end_age = 20

    if x.AgeGroup[2] == '-':
        end_age = start_age + 9 
        
    x['FromAge'] = start_age
    x['ToAge']   = end_age
    return x

def etl_age_group_casedetail_nds(CURRENT_TIME):
    mssql_hook_stage = MsSqlHook(mssql_conn_id="covid_stage")
    df = mssql_hook_stage.get_pandas_df(
        sql=f"""SELECT distinct(AgeGroup) as AgeGroup
                FROM CaseDetailsCanadaStage
            """
    )
    functionname = "etl_age_group_casedetail_nds"
    print("\n************\n"+ functionname + " update size ::   ", df.shape, "\n*****************")
    if df.shape[0] == 0: return
    # Transform data
    df = df.apply(func_age_case_details,axis=1)
    print(df)
    df['CreatedDate']  = CURRENT_TIME
    df['ModifiedDate'] = CURRENT_TIME
    df.drop(columns=['AgeGroup'],inplace=True)
    df_clean = df

     # Load data
    mssql_hook_nds = MsSqlHook(mssql_conn_id="covid_nds")
    colCondition = ['ToAge','FromAge']

    # insert with look up
    sql_script_list = insert_update_condition_sql(df_clean,'AgeGroup', lookup_col = colCondition)
    for sql_script in sql_script_list:
        mssql_conn_nds = mssql_hook_nds.run(sql_script)

############################################################
#############################################################
def func_age_case_report(x):
    start_age = 0
    end_age   = 1000
    if x.AgeGroup[0].isnumeric() == True:
        start_age = int(x.AgeGroup[0]) * 10
    elif x.AgeGroup[0] == '<':
        end_age = 20

    if x.AgeGroup[2] == 's':
        end_age = start_age + 9 
    x['FromAge'] = start_age
    x['ToAge']   = end_age
    return x

def etl_age_group_casereport_nds(CURRENT_TIME):
    mssql_hook_stage = MsSqlHook(mssql_conn_id="covid_stage")
    df = mssql_hook_stage.get_pandas_df(
        sql=f"""SELECT distinct(Age) as AgeGroup
                FROM CaseReportStage
            """
    )
    functionname = "etl_age_group_casereport_nds"
    print("\n************\n"+ functionname + " update size ::   ", df.shape, "\n*****************")
    if df.shape[0] == 0: return  
    # Transform data
    df = df.apply(func_age_case_report,axis=1)
    print(df)
    df['CreatedDate']  = CURRENT_TIME
    df['ModifiedDate'] = CURRENT_TIME
    df.drop(columns=['AgeGroup'],inplace=True)
    df_clean = df

    # Load data
    mssql_hook_nds = MsSqlHook(mssql_conn_id="covid_nds")
    colCondition = ['ToAge','FromAge']

    # insert with look up
    sql_script_list = insert_update_condition_sql(df_clean,'AgeGroup', lookup_col = colCondition)
    for sql_script in sql_script_list:
        mssql_conn_nds = mssql_hook_nds.run(sql_script)


#############################################################
#############################################################
def func_vaccinate_report(x):
    start_age = 0
    end_age   = 1000
    if x.AgeGroup[0].isnumeric() == True:
        start_age = int(x.AgeGroup[0]+x.AgeGroup[1]) 

        if len(x.AgeGroup)>3 and x.AgeGroup[3].isnumeric() == True:
            end_age = int(x.AgeGroup[3]+x.AgeGroup[4])
    else: # scecial list
        if x.AgeGroup == 'Ontario_5plus':
            start_age = 5
        elif x.AgeGroup == 'Ontario_12plus':
            start_age = 12
        elif x.AgeGroup == 'Adults_18plus':
            start_age = 18
    x['FromAge'] = start_age
    x['ToAge']   = end_age
    return x

def etl_age_group_vaccinate_nds(CURRENT_TIME):
    mssql_hook_stage = MsSqlHook(mssql_conn_id="covid_stage")
    df = mssql_hook_stage.get_pandas_df(
        sql=f"""SELECT distinct(AgeGroup) as AgeGroup
                FROM VaccinateStage
            """
    )
    functionname = "etl_age_group_vaccinate_nds"
    print("\n************\n"+ functionname + " update size ::   ", df.shape, "\n*****************")
    if df.shape[0] == 0: return  

    # Transform data
    df = df.apply(func_vaccinate_report,axis=1)
    print(df)
    df['CreatedDate']  = CURRENT_TIME
    df['ModifiedDate'] = CURRENT_TIME
    df.drop(columns=['AgeGroup'],inplace=True)
    df_clean = df

    # Load data
    mssql_hook_nds = MsSqlHook(mssql_conn_id="covid_nds")
    colCondition = ['ToAge','FromAge']

    # insert with look up
    sql_script_list = insert_update_condition_sql(df_clean,'AgeGroup', lookup_col = colCondition)
    for sql_script in sql_script_list:
        mssql_conn_nds = mssql_hook_nds.run(sql_script)
