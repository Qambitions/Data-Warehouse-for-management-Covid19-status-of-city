from datetime import datetime, timedelta
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
from datetime import datetime

from utils.database import insert_update_condition_sql, truncate_table_sql,insert_sql
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


def etl_phu_nds(CURRENT_TIME):
    mssql_hook_stage = MsSqlHook(mssql_conn_id="covid_stage")
    df = mssql_hook_stage.get_pandas_df(
        sql=f"""SELECT *
                FROM PHUStage
            """
    )
    functionname = "etl_outbreak_group_nds"
    print("\n************\n"+ functionname + " update size ::   ", df.shape, "\n*****************")
    if df.shape[0] == 0: return  
    ############################### transform #######################
    df.drop(columns=['PHUIDKey'],inplace=True)
    df.rename(columns={"ReportingPHU": "PHUName"}, inplace = True)
    df.columns = df.columns.str.replace('Reporting', '')

    ################# look up ############
    mssql_hook_nds = MsSqlHook(mssql_conn_id="covid_nds")
    df_city = mssql_hook_nds.get_pandas_df(
        sql=f"""SELECT PHUCityName as PHUCity, PHUCityID
                FROM PHUCity
            """
    )
    df = df.merge(df_city, how='left', on = 'PHUCity')
    df.drop(columns=['PHUCity'],inplace=True)
    df.drop_duplicates(subset=['PHUID'],inplace=True)
    df['CreatedDate']  = CURRENT_TIME
    df['ModifiedDate'] = CURRENT_TIME
    df['PHUProvince'] = 'Ontario'
    df_clean = df

    # Load data
    mssql_hook_nds = MsSqlHook(mssql_conn_id="covid_nds")
    colCondition =['PHUName','PHUID']

    # insert with look up
    sql_script_list = insert_update_condition_sql(df_clean,'PHU', lookup_col = colCondition, indentity_insert=True)
    for sql_script in sql_script_list:
        mssql_conn_nds = mssql_hook_nds.run(sql_script)

    ########## health region canada ######################
    mssql_hook_stage = MsSqlHook(mssql_conn_id="covid_stage")
    df = mssql_hook_stage.get_pandas_df(
        sql=f"""select DISTINCT HealthRegion as PHUName, Province as PHUProvince
                from CaseDetailsCanadaStage
            """
    )
    # df.drop_duplicates(subset=['PHUID'],inplace=True)
    df['CreatedDate']  = CURRENT_TIME
    df['ModifiedDate'] = CURRENT_TIME
    df_clean = df

    # Load data
    mssql_hook_nds = MsSqlHook(mssql_conn_id="covid_nds")
    colCondition =['PHUName']

    # insert with look up
    sql_script_list = insert_update_condition_sql(df_clean,'PHU', lookup_col = colCondition)
    for sql_script in sql_script_list:
        mssql_conn_nds = mssql_hook_nds.run(sql_script)

##############################################################
def etl_vaccinate_nds(CURRENT_TIME):
    mssql_hook_stage = MsSqlHook(mssql_conn_id="covid_stage")
    df = mssql_hook_stage.get_pandas_df(
        sql=f"""SELECT *
                FROM VaccinateStage
            """
    )
    functionname = "etl_vaccinate_nds"
    print("\n************\n"+ functionname + " update size ::   ", df.shape, "\n*****************")
    if df.shape[0] == 0: return
    df.drop(columns=['VaccinateID'],inplace=True)
    ############### take age ############
    df_age = mssql_hook_stage.get_pandas_df(
        sql=f"""SELECT distinct(AgeGroup) as AgeGroup
                FROM VaccinateStage
            """
    )
    mssql_hook_nds = MsSqlHook(mssql_conn_id="covid_nds")
    df_age = df_age.apply(func_vaccinate_report,axis=1)
    df_age_look_up = mssql_hook_nds.get_pandas_df(
        sql=f"""SELECT *
                FROM AgeGroup
            """
    )
    df_age = df_age.merge(df_age_look_up, how = 'left', on = ['FromAge','ToAge'])
    df_age = df_age[['AgeGroup','AgeGroupID']]
    df = df.merge(df_age, how = 'left', on = 'AgeGroup')
    df.drop(columns=['AgeGroup'],inplace=True)
    df['CreatedDate']  = CURRENT_TIME
    df['ModifiedDate'] = CURRENT_TIME
    # temp_cols=df.columns.tolist()
    # new_cols=temp_cols[-1:] + temp_cols[:-1]
    
    df_clean = df
    df_clean.rename(columns={"AtLeastOneDoseCumulative": "AtLeastOneVaccinated"}, inplace = True)
    
#########################################################################
    colCondition =['Date','PHUID','AgeGroupID']

    # insert with look up
    sql_script_list = insert_update_condition_sql(df_clean,'Vaccinate', lookup_col = colCondition,chunk_size=1000)
    for sql_script in sql_script_list:
        mssql_conn_nds = mssql_hook_nds.run(sql_script)

def etl_outbreakphu(CURRENT_TIME):
    mssql_hook_stage = MsSqlHook(mssql_conn_id="covid_stage")
    df = mssql_hook_stage.get_pandas_df(
        sql=f"""SELECT *
                FROM OutBreakStage
            """
    )
    functionname = "etl_outbreakphu"
    print("\n************\n"+ functionname + " update size ::   ", df.shape, "\n*****************")
    if df.shape[0] == 0: return
    df.drop(columns=['OutbreakID'],inplace=True)
    ############### take outbreak group ############
    df_group = mssql_hook_stage.get_pandas_df(
        sql=f"""SELECT distinct(outbreakgroup)
                FROM OutbreakStage
            """
    )
    mssql_hook_nds = MsSqlHook(mssql_conn_id="covid_nds")
    df_group_look_up = mssql_hook_nds.get_pandas_df(
        sql=f"""SELECT OutbreakGroupID, OutbreakGroupName as OutbreakGroup
                FROM outbreakgroup
            """
    )
    df = df.merge(df_group_look_up, how = 'left', on = 'OutbreakGroup')
    df.drop(columns=['OutbreakGroup'],inplace=True)
    df.rename(columns={"Date": "OutbreakDate",
                        "PhuNum": "PHUID"}, inplace = True)

    df['CreatedDate']  = CURRENT_TIME
    df['ModifiedDate'] = CURRENT_TIME
    df_clean = df

    # Load data
    mssql_hook_nds = MsSqlHook(mssql_conn_id="covid_nds")
    colCondition =['PHUID','OutbreakDate','OutbreakGroupID']

    # insert with look up
    sql_script_list = insert_update_condition_sql(df_clean,'OutbreakPHU', lookup_col = colCondition,chunk_size=1000)
    for sql_script in sql_script_list:
        mssql_conn_nds = mssql_hook_nds.run(sql_script)


def etl_case_report_nds(CURRENT_TIME):
    mssql_hook_stage = MsSqlHook(mssql_conn_id="covid_stage")
    df = mssql_hook_stage.get_pandas_df(
        sql=f"""SELECT *
                FROM CaseReportStage
            """
    )
    df.rename(columns={"Outcome": "CaseStatus"}, inplace = True)
    functionname = "etl_case_report_nds"
    print("\n************\n"+ functionname + " update size ::   ", df.shape, "\n*****************")
    if df.shape[0] == 0: return
    df.drop(columns=['CaseReportID'],inplace=True)
    ############### take age group ############
    df_age = mssql_hook_stage.get_pandas_df(
        sql=f"""SELECT distinct(Age) as AgeGroup
                FROM CaseReportStage
            """
    )
    mssql_hook_nds = MsSqlHook(mssql_conn_id="covid_nds")
    df_age = df_age.apply(func_age_case_report,axis=1)
    df_age_look_up = mssql_hook_nds.get_pandas_df(
        sql=f"""SELECT *
                FROM AgeGroup
            """
    )
    df_age = df_age.merge(df_age_look_up, how = 'left', on = ['FromAge','ToAge'])
    df_age.rename(columns={"AgeGroup": "Age"}, inplace = True)
    df_age = df_age[['Age','AgeGroupID']]
    print('Age', df.shape)
    df = df.merge(df_age, how = 'left', on = 'Age')
    df.drop(columns=['Age'],inplace=True)
    print('Age', df.shape)

    ############### take case accquistion ############
    df_acquisition = mssql_hook_stage.get_pandas_df(
        sql=f"""SELECT distinct(CaseAcquisitionInfo) as CaseAcquisitionInfo
                FROM CaseReportStage
            """
    )
    mssql_hook_nds = MsSqlHook(mssql_conn_id="covid_nds")
    df_acquisition_look_up = mssql_hook_nds.get_pandas_df(
        sql=f"""SELECT *
                FROM CaseAcquisition
            """
    )
    df_acquisition = df_acquisition.merge(df_acquisition_look_up, how = 'left', on = ['CaseAcquisitionInfo'])
    df_acquisition = df_acquisition[['CaseAcquisitionInfo','CaseAcquisitionID']]
    df = df.merge(df_acquisition, how = 'left', on = 'CaseAcquisitionInfo')
    df.drop(columns=['CaseAcquisitionInfo'],inplace=True)

    ############### PHU ############
    mssql_hook_nds = MsSqlHook(mssql_conn_id="covid_nds")
    df_phu_look_up = mssql_hook_nds.get_pandas_df(
        sql=f"""SELECT PHUName as ReportingPHU, PHUID
                FROM PHU
            """
    )
    print('ReportingPHU', df.shape)
    df = df.merge(df_phu_look_up, how = 'left', on = 'ReportingPHU')
    print('ReportingPHU', df.shape)
    df.drop(columns=['ReportingPHU','PHUCity',
                     'PHUAddress','PHUWebsite','PHULatitude',
                     'PHULongitude','PHUPostalCode'],inplace=True)
    df['Province']     = 'Ontario'
    df['CreatedDate']  = CURRENT_TIME
    df['ModifiedDate'] = CURRENT_TIME
    df_clean = df

    # Load data
    mssql_hook_nds = MsSqlHook(mssql_conn_id="covid_nds")

    # insert with look up
    sql_script_list = insert_sql(df_clean,'CaseReport',chunk_size=1000)
    for sql_script in sql_script_list:
        mssql_conn_nds = mssql_hook_nds.run(sql_script)

def etl_case_details_nds(CURRENT_TIME):
    mssql_hook_stage = MsSqlHook(mssql_conn_id="covid_stage")
    df = mssql_hook_stage.get_pandas_df(
        sql=f"""SELECT *
                FROM CaseDetailsCanadaStage
            """
    )
    functionname = "etl_case_details_nds"
    print("\n************\n"+ functionname + " update size ::   ", df.shape, "\n*****************")
    if df.shape[0] == 0: return
    df.drop(columns=['ObjectID','RowID','CaseDetailsCanadaStageID'],inplace=True)
    ############### take age group ############
    df_age = mssql_hook_stage.get_pandas_df(
        sql=f"""SELECT distinct(AgeGroup) as AgeGroup
                FROM CaseDetailsCanadaStage
            """
    )
    mssql_hook_nds = MsSqlHook(mssql_conn_id="covid_nds")
    df_age = df_age.apply(func_age_case_details,axis=1)
    df_age_look_up = mssql_hook_nds.get_pandas_df(
        sql=f"""SELECT *
                FROM AgeGroup
            """
    )
    df_age = df_age.merge(df_age_look_up, how = 'left', on = ['FromAge','ToAge'])
    df_age = df_age[['AgeGroup','AgeGroupID']]
    df = df.merge(df_age, how = 'left', on = 'AgeGroup')
    df.drop(columns=['AgeGroup'],inplace=True)

    ############### take case accquistion ############
    df_acquisition = mssql_hook_stage.get_pandas_df(
        sql=f"""SELECT distinct(Exposure) as CaseAcquisitionInfo
                FROM CaseDetailsCanadaStage
            """
    )
    mssql_hook_nds = MsSqlHook(mssql_conn_id="covid_nds")
    df_acquisition_look_up = mssql_hook_nds.get_pandas_df(
        sql=f"""SELECT *
                FROM CaseAcquisition
            """
    )
    df_acquisition = df_acquisition.merge(df_acquisition_look_up, how = 'left', on = ['CaseAcquisitionInfo'])
    df_acquisition = df_acquisition[['CaseAcquisitionInfo','CaseAcquisitionID']]
    df_acquisition.rename(columns={"CaseAcquisitionInfo": "Exposure"}, inplace = True)
    df = df.merge(df_acquisition, how = 'left', on = 'Exposure')
    df.drop(columns=['Exposure'],inplace=True)

    ############### PHU ############
    mssql_hook_nds = MsSqlHook(mssql_conn_id="covid_nds")
    df_phu_look_up = mssql_hook_nds.get_pandas_df(
        sql=f"""SELECT PHUName as HealthRegion, PHUID
                FROM PHU
            """
    )
    df = df.merge(df_phu_look_up, how = 'left', on = 'HealthRegion')
    
    df.rename(columns={"DateReported": "CaseReportedDate"}, inplace = True)
    df.drop(columns=['HealthRegion'],inplace=True)
    
    df['CreatedDate']  = CURRENT_TIME
    df['ModifiedDate'] = CURRENT_TIME
    df_clean = df

    # Load data
    mssql_hook_nds = MsSqlHook(mssql_conn_id="covid_nds")
    # colCondition =['PHUID','AgeGroupID','CaseAccquisitionID']

    # insert with look up
    sql_script_list = insert_sql(df_clean,'CaseReport',chunk_size=1000)
    for sql_script in sql_script_list:
        mssql_conn_nds = mssql_hook_nds.run(sql_script)