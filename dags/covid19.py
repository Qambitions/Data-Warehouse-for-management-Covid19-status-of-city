from datetime import datetime, timedelta

from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
from datetime import datetime
from airflow.utils.dates import days_ago

from etl_stage.stage import etl_casereport_stage,\
                            etl_casedetailscanada_stage,\
                            etl_outbreak_stage,etl_vaccinate_stage,\
                            etl_phugroup_stage,etl_phu_stage

from etl_nds.etl_level_0_nds import etl_phu_group_and_city_nds,\
                                    etl_outbreak_group_nds,\
                                    etl_case_acquisition_group_caserp_nds,\
                                    etl_case_acquisition_group_case_details_nds,\
                                    etl_age_group_casedetail_nds,\
                                    etl_age_group_casereport_nds,\
                                    etl_age_group_vaccinate_nds

from etl_nds.etl_level_1_nds import etl_phu_nds,etl_vaccinate_nds,\
                                    etl_outbreakphu,etl_case_report_nds,\
                                    etl_case_details_nds

from etl_dds.etl_dim_dds import etl_agegroup_dds, \
                                etl_city_dds,etl_date_dds,\
                                etl_gender_dds,etl_outbreakgroup_dds,\
                                etl_phu_dds,etl_phugroup_dds

from etl_dds.etl_fact_dds import etl_fact_case_acquisition_table,etl_fact_out_break_table,\
                                    etl_fact_statistic_vaccinate_table

CURRENT_TIME = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


default_args = {
    'owner': 'khoabui',
    'retries': 3,
    'retry_delay': timedelta(minutes=15),
    'start_date': days_ago(0),
}

@dag(
    dag_id='covid_19_system',
    default_args=default_args,
    schedule="@daily",
    dagrun_timeout=timedelta(minutes=60),
) 
def covid_19_etl():
    @task()
    def ETL_casereport_stage():
        etl_casereport_stage('/home/diastic/airflow/data/Cases Report.csv','covid_stage','CaseReportStage')
    
    @task()
    def ETL_casedetailscanada_stage():
        etl_casedetailscanada_stage('/home/diastic/airflow/data/Compiled_COVID-19_Case_Details_(Canada).csv','covid_stage','CaseDetailsCanadaStage')

    @task()
    def ETL_outbreak_stage():
        etl_outbreak_stage('/home/diastic/airflow/data/ongoing_outbreaks_phu.csv','covid_stage','OutbreakStage')

    @task()
    def ETL_phu_stage():
        etl_phu_stage('/home/diastic/airflow/data/Public health unit.xlsx','covid_stage','PHUStage','xlsx')

    @task()
    def ETL_vaccinate_stage():
        etl_vaccinate_stage('/home/diastic/airflow/data/vaccines_by_age_phu.csv','covid_stage','VaccinateStage')
        etl_vaccinate_stage('/home/diastic/airflow/data/vaccines_by_age_phu.xlsx','covid_stage','VaccinateStage','xlsx')

    @task()
    def ETL_phugroup_stage():
        etl_phugroup_stage('/home/diastic/airflow/data/Public Health Units GROUP.xlsx','covid_stage','PHUGroupStage','xlsx')

    @task()
    def fake_task():
        print('aaaa')
        ...

    @task()
    def ETL_phu_group_and_city_nds():
        etl_phu_group_and_city_nds(CURRENT_TIME)

    @task()
    def ETL_outbreak_group_nds():
        etl_outbreak_group_nds(CURRENT_TIME)

    @task()
    def ETL_case_acquisition_group_caserp_nds():
        etl_case_acquisition_group_caserp_nds(CURRENT_TIME)

    @task()
    def ETL_case_acquisition_group_case_details_nds():
        etl_case_acquisition_group_case_details_nds(CURRENT_TIME)

    @task()
    def ETL_age_group_nds():
        etl_age_group_casedetail_nds(CURRENT_TIME)
        etl_age_group_casereport_nds(CURRENT_TIME)
        etl_age_group_vaccinate_nds(CURRENT_TIME)

    @task()
    def ETL_phu_nds():
        etl_phu_nds(CURRENT_TIME)

    @task()
    def ETL_vaccinate_nds():
        etl_vaccinate_nds(CURRENT_TIME)

    @task()
    def ETL_outbreakphu_nds():
        etl_outbreakphu(CURRENT_TIME)

    @task()
    def ETL_case_report():
        etl_case_report_nds(CURRENT_TIME)
        etl_case_details_nds(CURRENT_TIME)
    
    @task()
    def query_metadata_dds(tenbang):
        import json
        hook = MsSqlHook(mssql_conn_id="covid_metadata")
        df = hook.get_pandas_df(sql=f"""select * from ddscovid where upper(tablename) = upper('{tenbang}');""")
        df['ModifiedDate'].fillna(datetime.min,inplace=True)
        # return LSET
        df = df.to_json(orient="records")
        df = json.loads(df)
        print(df)
        return df

    @task()
    def ETL_dim_date_dds(metadata):
        LSET_TIMESTAMP = metadata[0]["ModifiedDate"]
        print("time stamp", LSET_TIMESTAMP)
        LSET = datetime.utcfromtimestamp(int(LSET_TIMESTAMP)/1000).strftime('%Y-%m-%d %H:%M:%S')
        etl_date_dds(LSET,CURRENT_TIME)

    @task()
    def ETL_dim_agegroup_dds(metadata):
        LSET_TIMESTAMP = metadata[0]["ModifiedDate"]
        print("time stamp", LSET_TIMESTAMP)
        LSET = datetime.utcfromtimestamp(int(LSET_TIMESTAMP)/1000).strftime('%Y-%m-%d %H:%M:%S')
        etl_agegroup_dds(LSET,CURRENT_TIME)
    
    @task()
    def ETL_dim_gender_dds(metadata):
        LSET_TIMESTAMP = metadata[0]["ModifiedDate"]
        print("time stamp", LSET_TIMESTAMP)
        LSET = datetime.utcfromtimestamp(int(LSET_TIMESTAMP)/1000).strftime('%Y-%m-%d %H:%M:%S')
        etl_gender_dds(LSET,CURRENT_TIME)

    @task()
    def ETL_dim_phugroup_dds(metadata):
        LSET_TIMESTAMP = metadata[0]["ModifiedDate"]
        print("time stamp", LSET_TIMESTAMP)
        LSET = datetime.utcfromtimestamp(int(LSET_TIMESTAMP)/1000).strftime('%Y-%m-%d %H:%M:%S')
        etl_phugroup_dds(LSET,CURRENT_TIME)

    @task()
    def ETL_dim_outbreakgroup_dds(metadata):
        LSET_TIMESTAMP = metadata[0]["ModifiedDate"]
        print("time stamp", LSET_TIMESTAMP)
        LSET = datetime.utcfromtimestamp(int(LSET_TIMESTAMP)/1000).strftime('%Y-%m-%d %H:%M:%S')
        etl_outbreakgroup_dds(LSET,CURRENT_TIME)
    
    @task()
    def ETL_dim_city_dds(metadata):
        LSET_TIMESTAMP = metadata[0]["ModifiedDate"]
        print("time stamp", LSET_TIMESTAMP)
        LSET = datetime.utcfromtimestamp(int(LSET_TIMESTAMP)/1000).strftime('%Y-%m-%d %H:%M:%S')
        etl_city_dds(LSET,CURRENT_TIME)

    @task()
    def ETL_dim_phu_dds(metadata):
        LSET_TIMESTAMP = metadata[0]["ModifiedDate"]
        print("time stamp", LSET_TIMESTAMP)
        LSET = datetime.utcfromtimestamp(int(LSET_TIMESTAMP)/1000).strftime('%Y-%m-%d %H:%M:%S')
        etl_phu_dds(LSET,CURRENT_TIME)

    @task()
    def ETL_fact_case_acquisition_dds(metadata):
        LSET_TIMESTAMP = metadata[0]["ModifiedDate"]
        print("time stamp", LSET_TIMESTAMP)
        LSET = datetime.utcfromtimestamp(int(LSET_TIMESTAMP)/1000).strftime('%Y-%m-%d %H:%M:%S')
        etl_fact_case_acquisition_table(LSET,CURRENT_TIME)

    @task()
    def ETL_fact_out_break_dds(metadata):
        LSET_TIMESTAMP = metadata[0]["ModifiedDate"]
        print("time stamp", LSET_TIMESTAMP)
        LSET = datetime.utcfromtimestamp(int(LSET_TIMESTAMP)/1000).strftime('%Y-%m-%d %H:%M:%S')
        etl_fact_out_break_table(LSET,CURRENT_TIME)

    @task()
    def ETL_fact_statistic_vaccinate_dds(metadata):
        LSET_TIMESTAMP = metadata[0]["ModifiedDate"]
        print("time stamp", LSET_TIMESTAMP)
        LSET = datetime.utcfromtimestamp(int(LSET_TIMESTAMP)/1000).strftime('%Y-%m-%d %H:%M:%S')
        etl_fact_statistic_vaccinate_table(LSET,CURRENT_TIME)

    @task()
    def update_metadata_dds(tenbang):
        mssql_hook_metadata = MsSqlHook(mssql_conn_id="covid_metadata")
        mssql_hook_metadata.run(
            f"""
                UPDATE ddscovid
                SET ModifiedDate = '{CURRENT_TIME}'
                WHERE tablename = '{tenbang}';
            """
        )

    @task()
    def update_tableau():
        ...

    [ETL_casereport_stage(), ETL_casedetailscanada_stage(),\
    ETL_outbreak_stage(),ETL_phu_stage(),ETL_vaccinate_stage(),\
    ETL_phugroup_stage()] >> fake_task() \
    >> [ETL_phu_group_and_city_nds(),\
        ETL_outbreak_group_nds(), ETL_case_acquisition_group_caserp_nds(),\
        ETL_case_acquisition_group_case_details_nds(),ETL_age_group_nds()] \
    >> ETL_phu_nds() >> fake_task() >> [ETL_outbreakphu_nds(), ETL_vaccinate_nds(), ETL_case_report()]\
    >> fake_task() \
    >> [ETL_dim_date_dds(query_metadata_dds('Date')),\
        ETL_dim_agegroup_dds(query_metadata_dds('agegroup')),\
        ETL_dim_gender_dds(query_metadata_dds('gender')),\
        ETL_dim_phugroup_dds(query_metadata_dds('phugroup')),
        ETL_dim_outbreakgroup_dds(query_metadata_dds('outbreakgroup'))] \
    >> ETL_dim_city_dds(query_metadata_dds('city'))>> ETL_dim_phu_dds(query_metadata_dds('phu')) \
    >> fake_task() >> [ETL_fact_out_break_dds(query_metadata_dds('StatisticOutbreak')),\
                      ETL_fact_case_acquisition_dds(query_metadata_dds('StatisticCaseAcquisition')),
                      ETL_fact_statistic_vaccinate_dds(query_metadata_dds('StatisticVaccinate'))]\
    >> fake_task() >> [update_metadata_dds('date'),\
    update_metadata_dds('agegroup'),\
    update_metadata_dds('gender'),\
    update_metadata_dds('city'),\
    update_metadata_dds('phu'),\
    update_metadata_dds('phugroup'),\
    update_metadata_dds('outbreakgroup'),\
    update_metadata_dds('StatisticCaseAcquisition'),\
    update_metadata_dds('StatisticOutbreak'),update_metadata_dds('StatisticVaccinate')]
    
covid_19_etl()