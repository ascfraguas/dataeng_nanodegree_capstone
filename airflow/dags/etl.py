from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from helpers import SqlQueries, ImmigrationDimensions
from airflow.operators import (SchemaAndTableCreationOperator,
                               StageImmigrationDimensionsOperator,
                               StageImmigrationDataOperator,
                               StageTemperatureDataOperator,
                               CopyDimensionsOperator,
                               CopyDataOperator,
                               PostgresOperator,
                               RunQualityCheckOperator,
                               RunAnalysisOperator)


####################################
######## DAG CONFIGURATION #########
####################################

default_args = {'owner'          : 'ascfraguas',
                'depends_on_past': False,
                'retries'        : 3,
                'retry_delay'    : timedelta(minutes=5),
                'catchup'        : False,
                'email_on_retry' : False}

dag = DAG('immigration_and_temperature_reports_1',
          description       = 'Process immigration and temperature data to generate custom reports',
          default_args      = default_args,
          start_date        = datetime(2015, 12, 15),
          end_date          = datetime(2016, 12, 15),
          schedule_interval ='@monthly',
          max_active_runs   = 1)


####################################
########### DEFINE TASKS ###########
####################################


#### -------> DAG INITIALIZATION

start_operator = DummyOperator(
    task_id = 'Begin_execution',  
    dag     = dag)


#### -------> CREATION OF SCHEMA AND TABLES

create_schemas_and_tables = SchemaAndTableCreationOperator(
    task_id            = 'Create_schemas_and_tables',  
    dag                = dag,
    redshift_conn_id   = 'redshift',
    create_schemas_sql = SqlQueries.create_schemas_query,
    create_tables_sql  = SqlQueries.create_tables_query,
    )


#### -------> FILE STAGING

stage_immigration_dimensions  = StageImmigrationDimensionsOperator(
    task_id            = 'Stage_immigration_dimensions', 
    dag                = dag,
    aws_credentials_id = 'aws_credentials',
    dimensions         = [{'table_name': 'country_codes'      , 'records': ImmigrationDimensions.country_codes      },
                          {'table_name': 'port_codes'         , 'records': ImmigrationDimensions.port_codes         },
                          {'table_name': 'entry_channel_codes', 'records': ImmigrationDimensions.entry_channel_codes},
                          {'table_name': 'state_codes'        , 'records': ImmigrationDimensions.state_codes        },
                          {'table_name': 'trip_reason_codes'  , 'records': ImmigrationDimensions.trip_reason_codes  }],
    output_s3_bucket   = 'ascfraguas-udacity-deng-capstone',
    output_s3_key      = 'staging/immigration-dimensions')

stage_monthly_immigration_data  = StageImmigrationDataOperator(
    task_id            = 'Stage_monthly_immigration_data',  
    dag                = dag,
    aws_credentials_id = 'aws_credentials',
    input_s3_bucket    = "ascfraguas-udacity-deng-capstone",
    input_s3_key       = "raw/immigration-data",
    output_s3_bucket   = 'ascfraguas-udacity-deng-capstone',
    output_s3_key      = 'staging/immigration-data')

stage_temperatures_data  = StageTemperatureDataOperator(
    task_id            = 'Stage_temperatures_data',  
    dag                = dag,
    aws_credentials_id = 'aws_credentials',
    input_s3_bucket    = "ascfraguas-udacity-deng-capstone",
    input_s3_key       = "raw/temperatures-data",
    output_s3_bucket   = 'ascfraguas-udacity-deng-capstone',
    output_s3_key      = 'staging/temperatures-data')


#### -------> COPY TO REDSHIFT

copy_monthly_immigration_data  = CopyDataOperator(
    task_id            = 'Copy_monthly_immigration_data',  
    dag                = dag,
    redshift_conn_id   = 'redshift',
    iam_role           = Variable.get('iam_role'),
    immigration_data   = True,
    copy_statement     = SqlQueries.copy_immigration_data,
    input_s3_bucket    = 'ascfraguas-udacity-deng-capstone',
    input_s3_key       = 'staging/immigration-data')

copy_temperatures_data  = CopyDataOperator(
    task_id            = 'Copy_temperatures_data',  
    dag                = dag,
    redshift_conn_id   = 'redshift',
    iam_role           = Variable.get('iam_role'),
    immigration_data   = False,
    copy_statement     = SqlQueries.copy_temperature_data,
    input_s3_bucket    = 'ascfraguas-udacity-deng-capstone',
    input_s3_key       = 'staging/temperatures-data')

copy_immigration_dimensions  = CopyDimensionsOperator(
    task_id            = 'Copy_immigration_dimensions',  
    dag                = dag,
    redshift_conn_id   = 'redshift',
    iam_role           = Variable.get('iam_role'),
    dimensions         = ['country_codes', 'port_codes', 'entry_channel_codes', 'state_codes', 'trip_reason_codes'],
    truncate           = True,
    input_s3_bucket    = 'ascfraguas-udacity-deng-capstone',
    input_s3_key       = 'staging/immigration-dimensions')


#### -------> RUN TEMPERATURES SUMMARY

run_temperatures_sumary = PostgresOperator(
    task_id          = "Run_temperatures_summary",
    dag              = dag,
    postgres_conn_id = "redshift",
    sql              = SqlQueries.run_temps_summary)


#### -------> RUN DATA QUALITY CHECKS

run_table_quality_checks = RunQualityCheckOperator(
    task_id          = 'Run_data_quality_checks',
    dag              = dag,
    redshift_conn_id = 'redshift',
    test_tables      = {'immigration.us_entries',            'immigration.country_codes', 'immigration.port_codes',
                        'immigration.entry_channel_codes',   'immigration.state_codes',   'immigration.trip_reason_codes',
                        'temperature.full_temperature_data', 'temperature.temp_summary'},
    dq_checks        = [{'check_sql'        : "SELECT COUNT(*) FROM {}", 
                         'success_condition': "{}>0"}])


#### -------> RUN DATA QUALITY CHECKS

run_analysis_1  = RunAnalysisOperator(
    task_id          = 'Analyze_demographics_by_channel',  
    dag              = dag,
    redshift_conn_id = 'redshift',
    sql_statement    = SqlQueries.demographics_by_channel)

run_analysis_2  = RunAnalysisOperator(
    task_id          = 'Analyze_length_of_stay',  
    dag              = dag,
    redshift_conn_id = 'redshift',
    sql_statement    = SqlQueries.length_of_stay)

run_analysis_3  = RunAnalysisOperator(
    task_id          = 'Analyze_state_trip_reasons',  
    dag              = dag,
    redshift_conn_id = 'redshift',
    sql_statement    = SqlQueries.state_trip_reasons)

run_analysis_4  = RunAnalysisOperator(
    task_id          = 'Analyze_freqs_and_mean_temps',  
    dag              = dag,
    redshift_conn_id = 'redshift',
    sql_statement    = SqlQueries.freqs_and_mean_temps)


#### -------> EXIT THE DAG

end_operator = DummyOperator(
    task_id='Stop_execution',  
    dag=dag)


####################################
######## TASK DEPENDENCIES #########
####################################

start_operator                 >> create_schemas_and_tables
create_schemas_and_tables      >> [stage_monthly_immigration_data, stage_immigration_dimensions, stage_temperatures_data]
stage_monthly_immigration_data >> copy_monthly_immigration_data
stage_immigration_dimensions   >> copy_immigration_dimensions
stage_temperatures_data        >> copy_temperatures_data >> run_temperatures_sumary
[copy_monthly_immigration_data, 
 copy_immigration_dimensions, 
 run_temperatures_sumary]      >> run_table_quality_checks
run_table_quality_checks       >> [run_analysis_1, 
                                   run_analysis_2, 
                                   run_analysis_3, 
                                   run_analysis_4]
[run_analysis_1, 
 run_analysis_2, 
 run_analysis_3, 
 run_analysis_4]               >> end_operator

