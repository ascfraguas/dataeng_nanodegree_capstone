from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
import pandas as pd
import pyarrow.parquet as pq
import s3fs
import numpy as np


class StageImmigrationDataOperator(BaseOperator):
        
    ''' 
    Operator to stage the monthly immigration data into the selected s3 path, previous application of the defined preprocessing steps. 
    
    - Inputs: 
        * aws_credentials_id: AWS credentials passed from Airflow's UI
        * input_s3_bucket: Bucket containing the raw data to be staged
        * input_s3_key: Path to the raw data, where input files while have the naming convention "i94_{month_alphanum}{year[2:]}_sub.parquet", as defined by Airflow's {ds} execution variable
        * output_s3_bucket: Bucket where the staging data will be stored
        * output_s3_key: Path to the staged output data
        
    - Outputs: Parquet file with the monthly data corresponding to the selected execution, where file created will follow naming convention "i94_{month_alphanum}{year[2:]}_sub.parquet", as defined by Airflow's {ds} execution variable
    '''
    
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 aws_credentials_id  = "",
                 input_s3_bucket     = "",
                 input_s3_key        = "",
                 output_s3_bucket    = "",
                 output_s3_key       = "",
                 *args, 
                 **kwargs):

        super(StageImmigrationDataOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id  = aws_credentials_id
        self.input_s3_bucket     = input_s3_bucket
        self.input_s3_key        = input_s3_key
        self.output_s3_bucket    = output_s3_bucket
        self.output_s3_key       = output_s3_key

    def execute(self, context):
        
        self.log.info("Initializing connections")
        aws_hook = AwsHook(self.aws_credentials_id)
        s3_hook  = S3Hook (self.aws_credentials_id)
        year, month, day = context['ds'].split('-')
        month_alphanum = {'01': 'jan', '02': 'feb', '03': 'mar',
                          '04': 'apr', '05': 'may', '06': 'jun',
                          '07': 'jul', '08': 'aug', '09': 'sep',
                          '10': 'oct', '11': 'nov', '12': 'dec'}[month]
        path_to_file = f"{self.input_s3_bucket}/{self.input_s3_key}/i94_{month_alphanum}{year[2:]}_sub.parquet"
        
        self.log.info(f"Loading to memory the file corresponding to the execution date: {path_to_file}")
        fs = s3fs.S3FileSystem(anon   = False, 
                               key    = aws_hook.get_credentials().access_key, 
                               secret = aws_hook.get_credentials().secret_key)
        data = pq.ParquetDataset(path_to_file, filesystem = fs).read().to_pandas()
        
        self.log.info("Recasting data types")
        data.i94yr = data.i94yr.astype(int)
        data.i94mon = data.i94mon.astype(int)
        
        data.i94cit = data.i94cit.astype(str)
        data.i94res = data.i94res.astype(str)
        data.i94visa = data.i94visa.astype(str)
        data.i94mode = data.i94mode.astype(str)
        
        data.i94bir = data.i94bir.astype(float)

        self.log.info("Cleaning invalid admnum records")
        data = data[data['admnum']!=0]
        data = data[data.duplicated(subset=['admnum'])==False]
        data.admnum = data.admnum.astype(int)

        self.log.info("Ensuring correctness for the data level")
        assert data.i94yr.nunique()==1
        assert data.i94mon.nunique()==1

        self.log.info("Cleaning inconsistent age and gender info")
        data.i94bir = data.i94bir.apply(lambda x: x if x>=0 else np.nan)
        data.gender = data.gender.apply(lambda x: x if x in ['M', 'F', 'X', 'U'] else np.nan)

        self.log.info("Transformation of date formats and ensuring the extraction segment is consistent with arrival dates")
        data.arrdate = pd.to_timedelta(data.arrdate, unit='D') + pd.Timestamp('1960-1-1')
        data['arrival_day']   = data.arrdate.apply(lambda x: x.day)
        data['arrival_month'] = data.arrdate.apply(lambda x: x.month)
        data['arrival_year']  = data.arrdate.apply(lambda x: x.year)
        assert (data['arrival_month'] != data['i94mon']).sum()==0
        assert (data['arrival_year']  != data['i94yr']) .sum()==0

        self.log.info("Transformations to departure dates")
        data.depdate = pd.to_timedelta(data.depdate, unit='D') + pd.Timestamp('1960-1-1')
        data['departure_day']   = data.depdate.apply(lambda x: x.day)
        data['departure_month'] = data.depdate.apply(lambda x: x.month)
        data['departure_year']  = data.depdate.apply(lambda x: x.year)

        self.log.info("Computing length of stays")
        data['length_of_stay'] = (data.depdate - data.arrdate).apply(lambda x: x.days)
        
        self.log.info("Recasting date values and length of stay")
        for column in ['arrival_day', 'arrival_month', 'arrival_year',
                       'departure_day', 'departure_month', 'departure_year',
                       'length_of_stay']:
            data[column] = data[column].fillna(-9999).astype(int)

        self.log.info("Copying file to staging path")
        data = data[['admnum',
                     'i94bir', 'gender', 'i94visa',
                     'i94cit', 'i94res', 'i94addr', 
                     'i94mode',
                     'arrival_day',   'arrival_month',   'arrival_year', 
                     'departure_day', 'departure_month', 'departure_year', 'length_of_stay']]

        data.to_parquet(f"i94_{month_alphanum}{year[2:]}_sub.parquet", index=False)
        s3_hook.load_file(filename    = f"i94_{month_alphanum}{year[2:]}_sub.parquet",
                          key         = f"{self.output_s3_key}/i94_{month_alphanum}{year[2:]}_sub.parquet",
                          bucket_name = self.output_s3_bucket,
                          replace     = True)
                      
        
                      
        
        

            
