from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
import pandas as pd

class StageImmigrationDimensionsOperator(BaseOperator):
        
    ''' 
    Operator to stage the immigration dimensions. The mapping dictionaries representing these dimensions should be coded into /airflow/plugins/helpers/immigration_dimensions.py
    
    - Inputs:
        * aws_credentials_id: AWS credentials passed from Airflow's UI
        * dimensions: List mapping table names to the dimension mappings defined at/airflow/plugins/helpers/immigration_dimensions.py
        * output_s3_bucket: Bucket where the staging data will be stored
        * output_s3_key: Path to the staged output data within the selected bucket
        
    - Outputs: CSV file representing the staging dimensions, copied into the selected path under the naming convention {table_name}.csv
    '''
    
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 aws_credentials_id  = "",
                 dimensions          = [],
                 output_s3_bucket    = "",
                 output_s3_key       = "",
                 *args, 
                 **kwargs):

        super(StageImmigrationDimensionsOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id  = aws_credentials_id
        self.dimensions          = dimensions
        self.output_s3_bucket    = output_s3_bucket
        self.output_s3_key       = output_s3_key

    def execute(self, context):
        
        self.log.info("Initializing connections")
        s3_hook  = S3Hook (self.aws_credentials_id)
        
        for dimension in self.dimensions:
            
            table_name = dimension['table_name']
            records    = dimension['records']
            
            self.log.info(f"Staging the {table_name} table")
            pd.DataFrame([[x,y] for x,y in zip(records.keys(), records.values())],
                         columns = ['code', 'name']).to_csv(f'{table_name}.csv', sep=';', index=False)
            s3_hook.load_file(filename    = f'{table_name}.csv',
                              key         = f'{self.output_s3_key}/{table_name}.csv',
                              bucket_name = self.output_s3_bucket,
                              replace     = True)
            