from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CopyDataOperator(BaseOperator):
    
    ''' 
    Operator to copy the staged immigration or temperatures data into Redshift tables.
    
    - Inputs: 
        * redshift_conn_id: connection id defined from Airflow's UI
        * iam_role: IAM role defined in order to copy the data from S3 to Redshift
        * immigration_data: True if loading immigration data, in which case the input file will be defined based on the execution date
        * copy_satement: Copy statement used to load the data into Redshift
        * input_s3_bucket: Bucket containing the data to be copied into Redshift
        * input_s3_key: Path to the data, which should contain the files in the format produced by the staging operators
        
    - Output: Updated fact table in Redshift, populated with the corresponding data
    '''

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id   = "",
                 iam_role           = "",
                 immigration_data   = False,
                 copy_statement     = "",
                 input_s3_bucket    = "",
                 input_s3_key       = "",
                 *args, **kwargs):
        
        super(CopyDataOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.iam_role         = iam_role
        self.immigration_data = immigration_data
        self.copy_statement   = copy_statement
        self.input_s3_bucket  = input_s3_bucket
        self.input_s3_key     = input_s3_key
        
    def execute(self, context):
        
        self.log.info('Initializing connections')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info('Retrieving name of the file to stage')
        if self.immigration_data:
            year, month, day = context['ds'].split('-')
            month_alphanum = {'01': 'jan', '02': 'feb', '03': 'mar',
                              '04': 'apr', '05': 'may', '06': 'jun',
                              '07': 'jul', '08': 'aug', '09': 'sep',
                              '10': 'oct', '11': 'nov', '12': 'dec'}[month]
            path_to_file = f"s3://{self.input_s3_bucket}/{self.input_s3_key}/i94_{month_alphanum}{year[2:]}_sub.parquet"
        else:
            path_to_file = f"s3://{self.input_s3_bucket}/{self.input_s3_key}/cleanTemperatureData.csv"
                            
            
        self.log.info("Inserting records")
        formatted_sql = self.copy_statement.format(
            path_to_file,
            self.iam_role)
        redshift.run(formatted_sql)
