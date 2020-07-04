from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CopyDimensionsOperator(BaseOperator):
    
    ''' 
    Operator to copy into Redshift the staged dimensions data for the immigration schema.
    
    - Inputs:
        * redshift_conn_id: connection id defined from Airflow's UI
        * iam_role: IAM role defined in order to copy the data from S3 to Redshift
        * dimensions: List of tables to be copied into Redshift
        * truncate: Truncate the destination tables in Redshift if True
        * input_s3_bucket: Bucket containing the staged data to be uploaded
        * input_s3_key: Path to the data, which should contain the files in the format produced by the staging operators
        
    - Outputs: Populated dimensions tables in Redshift
        
    '''

    ui_color = '#F98866'
    
    base_copy_statement = """
        COPY immigration.{} FROM '{}' IGNOREHEADER AS 1 DELIMITER ';' IAM_ROLE '{}';
        COMMIT;
        """
    
    truncate_statement = """
        TRUNCATE TABLE immigration.{};
        """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 iam_role         = "",
                 dimensions       = [],
                 truncate         = True,
                 input_s3_bucket  = "",
                 input_s3_key     = "",
                 *args, **kwargs):
        
        super(CopyDimensionsOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.iam_role         = iam_role
        self.dimensions       = dimensions
        self.truncate         = truncate
        self.input_s3_bucket  = input_s3_bucket
        self.input_s3_key     = input_s3_key
        
    def execute(self, context):
        
        self.log.info('Initializing connections')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for dimension in self.dimensions:
            
            path_to_file = f"s3://{self.input_s3_bucket}/{self.input_s3_key}/{dimension}.csv"
            
            if self.truncate:
                self.log.info(f"Truncating {dimension}")
                CopyDimensionsOperator.truncate_statement.format(dimension)
                
            self.log.info(f"Copying into {dimension}")
            formatted_sql = CopyDimensionsOperator.base_copy_statement.format(
                dimension,
                path_to_file,
                self.iam_role)
            redshift.run(formatted_sql)
