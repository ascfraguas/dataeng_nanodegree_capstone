from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageTemperatureDataOperator(BaseOperator):
        
    ''' 
    Operator to stage the temperature data into the staging path selected.
    
    - Inputs:
        * aws_credentials_id: AWS credentials passed from Airflow's UI
        * input_s3_bucket: Bucket containing the raw data to be staged
        * input_s3_key: Path to the raw data, which should contain the file "GlobalLandTemperaturesByCity.csv" applicable to the execution
        * output_s3_bucket: Bucket where the staging data will be stored
        * output_s3_key: Path to the staged output data
        
    - Outputs: CSV file representing the temperatures data, which will be stored under the name "cleanTemperatureData.csv"
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

        super(StageTemperatureDataOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id  = aws_credentials_id
        self.input_s3_bucket     = input_s3_bucket
        self.input_s3_key        = input_s3_key
        self.output_s3_bucket    = output_s3_bucket
        self.output_s3_key       = output_s3_key

    def execute(self, context):
        
        self.log.info("Initializing connections")
        s3_hook  = S3Hook (self.aws_credentials_id)
        path_to_file = f"{self.input_s3_bucket}/{self.input_s3_key}/GlobalLandTemperaturesByCity.csv"
        
        try:
            s3_hook.delete_objects(bucket = self.output_s3_bucket,
                                   keys   = f"{self.output_s3_key}/cleanTemperatureData.csv")
            self.log.info("File currently exists in staging area. Removing previous version")
        except:
            pass
        
        s3_hook.copy_object(source_bucket_key  = f"{self.input_s3_key}/GlobalLandTemperaturesByCity.csv",
                            dest_bucket_key    = f"{self.output_s3_key}/cleanTemperatureData.csv",
                            source_bucket_name = self.input_s3_bucket,
                            dest_bucket_name   = self.output_s3_bucket)
        