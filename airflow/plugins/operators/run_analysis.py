from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class RunAnalysisOperator(BaseOperator):
        
    ''' 
    Operator to run the different analyses to be persisted in independent Redshift tables.
    
    - Inputs:
        * redshift_conn_id: Connection id defined from Airflow's UI
        * sql_statement: Statement to generate the desired analysis. This should be a formatted string allowing four arguments as shown below, for correct output versioning
        
    - Outputs: Redshift table containing the results of the specific analysis run
    '''
    
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 sql_statement    = "",
                 *args, 
                 **kwargs):

        super(RunAnalysisOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id  = redshift_conn_id
        self.sql_statement     = sql_statement

    def execute(self, context):
        
        self.log.info('Initializing connections')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        year, month, day = context['ds'].split('-')
        month_alphanum = {'01': 'jan', '02': 'feb', '03': 'mar',
                          '04': 'apr', '05': 'may', '06': 'jun',
                          '07': 'jul', '08': 'aug', '09': 'sep',
                          '10': 'oct', '11': 'nov', '12': 'dec'}[month]
        self.log.info(f"Execution prefix for outputs: {month_alphanum}{int(year)}")
        
        self.log.info(f"Generating output summary")
        formatted_sql = self.sql_statement.format(
            month_alphanum,
            int(year),
            int(month),
            int(year))
        redshift.run(formatted_sql)
        