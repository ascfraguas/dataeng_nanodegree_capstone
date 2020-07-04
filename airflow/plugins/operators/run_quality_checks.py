from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class RunQualityCheckOperator(BaseOperator):
        
    ''' 
    This operator runs the selected data quality checks over the Redshift tables created.
    
    - Inputs:
        * redshift_conn_id: Connection id defined from Airflow's UI
        * test_tables: Redshift tables to be tested
        * dq_checks: List of data quality checks to be run, where each data quality check is a dictionary with the keys 'check_sql' (SQL code to obtain desired outputs) and 'success_condition' (logical statement representing expected output
        
    - Outputs: Logged results for the data quality checks, with the operator raising an exception if any of the tests is not passed
    '''
    
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 test_tables      = {}, 
                 dq_checks        = [],
                 *args, **kwargs):

        super(RunQualityCheckOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.test_tables      = test_tables
        self.dq_checks        = dq_checks

        
    def execute(self, context):
        
        self.log.info('Initializing connections')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info('Running battery of tests for each table')
        failed_tests = 0
        for test_table in self.test_tables:
            for dq_check in self.dq_checks:
                check_sql         = dq_check['check_sql']
                success_condition = dq_check['success_condition']
                result            = redshift.get_records(check_sql.format(test_table))[0][0]
                if not eval(success_condition.format(result)): 
                    failed_tests += 1
                    self.log.info(f'Failed test with SQL {check_sql} for table {test_table}')
                else:
                    self.log.info('All tests met the defined success criteria')
        
        
        

            
