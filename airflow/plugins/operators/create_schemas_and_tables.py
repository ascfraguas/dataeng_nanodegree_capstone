from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class SchemaAndTableCreationOperator(BaseOperator):
        
    ''' 
    This operator creates the schemas and tables used in this ETL, if they don't currently exist.
    
    - Inputs:
        * redshift_conn_id: connection id defined from Airflow's UI
        * create_schemas_sql: SQL statement used to create the schemas in the data model
        * create_tables_sql: SQL statement used to create the tables in the data model
        
    - Outputs: Schemas and tables created in the Redshift cluster
    '''
    
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id   = "",
                 create_schemas_sql = "",
                 create_tables_sql  = "",
                 *args, 
                 **kwargs):

        super(SchemaAndTableCreationOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id   = redshift_conn_id
        self.create_schemas_sql = create_schemas_sql
        self.create_tables_sql  = create_tables_sql
        
    def execute(self, context):
        
        self.log.info("Initializing connections")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Creating schemas if currently not existing")
        redshift.run(self.create_schemas_sql)
        
        self.log.info("Creating tables if currently not existing")
        redshift.run(self.create_tables_sql)
        
