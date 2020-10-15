from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    Loads data in Fact table from staging tables
    
    :param redshift_conn_id: Redshift Connection Id
    :param table: Target table to load data in Redshift cluster
    :param select_query: SQL query to get the select query for Fact table load
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 select_query="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.select_query = select_query

    def execute(self, context):
        self.log.info('Begin Loading Fact table')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        fact_insert_sql = f"""
            INSERT INTO {self.table}
            {self.select_query}
        """
        
        redshift.run(fact_insert_sql)
        self.log.info('Fact table Loaded')
