from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    Loads data in Dimension tables from staging tables
    
    :param redshift_conn_id: Redshift Connection Id
    :param test_query: SQL Query to run for the test case
    :param expected_result: Expected return value for the SQL Query provided
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 test_query="",
                 expected_result="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.test_query = test_query
        self.expected_result = expected_result

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Running test")
        records = redshift_hook.get_records(self.test_query)
        if records[0][0] != self.expected_result:
            raise ValueError(f"""
                Data quality check failed. \
                {records[0][0]} does not equal {self.expected_result}
            """)
        else:
            self.log.info("Data quality check passed")