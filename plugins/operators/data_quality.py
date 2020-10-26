from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables=[],
                 *args, **kwargs):

        """
        Initiation of DataQualityOperator

        Arguments: 
            redshift_conn_id   --  Redshift connection Id
            tables             --  Tables that its data need to be verified
            
        Returns: 
            None
        """
        
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        
    def execute(self, context):
        """
        Initiation of DataQualityOperator

        Arguments: 
            redshift_conn_id   --  Redshift connection Id
            table       s      --  Tables that its data need to be verified
            
        Returns: 
            None
        """
        
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info('Check if table contains data')
        for table in self.tables:
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1 or records[0][0] < 1:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
            logging.info(f"Data quality on table {table} check passed with {records[0][0]} records")