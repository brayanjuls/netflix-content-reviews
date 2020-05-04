from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 conn_id="",
                 sql_test_cases={},
                 *args, **kwargs):
        """ 
            Constructor method where the parameters are initialized.
            params:
                conn_id = represent the identifier of the connector to the database.
                
                sql_test_cases = requesent the queries aimed to check the quality of the data
                                and the expected result of the queries.
        """

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id=conn_id
        self.sql_test_cases=sql_test_cases


    def execute(self, context):
        """ 
            This method check the quality of the data given the input test cases indicated in the sql_test_cases
            dictionary.
        """
        bigquery=BigQueryHook(bigquery_conn_id=self.conn_id)
        found_errors=[]
        for query, expected_result in self.sql_test_cases.items():
            records = bigquery.run_query(sql=query)
            if len(records) < 1 or records[0][0] != expected_result:
                found_errors.append(query)
        
        if len(found_errors) > 0:
            raise ValueError(f"The following query test cases were not successful {found_errors}")
        
        self.log.info('DataQualityOperator has been executed')