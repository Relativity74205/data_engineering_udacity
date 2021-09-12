from typing import List, Union

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str,
                 test_queries: List[str],
                 results: List[Union[str, int]],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.test_queries = test_queries
        self.results = results

    def execute(self, context):
        if len(self.test_queries) != len(self.results):
            raise ValueError("Amount of test_queries and amount of results does not match.")

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for test_query, result in zip(self.test_queries, self.results):
            test_result = redshift.get_records(test_query)
            self.log.info(f"Test query {test_query} executed.")
            assert test_result[0][0] == result
            self.log.info(f"Result of query {test_query} is as expected.")
