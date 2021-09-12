from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str,
                 table: str,
                 truncate_table: bool,
                 insert_query: str,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table,
        self.truncate_table = truncate_table,
        self.insert_query = insert_query

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.truncate_table:
            redshift.run(f"TRUNCATE TABLE {self.table}")
            self.log.info(f"{self.table} truncated.")

        redshift.run(f"INSERT INTO {self.table} {self.insert_query}")
        self.log.info(f"{self.table} inserted into.")
