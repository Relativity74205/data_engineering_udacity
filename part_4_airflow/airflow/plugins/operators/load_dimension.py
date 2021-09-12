from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    truncate_sql = "TRUNCATE TABLE {}"
    insert_sql = "INSERT INTO {} {}"

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str,
                 truncate_table: bool,
                 table: str,
                 insert_query: str,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.truncate_table = truncate_table
        self.table = table
        self.insert_sql = insert_query

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(self.table)

        if self.truncate_table is True:
            truncate_sql = LoadDimensionOperator.truncate_sql.format(self.table)
            redshift.run(truncate_sql)
            self.log.info(f"{self.table} truncated.")

        insert_sql = LoadDimensionOperator.insert_sql.format(self.table, self.insert_sql)
        redshift.run(insert_sql)
        self.log.info(f"{self.table} inserted into.")
