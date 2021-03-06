from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    insert_sql = "INSERT INTO {} {}"

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str,
                 table: str,
                 insert_query: str,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.insert_query = insert_query

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        insert_sql = LoadFactOperator.insert_sql.format(self.table, self.insert_query)
        redshift.run(insert_sql)
        self.log.info(f"{self.table} inserted into.")
