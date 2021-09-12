from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    copy_sql = """
        COPY {}
        FROM 's3://{}/{}'
        CREDENTIALS 'aws_iam_role={}'
        REGION '{}'
        JSON 'auto'
    """
    truncate_sql = "TRUNCATE TABLE {}"

    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 region="",
                 iam_role="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.iam_role = iam_role

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        truncate_sql = StageToRedshiftOperator.truncate_sql.format(self.table)
        redshift.run(truncate_sql)
        self.log.info(f"{self.table} truncated.")

        copy_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            self.s3_bucket,
            self.s3_key,
            self.iam_role,
            self.region
        )

        redshift.run(copy_sql)
