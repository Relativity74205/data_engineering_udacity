from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    copy_sql = """
        COPY {}
        FROM 's3://{}/{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        JSON 'auto'
    """
    truncate_sql = "TRUNCATE TABLE {}"

    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 region="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        truncate_sql = StageToRedshiftOperator.truncate_sql.format(self.table)
        redshift.run(truncate_sql)
        self.log.info(f"{self.table} truncated.")

        copy_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            self.s3_bucket,
            self.s3_key,
            credentials.access_key,
            credentials.secret_key,
            self.region
        )

        redshift.run(copy_sql)
