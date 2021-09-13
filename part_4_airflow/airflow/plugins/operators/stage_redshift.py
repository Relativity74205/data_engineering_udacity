from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        JSON '{}'
    """
    truncate_sql = "TRUNCATE TABLE {}"

    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str,
                 aws_credentials_id: str,
                 table: str,
                 s3_bucket: str,
                 s3_key: str,
                 region: str,
                 json_path: str = None,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.json_path = json_path

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        truncate_sql = StageToRedshiftOperator.truncate_sql.format(self.table)
        redshift.run(truncate_sql)
        self.log.info(f"{self.table} truncated.")

        s3_full_path = f's3://{self.s3_bucket}/{self.s3_key}'
        if self.json_path:
            s3_json_string = f's3://{self.s3_bucket}/{self.json_path}'
        else:
            s3_json_string = 'auto'
        copy_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_full_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            s3_json_string
        )

        redshift.run(copy_sql)
