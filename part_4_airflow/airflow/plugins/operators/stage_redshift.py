from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM 's3://{}/{}'
        CREDENTIALS 'aws_iam_role={}'
        REGION '{}'
        JSON 'auto'
    """
    # ACCESS_KEY_ID ':aws_access_key_id'
    # SECRET_ACCESS_KEY ':aws_secret_access_key'

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

        redshift.run(f"TRUNCATE TABLE {self.table}")
        self.log.info(f"{self.table} truncated.")

        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            self.s3_bucket,
            self.s3_key,
            self.iam_role,
            self.region
        )

        redshift.run(formatted_sql)
