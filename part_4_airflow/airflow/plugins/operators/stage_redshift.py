from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    template_fields = ("s3_key",)
    copy_sql = """
        COPY :table
        FROM 's3://:s3_bucket/:s3_key'
        ACCESS_KEY_ID ':aws_access_key_id'
        SECRET_ACCESS_KEY ':aws_secret_access_key'
        JSON 'auto'
    """

    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        redshift.run("TRUNCATE TABLE :table", parameters={'table': self.table})

        params = {
            'table': self.table,
            's3_bucket': self.s3_bucket,
            's3_key': self.s3_key,
            'aws_access_key_id': credentials.access_key,
            'aws_secret_access_key': credentials.secret_key
        }

        redshift.run(StageToRedshiftOperator.copy_sql, parameters=params)





