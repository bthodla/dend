from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(
                self,
                s3_bucket,
                s3_prefix,
                table,
                redshift_conn_id='redshift',
                aws_credentials='aws_credentials',
                copy_options='',
                *args, 
                **kwargs
                ):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials = aws_credentials
        self.copy_options = copy_options


    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials)
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(self.redshift_conn_id)

        self.log.info(f'Getting ready to load data from raw files in {self.s3_bucket}/{self.s3_prefix} to staging table {self.table}...')

        copy_query = """
                    copy {table}
                    from 's3://{s3_bucket}/{s3_prefix}'
                    with credentials
                    'aws_access_key_id={access_key};aws_secret_access_key={secret_key}'
                    {copy_options};
               """.format(
                        table=self.table,
                        s3_bucket=self.s3_bucket,
                        s3_prefix=self.s3_prefix,
                        access_key=credentials.access_key,
                        secret_key=credentials.secret_key,
                        copy_options=self.copy_options
                        )
        self.log.info('About to execute copy command...')
        redshift_hook.run(copy_query)
        self.log.info('Copy command completed')
