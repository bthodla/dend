from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(
                self,
                redshift_conn_id="",
                table_name="",
                *args, 
                **kwargs
                ):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name

    def execute(self):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        result = redshift_hook.get_records(f'select count(*) from {self.table_name}')

        if len(result) < 1 or len(result[0]) < 1:
            raise ValueError(f'Data quality check failed. {self.table_name} has no data')
        rec_count = results[0][0]

        if rec_count < 1:
            raise ValueError(f'Data quality check failed. {self.table_name} contains 0 rows')

        self.log.info(f'Data quality check on table {self.table_name} passed with {result[0][0]} rows')
