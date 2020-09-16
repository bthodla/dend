from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(
                self,
                table,
                redshift_conn_id='redshift',
                select_sql='',
                mode='append',
                *args, 
                **kwargs
                ):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.select_sql = select_sql
        self.mode = mode

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)

        if self.mode == 'truncate':
            self.log.info(f'Deleting any existing data from {self.table} dimension table...')
            redshift_hook.run(f'delete from {self.table};')
            self.log.info(f'Deleting data from {self.table} dimension table completed.')

        sql = """
            insert into {table}
            {select_sql};
        """.format(table=self.table, select_sql=self.select_sql)

        self.log.info(f'Loading data into {self.table} dimension table...')
        redshift_hook.run(sql)
        self.log.info(f'Loading data into {self.table} dimension table completed.')
