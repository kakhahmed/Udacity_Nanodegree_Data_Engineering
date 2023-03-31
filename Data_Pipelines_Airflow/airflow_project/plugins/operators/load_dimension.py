from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    insert_table = (
        """
        INSERT INTO {}
        {};
        """
    )
    
    truncate_table = (
    """
        TRUNCATE TABLE {}
    """
    )
    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="",
        table="",
        sql="",
        truncate_table=False,
        *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.truncate_table = truncate_table
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.truncate_table:
            self.log.info(f"Truncating dimension table: {self.table}")
            redshift.run(LoadDimensionOperator.truncate_table.format(self.table))

        formatted_sql = LoadDimensionOperator.insert_table.format(
            self.table, self.sql
        )
        self.log.info(f"Executing query: {formatted_sql}")
        redshift.run(formatted_sql)
