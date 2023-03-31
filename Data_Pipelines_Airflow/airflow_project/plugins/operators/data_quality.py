from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="",
        quality_checks=[],  # A lits of tuples with sql and expected results
        *args, **kwargs
    ):

        super(DataQualityOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.quality_checks = quality_checks

    def execute(self, context):
        failed_tests = []
        error_count = 0
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Run data quality checks.")
        for check in self.quality_checks:
            check_sql = check.get('check_sql')
            expected = check.get('expected_result')
            self.log.info(f"Quality check: {check_sql}")

            records = redshift.get_records(check_sql)[0]
            self.log.info(f"expected: {expected}, records: {records[0]}")

            if expected != records[0]:
                error_count += 1
                failed_tests.append(check_sql)

        if error_count > 0:
            self.log.error('Failed data quality check!')
            raise ValueError('Failed data quality check!')
