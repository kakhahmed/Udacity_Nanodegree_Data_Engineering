from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators import (DataQualityOperator, LoadFactOperator,
                               PostgresOperator, StageToRedshiftOperator)
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from helpers.sql_queries import SqlQueries
from load_dimensions_subdag import load_dimensions_dag

start_date = datetime(2019, 1, 12)
default_args = {
    'owner': 'udacity',
    'start_date': start_date,
    'retries': 5,
    'retry_delay': timedelta(minutes=2),
    'catchup': False,
    'depends_on_past': False,
    'email_on_retry': False,
}

dag = DAG('sparkify_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_all_tables = PostgresOperator(
    task_id="create_all_tables",
    dag=dag,
    postgres_conn_id="redshift",
    sql="create_tables.sql"
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    table="staging_events",
    json="s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    table="staging_songs",
    json="auto"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
    sql=SqlQueries.songplay_table_insert,
)

trips_task_id = "load_dimensions_subdag"
load_dimensions_subdag = SubDagOperator(
    subdag=load_dimensions_dag(
        "sparkify_dag",
        trips_task_id,
        redshift_conn_id="redshift",
        start_date=start_date,
    ),
    task_id=trips_task_id,
    dag=dag,
)

# load_user_dimension_table = LoadDimensionOperator(
#     task_id='Load_user_dim_table',
#     dag=dag,
#     redshift_conn_id="redshift",
#     table="user",
#     sql=SqlQueries.user_table_insert,
#     truncate_table=False,
# )

# load_song_dimension_table = LoadDimensionOperator(
#     task_id='Load_song_dim_table',
#     dag=dag,
#     redshift_conn_id="redshift",
#     table="song",
#     sql=SqlQueries.song_table_insert,
#     truncate_table=False,
# )

# load_artist_dimension_table = LoadDimensionOperator(
#     task_id='Load_artist_dim_table',
#     dag=dag,
#     redshift_conn_id="redshift",
#     table="artist",
#     sql=SqlQueries.artist_table_insert,
#     truncate_table=False,
# )

# load_time_dimension_table = LoadDimensionOperator(
#     task_id='Load_time_dim_table',
#     dag=dag,
#     redshift_conn_id="redshift",
#     table="time",
#     sql=SqlQueries.time_table_insert,
#     truncate_table=False,
# )

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    quality_checks=[  # inspired by answer for question 54406 - https://knowledge.udacity.com/questions/54406
        {'check_sql': "SELECT COUNT(*) FROM users WHERE userid is null", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM songs WHERE songid is null", 'expected_result': 0}
    ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> create_all_tables
create_all_tables >> stage_events_to_redshift
create_all_tables >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_dimensions_subdag
load_dimensions_subdag >> run_quality_checks
run_quality_checks >> end_operator
# load_songplays_table >> load_song_dimension_table
# load_songplays_table >> load_user_dimension_table
# load_songplays_table >> load_artist_dimension_table
# load_songplays_table >> load_time_dimension_table
# load_song_dimension_table >> run_quality_checks
# load_user_dimension_table >> run_quality_checks
# load_artist_dimension_table >> run_quality_checks
# load_time_dimension_table >> run_quality_checks
# run_quality_checks >> end_operator
