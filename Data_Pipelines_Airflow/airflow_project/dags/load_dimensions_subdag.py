# Run all load dimensions tasks in a sub dag.


from airflow import DAG
from airflow.operators import LoadDimensionOperator
from airflow.operators.dummy_operator import DummyOperator
from helpers.sql_queries import SqlQueries


def load_dimensions_dag(
        parent_dag_name,
        task_id,
        redshift_conn_id,
        truncate_table,
        start_date,
        *args, **kwargs):
    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        schedule_interval=None,
        start_date=start_date
    )
    start_subdag = DummyOperator(task_id='Begin_subdag',  dag=dag)

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        dag=dag,
        redshift_conn_id=redshift_conn_id,
        table="users",
        sql=SqlQueries.user_table_insert,
        truncate_table=truncate_table,
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        dag=dag,
        redshift_conn_id=redshift_conn_id,
        table="songs",
        sql=SqlQueries.song_table_insert,
        truncate_table=truncate_table,
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        dag=dag,
        redshift_conn_id=redshift_conn_id,
        table="artists",
        sql=SqlQueries.artist_table_insert,
        truncate_table=truncate_table,
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        dag=dag,
        redshift_conn_id=redshift_conn_id,
        table="time",
        sql=SqlQueries.time_table_insert,
        truncate_table=truncate_table,
    )
    end_subdag = DummyOperator(task_id='end_subdag', dag=dag)

    start_subdag >> [
        load_user_dimension_table,
        load_song_dimension_table,
        load_artist_dimension_table,
        load_time_dimension_table,
    ] >> end_subdag

    return dag
