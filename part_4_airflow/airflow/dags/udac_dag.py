from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries


default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False,
    'depends_on_past': False,
}

dag = DAG(
    'udac_dag',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *',
)

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table='public.staging_events',
    redshift_conn_id='redshift_conn_id',
    s3_bucket='udacity-dend',
    s3_key='log_data/2018/11/2018-11-0',
    region='us-west-2',
    iam_role='arn:aws:iam::598463720578:role/myRedshiftRole'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table='public.staging_songs',
    redshift_conn_id='redshift_conn_id',
    s3_bucket='udacity-dend',
    s3_key='song_data/A/A',
    region='us-west-2',
    iam_role='arn:aws:iam::598463720578:role/myRedshiftRole'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift_conn_id',
    table='public.songplays',
    insert_query=SqlQueries.songplay_table_insert,
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id='redshift_conn_id',
    table='public.users',
    insert_query=SqlQueries.user_table_insert,
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id='redshift_conn_id',
    table='public.songs',
    insert_query=SqlQueries.song_table_insert,
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id='redshift_conn_id',
    table='public.artists',
    insert_query=SqlQueries.artist_table_insert,
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift_conn_id',
    table='public.time',
    insert_query=SqlQueries.time_table_insert,
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> (stage_events_to_redshift, stage_songs_to_redshift) >> load_songplays_table
load_songplays_table >> (load_song_dimension_table, load_user_dimension_table, load_artist_dimension_table, load_time_dimension_table) >> run_quality_checks
run_quality_checks >> end_operator
