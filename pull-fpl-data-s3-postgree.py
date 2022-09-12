from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from includes import write_players_week_data_to_s3_bucket, write_general_data_to_s3_bucket
from includes import ply_info_s3_to_postgre, ply_weeks_s3_to_postgre, pull_last_ply_id, write_to_postgree
import logging


task_logger = logging.getLogger('airflow.task')

args = {
    'owner': 'Mylo',
    'start_date': datetime(year=2022, month=9, day=12, hour=12)
}

dag = DAG(
    dag_id='pull-fpl-data-s3-postgree_v9',
    default_args=args,
    schedule_interval='@hourly'
)


with dag:
    pull_data = PythonOperator(
        task_id='fpl_ply_data_s3',
        #python_callable=get_crypto,
        #python_callable=player_week_json_to_local,
        python_callable=write_players_week_data_to_s3_bucket,
        op_kwargs={
            'filename':'/Users/mylo/Documents/data.csv',
            'key':'data.csv',
            'bucket_name':'mylo'
        }
        # provide_context=True
    )
    get_id_data = PythonOperator(
        task_id='fpl_ply_get_id',
        python_callable=pull_last_ply_id
    )

    pull_gen_data = PythonOperator(
        task_id='fpl_general_data_s3',
        python_callable=write_general_data_to_s3_bucket
    )

    insert_gen_data_postgree = PythonOperator(
        task_id='fpl_general_data_postgre',
        python_callable=ply_info_s3_to_postgre
    )

    insert_week_data_postgree = PythonOperator(
        task_id='fpl_week_data_postgre',
        python_callable=ply_weeks_s3_to_postgre
    )

    create_db_schema_tables = PythonOperator(
        task_id='create_db',
        python_callable=write_to_postgree
    )


create_db_schema_tables >> pull_gen_data >> get_id_data >> pull_data >> insert_gen_data_postgree >> insert_week_data_postgree
