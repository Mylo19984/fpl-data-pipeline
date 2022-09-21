from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from includes import write_players_week_data_to_s3_bucket, write_general_data_to_s3_bucket, team_info_s3_to_postgre
from includes import ply_info_s3_to_postgre, ply_weeks_s3_to_postgre, pull_last_ply_id, create_schema_tables_postgre
from includes import ply_stats_s3_to_postgre
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup


week_data_insert = Variable.get('week_data_insert')
ply_data_insert = Variable.get('gen_data_insert')
team_data_insert = Variable.get('team_data_insert')
ply_stat_data_insert = Variable.get('ply_stat_data_insert')

args = {
    'owner': 'Mylo',
    'start_date': datetime(year=2022, month=9, day=19, hour=12)
}

dag = DAG(
    dag_id='pull-fpl-data-postgree_v1',
    default_args=args,
    schedule_interval='@daily'
)


with dag:

    get_id_data = PythonOperator(
        task_id='fpl_ply_get_id',
        python_callable=pull_last_ply_id
    )

    with TaskGroup('pull_data_to_s3') as pul_data_s3:
        pull_gen_data = PythonOperator(
            task_id='fpl_general_data_s3',
            python_callable=write_general_data_to_s3_bucket
        )

        pull_week_data = PythonOperator(
            task_id='fpl_ply_week_data_s3',
            python_callable=write_players_week_data_to_s3_bucket,
        )

    insert_gen_data_postgree = PythonOperator(
        task_id='fpl_general_data_postgre',
        provide_context=True,
        python_callable=ply_info_s3_to_postgre,
        op_kwargs={'data_flow': ply_data_insert},
    )

    insert_week_data_postgree = PythonOperator(
        task_id='fpl_week_data_postgre',
        python_callable=ply_weeks_s3_to_postgre,
        op_kwargs={'data_flow': week_data_insert}
    )

    create_db_schema_tables = PythonOperator(
        task_id='create_db',
        python_callable=create_schema_tables_postgre
    )

    insert_team_data_postgre = PythonOperator(
        task_id='fpl_team_data_postgre',
        provide_context=True,
        python_callable=team_info_s3_to_postgre,
        op_kwargs={'data_flow': team_data_insert}
    )

    insert_player_stats_postgre = PythonOperator(
        task_id='ply_stats_data_postgre',
        provide_context=True,
        python_callable=ply_stats_s3_to_postgre,
        op_kwargs={'data_flow': ply_stat_data_insert}
    )


create_db_schema_tables >> get_id_data >> pul_data_s3 >> insert_team_data_postgre >> insert_gen_data_postgree >> insert_week_data_postgree >> insert_player_stats_postgre