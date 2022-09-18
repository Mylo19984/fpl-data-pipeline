import json

import pandas
import requests
import boto3
from sqlalchemy import create_engine
import logging
import time
import configparser
import pandas as pd
import sql_queries

task_logger = logging.getLogger('airflow.task')

config_obj = configparser.ConfigParser()
config_obj.read('config.ini')
# /opt/airflow/dags/
db_param = config_obj["postgresql"]
aws_user = config_obj["aws"]
db_user = db_param['user']
db_pass = db_param['password']
db_host = db_param['host']
db_name = db_param['db']

dict_element_types = {1:'gk', 2:'def', 3:'mid', 4:'fwd'}


def create_schema_tables_postgre():
    # Creates the database structure in postgre db
    #
    #
    # Returns nothing just runs scripts

    engine = create_engine(
        F'postgresql+psycopg2://{db_user}:{db_pass}@{db_host}/{db_name}')
    conn = engine.connect()

    task_logger.info('Creating schema and running scripts')

    conn.execute(sql_queries.sql_create_schema)
    conn.execute(sql_queries.sql_create_teams_data)
    conn.execute(sql_queries.sql_create_players_data)
    conn.execute(sql_queries.sql_create_players_week)


    conn.close()

    task_logger.info('Finished schema and scripts')


def write_players_week_data_to_s3_bucket(ti, num_players=10) -> None:
    # Writes weekly players data from fpl api to s3
    #
    #
    # Returns nothing just copying files

    s3 = boto3.resource(
        service_name='s3',
        region_name=aws_user['region'],
        aws_access_key_id=aws_user['acc_key'],
        aws_secret_access_key=aws_user['secret_acc_key']
    )

    # turned off thus it doesnt non stop pull the fpl data
    num_players = ti.xcom_pull(task_ids='fpl_ply_get_id')

    task_logger.info('Copying json ply data to s3')
    num_s3_ply_data = 0

    # !!! dodati preskakanje ovog koraka.
    for id in range(1,10):
        response_ply = requests.get(F'https://fantasy.premierleague.com/api/element-summary/{id}/')
        dd_ply = json.loads(response_ply.text)
        time.sleep(0.2)
        s3object = s3.Object('mylosh', F'ply_data_gw/{id}.json')
        s3object.put(
            Body=(bytes(json.dumps(dd_ply).encode('UTF-8')))
        )
        num_s3_ply_data += 1

    task_logger.info(F'Finished copying ply data jsons, total: {num_s3_ply_data}')


def write_general_data_to_s3_bucket() -> None:
    # Writes general data about players from fpl api to s3
    #
    #
    # Returns nothing just copying files

    s3 = boto3.resource(
        service_name='s3',
        region_name=aws_user['region'],
        aws_access_key_id=aws_user['acc_key'],
        aws_secret_access_key=aws_user['secret_acc_key']
    )

    task_logger.info('Pulling general data started')

    response_gen = requests.get('https://fantasy.premierleague.com/api/bootstrap-static/')
    dd_ply = json.loads(response_gen.text)

    s3object = s3.Object('mylosh', 'general_data/general_info.json')
    s3object.put(
       Body=(bytes(json.dumps(dd_ply).encode('UTF-8')))
    )

    task_logger.info('Pulling general data finished')


def ply_info_s3_to_postgre(**kwargs):
    # Inserts players general data from s3 file to postgre db, table mylo.player_general
    #
    #
    # Returns nothing, just inserting the file in postgre db

    if int(kwargs['data_flow']) == 1:

        engine = create_engine(
            F'postgresql+psycopg2://{db_user}:{db_pass}@{db_host}/{db_name}')

        s3 = boto3.resource(
            service_name='s3',
            region_name=aws_user['region'],
            aws_access_key_id=aws_user['acc_key'],
            aws_secret_access_key=aws_user['secret_acc_key']
        )

        s3.Bucket('mylosh').download_file('general_data/general_info.json', 'general_info.json')
        f = open('general_info.json')
        data = json.load(f)['elements']
        df = pd.json_normalize(data)
        df['position'] = df['element_type'].map(dict_element_types)
        data = df.to_dict('records')

        task_logger.info('Postgrees inserting general data started')
        num_postgre_ply_data = 0

        for i in range(0, len(data)):
            engine.execute(sql_queries.sql_insert_ply_gen_postgree,
            (data[i]['id'], data[i]['first_name'], data[i]['second_name'], data[i]['form'], data[i]['total_points'],
            data[i]['now_cost'], data[i]['team'], data[i]['position'], data[i]['first_name'], data[i]['second_name'],
            data[i]['form'], data[i]['total_points'], data[i]['now_cost'], data[i]['team'], data[i]['position']))

            num_postgre_ply_data += 1

        task_logger.info(F'Postgrees inserting general data finished, no of inserted records {num_postgre_ply_data}')
    else:
        task_logger.info('Postgrees inserting general data SKIPPED')


def ply_weeks_s3_to_postgre(**kwargs):
    # Inserts players weekly data from s3 file to postgre db, table mylo.player_general
    #
    #
    # Returns nothing, just inserting the file in postgre db

    ti = kwargs['ti']

    if int(kwargs['data_flow']) == 1:

        engine = create_engine(
            F'postgresql+psycopg2://{db_user}:{db_pass}@{db_host}/{db_name}')

        s3 = boto3.client(
            's3',
            region_name=aws_user['region'],
            aws_access_key_id=aws_user['acc_key'],
            aws_secret_access_key=aws_user['secret_acc_key']
        )

        num_players = ti.xcom_pull(task_ids='fpl_ply_get_id')

        task_logger.info('Postgrees inserting plyr data started')
        num_postgre_week_data = 0

        for id in range(1, num_players):
            #s3.Bucket('mylosh').download_file(F'ply_data_gw/{id}.json', F'{id}.json')
            #f = open(F'{id}.json')
            obj = s3.get_object(Bucket='mylosh', Key=F'ply_data_gw/{id}.json')
            j = json.loads(obj['Body'].read())
            data = j['history']

            for i in range(0, len(data)):
                engine.execute(sql_queries.sql_insert_weeks_postgree,
                               (data[i]['element'], data[i]['fixture'], data[i]['total_points'], data[i]['opponent_team'], data[i]['was_home'],
                                data[i]['team_h_score'], data[i]['team_a_score'], data[i]['round'], data[i]['minutes'], data[i]['goals_scored'],
                                data[i]['assists'], data[i]['clean_sheets'], data[i]['goals_conceded'], data[i]['own_goals'], data[i]['penalties_saved'],
                                data[i]['penalties_missed'], data[i]['yellow_cards'], data[i]['red_cards'], data[i]['saves'], data[i]['bonus'],
                                data[i]['bps'], data[i]['influence'], data[i]['creativity'], data[i]['threat'], data[i]['ict_index'],
                                data[i]['value'], data[i]['fixture'], data[i]['total_points'], data[i]['opponent_team'], data[i]['was_home'],
                                data[i]['team_h_score'], data[i]['team_a_score'], data[i]['minutes'], data[i]['goals_scored'],
                                data[i]['assists'], data[i]['clean_sheets'], data[i]['goals_conceded'], data[i]['own_goals'], data[i]['penalties_saved'],
                                data[i]['penalties_missed'], data[i]['yellow_cards'], data[i]['red_cards'], data[i]['saves'], data[i]['bonus'],
                                data[i]['bps'], data[i]['influence'], data[i]['creativity'], data[i]['threat'], data[i]['ict_index'],
                                data[i]['value']
                                ))

            num_postgre_week_data += 1

        task_logger.info(F'Postgrees inserting plyr data finished, no of inserted record {num_postgre_week_data}')
    else:
        task_logger.info('Postgrees inserting plyr data SKIPPED')


def pull_last_ply_id():
    # Pulls the number of players from general data, thus it can be used for getting the weekly players data
    #
    #
    # Returns the highest player id

    s3 = boto3.resource(
        service_name='s3',
        region_name=aws_user['region'],
        aws_access_key_id=aws_user['acc_key'],
        aws_secret_access_key=aws_user['secret_acc_key']
    )

    s3.Bucket('mylosh').download_file('general_data/general_info.json', 'general_info.json')
    f = open('general_info.json')
    data = json.load(f)

    return data['elements'][-1]['id']


def team_info_s3_to_postgre(**kwargs):
    # Inserts players general data from s3 file to postgre db, table mylo.player_general
    #
    #
    # Returns nothing, just inserting the file in postgre db

    if int(kwargs['data_flow']) == 1:

        engine = create_engine(
            F'postgresql+psycopg2://{db_user}:{db_pass}@{db_host}/{db_name}')

        s3 = boto3.resource(
            service_name='s3',
            region_name=aws_user['region'],
            aws_access_key_id=aws_user['acc_key'],
            aws_secret_access_key=aws_user['secret_acc_key']
        )

        s3.Bucket('mylosh').download_file('general_data/general_info.json', 'general_info.json')
        f = open('general_info.json')
        data = json.load(f)['teams']

        task_logger.info('Postgrees inserting general team data started')
        num_postgre_team_data = 0

        for i in range(0, len(data)):
            engine.execute(sql_queries.sql_insert_teams_postgree,
            (data[i]['id'], data[i]['name'], data[i]['short_name'], data[i]['strength_attack_home'], data[i]['strength_defence_home'],
             data[i]['strength_attack_away'], data[i]['strength_defence_away'], data[i]['code'], data[i]['name'], data[i]['short_name'],
            data[i]['strength_attack_home'], data[i]['strength_defence_home'], data[i]['strength_attack_away'], data[i]['strength_defence_away'], data[i]['code']))

            num_postgre_team_data += 1

        task_logger.info(F'Postgrees inserting general team data finished, no of inserted records {num_postgre_team_data}')

    else:
        task_logger.info('Postgrees inserting team general data SKIPPED')


### functions for pytest; unity testing of data

def get_sala_id():

    engine = create_engine(
        F'postgresql+psycopg2://{db_user}:{db_pass}@{db_host}/{db_name}')

    salah_id = engine.execute(sql_queries.sql_get_salah_id).fetchall()

    return salah_id[0][0]


def ply_weeks_join_quality():

    engine = create_engine(
        F'postgresql+psycopg2://{db_user}:{db_pass}@{db_host}/{db_name}')

    number_of_not_joined_week_rows = engine.execute(sql_queries.sql_control_of_joins, engine).fetchall()

    return number_of_not_joined_week_rows[0][0]
