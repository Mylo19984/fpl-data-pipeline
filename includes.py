from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json
import requests
import pandas as pd
from pandas.io import sql
import sqlalchemy
import boto3
from boto3.session import Session
from datetime import datetime, timedelta
import psycopg2
from sqlalchemy import create_engine
import logging
import time
import sys
import configparser

task_logger = logging.getLogger('airflow.task')
config_obj = configparser.ConfigParser()
config_obj.read('/opt/airflow/dags/config.ini')
db_param = config_obj["postgresql"]
aws_user = config_obj["aws"]
db_user = db_param['user']
db_pass = db_param['password']
db_host = db_param['host']
db_name = db_param['db']


def ply_info_s3_to_postgre_test():
    # test
    # radi ovaj model sa get_object, pa load u json. Superika. Unesi u airflow.
    #sys.exit()

    engine = create_engine(
        F'postgresql+psycopg2://{db_user}:{db_pass}@{db_host}/{db_name}')

    s3 = boto3.client(
        's3',
        region_name=aws_user['region'],
        aws_access_key_id=aws_user['acc_key'],
        aws_secret_access_key=aws_user['secret_acc_key']
    )

    obj = s3.get_object(Bucket='mylosh', Key='general_data/general_info.json')
    j = json.loads(obj['Body'].read())
    print(j)

    task_logger.info('Task finished')



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


def ply_info_s3_to_postgre() -> None:
    # Inserts players general data from s3 file to postgre db, table mylo.player_general
    #
    #
    # Returns nothing, just inserting the file in postgre db

    #sys.exit()

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

    task_logger.info('Postgrees inserting general data started')
    for i in range(0, len(data)-1):
        print(str(data[i]['id']) + ' ' + data[i]['first_name'])
        engine.execute("""INSERT INTO mylo.player_general (id, name, surname, form, total_points, now_costs) VALUES
                    (%s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO UPDATE SET 
                    name = %s,
                    surname = %s,
                    form = %s,
                    total_points = %s,
                    now_costs = %s""", (
        data[i]['id'], data[i]['first_name'], data[i]['second_name'], data[i]['form'], data[i]['total_points'],
        data[i]['now_cost'], data[i]['first_name'], data[i]['second_name'], data[i]['form'], data[i]['total_points'],
        data[i]['now_cost']))

    task_logger.info('Postgrees inserting general data finished')


def ply_weeks_s3_to_postgre() -> None:
    # Inserts players weekly data from s3 file to postgre db, table mylo.player_general
    #
    #
    # Returns nothing, just inserting the file in postgre db

    engine = create_engine(
        F'postgresql+psycopg2://{db_user}:{db_pass}@{db_host}/{db_name}')

    #s3 = boto3.resource(
    #    service_name='s3',
    #    region_name=aws_user['region'],
    #    aws_access_key_id=aws_user['acc_key'],
    #    aws_secret_access_key=aws_user['secret_acc_key']
    #)

    s3 = boto3.client(
        's3',
        region_name=aws_user['region'],
        aws_access_key_id=aws_user['acc_key'],
        aws_secret_access_key=aws_user['secret_acc_key']
    )

    num_players = ti.xcom_pull(task_ids='fpl_ply_get_id')

    for id in range(1,num_players):
        #s3.Bucket('mylosh').download_file(F'ply_data_gw/{id}.json', F'{id}.json')
        #f = open(F'{id}.json')
        obj = s3.get_object(Bucket='mylosh', Key=F'ply_data_gw/{id}.json')
        j = json.loads(obj['Body'].read())
        data = j['history']

        task_logger.info('Postgrees inserting plyr data started')
        for i in range(0, len(data)-1):

            engine.execute("""INSERT INTO mylo.player_weeks
                        (element_,
                        fixture,
                        total_points,
                        opp_team,
                        was_home,
                        team_h_score,
                        team_a_score,
                        round_gw,
                        minutes,
                        goals_scored,
                        assists,
                        clean_sheets,
                        goals_conceded,
                        own_goals,
                        penalties_saved,
                        penalties_missed,
                        yellow_card,
                        red_card,
                        save,
                        bonus,
                        bps,
                        influence,
                        creativity,
                        threat,
                        ict_index,
                        value_ply)
                        VALUES (%s, %s, %s, %s, %s, 
                                %s, %s, %s, %s, %s, 
                                %s, %s, %s, %s, %s, 
                                %s, %s, %s, %s, %s, 
                                %s, %s, %s, %s, %s, 
                                %s)""",
                           (data[i]['element'], data[i]['fixture'], data[i]['total_points'], data[i]['opponent_team'], data[i]['was_home']
                            , data[i]['team_h_score'], data[i]['team_a_score'], data[i]['round'], data[i]['minutes'], data[i]['goals_scored']
                            , data[i]['assists'], data[i]['clean_sheets'], data[i]['goals_conceded'], data[i]['own_goals'], data[i]['penalties_saved']
                            , data[i]['penalties_missed'], data[i]['yellow_cards'], data[i]['red_cards'], data[i]['saves'], data[i]['bonus']
                            , data[i]['bps'], data[i]['influence'], data[i]['creativity'], data[i]['threat'], data[i]['ict_index']
                            , data[i]['value']))
        task_logger.info('Postgrees inserting plyr data finished')




def test_insert():
    engine = create_engine(
        F'postgresql+psycopg2://{db_user}:{db_pass}@{db_host}/{db_name}')

    engine.execute("""INSERT INTO mylo.player_weeks_data_test (el_, fixture) VALUES
    (%s, %s)""", (1, 322))


def write_to_postgree():

    engine = create_engine(
        F'postgresql+psycopg2://{db_user}:{db_pass}@{db_host}/{db_name}')
    conn = engine.connect()

    sql_create_schema = """CREATE SCHEMA IF NOT EXISTS mylo;"""

    sql_create_players_week = """CREATE TABLE IF NOT EXISTS 
    mylo.player_weeks (element_ integer, fixture integer, total_points integer, opp_team integer,
    was_home boolean, team_h_score integer, team_a_score integer, round_gw integer, minutes integer,
    goals_scored integer, assists integer, clean_sheets integer, goals_conceded integer, own_goals integer,
    penalties_saved integer, penalties_missed integer, yellow_card integer, red_card integer, save integer,
    bonus integer, bps integer, influence varchar(10), creativity varchar(10), threat varchar(10), ict_index varchar(10),
    value_ply decimal(18,2))"""

    sql_create_players_data = """CREATE TABLE IF NOT EXISTS 
    mylo.player_general (id integer, name varchar(30), surname varchar(30), form decimal(18,2), 
    total_points integer, now_costs decimal(18,2), 
    constraint pk_player_id primary key (id))"""

    conn.execute(sql_create_schema)
    conn.execute(sql_create_players_week)
    conn.execute(sql_create_players_data)

    conn.close()


def write_to_postgree_delete():

    engine = create_engine('postgresql+psycopg2://user:password@hostname/database_name')
    engine.execute("""DROP TABLE IF EXISTS player_weeks;""")


def get_plyr_week(id, wk):
    # ne prosledjujemo wk, posto mogu da imaju manje nedelja od full
    response_ply = requests.get(F'https://fantasy.premierleague.com/api/element-summary/{id}/')
    dd_ply = json.loads(response_ply.text)
    dd_ply = dd_ply['history']
    data = pd.DataFrame()

    for i in range(0,wk):
        temp_db = pd.DataFrame.from_dict(dd_ply[i], orient='index')
        #print(temp_db)
        #data.append(temp_db, ignore_index=False)
        data = pd.concat([data,temp_db], axis = 1)

    data = data.transpose()
    return data[['element', 'fixture', 'total_points', 'opponent_team', 'was_home', 'team_h_score', 'team_a_score', 'round',
                'minutes', 'goals_scored', 'assists', 'clean_sheets', 'goals_conceded', 'own_goals', 'penalties_saved',
                'penalties_missed', 'yellow_cards', 'red_cards', 'saves', 'bonus', 'bps', 'influence', 'creativity', 'threat',
                'ict_index', 'value' ]]


def write_to_json_file(path, fileName, data):
    filePathNameWExt = path + '/' + fileName + '.json'
    with open(filePathNameWExt, 'w') as fp:
        json.dump(data, fp)


def player_week_json_to_local(num_players=10):
    for id in range(0,num_players):
        response_ply = requests.get(F'https://fantasy.premierleague.com/api/element-summary/{id}/')
        dd_ply = json.loads(response_ply.text)
        # this path is ok for local python ./data/ it is not for dags, because of docker
        write_to_json_file('/opt/airflow/dags/data', F'{id}', dd_ply)


# write file from local to s3 bucket; this will be used in the future
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
    print(num_players)

    for id in range(1,num_players):
        response_ply = requests.get(F'https://fantasy.premierleague.com/api/element-summary/{id}/')
        dd_ply = json.loads(response_ply.text)
        time.sleep(0.2)
        s3object = s3.Object('mylosh', F'ply_data_gw/{id}.json')
        s3object.put(
            Body=(bytes(json.dumps(dd_ply).encode('UTF-8')))
        )

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


# odeljak sa proslog projekta, sada nije od interesa

