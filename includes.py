import json
import requests
import boto3
from sqlalchemy import create_engine
import logging
import time
import configparser
import pandas as pd
import sql_queries
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from io import StringIO
import csv
from pathlib import Path


task_logger = logging.getLogger('airflow.task')

config_obj = configparser.ConfigParser()
my_file = Path('/opt/airflow/dags/config.ini')

if my_file.is_file():
    config_obj.read('/opt/airflow/dags/config.ini')
else:
    config_obj.read('config.ini')

db_param = config_obj["postgresql"]
aws_user = config_obj["aws"]
db_user = db_param['user']
db_pass = db_param['password']
db_host = db_param['host']
db_name = db_param['db']

dict_element_types = {1:'gk', 2:'def', 3:'mid', 4:'fwd'}


def create_schema_tables_postgre() -> None:
    """ Creates the database structure in postgre db

    """

    engine = create_engine(
        F'postgresql+psycopg2://{db_user}:{db_pass}@{db_host}/{db_name}')
    conn = engine.connect()

    task_logger.info('Creating schema and running scripts')

    conn.execute(sql_queries.sql_create_schema)
    conn.execute(sql_queries.sql_create_teams_data)
    conn.execute(sql_queries.sql_create_players_data)
    conn.execute(sql_queries.sql_create_players_week)
    conn.execute(sql_queries.sql_create_players_x_stats)
    conn.execute(sql_queries.sql_create_week_info)

    conn.close()

    task_logger.info('Finished schema and scripts')


def write_players_week_data_to_s3_bucket(ti, num_players=10) -> None:
    """ Writes weekly players data from fpl api to s3

    :param ti: used for xcom pull, task_instance

    :param num_players: used to control number of players pulled to s3, for testing purpose
    """

    s3 = create_boto3(True)

    num_players = ti.xcom_pull(task_ids='fpl_ply_get_id')

    task_logger.info('Copying json ply data to s3')
    num_s3_ply_data = 0

    # !!! add data flow
    # !!! changed to 15; num_players
    for id in range(1, num_players):
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
    """ Writes general data about players from fpl api to s3

    """

    s3 = create_boto3(True)

    task_logger.info('Pulling general data started')

    response_gen = requests.get('https://fantasy.premierleague.com/api/bootstrap-static/')
    dd_ply = json.loads(response_gen.text)

    s3object = s3.Object('mylosh', 'general_data/general_info.json')
    s3object.put(
       Body=(bytes(json.dumps(dd_ply).encode('UTF-8')))
    )

    task_logger.info('Pulling general data finished')


def ply_info_s3_to_postgre(**kwargs) -> None:
    """ Inserts players general data from s3 file to postgre db, table mylo.player_general

    :param kwargs: data_flow parameter, thus function can know if some code should be skipped
    """

    if int(kwargs['data_flow']) == 1:

        engine = create_engine(
            F'postgresql+psycopg2://{db_user}:{db_pass}@{db_host}/{db_name}')

        s3 = create_boto3(True)

        s3.Bucket('mylosh').download_file('general_data/general_info.json', 'general_info.json')
        f = open('general_info.json')
        data = json.load(f)['elements']
        df = pd.json_normalize(data)
        df['position'] = df['element_type'].map(dict_element_types)
        data = df.to_dict('records')

        task_logger.info('Postgres inserting general data started')
        num_postgre_ply_data = 0

        for i in range(0, len(data)):
            engine.execute(sql_queries.sql_insert_ply_gen_postgre,
            (data[i]['id'], data[i]['first_name'], data[i]['second_name'], data[i]['form'], data[i]['total_points'],
            data[i]['now_cost'], data[i]['team'], data[i]['position'], data[i]['first_name'], data[i]['second_name'],
            data[i]['form'], data[i]['total_points'], data[i]['now_cost'], data[i]['team'], data[i]['position']))

            num_postgre_ply_data += 1

        task_logger.info(F'Postgres inserting general data finished, no of inserted records {num_postgre_ply_data}')
    else:
        task_logger.info('Postgres inserting general data SKIPPED')


def ply_weeks_s3_to_postgre(**kwargs) -> None:
    """ Inserts players weekly data from s3 file to postgre db, table mylo.player_general

    :param kwargs: data_flow parameter, thus function can know if some code should be skipped
    """

    engine = create_engine(
        F'postgresql+psycopg2://{db_user}:{db_pass}@{db_host}/{db_name}')

    ti = kwargs['ti']
    num_of_weeks = 0
    s3 = create_boto3(False)
    #num_players = 631
    num_players = ti.xcom_pull(task_ids='fpl_ply_get_id')
    # !!!

    task_logger.info('Postgres inserting plyr data started')
    num_postgre_week_data = 0

    # !!! changed to 30; num_players
    for id in range(1, num_players):
        obj = s3.get_object(Bucket='mylosh', Key=F'ply_data_gw/{id}.json')
        j = json.loads(obj['Body'].read())
        data = j['history']

        if (kwargs['data_flow'] == 'All') or (
                engine.execute(sql_queries.sql_select_last_data_checked_week).fetchone() == 0):
            end_weeks = len(data)-1
            beg_weeks = 0
        elif kwargs['data_flow'] != 'All':
            end_weeks = int(kwargs['data_flow'])
            beg_weeks = int(kwargs['data_flow'])
        else:
            end_weeks = int(engine.execute(sql_queries.sql_select_last_data_checked_week).fetchone()['week_id'])
            beg_weeks = end_weeks

        task_logger.info(F'Num of week for pull is: {num_of_weeks}')

        for i in range(beg_weeks, end_weeks+1):
            try:
                data[i]
            except IndexError:
                print(F'Index error, list of out range, for id player: {id}')
            else:
                engine.execute(sql_queries.sql_insert_weeks_postgre,
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
                task_logger.info(F'Week number: {i} for id player: {id}')
                print(F'Week number: {i} for id player: {id}')

    task_logger.info(F'Postgres inserting plyr data finished, no of inserted record {num_postgre_week_data}')


def pull_last_ply_id() -> int:
    """ Pulls the number of players from general data, thus it can be used for getting the weekly players data

    :return: Returns the highest player id
    """

    s3 = create_boto3(True)

    s3.Bucket('mylosh').download_file('general_data/general_info.json', 'general_info.json')
    f = open('general_info.json')
    data = json.load(f)

    return data['elements'][-1]['id']


def team_info_s3_to_postgre(**kwargs) -> None:
    """ Inserts players general data from s3 file to postgre db, table mylo.player_general

    :param kwargs: data_flow parameter, thus function can know if some code should be skipped
    """

    if int(kwargs['data_flow']) == 1:

        engine = create_engine(
            F'postgresql+psycopg2://{db_user}:{db_pass}@{db_host}/{db_name}')

        s3 = create_boto3(True)

        s3.Bucket('mylosh').download_file('general_data/general_info.json', 'general_info.json')
        f = open('general_info.json')
        data = json.load(f)['teams']

        task_logger.info('Postgres inserting general team data started')
        num_postgre_team_data = 0

        for i in range(0, len(data)):
            engine.execute(sql_queries.sql_insert_teams_postgre,
            (data[i]['id'], data[i]['name'], data[i]['short_name'], data[i]['strength_attack_home'], data[i]['strength_defence_home'],
             data[i]['strength_attack_away'], data[i]['strength_defence_away'], data[i]['code'], data[i]['name'], data[i]['short_name'],
            data[i]['strength_attack_home'], data[i]['strength_defence_home'], data[i]['strength_attack_away'], data[i]['strength_defence_away'], data[i]['code']))

            num_postgre_team_data += 1

        task_logger.info(F'Postgres inserting general team data finished, no of inserted records {num_postgre_team_data}')

    else:
        task_logger.info('Postgres inserting team general data SKIPPED')


def scrapp_xg_xa_uderstat(match_str: str) -> None:
    ''' Web scraping matches from the site to gain data about xG, xA and other statistical data.

    :param match_str: list of match ids to scrap
    '''

    base_url = 'https://understat.com/match/'
    #match = '18242'
    url = base_url + match_str

    res = requests.get(url)
    soup = BeautifulSoup(res.content, 'lxml')
    scripts = soup.find_all('script')
    strings = scripts[2].string

    ind_start = strings.index("('") + 2
    ind_end = strings.index("')")
    json_data = strings[ind_start:ind_end]
    json_data = json_data.encode('utf8').decode('unicode_escape')

    data = json.loads(json_data)

    # load home data
    df_home = pd.DataFrame.from_dict(data['h'], orient='index')
    df_home_f = df_home.reset_index()

    # load away data
    df_away = pd.DataFrame.from_dict(data['a'], orient='index')
    df_away_f = df_away.reset_index()

    df_final = pd.concat([df_home_f, df_away_f])
    df_final = df_final[['player_id', 'player', 'time', 'key_passes', 'assists', 'shots', 'xG', 'xA']]
    df_final['match_id'] = match_str

    saving_scrapped_data_s3(df_final)
    time.sleep(0.1)


def get_matches_ids_4_weeks(weeks=4) -> list:
    ''' Function goes to the understat website and picks up (through selenium) last 4 weeks of data
    There is the part of the code which is automatically changing the pages and picking up the wanted data -> match ids

    :param weeks: number of weeks to pull from understat premier league

    :return: List with match ids
    '''

    options = Options()
    options = webdriver.ChromeOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=800,600")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--headless")
    driver = webdriver.Chrome(options=options)
    driver.get('https://understat.com/league/EPL')

    full_list = []

    for i in range(1, (weeks+1)):
        print(F'Number of iteration {i}')
        #print('Changing page')

        folder = driver.find_element(By.XPATH, "//button[@class='calendar-prev']")
        folder.click()

        links = driver.find_elements(By.XPATH, "//*[@class='match-info']")
        list = [f.get_attribute('href').split('match/')[1] for f in links if f.get_attribute('href') is not None]

        for l in list:
            full_list.append(l)

    return full_list


def saving_scrapped_data_s3(df_scrapped_data) -> None:
    """ Saves the scrapped data from premier league matches to s3

    :param df_scrapped_data: dataframe with scraped match data
    """

    s3 = create_boto3(True)

    task_logger.info('Saving scrapped data to s3 started')

    csv_buffer = StringIO()
    df_scrapped_data.to_csv(csv_buffer)
    match = df_scrapped_data['match_id'].iloc[0]
    s3.Object('mylosh', F"scrapp_stat_data/{match}.csv").put(Body=csv_buffer.getvalue())

    task_logger.info('Saving scrapped data to s3 finished')


def ply_stats_s3_to_postgre(**kwargs) -> None:
    """ Inserts players general data from s3 file to postgre db, table mylo.player_general

    :param kwargs: data_flow parameter, thus function can know if some code should be skipped
    """

    if int(kwargs['data_flow']) == 1:

        engine = create_engine(
            F'postgresql+psycopg2://{db_user}:{db_pass}@{db_host}/{db_name}')

        s3 = create_boto3(False)

        data_bucket = s3.list_objects(Bucket = 'mylosh', Prefix='scrapp_stat_data/')['Contents']

        num_postgre_ply_data = 0

        for data in data_bucket:
            if data['Size'] > 0:
                print(data)
                obj = s3.get_object(Bucket='mylosh', Key=data['Key'])
                body = obj['Body']
                csv_string = body.read().decode('utf-8')

                spamreader = csv.reader(StringIO(csv_string), quotechar='|')
                next(spamreader)
                for row in spamreader:
                    print(row)
                    engine.execute(sql_queries.sql_insert_players_x_stats,
                               (row[1], row[2], row[3], row[4],
                                row[5], row[6], row[7], row[8], row[9],
                                row[2], row[3], row[4], row[5],
                                row[6], row[7], row[8]))


                    num_postgre_ply_data += 1

        task_logger.info(F'Postgres inserting stats data finished, no of inserted records {num_postgre_ply_data}')
    else:
        task_logger.info('Postgres inserting stats data SKIPPED')


def week_info_s3_to_postgre() -> None:
    """ Inserts finished weeks from s3 file to postgre db, table mylo.week_info

    """

    engine = create_engine(
        F'postgresql+psycopg2://{db_user}:{db_pass}@{db_host}/{db_name}')

    s3 = create_boto3(True)

    s3.Bucket('mylosh').download_file('general_data/general_info.json', 'general_info.json')
    f = open('general_info.json')
    data = json.load(f)['events']

    print(data)

    task_logger.info('Postgres inserting week data started')
    num_postgre_week_data = 0

    for i in range(0, len(data)):
        engine.execute(sql_queries.sql_insert_week_info_postgre,
        (data[i]['id'], data[i]['name'], data[i]['average_entry_score'], data[i]['finished'], data[i]['data_checked'],
         data[i]['name'], data[i]['average_entry_score'], data[i]['finished'], data[i]['data_checked']))

        num_postgre_week_data += 1

    task_logger.info(F'Postgres inserting week data finished, no of inserted records {num_postgre_week_data}')


def create_boto3(resource: bool) -> boto3:

    if resource == 1:
        s3 = boto3.resource(
            service_name='s3',
            region_name=aws_user['region'],
            aws_access_key_id=aws_user['acc_key'],
            aws_secret_access_key=aws_user['secret_acc_key']
        )
    else:
        s3 = boto3.client(
            service_name='s3',
            region_name=aws_user['region'],
            aws_access_key_id=aws_user['acc_key'],
            aws_secret_access_key=aws_user['secret_acc_key']
        )

    return s3

### functions for pytest; unity testing of data

def get_salah_id():
    """ Controls if the Salah id is present in postgre db

    :return: Returns the id of M. Salah from player_dm table
    """

    engine = create_engine(
        F'postgresql+psycopg2://{db_user}:{db_pass}@{db_host}/{db_name}')

    salah_id = engine.execute(sql_queries.sql_get_salah_id).fetchall()

    return salah_id[0][0]


def ply_weeks_join_quality():
    """ Controls if the all rows in player_week_ft table have joins

    :return: Returns the number of rows from player_week_ft which doesnt have join
    """

    engine = create_engine(
        F'postgresql+psycopg2://{db_user}:{db_pass}@{db_host}/{db_name}')

    number_of_not_joined_week_rows = engine.execute(sql_queries.sql_control_of_joins, engine).fetchall()

    return number_of_not_joined_week_rows[0][0]
