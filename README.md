# fpl-data-pipeline

This project is made to automatize analysis of the fantasy premier league data from https://fantasy.premierleague.com site. Since I am huge fan of fantasy/real life gaming, my goal is to make the git project which can help players all around the world to optimize theirs fpl decision making. 

Final user will get the overview of the best performing players in the current season.

Project is consisted of:
- python code (packages used: flask, flask_sqlalchemy, psycopg2-binary, pandas, requests, boto3, selenium)
- airflow run locally on docker
- aws s3 bucket
- aws rds postgre db


### Dag flow

<img src="/images/fpl-dag.png" alt="photo of tasks in dag" title="Dag flow">

Airflow runs 6 dags, one of them is task grouping:
- created_db; created schema and necessary tables
- fpl_ply_get_id; gets the last id from players json
- pull_data_to_s3 [fpl_general_data_s3, fpl_ply_week_data_s3]; pulls data from fpl api to s3 buckets
- fpl_team_data_postgre; inserts data from json file to team_dm table in postgre
- fpl_general_data_postgre; inserts data from json file to player_dm table in postgre
- fpl_week_data_postgre; goes through the week files for each player and inserts data to player_week_ft table in postgre


## Structure of tables

<img src="/images/Er-diagram.png" alt="postgre db structure" title="ER diagram">

mylo.team_dm (contains basic data about teams)
- id int4 NOT NULL
- "name" varchar(30) NULL
- short_name varchar(10) NULL
- strength_att_home int4 NULL
- strength_def_home int4 NULL
- strength_att_away int4 NULL
- strength_def_away int4 NULL
- code int4 NULL
- CONSTRAINT pk_team_id PRIMARY KEY (id)

mylo.player_dm (contains basic data about players)
- id int4 NOT NULL
- "name" varchar(30) NULL
- surname varchar(30) NULL
- form numeric(18, 2) NULL
- total_points int4 NULL
- now_costs numeric(18, 2) NULL
- team_id int4 NULL
- "position" varchar(5) NULL
- CONSTRAINT pk_player_id PRIMARY KEY (id)

mylo.player_week_ft (contains results of players game weeks)
- element_ int4 NOT NULL
- fixture int4 NULL
- total_points int4 NULL
- opp_team int4 NULL
- was_home bool NULL
- team_h_score int4 NULL
- team_a_score int4 NULL
- round_gw int4 NOT NULL
- minutes int4 NULL
- goals_scored int4 NULL
- assists int4 NULL
- clean_sheets int4 NULL
- goals_conceded int4 NULL
- own_goals int4 NULL
- penalties_saved int4 NULL
- penalties_missed int4 NULL
- yellow_card int4 NULL
- red_card int4 NULL
- save int4 NULL
- bonus int4 NULL
- bps int4 NULL
- influence varchar(10) NULL
- creativity varchar(10) NULL
- threat varchar(10) NULL
- ict_index varchar(10) NULL
- value_ply numeric(18, 2) NULL
- CONSTRAINT pk_player_week_id PRIMARY KEY (element_, round_gw)

One extra table is available in ED - mylo.player_stat_dm; I am currently working into filling this data into fact table; but joining data is complex, thus it is in the development stage.

## Files are separated in few groups
- code for airflow dags
- code for flask representation of data
- code for unit tests
- code for scrapping understat premier league data

## Code for airflow dags
- sql_queries.py; containts all sql queris used in the python project
- includes.py; contains all functions needed for transfering data from fantasy premier league api to postgre db
- pull_fpl_data_s3_postgre.py; contains airflow dags

### Includes.py
This python file has 3 variables which are used on Airflow:
- gen_data_insert bool
- team_data_insert bool
- week_data_insert bool

All of them are signalling if inserting of data should be done or skipped.

## Code for flask
- run.py; running the flask server
- __init__.py; initzialize flask app and db connection
- routes.py; defines routes of flask server
- layout.html; defines basi template of html structure
- dashboard.html; shows fantasy premeir league data in charts and table

## Config file
- config.ini; must contain the postgresql parameters, and was parameters thus that s3 bucket and postgre rds can work

## Test file
- test_includes.py; running tests: to check if The Egyptian King is the db, and to check that all weeks have good join.

## Code for scrapping understat
- manually_scrapp_s3; used to scrape statistic data from understat via selenium. Since I am working on mac M1, I couldn't manage to place this chrome driver in airflow docker, thus it is separate file

## User guide

- docker and docker compose must be installed. Attached is also the docker-compose.yaml which I have used for airflow containers.
- in the airflow folder, there must be 3 folders created: tags, logs, plugins. The most important folder is dags, where python dags files should be placed. 3 of them: sql_queries.py, includes.py, pull-fpl-data-s3-postgre.py.
- pull-fpl-data-s3-postgre_v1 is the name of the dag which should be run. The airflow ui will be available on link: http://localhost:8080.
- manually_scrapp_s3 is used to scrape statistic data from understat via selenium, and place the files in s3 bucket.
- flask application is started via run.py file. Flask web server is then available on link: http://127.0.0.1:5000.


When postgre db is loaded with data from fpl ; flask server can be started and final overview of the data will pre presented to the user.

Data is shown in 2 pages, first one has 3 graphs (2 graphs and a table). Photo below.
- First graph shows the form of the premier league players; sorted by form desc
- Second shows the total points and value per points (Total points / player price); sorted by total points desc
- Third is the table which shows: points, bonus points of player, average points per 4 weeks, and points in the previous 3 matches (each match has column)

<img src="/images/fpl-dash.png" alt="photo of the dashboard site" title="Fpl dashboard">

Second page (Stats), has the look on general stats of the players.

<img src="/images/fpl-dash-stat.png" alt="photo of the dashboard site" title="Fpl stats">


### In development

- making connection (join) between the xG, xA (stats table) and fact table; since this data should also be in fact table
- switching table presentation to: https://datatables.net
- adding dynamic dashboards; thus person can filter the players by position
- adding fixtures tables, and fixture analysis for top players
- adding the possibility for user to choose which weeks will dag automatically pick up

### Future plans

- making final flask app for fantasy players as separate project; since they are not that into IT and python and docker, thus should be easier for use. One project for myself (practicing Data engineering) and another one for the final users
