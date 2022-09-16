from flask import Flask
from flask_sqlalchemy import SQLAlchemy
import os
import psycopg2
import configparser
import os.path

path = ("/Users/mylo/PycharmProjects/simpleDAGAirF/config.ini")

print(path)

config_obj = configparser.ConfigParser()
config_obj.read(path)
db_param = config_obj["postgresql"]
aws_user = config_obj["aws"]
db_user = db_param['user']
db_pass = db_param['password']
db_host = db_param['host']
db_name = db_param['db']


app = Flask(__name__)

app.config['SQLALCHEMY_DATABASE_URI'] = F'postgresql+psycopg2://{db_user}:{db_pass}@{db_host}/{db_name}'
    #F'postgresql+psycopg2://{db_user}:{db_pass}@{db_host}/{db_name}'
SECRET_KEY = os.urandom(32)
app.config['SECRET_KEY'] = SECRET_KEY

db = SQLAlchemy(app)

from application import routes

