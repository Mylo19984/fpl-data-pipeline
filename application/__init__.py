from flask import Flask
from flask_sqlalchemy import SQLAlchemy
import os
import psycopg2


app = Flask(__name__)

app.config['SQLALCHEMY_DATABASE_URI'] = ''
SECRET_KEY = os.urandom(32)
app.config['SECRET_KEY'] = SECRET_KEY

db = SQLAlchemy(app)

from application import routes

