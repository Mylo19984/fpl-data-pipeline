import json

from application import app
from flask import render_template
from application.form import UserInputForm
from application.models import FplPlayerData
from sqlalchemy import text
from application import db

@app.route('/')
def index():
    entries = FplPlayerData.query.order_by(FplPlayerData.form.desc()).limit(9).all()
    return render_template('index.html', entries=entries)

@app.route('/layout')
def layout():
    return render_template('layout.html', title='Layouuuts')

@app.route('/add', methods= ['GET', 'POST'])
def add():
    form = UserInputForm()
    return render_template('add.html', title='Addd', form=form)

@app.route('/dashboard')
def dashboard():
    sql = text('select name, surname, form from mylo.player_general pg order by form desc limit 9')
    result = db.engine.execute(sql).fetchall()
    fpl_data = [float(row['form']) for row in result]
    fpl_data_label = [row['name'] + ' ' + row['surname']  for row in result]
    #db.dispose()

    return render_template('dashboard.html', title='Dash', fpl_data = fpl_data, fpl_data_name = fpl_data_label)



