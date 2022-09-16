from application import app
from flask import render_template
from application.form import UserInputForm
from application.models import FplPlayerData
from sqlalchemy import text
from application import db
import sql_queries

#@app.route('/')
#def index():
#    entries = FplPlayerData.query.order_by(FplPlayerData.form.desc()).limit(9).all()
#    return render_template('index.html', entries=entries)

@app.route('/layout')
def layout():
    return render_template('layout.html', title='Layouuuts')

@app.route('/add', methods= ['GET', 'POST'])
def add():
    form = UserInputForm()
    return render_template('add.html', title='Addd', form=form)

@app.route('/')
def dashboard():
    result = db.engine.execute(sql_queries.basic_ply_data).fetchall()
    sql_total_cost = text(sql_queries.value_per_points)
    result_total_cost = db.engine.execute(sql_total_cost).fetchall()
    sql_general_data_overview = text(sql_queries.detailed_ply_data_per_week)

    result_main_graph = db.engine.execute(sql_general_data_overview).fetchall()

    fpl_data = [float(row['form']) for row in result]
    fpl_data_label = [row['name'] + ' ' + row['surname'] for row in result]
    fpl_data_points = [float(row['total_points']) for row in result_total_cost]
    fpl_data_points_name = [row['name'] + ' ' + row['surname'] for row in result_total_cost]
    fpl_data_points_value = [float(row['point_value']) for row in result_total_cost]
    fpl_data_main_table = [row for row in result_main_graph]
    #db.dispose()

    return render_template('dashboard.html', title='Dash', fpl_data = fpl_data, fpl_data_name = fpl_data_label,
                           fpl_data_points=fpl_data_points, fpl_data_points_name=fpl_data_points_name, fpl_data_points_value=fpl_data_points_value,
                           fpl_data_main_table=fpl_data_main_table)



