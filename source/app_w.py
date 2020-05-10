# 1. Import Flask
import numpy as np
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
from flask import Flask, jsonify, request
from datetime import datetime
import pandas as pd
import datetime as dt

# connecting SQLite db
#engine = create_engine("sqlite:///C:\\Users\\user\\Documents\\ITESM_DA\\sqlalchemy-challenge\\data\\hawaii.sqlite")
engine = create_engine("sqlite:///data/hawaii.sqlite")
# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# Save reference to the table
Measurement = Base.classes.measurement
Station= Base.classes.station
# 2. Create an app
app = Flask(__name__)

@app.route("/")
def index():
    
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/query_user<br/>"
        
    )




def get_date():
    # Create our session (link) from Python to the DB
    session = Session(engine)
# Calculate the date 1 year ago from the last data point in the database
    result_date = session.query(func.max(Measurement.date)) 
    result_date=pd.DataFrame(result_date , columns=['date']) 
    f_date_l=result_date.values.tolist()
    f_date_l[0]
    v_date = ''.join(map(str, f_date_l[0]))
    query_date = datetime.strptime(v_date,'%Y-%m-%d').date()
    query_date = query_date - dt.timedelta(days=364)
    query_date
    return query_date

@app.route("/api/v1.0/precipitation")
def precipitation():
# Create our session (link) from Python to the DB
    session = Session(engine)
# Perform a query to retrieve the data and precipitation scores
    #v_date = query_date 
    v_date=get_date()
    preci_12m = session.query(Measurement.date,func.sum(Measurement.prcp)).\
                          filter(Measurement.date > v_date,Measurement.prcp > 0 ).\
                          group_by(Measurement.date)
    session.close()
    # Create a dictionary from the row data and append to a list of all_tempertures
    all_preci = []
    for name, station in preci_12m:
        preci_dict = {}
        preci_dict["name"] = name
        preci_dict["station"] = station
        all_preci.append(preci_dict)
    return jsonify(all_preci )

@app.route("/api/v1.0/stations")
def stations():
    
# Create our session (link) from Python to the DB
    session = Session(engine)

    """Return a list of all stations names"""
    # Query all passengers
    stat_results = session.query(Station.station,Station.name).all()
    session.close()
    
    # Create a dictionary from the row data and append to a list of all_tempertures
    all_stations = []
    for name, station in stat_results:
        stat_dict = {}
        stat_dict["name"] = name
        stat_dict["station"] = station
        all_stations.append(stat_dict)
    return jsonify(all_stations )
    

@app.route("/api/v1.0/tobs")
def tobs():
    # Create our session (link) from Python to the DB
    session = Session(engine)
    v_date=get_date()
    v_station='USC00519281'
    
    qo_temp_12m = session.query(Measurement.date,func.sum(Measurement.tobs)).\
                          filter(Measurement.date > v_date ,Measurement.tobs > 0 ).\
                          filter(Measurement.station == v_station).\
                          group_by(Measurement.date) 
    session.close()
    # Create a dictionary from the row data and append to a list of all_tempertures
    all_temp = []
    for date, temp in qo_temp_12m:
        temp_dict = {}
        temp_dict["date"] = date
        temp_dict["temp"] = temp
        all_temp .append(temp_dict)
    return jsonify(all_temp )
 


@app.route('/query_user',methods=['POST','GET'])
def query_user():
    if request.method =='POST':
       beg_date =request.form['beg_date']
       end_date  =request.form['end_date']   
       
       b=len(beg_date)
       e=len(end_date)
       #print('val b',b)
       #print('val e',e)
       #print('beg_date',beg_date)
       if b !=0  and  beg_date <= end_date:
           session = Session(engine)
           q_station_mact = session.query(Measurement.station,Station.name,func.min(Measurement.tobs).label('min_temp'),
                               func.max(Measurement.tobs).label('max_temp'),func.avg(Measurement.tobs).label('avg_temp')).\
                               filter(Measurement.date>=beg_date,Measurement.date<=end_date)
           session.close()
                     
           # Create a dictionary from the row data and append to a list of all_tempertures
           all_temp = []
           for station,name,min_temp, max_temp,avg_temp in q_station_mact:
               temp_dict = {}
               temp_dict["station"] = station
               temp_dict["name"] = name
               temp_dict["min_temp"] = min_temp
               temp_dict["max_temp"] = max_temp
               temp_dict["avg_temp"] = avg_temp
               all_temp .append(temp_dict)
               return jsonify(all_temp )
           
       elif b == 0 and e !=0 :
           
           return '<h1> Can not process this request  beg_date is empty   try again <h1>'
       elif e == 0 and b !=0:
           session = Session(engine)
           q_station_mact = session.query(Measurement.station,Station.name,func.min(Measurement.tobs).label('min_temp'),
                               func.max(Measurement.tobs).label('max_temp'),func.avg(Measurement.tobs).label('avg_temp')).\
                               filter(Measurement.date>=beg_date)
           session.close()
           all_temp = []
           for station,name,min_temp, max_temp,avg_temp in q_station_mact:
               temp_dict = {}
               temp_dict["station"] = station
               temp_dict["name"] = name
               temp_dict["min_temp"] = min_temp
               temp_dict["max_temp"] = max_temp
               temp_dict["avg_temp"] = avg_temp
               all_temp .append(temp_dict)
               return jsonify(all_temp )
           return '<h1> end_date == 0 and beg_date !=0<h1>'
       elif beg_date > end_date:
           return '<h1> Can not process this request beg_date > end_date, try again<h1>'
    return '''<form method="POST">
    beg_date <input type ="date" name = "beg_date">
    end_date <input type ="date" name = "end_date">
    <input type = "submit">
    </form>'''
       
      
# 4. Define main behavior
if __name__ == "__main__":
    app.run(debug=True)