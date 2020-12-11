# %%
# Import dependencies
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
import json
import flask
from flask import Flask, jsonify

# %%
# Create engine for the database
engine = create_engine('sqlite:///Resources/hawaii.sqlite')

# %%
app = Flask(__name__)

# %%
@app.route("/")
def welcome():
    return (
        f"Welcome to Surf's Up: Hawaii's Temperature API!<br/>"
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/start<br/>"
        f"/api/v1.0/start/end<br/>"
    )

# %%
# Return a JSON for precipitation
@app.route('/api/v1.0/precipitation')
def prcp():
    con = engine.connect()
    
    query = '''
        SELECT
            date,
            AVG(prcp) as avg_prcp
        FROM
            measurement
        WHERE
            date >= (SELECT DATE(MAX(date), '-1 year') FROM measurement)
        GROUP BY
            date
        ORDER BY
            date
'''
    
    prcp_df = pd.read_sql(query, con)
    prcp_df['date'] = pd.to_datetime(prcp_df['date'])
    prcp_df.sort_values('date')
    prcp_df.set_index('date', inplace = True)
    prcp_json = prcp_df.to_json(orient= 'records', date_format = 'iso')
    con.close()
    
    return prcp_json

#%%
# Return JSON list of stations
@app.route('/api/v1.0/stations')
def sttn():
    con = engine.connect()
    
    query = '''
        SELECT
            s.station AS station,
            s.name AS station_name
        FROM
            measurement m
            INNER JOIN station s
            ON m.station = s.station
        GROUP By
            s.station,
            s.name
'''
    
    activestation_df = pd.read_sql(query, con)
    activestation_json = activestation_df.to_json(orient = 'records')
    con.close()
    
    return activestation_json

#%%
# Return a JSON list for temperature observations
@app.route('/api/v1.0/tobs')
def tempobs():
    con = engine.connect()
    
    query = '''
        SELECT
            s.station AS station,
            s.name AS station_name,
            COUNT(*) AS station_count
        FROM
            measurement m
            INNER JOIN station s
            ON m.station = s.station
        GROUP BY
            s.station,
            s.name
        ORDER BY
            station_count DESC
'''

    activestation_df = pd.read_sql(query, con)
    activestation_df.sort_values('station_count', ascending=False, inplace=True)
    most_active = activestation_df['station'].values[0]
    
    query = f'''
        SELECT
            tobs
        FROM
            measurement
        WHERE
            station = '{most_active}'
            AND
            date >= (SELECT DATE(MAX(date), '-1 year') FROM measurement)
'''
    con.close()
    
    return tempobs_tobs_json

#%%
# Return a JSON list of the minimum temperature, the average temperature,
# and the max temperature for a given start AND start-end range
@app.route('/api/v1.0/<start>')
def datestats_noend(start):
    con = engine.connect()
    
    query = f'''
        SELECT
            MIN(tobs) AS TMIN,
            MAX(tobs) AS TMAX,
            AVG(tobs) AS TAVG
        FROM
            measurement
        WHERE
            date >= '{start}'
'''
    
    datestats_noend_df = pd.read_sql(query, con)
    datestats_noend_json = datestats_noend_df.to_json(orient = 'records')
    con.close()
    
    return datestats_noend_json

#%%
@app.route('/api/v1.0/<start>/<end>')
def datestats(start, end):
    con = engine.connect()
    
    query = f'''
        SELECT
            MIN(tobs) AS TMIN,
            MAX(tobs) AS TMAX,
            AVG(tobs) AS TAVG
        FROM
            measurement
        WHERE
            date BETWEEN '{start}' AND '{end}'
'''
    
    datestats_df = pd.read_sql(query, con)
    datestats_json = datestats_df.to_json(orient = 'records')
    con.close()
    
    return datestats_json

#%%
if __name__ == '__main__':
    app.run(debug=True)