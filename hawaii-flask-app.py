from flask import Flask, jsonify
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from sqlalchemy import func
import datetime
from dateutil.relativedelta import relativedelta


engine = create_engine("sqlite:///hawaii_weather.sqlite")

Base = automap_base()

Base.prepare(engine, reflect=True)

Base.classes.keys()

Measurements = Base.classes.measurement
Stations = Base.classes.station

session = Session(engine)

# Find the latest observation, useful for doing queries on the last 12 months of data.
latest_observation = session.query(Measurements.date).order_by(Measurements.date.desc()).first()
latest_obseration_str = latest_observation[0].strftime('%Y-%m-%d')

# Retrieve the last 12 months of precipitation data in the data set.
precipitation_last_12 = session.query(Stations.station_name,\
                                      Stations.location_name\
                                     )\
    .join(Measurements, Stations.station_name==Measurements.station_name)\
    .add_columns(Measurements.precipitation, Measurements.date)\
    .filter(Measurements.precipitation.isnot(None))\
    .filter(Measurements.date > (latest_observation[0] - relativedelta(years=1)).strftime('%Y-%m-%d'))\
    .order_by(Measurements.date).all()

precipitation_last_12_dict = [
    dict( (k,v) for (k,v) in zip(('station_id', 'station_name', 'prcp', 'date'), (x[0], x[1], float(x[2]), x[3].strftime('%Y-%m-%d')) ) )
for x in precipitation_last_12
]

# Retrieve just the station names and details.
station_info = session.query(Stations.station_name,\
                                         Stations.location_name,\
                                         Stations.latitude,\
                                         Stations.longitude,\
                                         Stations.elevation,\
                                        ).all()

stations_labels = ('station_id', 'station_name', 'latitude', 'longitude', 'elevation')

station_info_dict = [
   dict( (k,v) for (k,v) in zip(stations_labels, (str(x[0]), str(x[1]), float(x[2]), float(x[3]), float(x[4])) ) )
for x in station_info
]

print(station_info_dict[-1])

app = Flask(__name__)

# Returns the precipitation observations for the last 12 months of recorded data.
@app.route("/api/v1.0/precipitation")
def last_12_mos_pcrp():
    return jsonify(precipitation_last_12_dict)

# Returns a list of the stations in the Satation table.
@app.route("/api/v1.0/stations")
def stations():
    return jsonify(station_info_dict)





if __name__ == "__main__":
    app.run(debug=True, port=8000)
