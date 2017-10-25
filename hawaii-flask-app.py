from flask import Flask, jsonify, request
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from sqlalchemy import func
import datetime
from dateutil.relativedelta import relativedelta


engine = create_engine('sqlite:///hawaii_weather.sqlite')

Base = automap_base()

Base.prepare(engine, reflect=True)

Base.classes.keys()

Measurements = Base.classes.measurement
Stations = Base.classes.station

session = Session(engine)


app = Flask(__name__)

# Returns the precipitation observations for the last 12 months of recorded data.
@app.route('/api/v1.0/precipitation')
def last_12_mos_pcrp():
    # Find the latest observation, useful for doing queries on the last 12 months of data.
    latest_observation = session.query(Measurements.date).filter(Measurements.precipitation.isnot(None)).order_by(Measurements.date.desc()).first()

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

    return jsonify(precipitation_last_12_dict)

# Returns a list of the stations in the Satation table.
@app.route('/api/v1.0/stations')
def stations():
    # Retrieve just the station names and details.
    stations_labels = ('station_id', 'station_name', 'latitude', 'longitude', 'elevation')

    station_info = session.query(Stations.station_name,\
                                             Stations.location_name,\
                                             Stations.latitude,\
                                             Stations.longitude,\
                                             Stations.elevation,\
                                            ).all()

    station_info_dict = [
       dict( (k,v) for (k,v) in 
            zip(stations_labels, (str(x[0]), str(x[1]), float(x[2]), float(x[3]), float(x[4])) ) )
    for x in station_info
    ]
    return jsonify(station_info_dict)

# Return a json list of Temperature Observations (tobs) for the previous year
@app.route('/api/v1.0/tobs')
def last_12_mos_tobs():
    # Find the latest observation, useful for doing queries on the last 12 months of data.
    latest_observation = session.query(Measurements.date).order_by(Measurements.date.desc()).first()

    # Retrieve the last 12 months of temperature observations data in the data set.
    precipitation_last_12 = session.query(Stations.station_name,\
                                            Stations.location_name\
                                            )\
        .join(Measurements, Stations.station_name==Measurements.station_name)\
        .add_columns(Measurements.temperature, Measurements.date)\
        .filter(Measurements.temperature.isnot(None))\
        .filter(Measurements.date > (latest_observation[0] - relativedelta(years=1)).strftime('%Y-%m-%d'))\
        .order_by(Measurements.date).all()

    precipitation_last_12_dict = [
    dict( 
        (k,v) 
        for (k,v) 
        in zip(('station_id', 'station_name', 'temp', 'date'), (x[0], x[1], float(x[2]), x[3].strftime('%Y-%m-%d')) ) )
    for x in precipitation_last_12
    ]

    return jsonify(precipitation_last_12_dict)

# Return a json list of the minimum temperature, the average temperature, and the max temperature for a given start or start-end range.
# When given the start only, calculate TMIN, TAVG, and TMAX for all dates greater than and equal to the start date.
# When given the start and the end date, calculate the TMIN, TAVG, and TMAX for dates between the start and end date inclusive.
@app.route("/api/v1.0/normals")
def normals():
    start = request.args.get('start')
    end = request.args.get('end')
    error = 0

    def calc_normals(start_date, end_date=None):
        if end_date == None:
            end_date = datetime.datetime.now().strftime("%Y-%m-%d")
        else:
            end_date
        
        labels = ['date','min', 'max', 'avg']

        normals = session.query(
            Measurements.date,
            func.min(Measurements.temperature),
            func.max(Measurements.temperature),
            func.avg(Measurements.temperature)
            )\
            .group_by(Measurements.date)\
            .filter(Measurements.date >= start_date, Measurements.date <= end_date).all()
        l = [[x[0].strftime('%Y-%m-%d'), float(x[1]), float(x[2]), float(x[3])] for x in normals]
        d = [dict((k,v) for (k,v) in zip(labels,x)) for x in l]
        return d

    if end is None:
        try:
            datetime.datetime.strptime(start, '%Y-%m-%d')
        except ValueError:
            error = 1
    else:
        try:
            datetime.datetime.strptime(start, '%Y-%m-%d')
            datetime.datetime.strptime(end, '%Y-%m-%d')

        except ValueError:
            error = 1

    if error > 0:
        return jsonify({'Error' : 'Start or end date is misformatted, use format \'2017-12-31\''})
    else:
        response = calc_normals(start, end)
        return jsonify(response)

if __name__ == '__main__':
    app.run(debug=True, port=8000)
