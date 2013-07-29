from tempodb import Client, DataPoint, DataSet
import datetime
import json
import numpy as np
import pandas as pd
import statsmodels.api as sm
import pylab
import matplotlib.pyplot as plt
API_KEY="68ba2c28675945248b907168ff69b5f5"
API_SECRET= "65577cb6e5684d0b9a71eb9a6a551709"
client = Client(API_KEY, API_SECRET, host='api-staging.tempo-db.com')


class MyEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, DataPoint):
            return obj.to_json()

        return json.JSONEncoder.default(self, obj)


def pressure_series():
	attrs = {"sensor-type":"pressure"}
	pressure_series = client.get_series(attributes= attrs)
	start = datetime.datetime(2000,01,01)
	end = datetime.datetime(2000,01,30)
	
	for series in pressure_series:
		key = series.key
		dset=client.read_key(key,start,end)
		print key, dset.summary.mean, dset.summary.count

def precip_series():
	attrs = {"sensor-type":"precipitation",
			"precipitation_type":"R"}
	precip_series = client.get_series(attributes= attrs)
	start = datetime.datetime(2000,01,01)
	end = datetime.datetime(2000,01,30)
	
	for series in precip_series:
		key = series.key
		dset=client.read_key(key,start,end)
		print key, dset.summary.mean, dset.summary.count

def temp_series():
	attrs = {"sensor-type":"temperature",
			"temp_type":"dew-pt"}
	temp_series = client.get_series(attributes= attrs)
	start = datetime.datetime(2000,01,01)
	end = datetime.datetime(2000,01,30)
	
	for series in temp_series:
		key = series.key
		dset=client.read_key(key,start,end)
		print key, dset.summary.mean, dset.summary.count

def get_stations_keys():
	all_series = client.get_series()
	stations = {}
	for s in all_series:
		curr_station_id = s.attributes['station-id']
		if not stations.has_key(curr_station_id):
			stations[curr_station_id] = {}
	print len(stations)
	for k,v in stations.iteritems():
		attrs = {"sensor-type":"pressure", "station-id": k, "sensor-number":0}
		pressure_series = client.get_series(attributes=attrs)
		try:
			v['pressure_key'] = pressure_series[0].key
		except:
			print "no pressure"
			v['pressure_key'] = ""

		attrs = {"sensor-type":"precipitation", "precipitation_type":'R', "station-id": k, "sensor-number":0}
		precip_series = client.get_series(attributes=attrs)
		try:
			v['precip_key'] = precip_series[0].key
		except:
			print "no precip"
			v['precip_key']  = ""

		attrs = {"sensor-type":"temperature", "temp_type":'dew-pt', "station-id": k, "sensor-number":0}
		precip_series = client.get_series(attributes=attrs)
		try:
			v['temp_key'] = precip_series[0].key
		except:
			print "no precip"
			v['temp_key']  = ""
	json.dump(stations, open('stations_keys.json','w'))

	#print stations


def get_avg_for_interval(key, interval):
	start = datetime.datetime(2000,1,1)
	end = datetime.datetime(2001,1,1)
	try:
		dset = client.read_key(key, start, end, interval=interval,function='mean')
		return dset.data
	except:
		return []

#get_stations_keys()

def pull_full_data():
	stations=json.load(open('stations_keys.json'))
	for station_id in stations.iterkeys():
		stations[station_id]['pressure_data'] = get_avg_for_interval(stations[station_id]['pressure_key'], "1day")
		stations[station_id]['precipitation_data'] = get_avg_for_interval(stations[station_id]['precip_key'], "1day")
		stations[station_id]['temperature_data'] = get_avg_for_interval(stations[station_id]['temp_key'], "1day")

	json.dump(stations, open('stations_with_data.json','w'), cls = MyEncoder)

#pull_full_data()
def prepare_dataframe(pressure, precip, temp):
	pressure_series = pd.Series([x['v'] for x in pressure], index=[x['t'] for x in pressure])
	precip_series = pd.Series([x['v'] for x in precip], index=[x['t'] for x in precip])
	temp_series = pd.Series([x['v'] for x in temp], index=[x['t'] for x in temp])
	frame = pd.DataFrame({ 'pressure': pressure_series, 'precip' : precip_series, 'temperature': temp_series})
	frame = frame.dropna()
	return frame

def main():
	stations = json.load(open('stations_with_data.json'))
	rsq = []
	for station_id in stations.keys():
		pressure = stations[station_id]['pressure_data']
		precip = stations[station_id]['precipitation_data']
		temp = stations[station_id]['temperature_data']
		frame = prepare_dataframe(pressure, precip, temp)
		try:
			rsq.append((station_id, do_analysis(station_id, frame)))
		except Exception as e:
			print e 
	rsq.sort(key= lambda x: x[1])
	rsqs = [r[1] for r in rsq]
	pylab.hist(rsqs, bins = 50)
	plt.show()
	print rsq

def do_analysis(s_id,frame):
	x1 = frame.pressure.values
	x2 = frame.temperature.values
	x = np.column_stack((x1,x2))
	#x = x1
	y = frame.precip.values
	X = sm.add_constant(x, prepend=True)
	res = sm.OLS(y,X).fit()
	return res.rsquared

main()