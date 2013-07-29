
import math, datetime
from tempodb import Client, DataPoint

client = Client("a755539a9e124278b04f988d39bc5ef9", "43f97dc4dbbc46499bd6694a3455210c")
sin = [math.sin(math.radians(d)) for d in range(0,3600)]
cos = [math.cos(math.radians(d)) for d in range(0,3600)]
start = datetime.datetime(2013,01,01)
sin_data = []
cos_data = []

for i in range(len(sin)):
	sin_data.append(DataPoint(start + datetime.timedelta(minutes=i), sin[i]))
	cos_data.append(DataPoint(start + datetime.timedelta(minutes=i), cos[i]))

client.write_key('type:sin.1',sin_data)
client.write_key('type:cos.1', cos_data)

client.read_key('type:sin.1', start, datetime.datetime(2013,01,05))

attributes={
		"type": "sin"
}

client.read(start, datetime.datetime(2013,01,05), attributes= attributes)

