# Build

`docker build -t ruuvitag-mqtt-ble-influxdb:latest .`

# Run

`docker run --rm -i -e PYTHONUNBUFFERED=1 --link influxdb.service:influx -e INFLUXDB_HOST=influx -e MQTT_URL=mqtt://your-mqtt-host/ ruuvitag-mqtt-ble-influxdb:latest python3 mqtt-to-influx.py < mac-mapping.json`
