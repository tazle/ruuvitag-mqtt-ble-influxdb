# Build

`docker build -t ruuvitag-mqtt-ble-influxdb:latest .`

# Run

`docker run --rm -i -v $PWD/mac-mapping.json:/mac-mapping.json --link influxdb.service:influx -e INFLUXDB_HOST=influx -e MQTT_URL=mqtt://your-mqtt-host/ ruuvitag-mqtt-ble-influxdb:latest python3 mqtt-to-influx.py --mapping-file=/mac-mapping.json`
