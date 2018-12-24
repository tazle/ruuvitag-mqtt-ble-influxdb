from ruuvitag_sensor.decoder import get_decoder

from hbmqtt.client import MQTTClient, ConnectException, ClientException
from hbmqtt.mqtt.constants import QOS_0
import os
import json
import asyncio
import sys
import argparse
from influxdb import InfluxDBClient

parser = argparse.ArgumentParser(description='Send RuuviTag observations to InfluxDB.')
parser.add_argument('--test', dest='test', action='store_const',
                    const=True, default=False,
                    help="Test mode, don't send to Influx")
parser.add_argument('--quiet', dest='quiet', action='store_const',
                    const=True, default=False,
                    help="Quiet mode, don't print messages to stdout")
args = parser.parse_args()

mappings = sys.stdin.read()
try:
    NAMES = json.loads(mappings)
except Exception as e:
    print("Unable to load mappings", mappings, file=sys.stderr)
    NAMES = {}

def convert_to_influx(receiver_mac, mac, payload):
    return {
        "measurement": "ruuvitag",
        "tags": {
            "mac": mac,
            "name": NAMES.get(mac, "Unknown")
        },
        "fields": {
            "receiver_mac": receiver_mac,
            "temperature": payload["temperature"],
            "humidity": payload["humidity"],
            "pressure": payload["pressure"],
            "battery": payload["battery"],
            "acceleration": payload["acceleration"],
            "acceleration_x": payload["acceleration_x"],
            "acceleration_y": payload["acceleration_y"],
            "acceleration_z": payload["acceleration_z"]
        }
    }

influx_client = InfluxDBClient(host=os.environ.get("INFLUXDB_HOST","localhost"), port=int(os.environ.get("INFLUXDB_PORT", "8086")), database="tag_data", timeout=10)
try:
    client.create_database('tag_data')
except Exception as e:
    print("Unable to create database", e, file=sys.stderr)

ignored_macs = set()
def handle_message(receiver_mac, mac, rssi, data, raw_msg):
    if data is None:
        if mac not in ignored_macs:
            print("Got null data from %s" % mac, raw_msg)
            ignored_macs.add(mac)
        return

    if len(data) < 2:
        print("Got too short message from %s, length: %d" %( mac, len(data)))
        return

    if data[0:2] != bytes.fromhex("9904"):
        if mac not in ignored_macs:
            print("Got non-ruuvitag message from %s" % mac)
            ignored_macs.add(mac)
        return

    if len(data) < 3:
        print("Unexpected message with ruuvitag manufacturer code from: %s, data: %s" %(mac, data.hex()))
        return

    format = data[2]
    payload = get_decoder(format).decode_data(data[2:].hex())
    if payload is not None:
        json_body = convert_to_influx(receiver_mac, mac, payload)
        if not args.quiet:
            print(json_body)
        if not args.test:
            influx_client.write_points([json_body])


mqtt_url = os.environ.get("MQTT_URL", "mqtt://localhost/")
mqtt_topic = os.environ.get("MQTT_TOPIC", "/home/ble-deduped")

client = MQTTClient()

async def main():
    try:
        await client.connect(mqtt_url)
        print("Connected")
    except Exception as e:
        print("Connection failed: %s" % e)
        asyncio.get_event_loop().stop()
    await client.subscribe([
        (mqtt_topic, QOS_0),
        ])
    print("Subscribed")
    while True:
        message = await client.deliver_message()
        data = message.data
        ble_msg = json.loads(data)
        receiver_mac = ble_msg['receiver_mac']
        mac = ble_msg['address']['address']
        rssi = ble_msg['rssi']
        data = ble_msg['service_data'] or ble_msg['mfg_data']
        if data is not None:
            data = bytes(data, 'iso-8859-1') # it was encoded as codepoints for transport over JSON

        # TODO Should be async, but doesn't matter much with a handful of ruuvitags
        handle_message(receiver_mac, mac, rssi, data, ble_msg)
        
        
asyncio.ensure_future(main())
asyncio.get_event_loop().run_forever()
