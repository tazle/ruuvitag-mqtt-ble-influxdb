from ruuvitag_sensor.decoder import get_decoder

from amqtt.client import MQTTClient, ConnectException, ClientException
from amqtt.mqtt.constants import QOS_0
import os
import json
import asyncio
import sys
import argparse
from influxdb import InfluxDBClient
import time

parser = argparse.ArgumentParser(description='Send RuuviTag observations to InfluxDB.')
parser.add_argument('--test', dest='test', action='store_const',
                    const=True, default=False,
                    help="Test mode, don't send to Influx")
parser.add_argument('--quiet', dest='quiet', action='store_const',
                    const=True, default=False,
                    help="Quiet mode, don't print messages to stdout")
parser.add_argument('--mapping-file', dest='mapping_file', action='store', default='-',
                    help="File to read MAC mappings from, defaults to stdin")
args = parser.parse_args()


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


ignored_macs = set()
def handle_message(influx_client, receiver_mac, mac, rssi, data, raw_msg):
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



last_receive_timestamp = time.time()
async def main(influx_client):
    mqtt_url = os.environ.get("MQTT_URL", "mqtt://localhost/")
    mqtt_topic = os.environ.get("MQTT_TOPIC", "/home/ble-deduped")

    client = MQTTClient()

    print("Connecting", mqtt_url)
    global last_receive_timestamp
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
        handle_message(influx_client, receiver_mac, mac, rssi, data, ble_msg)
        last_receive_timestamp = time.time()


MQTT_RECEIVE_TIMEOUT = 30

async def watchdog():
    print("Starting watchdog")
    while True:
        now = time.time()
        since_last_receive = (now - last_receive_timestamp)

        if since_last_receive > MQTT_RECEIVE_TIMEOUT:
            print("No new BLE data received in %d seconds, exiting" %MQTT_RECEIVE_TIMEOUT)
            sys.exit(1)

        await asyncio.sleep(1)

def init(_NAMES):
    global NAMES
    NAMES = _NAMES
    influx_client = InfluxDBClient(host=os.environ.get("INFLUXDB_HOST","localhost"), port=int(os.environ.get("INFLUXDB_PORT", "8086")), database="tag_data", timeout=10)
    try:
        influx_client.create_database('tag_data')
    except Exception as e:
        print("Unable to create database", e, file=sys.stderr)

    asyncio.ensure_future(watchdog())
    asyncio.ensure_future(main(influx_client))
    asyncio.get_event_loop().run_forever()

if __name__ == '__main__':
    mapping_file_name = args.mapping_file
    if mapping_file_name == '-':
        mappings = sys.stdin.read()
    else:
        with open(mapping_file_name, 'r') as file:
            mappings = file.read()
    try:
        NAMES = json.loads(mappings)
        print(NAMES)
    except Exception as e:
        print("Unable to load mappings", mappings, file=sys.stderr)
        NAMES = {}
    init(NAMES)

