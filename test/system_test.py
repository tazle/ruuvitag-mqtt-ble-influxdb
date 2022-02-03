import os
import subprocess
system = __import__('mqtt-to-influx')
import pytest

import paho.mqtt.client as mqtt
import multiprocessing
import time
import json

from influxdb import InfluxDBClient

TEST_TOPIC = "/home/ble-deduped"
WAIT_SECONDS = 5

@pytest.mark.skipif(os.environ.get("HAVE_MQTT", None) is None, reason="Need MQTT environment")
@pytest.mark.skipif(os.environ.get("HAVE_INFLUXDB", None) is None, reason="Need InfluxDB environment")
def test_write_ruuvi_data_to_influx(mqtt_broker, influxdb):
    hostname, port = mqtt_broker
    influx_host, influx_port = influxdb
    influx_client = InfluxDBClient(host=influx_host, port=influx_port, database="tag_data", timeout=10)
    
    received_dedupes = []
    test_messages = []
    client = mqtt.Client()
    
    try:
        client.loop_start()
        client.connect(hostname, port, 60)

        shovel = multiprocessing.Process(target=lambda: system.init({"FB:72:49:EA:C7:4A":"test"}), daemon=True)
        shovel.start()

        payload = json.loads(r'{"uuid16s": [], "address": {"address": "FB:72:49:EA:C7:4A"}, "svc_data_uuid32": null, "uuid32s": [], "service_data": null, "svc_data_uuid128": null, "rssi": null, "type": "ADV_NONCONN_IND", "uri": null, "adv_itvl": null, "svc_data_uuid16": null, "uuid128s": [], "receiver_mac": "B8:27:EB:8E:F1:12", "tx_pwr_lvl": null, "raw_data": null, "appearance": null, "address_type": "RANDOM", "public_tgt_addr": null, "_name": null, "flags": 6, "name_is_complete": false, "mfg_data": "\u0099\u0004\u0003\u00c6\u0001?\u00ca\u00dd\u00ff\u00e8\u0000\u0015\u0003\u00fe\u000b;"}')

        wait_start = time.time()
        while time.time() - wait_start < 5:
            # Publishing repeatedly to make sure the MQTT Influx converter gets it when it comes online
            client.publish(TEST_TOPIC, json.dumps(payload))
            print("Published test message")
            try:
                response = influx_client.query('SELECT time, last(battery) FROM ruuvitag;')
                print("Response:", response, len(response))
                if len(response) > 0:
                    print("Got data: " + str(response))
                    break
                else:
                    print("No data")
            except Exception as e:
                print("Got exception", e)
                pass
            time.sleep(.1)
        else:
            pytest.fail("Unable to read data from InfluxDB")
    finally:
        client.loop_stop()
        shovel.kill()

@pytest.mark.skipif(os.environ.get("HAVE_MQTT", None) is None, reason="Need MQTT environment")
@pytest.mark.skipif(os.environ.get("HAVE_INFLUXDB", None) is None, reason="Need InfluxDB environment")
def test_dockerized_write_ruuvi_data_to_influx(docker_image, mqtt_broker, influxdb):
    hostname, port = mqtt_broker
    influx_host, influx_port = influxdb
    influx_client = InfluxDBClient(host=influx_host, port=influx_port, database="tag_data", timeout=10)
    
    received_dedupes = []
    test_messages = []
    client = mqtt.Client()
    
    try:
        client.loop_start()
        client.connect(hostname, port, 60)

        time.sleep(5)
        mqtt_url = "mqtt://mqtt:1883"
        cmd = ["docker", "run", "--rm", "--link", "test_influx:influx", "--link", "test_vernemq:mqtt", "-e", "MQTT_URL=" + mqtt_url, "-e", "INFLUXDB_HOST=influx", docker_image, "python3", "mqtt-to-influx.py", "--mapping-file=/test-mac-mapping.json"]
        print(" ".join(cmd))
        shovel = multiprocessing.Process(target=lambda: subprocess.check_output(cmd), daemon=True)
        shovel.start()

        payload = json.loads(r'{"uuid16s": [], "address": {"address": "FB:72:49:EA:C7:4A"}, "svc_data_uuid32": null, "uuid32s": [], "service_data": null, "svc_data_uuid128": null, "rssi": null, "type": "ADV_NONCONN_IND", "uri": null, "adv_itvl": null, "svc_data_uuid16": null, "uuid128s": [], "receiver_mac": "B8:27:EB:8E:F1:12", "tx_pwr_lvl": null, "raw_data": null, "appearance": null, "address_type": "RANDOM", "public_tgt_addr": null, "_name": null, "flags": 6, "name_is_complete": false, "mfg_data": "\u0099\u0004\u0003\u00c6\u0001?\u00ca\u00dd\u00ff\u00e8\u0000\u0015\u0003\u00fe\u000b;"}')

        wait_start = time.time()
        while time.time() - wait_start < 5:
            # Publishing repeatedly to make sure the MQTT Influx converter gets it when it comes online
            client.publish(TEST_TOPIC, json.dumps(payload))
            print("Published test message")
            try:
                response = influx_client.query('SELECT time, last(battery) FROM ruuvitag;')
                print("Response:", response, len(response))
                if len(response) > 0:
                    print("Got data: " + str(response))
                    break
                else:
                    print("No data")
            except Exception as e:
                print("Got exception", e)
                pass
            time.sleep(.1)
        else:
            pytest.fail("Unable to read data from InfluxDB")
    finally:
        client.loop_stop()
        shovel.kill()

@pytest.fixture(scope="function")
def docker_image():
    import random
    n = random.randint(0, 10000)
    tag = "test-" + str(n)
    image = subprocess.check_output(["docker", "build", ".", "-t", tag])
    return tag

@pytest.fixture(scope="function")
def mqtt_broker(monkeypatch):
    vernemq = multiprocessing.Process(target=lambda: subprocess.run(["docker", "run", "--rm", "--name", "test_vernemq", "-e", "DOCKER_VERNEMQ_ALLOW_ANONYMOUS=on", "-e", "DOCKER_VERNEMQ_ACCEPT_EULA=yes", "-p", "1883", "vernemq/vernemq:1.12.3"], check=True), daemon=True)
    vernemq.start()
    wait_start = time.time()
    while not 'test_vernemq' in subprocess.check_output(["docker", "ps"]).decode("utf-8"):
        if time.time() > wait_start + 5:
            break
        print("Tester waiting for VerneMQ to start")
        time.sleep(1)
    port = subprocess.check_output(["docker", "port", "test_vernemq", "1883"]).decode("utf-8").split("\n")[0].split(":")[1]
    monkeypatch.setenv("MQTT_URL", "mqtt://localhost:" + port + "/")
    yield "localhost", int(port)
    subprocess.run(["docker", "kill", "test_vernemq"])
    vernemq.kill()

@pytest.fixture(scope="function")
def influxdb(monkeypatch):
    influxdb = multiprocessing.Process(target=lambda: subprocess.run(["docker", "run", "--rm", "-d", "--name", "test_influx", "-p", "8086", "influxdb:1.8.10"], check=True), daemon=True)
    influxdb.start()
    wait_start = time.time()
    while not 'test_influx' in subprocess.check_output(["docker", "ps"]).decode("utf-8"):
        if time.time() > wait_start + 5:
            break
        print("Tester waiting for InfluxDB to start")
        time.sleep(1)
    port = subprocess.check_output(["docker", "port", "test_influx", "8086"]).decode("utf-8").split("\n")[0].split(":")[1]
    monkeypatch.setenv("INFLUXDB_HOST", "localhost")
    monkeypatch.setenv("INFLUXDB_PORT", port)
    yield "localhost", int(port)
    subprocess.run(["docker", "kill", "test_influx"])
    influxdb.kill()
