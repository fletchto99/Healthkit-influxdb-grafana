import json
import sys
import socket
import logging
import os
from flask import request, Flask
from influxdb_client import InfluxDBClient, Point, WriteOptions
from geolib import geohash

logger = logging.getLogger("console-output")
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

app = Flask(__name__)
app.debug = True


def get_os_or_fail(env_var):
    res = os.getenv(env_var)
    if res is None:
        sys.exit("Environment variable %s is not set", env_var)
    return res


def get_os_or_default(env_var, default):
    res = os.getenv(env_var)
    if res is None:
        return default
    return res


INFLUX_HOST = get_os_or_fail('INFLUX_HOST')
INFLUX_PORT = get_os_or_fail('INFLUX_PORT')
INFLUX_ORG = get_os_or_fail("INFLUX_ORG")
INFLUX_BUCKET = get_os_or_fail("INFLUX_BUCKET")
INFLUX_TOKEN = get_os_or_fail('INFLUX_TOKEN')
DEBUG_MODE = get_os_or_default('DEBUG_MODE', False)

if DEBUG_MODE:
    logger.info("Debug mode enabled")

client = InfluxDBClient(
    url=f"http://{INFLUX_HOST}:{INFLUX_PORT}", token=INFLUX_TOKEN, org=INFLUX_ORG)


@app.route('/collect', methods=['POST', 'GET'])
def collect():
    logger.info("Request received")

    healthkit_data = None

    try:
        healthkit_data = json.loads(request.data)
    except:
        return "Invalid JSON Received", 400

    if DEBUG_MODE:
        logger.info("Received Data: %s", healthkit_data)

    try:
        with client.write_api(write_options=WriteOptions(batch_size=10_000, flush_interval=5_000)) as write_api:
            logger.info("Ingesting Metrics")
            for metric in healthkit_data.get("data", {}).get("metrics", []):
                for datapoint in metric["data"]:
                    metric_fields = set(datapoint.keys())
                    metric_fields.discard("date")
                    metric_fields.discard("startDate")
                    metric_fields.discard("endDate")

                    measurement_name = metric["name"]
                    if metric["name"] == 'sleep_analysis':
                        measurement_name += "_" + datapoint["value"].lower()

                    point = Point(measurement_name)

                    for mfield in metric_fields:
                        if isinstance(datapoint[mfield], int) or isinstance(datapoint[mfield], float):
                            point.field(mfield, float(datapoint[mfield]))
                        else:
                            point.tag(mfield, datapoint[mfield])

                    point.time(
                        datapoint["date"] if "date" in datapoint else datapoint["endDate"])

                    write_api.write(
                        org=INFLUX_ORG, bucket=INFLUX_BUCKET, record=point)

            logger.info("Done Ingesting Metrics")
            logger.info("Ingesting Workouts")

            for workout in healthkit_data.get("data", {}).get("workouts", []):
                for gps_point in workout["route"]:
                    point = Point("workouts")

                    point.tag("id", workout["name"] + "-" +
                              workout["start"] + "-" + workout["end"])

                    point.field("lat", gps_point["lat"])
                    point.field("lng", gps_point["lon"])
                    point.field("geohash", geohash.encode(
                        gps_point["lat"], gps_point["lon"], 7))

                    point.time(gps_point["timestamp"])

                    write_api.write(
                        org=INFLUX_ORG, bucket=INFLUX_BUCKET, record=point)

            logger.info("Done Ingesting Workouts")
    except:
        logger.exception("Caught Exception. See stacktrace for details.")
        return "Server Error", 500

    return "Success", 200


if __name__ == "__main__":
    hostname = socket.gethostname()
    ip_address = socket.gethostbyname(hostname)
    logger.info("Local Network Endpoint: http://%s/collect", ip_address)
    app.run(host='0.0.0.0', port=5353)
