import logging
import time

import retry
import hyundai_kia_connect_api as kia
import yaml
import requests
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

FORMAT = '%(asctime)s %(levelname)s [%(name)s] %(message)s'
logging.basicConfig(format=FORMAT, level=logging.INFO)

# Monkeypatch requests library to add a timeout
base_requests_post = requests.post
def requests_post(url, **kwargs):
  return base_requests_post(url, **kwargs, timeout=30)
requests.post = requests_post

base_requests_get = requests.get
def requests_get(url, **kwargs):
  return base_requests_get(url, **kwargs, timeout=30)
requests.get = requests_get


class KiaConnectClient:

    logger = logging.getLogger('KiaConnectClient')

    def __init__(self, kia_config):
        self.vehicle_manager = kia.VehicleManager(region=kia_config["region"], brand=kia_config["brand"],
                                username=kia_config["username"], password=kia_config["password"], pin=kia_config["pin"])
        self.logger.info(f"Logging in to KIA as {kia_config['username']}")
        self.vehicle_manager.check_and_refresh_token()
        self.logger.info("Logged in successfully")

    @retry.retry(tries=10, delay=1, backoff=2, logger=logger)
    def get_snapshot(self):
        self.vehicle_manager.check_and_refresh_token()
        self.vehicle_manager.check_and_force_update_vehicles(60 * 60)
        return self.vehicle_manager.vehicles


class InfluxDBWriter:

    logger = logging.getLogger('InfluxDBWriter')

    def __init__(self, influxdb_config):
        self.influxdb_config = influxdb_config
        self.client = InfluxDBClient(**influxdb_config)
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)

    @retry.retry(tries=10, delay=1, logger=logger)
    def write_snapshot(self, snapshot):
        for car in snapshot.values():

            self.logger.info(f"Updating {car.name} at {car.last_updated_at}")
            point = Point("kia").tag("id", car.id).tag("name", car.name).time(car.last_updated_at, WritePrecision.S)
            point.field("odometer", float(car.odometer))
            point.field("ev_battery_percentage", int(car.ev_battery_percentage))
            point.field("ev_battery_is_charging", bool(car.ev_battery_is_charging))
            point.field("ev_battery_is_plugged_in", int(car.ev_battery_is_plugged_in))  # should really be bool but some data persistent as int
            point.field("12v_battery_percentage", int(car.data["vehicleStatus"]["battery"].get("batSoc", -1)))
            point.field("12v_battery_state", int(car.data["vehicleStatus"]["battery"].get("batState", -1)))
            self.write_api.write("kia_connect", self.client.org, point)


class KiaScraper:

    logger = logging.getLogger('KiaScraper')

    def __init__(self, config):
        self.config = config

        kia_config = config["kia"]
        self.kia_connect = KiaConnectClient(kia_config)

        influxdb_config = config["influxdb"]
        self.influxdb = InfluxDBWriter(influxdb_config)

    def process_snapshot(self):
        self.logger.info(f"Processing snapshot")
        snapshot = self.kia_connect.get_snapshot()
        self.influxdb.write_snapshot(snapshot)


def sleep(seconds):
    """Like time.sleep(seconds) but shortening the sleep if the computer suspends and rewakes."""
    finish_at = time.time() + seconds
    while True:
        time_remaining = finish_at - time.time()
        if time_remaining <= 0:
            return
        time.sleep(min(time_remaining, 60))


# @retry.retry(tries=10, delay=60)
def main():
    with open(".solarman-scraper.yml", "r") as yamlfile:
        config = yaml.load(yamlfile, Loader=yaml.FullLoader)
    scraper = KiaScraper(config)

    while True:
        # Get current value (KIA throttles maximum number of checks per day)
        scraper.process_snapshot()
        sleep(30 * 60)


if __name__ == '__main__':
    main()
