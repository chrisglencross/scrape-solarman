import time

import yaml
from datetime import datetime, timedelta, date, timezone

import logging
import retry
import requests
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from requests.auth import HTTPDigestAuth

FORMAT = '%(asctime)s %(levelname)s [%(name)s] %(message)s'
logging.basicConfig(format=FORMAT, level=logging.INFO)

class MyEnergiClient:

    logger = logging.getLogger('MyEnergiClient')

    def __init__(self, login_config):
        self.auth = HTTPDigestAuth(login_config["hub_serial"], login_config["hub_password"])
        self.session = requests.session()
        response = self.session.get(
            'https://director.myenergi.net',
            auth=self.auth,
            timeout=60
        )
        response.raise_for_status()
        self.asn = response.headers['X_MYENERGI-asn']

    def get_zappi_serial(self):
        response = requests.get(f"https://{self.asn}/cgi-jstatus-Z", auth=self.auth)
        zappi_status = response.json()
        return zappi_status["zappi"][0]["sno"]

    @retry.retry(tries=10, delay=1, backoff=2, logger=logger)
    def get_snapshot(self):
        response = requests.get(f"https://{self.asn}/cgi-jstatus-Z", auth=self.auth)
        zappi_status = response.json()
        return zappi_status["zappi"][0]

    @retry.retry(tries=10, delay=1, backoff=2, logger=logger)
    def get_day_data(self, zappi_serial: str, date: str):
        response = self.session.get(f"https://{self.asn}/cgi-jday-Z{zappi_serial}-{date}", auth=self.auth)
        result = []
        for item in response.json()[f"U{zappi_serial}"]:
            minute = item.get("min", 0)
            hour = item.get("hr", 0)
            day = item.get("dom", 0)
            month = item.get("mon", 0)
            year = item.get("yr", 0)
            timestamp = datetime(year, month, day, hour, minute, 0, tzinfo=timezone.utc)
            volts = item.get("v1", 0) / 10.0
            energy = (item.get("h1d", 0) + item.get("h2d", 0) + item.get("h3d", 0) +
                      item.get("h1b", 0) + item.get("h2b", 0) + item.get("h3b", 0))
            watts = (energy / volts) * 4
            result.append({"ts": timestamp, "voltage": volts, "power": watts, "zappi_serial": zappi_serial})
        return result


class InfluxDBWriter:

    logger = logging.getLogger('InfluxDBWriter')

    def __init__(self, influxdb_config):
        self.influxdb_config = influxdb_config
        self.client = InfluxDBClient(**influxdb_config)
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)

    @retry.retry(tries=10, delay=1, logger=logger)
    def write_snapshot(self, snapshot):
        zappi_serial = snapshot['sno']
        day, month, year = snapshot['dat'].split("-")
        timestr = f"{year}-{month}-{day}T{snapshot['tim']}+00:00"
        ts = datetime.fromisoformat(timestr)
        point = Point("zappi").tag("zappi_serial", zappi_serial).time(ts, WritePrecision.S)
        point.field("power", float(snapshot["div"]))
        point.field("voltage", float(snapshot["vol"]))
        self.write_api.write("myenergi", self.client.org, point)

    @retry.retry(tries=10, delay=1, logger=logger)
    def write_day_chart_data(self, zappi_data):
        for ts_entry in zappi_data:
            ts = ts_entry["ts"]
            point = Point("zappi").tag("zappi_serial", ts_entry["zappi_serial"]).time(ts, WritePrecision.S)
            for key in ["voltage", "power"]:
                point.field(key, ts_entry.get(key, 0.0))
            self.write_api.write("myenergi", self.client.org, point)


class ZappiScraper:

    logger = logging.getLogger('ZappiScraper')

    def __init__(self, config):
        self.config = config

        login_config = config["myenergi"]
        self.myenergi = MyEnergiClient(login_config)

        influxdb_config = config["influxdb"]
        self.influxdb = InfluxDBWriter(influxdb_config)

        self.zappi_serial = self.myenergi.get_zappi_serial()

    def process_snapshot(self):
        self.logger.info(f"Processing snapshot")
        snapshot = self.myenergi.get_snapshot()
        self.influxdb.write_snapshot(snapshot)

    def process_day(self, date):
        self.logger.info(f"Processing data for date {date}")
        day_data = self.myenergi.get_day_data(self.zappi_serial, date.strftime("%Y-%m-%d"))
        self.influxdb.write_day_chart_data(day_data)

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


@retry.retry(tries=10, delay=60)
def main():
    with open(".solarman-scraper.yml", "r") as yamlfile:
        config = yaml.load(yamlfile, Loader=yaml.FullLoader)
    scraper = ZappiScraper(config)

    today = date.today()
    backfill_days = 1
    for previous_day in range(backfill_days, 0, -1):
        scraper.process_day(today - timedelta(previous_day))

    while True:

        new_today = date.today()

        # After a date roll do one last scan of the previous day for completeness
        if new_today != today:
            scraper.process_day(today)
            today = new_today

        # Get current value
        scraper.process_snapshot()
        time.sleep(60)


if __name__ == '__main__':
    main()
