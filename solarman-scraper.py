import json
import time
import yaml
from datetime import datetime, timedelta, date

import logging
import retry
import requests
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

FORMAT = '%(asctime)s %(levelname)s [%(name)s] %(message)s'
logging.basicConfig(format=FORMAT, level=logging.INFO)

TIMESTAMP_FIELDS = {
    'energy_batter_',
    'energy_batter_in',
    'energy_batter_out',
    'power',
    'power_',
    'power_buy',
    'power_sell',
    'power_useage',
}

DAY_SUMMARY_FIELDS = {
    'energy',
    'energy_batter_in',
    'energy_batter_out',
    'energy_buy',
    'energy_sell',
    'energy_useage',
    'energy_useage_buy',
    'energy_useage_gen',
    'energy_useage_out',
    'self_energy_in',
    'self_energy_sell',
    'selfuseage',
}


class SolarmanClient:

    logger = logging.getLogger('SolarmanClient')

    def __init__(self, login_config):
        self.login_config = login_config
        self.session = requests.session()
        self.login()

    def login(self):
        self.session.post(
            'https://home.solarman.cn/cpro/login/validateLogin.json',
            data={
                     "userName": self.login_config["username"],
                     "password": self.login_config["password"],
                     "lan": self.login_config.get("lan", 2),
                     "userType": self.login_config.get("userType", "C"),
                     "domain": self.login_config["domain"]
                 },
            timeout=60
        )

    @retry.retry(tries=5, delay=1, backoff=2, logger=logger)
    def get_plant_snapshot(self, plant):
        plant_id = plant["plant_id"]
        r = self.session.post(
            'https://home.solarman.cn/cpro/epc/plantDetail/showPlantDetailAjax.json',
            data={"plantId": plant_id},
            timeout=60
        )
        data = r.json()
        return {
            "plantId": plant_id,
            "timezoneId": data["result"]["plantAllWapper"]["plant"]["timezoneId"],
            "plantData": data["result"]["plantAllWapper"]["plantData"]
        }

    @retry.retry(tries=5, delay=1, backoff=2, logger=logger)
    def get_day_data(self, plant, date: str):
        plant_id = plant["plant_id"]
        r = self.session.post(
            'https://home.solarman.cn/cpro/epc/plantDetail/showCharts.json',
            data={"plantId": plant_id, "type": 1, "date": date, "plantTimezoneId": plant["timezone_id"]},
            timeout=60
        )
        data = r.json()
        return {
            "plantId": plant_id,
            "daySummary": data["result"]["plantSta"],
            "chartData": data["result"]["chartsDataAll"]
        }

    @retry.retry(tries=5, delay=1, backoff=2, logger=logger)
    def get_month_data(self, plant, month: str):
        plant_id = plant["plant_id"]
        r = self.session.post(
            'https://home.solarman.cn/cpro/epc/plantDetail/showCharts.json',
            data={"plantId": plant_id, "type": 2, "date": month, "plantTimezoneId": plant["timezone_id"]},
            timeout=60
        )
        data = r.json()
        return {
            "plantId": plant_id,
            "monthSummary": data["result"]["plantSta"],
            "chartData": data["result"]["chartsDataAll"]
        }

    @retry.retry(tries=5, delay=1, backoff=2, logger=logger)
    def get_day_battery_charge(self, plant, date: str):
        plant_id = plant["plant_id"]
        r = self.session.post(
            'https://home.solarman.cn/cpro/epc/plantDetail/showSocCharts.json',
            data={"plantId": plant_id, "type": 1, "date": date, "plantTimezoneId": plant["timezone_id"]},
            timeout=60
        )
        data = r.json()
        return {
            "plantId": plant_id,
            "chartData": data["result"]["plantData"]
        }


class InfluxDBWriter:

    logger = logging.getLogger('InfluxDBWriter')

    def __init__(self, influxdb_config):
        self.influxdb_config = influxdb_config
        self.client = InfluxDBClient(**influxdb_config)
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)

    @retry.retry(tries=5, delay=1, logger=logger)
    def write_day_chart_data(self, measurement_name, day_data):
        plant_id = day_data["plantId"]
        chart_data = day_data["chartData"]
        for ts_entry in chart_data:
            ts = datetime.utcfromtimestamp(int(ts_entry['date']) / 1000)
            point = Point(measurement_name).tag("plant_id", plant_id).time(ts, WritePrecision.S)
            for key in TIMESTAMP_FIELDS:
                point.field(key, ts_entry.get(key, 0.0))
            self.write_api.write("solarman", self.client.org, point)

    @retry.retry(tries=5, delay=1, logger=logger)
    def write_month_chart_data(self, measurement_name, month_data):
        plant_id = month_data["plantId"]
        chart_data = month_data["chartData"]
        for day_summary in chart_data:
            self.write_day_summary_data(plant_id, measurement_name, day_summary)

    @retry.retry(tries=5, delay=1, logger=logger)
    def write_day_summary_data(self, plant_id, measurement_name, day_summary):
        day_str = day_summary.get('date')
        if not day_str:
            # This can happen if the date has just rolled and solarman does not have info for the new day
            self.logger.info(f"No data for date yet: {day_summary} ")
            return
        # Inconsistent about date format: yyyy-MM-dd or yyyyMMdd
        date_ts = datetime.fromisoformat(day_str) if '-' in day_str else datetime.strptime(day_str, "%Y%m%d")
        point = Point(measurement_name).tag("plant_id", plant_id).time(date_ts, WritePrecision.S)
        for key in DAY_SUMMARY_FIELDS:
            point.field(key, day_summary.get(key, 0.0))
        self.write_api.write("solarman", self.client.org, point)

    @retry.retry(tries=5, delay=1, logger=logger)
    def write_plant_snapshot(self, measurement_name, plant_snapshot):
        plant_id = plant_snapshot['plantId']
        plant_data = plant_snapshot['plantData']
        ts = datetime.utcfromtimestamp(int(plant_data['plantUpdateTime']) / 1000)
        self.logger.info(f"Writing snapshot for {ts}")
        point = Point(measurement_name).tag("plant_id", plant_id).time(ts, WritePrecision.S)
        for key in ['power', 'powerBattery', 'powerGrid', 'powerUseage']:
            point.field(key, float(plant_data[key]))
        self.write_api.write("solarman", self.client.org, point)

    @retry.retry(tries=5, delay=1, logger=logger)
    def write_day_battery_charge_data(self, measurement_name, day_battery_charge_data):
        plant_id = day_battery_charge_data['plantId']
        chart_data = day_battery_charge_data["chartData"]
        for epoch_millis, percent in chart_data:
            ts = datetime.utcfromtimestamp(int(epoch_millis) / 1000)
            point = Point(measurement_name).tag("plant_id", plant_id).time(ts, WritePrecision.S)
            point.field("charge_pc", float(percent))
            self.write_api.write("solarman", self.client.org, point)


class SolarmanScraper:

    logger = logging.getLogger('SolarmanScraper')

    def __init__(self, config):
        self.config = config

        solarman_config = config["solarman"]
        self.solarman = SolarmanClient(solarman_config["login"])
        self.plant_config = dict(solarman_config["plant"])

        # Get default config settings
        if "timezone_id" not in self.plant_config.keys():
            plant_info = self.solarman.get_plant_snapshot(self.plant_config)
            self.plant_config["timezone_id"] = plant_info["timezoneId"]

        influxdb_config = config["influxdb"]
        self.influxdb = InfluxDBWriter(influxdb_config)

    def process_month(self, date):
        month = date.strftime("%Y-%m")
        self.logger.info(f"Processing data for month {month}")
        month_data = self.solarman.get_month_data(self.plant_config, month)
        self.influxdb.write_month_chart_data( "solarman_daily_summary", month_data)

    def process_day(self, date):
        self.logger.info(f"Processing data for date {date}")
        day_data = self.solarman.get_day_data(self.plant_config, date.strftime("%Y/%m/%d"))
        plant_id = day_data["plantId"]
        self.influxdb.write_day_summary_data(plant_id, "solarman_daily_summary", day_data["daySummary"])
        self.influxdb.write_day_chart_data("solarman", day_data)
        day_battery_charge_data = self.solarman.get_day_battery_charge(self.plant_config, date.strftime("%Y/%m/%d"))
        self.influxdb.write_day_battery_charge_data("solarman_battery", day_battery_charge_data)

    def process_snapshot(self):
        self.logger.info(f"Processing snapshot")
        plant_snapshot = self.solarman.get_plant_snapshot(self.plant_config)
        self.influxdb.write_plant_snapshot("solarman_power", plant_snapshot)


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


with open(".solarman-scraper.yml", "r") as yamlfile:
    config = yaml.load(yamlfile, Loader=yaml.FullLoader)
scraper = SolarmanScraper(config)

today = date.today()
scraper.process_month(today)

yesterday = today - timedelta(1)
scraper.process_day(yesterday)

while True:

    try:

        # Get current values for now
        scraper.process_snapshot()

        new_today = date.today()

        # After a date roll do one last scan of the previous day for completeness
        if new_today != today:
            scraper.process_day(today)
            scraper.process_month(today)
            today = new_today

        # Get time series data for today
        scraper.process_day(today)

    except json.decoder.JSONDecodeError:
        # Solarman returns HTML instead of JSON when logged out
        scraper.solarman.login()

    time.sleep(120)
