import time

import yaml
from datetime import datetime, timedelta, date, timezone
import dateutil.parser

import logging
import retry
import requests
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from requests.auth import HTTPBasicAuth

BACKFILL_DAYS=4

FORMAT = '%(asctime)s %(levelname)s [%(name)s] %(message)s'
logging.basicConfig(format=FORMAT, level=logging.INFO)

def is_current(now, record):
    valid_from = dateutil.parser.isoparse(record["valid_from"])
    valid_to = dateutil.parser.isoparse(record["valid_to"]) if record["valid_to"] else None
    return valid_from <= now and (valid_to is None or valid_to > now)


class OctopusClient:

    logger = logging.getLogger('OctopusClient')

    def __init__(self, config):
        self.url = "https://api.octopus.energy/v1"
        self.auth = HTTPBasicAuth(f"{config['key']}", "")
        self.account = config["account"]

    @retry.retry(tries=10, delay=1, backoff=2, logger=logger)
    def get_account(self):
        response = requests.get(f"{self.url}/accounts/{self.account}/", auth=self.auth)
        response.raise_for_status()
        account = response.json()
        return account

    def get_results(self, url):
        results = []
        while url:
            response = requests.get(url, auth=self.auth, timeout=300)
            data = response.json()
            if "results" in data:
                results.extend(data["results"])
            url = data.get("next")
        return results

    @retry.retry(tries=10, delay=1, backoff=2, logger=logger)
    def get_electricity_tariff_rates(self, tariff: str):
        product = self.product_for_tariff(tariff)
        return self.get_results(
            f"{self.url}/products/{product}/electricity-tariffs/{tariff}/standard-unit-rates/")

    @retry.retry(tries=10, delay=1, backoff=2, logger=logger)
    def get_gas_tariff_rates(self, tariff: str):
        product = self.product_for_tariff(tariff)
        return self.get_results(
            f"{self.url}/products/{product}/gas-tariffs/{tariff}/standard-unit-rates/")

    def product_for_tariff(self, tariff: str):
        parts = tariff.split("-")
        return "-".join(parts[2:-1])

    @retry.retry(tries=10, delay=1, backoff=2, logger=logger)
    def get_electricity_usage(self, mpan, serial_number):
        return self.get_results(f"{self.url}/electricity-meter-points/{mpan}/meters/{serial_number}/consumption/?period_from={self.period_from()}")

    @retry.retry(tries=10, delay=1, backoff=2, logger=logger)
    def get_gas_usage(self, mprn, serial_number):
        result = self.get_results(f"{self.url}/gas-meter-points/{mprn}/meters/{serial_number}/consumption/?period_from={self.period_from()}")
        # Convert m^3 to kWh with 1.02264
        for usage in result:
            usage["consumption"] *= (1.02264 * 39.0 / 3.6)
        return result

    def period_from(self, days_ago=BACKFILL_DAYS):
      period_from = datetime.now(tz=timezone.utc) - timedelta(days_ago)
      return period_from.strftime("%Y-%m-%d %H:%M")
     
     

class InfluxDBWriter:

    logger = logging.getLogger('InfluxDBWriter')

    def __init__(self, influxdb_config):
        self.influxdb_config = influxdb_config
        self.client = InfluxDBClient(**influxdb_config)
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)

    @retry.retry(tries=10, delay=1, logger=logger)
    def write_snapshot(self,
                       account,
                       mpan,
                       meter,
                       is_export,
                       interval_start,
                       energy,
                       power,
                       tariff_code,
                       rate_pence,
                       cost,
                       is_gas):
        point = Point("octopus")\
            .tag("account", account)\
            .tag("mpan", mpan)\
            .tag("meter", meter) \
            .tag("is_export", is_export)\
            .tag("tariff", tariff_code)\
            .tag("is_gas", is_gas)\
            .time(interval_start, WritePrecision.S)
        point.field("energy", energy)
        point.field("power", power)
        if rate_pence is not None:
            point.field("rate", rate_pence)
        if cost is not None:
            point.field("cost", cost)
        self.write_api.write("octopus", self.client.org, point)


class OctopusScraper:

    logger = logging.getLogger('OctopusScraper')

    def __init__(self, config):
        self.config = config

        octopus_config = config["octopus"]
        self.octopus = OctopusClient(octopus_config)

        influxdb_config = config["influxdb"]
        self.influxdb = InfluxDBWriter(influxdb_config)

    def get_account_info(self):
        self.account = self.octopus.get_account()
        # Legacy Bulb tariff not returned from Octopus API
        self.electricity_rates = {
            "E-1R-BULB-SEG-FIX-V1-21-04-01-J": [
                {'value_exc_vat': 0.0557, 'value_inc_vat': 0.0557, 'valid_from': '1970-01-01T00:00:00Z', 'valid_to': None}
            ]
        }
        self.gas_rates = {}

    def get_electricity_tariff(self, tariff_code: str):
        if tariff_code not in self.electricity_rates:
            self.electricity_rates[tariff_code] = self.octopus.get_electricity_tariff_rates(tariff_code)
        return self.electricity_rates[tariff_code]

    def get_gas_tariff(self, tariff_code: str):
        if tariff_code not in self.gas_rates:
            self.gas_rates[tariff_code] = self.octopus.get_gas_tariff_rates(tariff_code)
        return self.gas_rates[tariff_code]

    def process_snapshot(self):
        self.logger.info(f"Processing snapshot")
        self.process_electricity()
        self.process_gas()
        self.logger.info(f"Snapshot complete")

    def process_electricity(self):
        meter_points = [e for p in self.account["properties"] for e in p["electricity_meter_points"]]
        for meter_point in meter_points:
            is_export = meter_point["is_export"]
            mpan = meter_point["mpan"]
            agreements = meter_point["agreements"]
            for meter in meter_point["meters"]:
                meter_serial_number = meter["serial_number"]
                self.logger.info(f"Processing electricity meter {mpan} {meter_serial_number} (export={is_export})")
                usage = self.octopus.get_electricity_usage(mpan, meter_serial_number)
                self.process_meter_usage(False, is_export, mpan, meter_serial_number, agreements, self.get_electricity_tariff, usage)

    def process_gas(self):
        meter_points = [e for p in self.account["properties"] for e in p["gas_meter_points"]]
        for meter_point in meter_points:
            mprn = meter_point["mprn"]
            agreements = meter_point["agreements"]
            for meter in meter_point["meters"]:
                meter_serial_number = meter["serial_number"]
                self.logger.info(f"Processing gas meter {mprn} {meter_serial_number}")
                usage = self.octopus.get_gas_usage(mprn, meter_serial_number)
                self.process_meter_usage(True, False, mprn, meter_serial_number, agreements, self.get_gas_tariff, usage)

    def process_meter_usage(self, is_gas, is_export, meter_point_id, meter_serial_number, agreements, get_tariff, usage):
        self.logger.info(f"Processing {len(usage)} records for meter {meter_serial_number}")
        for interval in usage:
            interval_start = dateutil.parser.isoparse(interval["interval_start"])
            interval_end = dateutil.parser.isoparse(interval["interval_end"])
            energy = interval["consumption"]  # kWh
            duration = (interval_end - interval_start).total_seconds()
            power = energy * 1000 / duration  # Average Watts
            tariff_code = next(agreement["tariff_code"]
                               for agreement in agreements
                               if is_current(interval_start, agreement))
            tariff_rates = get_tariff(tariff_code)
            rate_pence = next((rate["value_inc_vat"]
                              for rate in tariff_rates
                              if is_current(interval_start, rate)), None)
            if rate_pence is None:
                raise KeyError(f"Rate not found for tariff {tariff_code} at {interval_start}")
            cost = energy * rate_pence / 100

            self.influxdb.write_snapshot(
                account=self.account["number"],
                mpan=meter_point_id,
                meter=meter_serial_number,
                is_export=is_export,
                interval_start=interval_start,
                energy=energy,
                power=power,
                tariff_code=tariff_code,
                rate_pence=rate_pence,
                cost=cost,
                is_gas=is_gas
            )

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


@retry.retry(tries=10, delay=60)
def main():
    with open(".solarman-scraper.yml", "r") as yamlfile:
        config = yaml.load(yamlfile, Loader=yaml.FullLoader)
    scraper = OctopusScraper(config)

    scraper.get_account_info()
    today = date.today()
    while True:

        new_today = date.today()

        # After a date roll do one last scan of the previous day for completeness
        if new_today != today:
            scraper.get_account_info()
            today = new_today

        # Get current value
        scraper.process_snapshot()

        # No need to poll more than once every 4 hours, data updates daily
        time.sleep(4*60*60)


if __name__ == '__main__':
    main()
