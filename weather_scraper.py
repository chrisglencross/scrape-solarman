import time
import yaml
from datetime import datetime

import logging
import retry
import requests
from cachetools import TTLCache
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

FORMAT = '%(asctime)s %(levelname)s [%(name)s] %(message)s'
logging.basicConfig(format=FORMAT, level=logging.INFO)

FIELDS = {
    "hourly": {
        "screenTemperature": float,
        "screenDewPointTemperature": float,
        "feelsLikeTemperature": float,
        "windSpeed10m": float,
        "windDirectionFrom10m": float,
        "windGustSpeed10m": float,
        "visibility": int,
        "screenRelativeHumidity": float,
        "mslp": int,
        "uvIndex": int,
        "significantWeatherCode": int,
        "precipitationRate": float,
        "probOfPrecipitation": int
    },
    "three-hourly": {
        'maxScreenAirTemp': float,
        'minScreenAirTemp': float,
        'max10mWindGust': float,
        'significantWeatherCode': int,
        'totalPrecipAmount': float,
        'totalSnowAmount': float,
        'windSpeed10m': float,
        'windDirectionFrom10m': float,
        'windGustSpeed10m': float,
        'visibility': int,
        'mslp': int,
        'screenRelativeHumidity': float,
        'feelsLikeTemp': float,
        'uvIndex': int,
        'probOfPrecipitation': int,
        'probOfSnow': int,
        'probOfHeavySnow': int,
        'probOfRain': int,
        'probOfHeavyRain': int,
        'probOfHail': int,
        'probOfSferics': int,
    },
    "daily": {
        'midday10MWindSpeed': float,
        'midnight10MWindSpeed': float,
        'midday10MWindDirection': float,
        'midnight10MWindDirection': float,
        'midday10MWindGust': float,
        'midnight10MWindGust': float,
        'middayVisibility': int,
        'midnightVisibility': int,
        'middayRelativeHumidity': float,
        'midnightRelativeHumidity': float,
        'middayMslp': int,
        'midnightMslp': int,
        'maxUvIndex': int,
        'daySignificantWeatherCode': int,
        'nightSignificantWeatherCode': int,
        'dayMaxScreenTemperature': float,
        'nightMinScreenTemperature': float,
        'dayUpperBoundMaxTemp': float,
        'nightUpperBoundMinTemp': float,
        'dayLowerBoundMaxTemp': float,
        'nightLowerBoundMinTemp': float,
        'dayMaxFeelsLikeTemp': float,
        'nightMinFeelsLikeTemp': float,
        'dayUpperBoundMaxFeelsLikeTemp': float,
        'nightUpperBoundMinFeelsLikeTemp': float,
        'dayLowerBoundMaxFeelsLikeTemp': float,
        'nightLowerBoundMinFeelsLikeTemp': float,
        'dayProbabilityOfPrecipitation': int,
        'nightProbabilityOfPrecipitation': int,
        'dayProbabilityOfSnow': int,
        'nightProbabilityOfSnow': int,
        'dayProbabilityOfHeavySnow': int,
        'nightProbabilityOfHeavySnow': int,
        'dayProbabilityOfRain': int,
        'nightProbabilityOfRain': int,
        'dayProbabilityOfHeavyRain': int,
        'nightProbabilityOfHeavyRain': int,
        'dayProbabilityOfHail': int,
        'nightProbabilityOfHail': int,
        'dayProbabilityOfSferics': int,
        'nightProbabilityOfSferics': int,
    }
}

class MetOfficeClient:

    logger = logging.getLogger('MetOfficeClient')

    cache = TTLCache(maxsize=10, ttl=600)

    def __init__(self, longitude, latitude, credentials):
        self.longitude = longitude
        self.latitude = latitude
        self.credentials = credentials

    @retry.retry(tries=10, delay=1, backoff=2, logger=logger)
    def get_forecast(self, path):

        result = self.cache.get(path)
        if not result:

            url = f"https://api-metoffice.apiconnect.ibmcloud.com/metoffice/production/v0/forecasts/point/{path}"
            params = {
                "latitude": self.latitude,
                "longitude": self.longitude
            }
            headers = {
                "accept": "application/json",
                "x-ibm-client-id": self.credentials['clientId'],
                "x-ibm-client-secret": self.credentials['secret']
            }
            response = requests.get(url, params=params, headers=headers)
            result = response.json()
            self.cache[path] = result

        return result


class InfluxDBWriter:

    logger = logging.getLogger('InfluxDBWriter')

    def __init__(self, influxdb_config):
        self.influxdb_config = influxdb_config
        self.client = InfluxDBClient(**influxdb_config)
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)

    @retry.retry(tries=10, delay=1, logger=logger)
    def write_data(self, measurement_name, location_name, data):
        time_series = data["features"][0]["properties"]["timeSeries"]
        for ts_entry in time_series:
            ts = datetime.strptime(ts_entry["time"], "%Y-%m-%dT%H:%M%z")
            point = Point(measurement_name).tag("location", location_name).time(ts, WritePrecision.S)
            for key, field_type in FIELDS[measurement_name].items():
                point.field(key, field_type(ts_entry.get(key, 0)))
            self.write_api.write("met_office", self.client.org, point)


class MetOfficeScraper:

    logger = logging.getLogger('MetOfficeScraper')

    def __init__(self, config):
        metoffice_config = config["met_office"]
        self.metoffice_client = MetOfficeClient(
            metoffice_config["longitude"],
            metoffice_config["latitude"],
            metoffice_config["credentials"])
        self.location = metoffice_config["location"]

        influxdb_config = config["influxdb"]
        self.influxdb = InfluxDBWriter(influxdb_config)

    def process_snapshot(self):
        for forecast in ["hourly", "three-hourly", "daily"]:
            logging.info(f"Getting {forecast} data")
            response = self.metoffice_client.get_forecast(forecast)
            self.influxdb.write_data(forecast, self.location, response)


@retry.retry(tries=10, delay=60)
def main():
    with open(".solarman-scraper.yml", "r") as yamlfile:
        config = yaml.load(yamlfile, Loader=yaml.FullLoader)
    scraper = MetOfficeScraper(config)

    while True:
        scraper.process_snapshot()
        time.sleep(60*60)


if __name__ == '__main__':
    main()
