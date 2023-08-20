# Solarman Scraper

Scrapes solar power data from the Solarman web site at https://home.solarman.cn/
and writes data to an InfluxDB time series database.

This script was created for personal use to create some custom Grafana dashboards.

The Python script has only been tested with my solar setup; it may not work for installations with multiple plants
or without a battery. However, the code should be fairly easy to understand
and modify for anyone with Python experience.

Note that metrics are saved to InfluxDB with the same field names as used by the old version of the Solarman API,
including their spelling  mistakes: e.g. `useage` instead of `usage`. This isn't a bug.

I may write a new simplified version of the script which stores data using the same field names as the new Solarman
API, but that will require some data migration and changes to the Grafana dashboards.

# Prerequisites

1. Developed with Python 3.10, although possibly works with >= 3.8.
2. Credentials for logging in to https://home.solarman.cn/ with
the ability to see your solar plant details in a web browser.
3. Some solar panels and some sunshine ☀️.

# Installation

1. Install and run InfluxDB: https://docs.influxdata.com/influxdb/v2.1/install/
2. Create an organisation for InfluxDB: https://docs.influxdata.com/influxdb/v2.1/organizations/create-org/
3. Create an API token for InfluxDB: https://docs.influxdata.com/influxdb/v2.1/security/tokens/create-token/
4. Create a configuration file called `.solarman-scraper.yml` in the same directory as the `solarman-scraper.py` script 
   with the following structure:

```yaml
solarman:
  login:
    domain: "home.solarman.cn"
    username: "<your username for home.solarman.cn>"
    password: "<your password for home.solarman.cn>"
  plant:
    plant_id: <numeric plant id from the 'Plant Info' tab of https://home.solarman.cn/main.html>

influxdb:
  url: "http://localhost:8086"
  token: "influx db token"
  org: "influx db org"
```

5. Install dependencies:

```
python3.10 -m pip install -r requirements.txt
```

# Run
```
python3.10 ./solarman-scraper.py
```

# Grafana Dashboards

The Grafana dashboards I created from this data can be found in the [grafana-dashboards](./grafana-dashboards) directory.
Screenshots cover the period of the UK winter. I'm hoping for significantly better numbers in the summer months.

## Solar Power Today
Shows details of solar power generated today, together with electricity consumption and battery charge and discharge.
![Solar Power Today](./grafana-dashboards/Solar%20Power%20Today.png "Solar Power Today")

## Solar Power History (Daily)
Shows daily totals of electricity generation, consumption and export.
![Solar Power History (Daily)](./grafana-dashboards/Solar%20Power%20History%20(Daily).png "Solar Power History (Daily)")

## Solar Finances
Shows summary information about solar energy used, plus energy bought and sold. Approximate
cost is included with each figure, helping to estimate the energy cost savings
from the solar installation.

![Solar Finances](./grafana-dashboards/Solar%20Finances.png "Solar Power Finances")


