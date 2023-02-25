#!/bin/bash -e

cd $(dirname $0)

for script in solarman-scraper zappi-scraper kia-scraper weather-scraper; do
  echo "Starting ${script}"
  ./ka.sh python ./${script}.py > ./logs/${script}.log 2>&1 &
done
