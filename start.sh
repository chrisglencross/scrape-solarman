#!/bin/bash -e

cd $(dirname $0)
source ./venv/bin/activate

for script in solarman-scraper zappi-scraper weather-scraper octopus-scraper kia-scraper; do
  echo "Starting ${script}"
  ./ka.sh python ./${script}.py > ./logs/${script}.log 2>&1 &
done
