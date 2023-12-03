#!/bin/bash -e

cd $(dirname $0)
source ./venv/bin/activate

for script in $*; do
  echo "Starting ${script}"
  ./ka.sh python ./${script}.py > ./logs/${script}.log 2>&1 &
done
