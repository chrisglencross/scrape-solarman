#!/bin/bash

cmd="$*"

echo "$(date -Is) Running $cmd"

until eval "$cmd"; do
  echo "$(date -Is) Command failed with exit code $?"
  sleep 60
  echo "$(date -Is) Restarting $cmd"
done

