#!/bin/bash

cmd="$*"

echo "$(date -Is) Running $cmd"

until eval "$cmd"; do
  echo "$(date -Is) Command failed with exit code $?"
  # Long sleep to prevent draining KIA battery
  sleep 1800
  echo "$(date -Is) Restarting $cmd"
done

