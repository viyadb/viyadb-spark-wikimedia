#!/bin/bash

curl \
    --request PUT \
    --data @table.json \
    http://localhost:8500/v1/kv/viyadb/tables/wikimedia/config
