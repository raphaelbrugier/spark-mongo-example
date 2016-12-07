#!/bin/bash

apt-get update && apt-get install -y wget
wget -O /tmp/zips.json "http://media.mongodb.org/zips.json"
mongoimport --db=test --collection=zips /tmp/zips.json