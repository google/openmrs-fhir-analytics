#!/bin/bash

docker service rm fhir-batch-kafka-service
docker service rm spark-thriftserver
docker service rm fhir-views-service

docker volume rm spark_vol_single

sudo rm -r output/*
