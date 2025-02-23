#!/bin/bash

# Build the Java application (assuming Maven project)
# echo "Building Java application..."
mvn clean package -DskipTests

# Build the Docker image
echo "Building Docker image..."
docker build -f Dockerfile-kafka -t fhir-batch-kafka .

# Build the Docker image for FHIR views
echo "Building FHIR views Docker image..."
docker build -t my-fhir-views ../query

# Create a volume for Spark
docker volume create spark_vol_single

# Run the Spark thrift server
echo "Starting Spark thrift server..."
docker service create \
  --name spark-thriftserver \
  --network kafka_public \
  --mount type=bind,source="$(pwd)/output",target=/dwh \
  --mount type=volume,source=spark_vol_single,target=/opt/bitnami/spark \
  --publish 10001:10000 \
  --publish 4041:4040 \
  docker.io/bitnami/spark:3.3 \
  sbin/start-thriftserver.sh --driver-memory 5g

# Wait for Spark to be ready
echo "Waiting for Spark thrift server to be ready..."
sleep 30

# Run the FHIR batch container
echo "Running FHIR batch container..."
docker service create \
  --name fhir-batch-kafka-service \
  --network kafka_public \
  --mount type=bind,source="$(pwd)/output",target=/tmp/fhir-data \
  fhir-batch-kafka && \

# Run the FHIR views container as a service
echo "Starting FHIR views service..."
docker service create \
  --name fhir-views-service \
  --network kafka_public \
  --publish 10002:8888 \
  my-fhir-views

# Show the logs to get the Jupyter token
echo "Waiting for service to start and showing logs..."
sleep 5
docker service logs -f fhir-views-service
