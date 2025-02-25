FROM alpine:3.17.3

WORKDIR /jdbcDrivers

ARG POSTGRESQL_DRIVER_VERSION=42.6.0

# Install required packages
RUN apk update && apk add curl

# Fetch drivers
RUN curl -s https://jdbc.postgresql.org/download/postgresql-$POSTGRESQL_DRIVER_VERSION.jar \
    -o postgresql-$POSTGRESQL_DRIVER_VERSION.jar
