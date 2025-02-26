# FHIR Data Pipes

FHIR Data Pipes provides pipelines for transforming data from a FHIR server into
a queryable format that you can use for data analysis and visualization. It can
also be used to move data from one FHIR server to another. FHIR Data Pipes is
built on technology designed for horizontal scalability and has multiple
deployment options depending on your use case.

There are a few ways to run FHIR Data Pipes; see these pages to learn more about
them:

- [FHIR Data Pipes Pipelines](batch) is a Java JAR which moves and transforms
  data from a FHIR server to Apache Parquet files for data analysis or another
  FHIR server for data integration. Pipelines is built using Apache Beam and can
  run on a single machine or cluster.

- [The Pipelines Controller module](controller) is a user-interface wrapper for
  the above pipelines, integrating "full", "incremental", and "merger" pipelines
  together. You can schedule periodic incremental updates or use a control panel
  to start the pipeline manually. The Pipelines Controller is built on top of
  pipelines and shares many of the same settings.

- [The single machine Docker Compose configuration](https://github.com/google/fhir-data-pipes/wiki/Analytics-on-a-single-machine-using-Docker)
  runs the Pipelines Controller plus a Spark Thrift server so you can more
  easily run Spark SQL queries on the Parquet files output by the Pipelines
  Controller. It can automatically create and update tables on the Spark Thrift
  server each time the Pipelines Controller runs the data pipeline, simplifying
  data analysis or dashboard creation. This configuration also serves as a very
  simple example of how to integrate the Parquet output of Pipelines in a Spark
  cluster environment.

[See the documentation site](https://google.github.io/fhir-data-pipes/) for
guides and tutorials.
