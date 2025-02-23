/*
 * Copyright 2020-2024 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.fhir.analytics;

import ca.uhn.fhir.context.FhirContext;
import com.cerner.bunsen.exception.ProfileException;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.fhir.analytics.metrics.PipelineMetrics;
import com.google.fhir.analytics.metrics.PipelineMetricsProvider;
import com.google.fhir.analytics.model.BulkExportResponse;
import com.google.fhir.analytics.model.BulkExportResponse.Output;
import com.google.fhir.analytics.model.DatabaseConfiguration;
import com.google.fhir.analytics.view.ViewDefinitionException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Beam pipeline for reading FHIR resources from OpenMRS and pushing them into a data warehouse.
 */
public class FhirEtl {

  private static final Logger log = LoggerFactory.getLogger(FhirEtl.class);

  /**
   * This map is used when the activePeriod feature is enabled in the JDBC mode. For each table it
   * indicates the column on which the date filter is applied. It is best if these columns are not
   * nullable and there is an index on them.
   */
  private static final Map<String, String> tableDateColumn;

  static {
    Map<String, String> tempMap = Maps.newHashMap();
    tempMap.put("encounter", "encounter_datetime");
    tempMap.put("obs", "obs_datetime");
    tempMap.put("visit", "date_started");
    tableDateColumn = Collections.unmodifiableMap(tempMap);
  }

  static FhirSearchUtil createFhirSearchUtil(FhirEtlOptions options, FhirContext fhirContext) {
    return new FhirSearchUtil(createFetchUtil(options, fhirContext));
  }

  static BulkExportUtil createBulkExportUtil(FhirEtlOptions options, FhirContext fhirContext) {
    return new BulkExportUtil(new BulkExportApiClient(createFetchUtil(options, fhirContext)));
  }

  static FetchUtil createFetchUtil(FhirEtlOptions options, FhirContext fhirContext) {
    return new FetchUtil(
        options.getFhirServerUrl(),
        options.getFhirServerUserName(),
        options.getFhirServerPassword(),
        options.getFhirServerOAuthTokenEndpoint(),
        options.getFhirServerOAuthClientId(),
        options.getFhirServerOAuthClientSecret(),
        fhirContext);
  }

  /**
   * For each SearchSegmentDescriptor, it fetches the given resources, convert them to output
   * Parquet/JSON files, and output the IDs of the fetched resources.
   *
   * @param inputSegments each element defines a set of resources to be fetched in one FHIR call.
   * @param resourceType the type of the resources, e.g., Patient or Observation
   * @param options the pipeline options
   * @return a PCollection of all patient IDs of fetched resources or empty if `resourceType` has no
   *     patient ID association.
   */
  private static PCollection<KV<String, Integer>> fetchSegmentsAndReturnPatientIds(
      PCollection<SearchSegmentDescriptor> inputSegments,
      String resourceType,
      FhirEtlOptions options) {
    FetchResources fetchResources = new FetchResources(options, resourceType + "_main");
    return inputSegments.apply(fetchResources);
  }

  static void fetchPatientHistory(
      Pipeline pipeline,
      List<PCollection<KV<String, Integer>>> allPatientIds,
      Set<String> patientAssociatedResources,
      FhirEtlOptions options,
      AvroConversionUtil avroConversionUtil)
      throws ProfileException {
    PCollectionList<KV<String, Integer>> patientIdList =
        PCollectionList.<KV<String, Integer>>empty(pipeline).and(allPatientIds);
    PCollection<KV<String, Integer>> flattenedPatients =
        patientIdList.apply(Flatten.pCollections());
    PCollection<KV<String, Integer>> mergedPatients = flattenedPatients.apply(Sum.integersPerKey());
    final String patientType = "Patient";
    FetchPatients fetchPatients =
        new FetchPatients(options, avroConversionUtil.getResourceSchema(patientType));
    mergedPatients.apply(fetchPatients);
    for (String resourceType : patientAssociatedResources) {
      FetchPatientHistory fetchPatientHistory = new FetchPatientHistory(options, resourceType);
      mergedPatients.apply(fetchPatientHistory);
    }
  }

  private static List<Pipeline> buildFhirSearchPipeline(
      FhirEtlOptions options, AvroConversionUtil avroConversionUtil) throws ProfileException {
    FhirSearchUtil fhirSearchUtil =
        createFhirSearchUtil(options, avroConversionUtil.getFhirContext());
    Map<String, List<SearchSegmentDescriptor>> segmentMap = Maps.newHashMap();
    try {
      // TODO in the activePeriod case, among patientAssociatedResources, only fetch Encounter here.
      // TODO Capture the total resources to be processed as a metric which can be used to derive
      //  the stats of how many records has been completed.
      segmentMap = fhirSearchUtil.createSegments(options);
    } catch (IllegalArgumentException e) {
      log.error(
          "Either the date format in the active period is wrong or none of the resources support"
              + " 'date' feature"
              + e.getMessage());
      throw e;
    }
    if (segmentMap.isEmpty()) {
      return null;
    }

    Pipeline pipeline = Pipeline.create(options);
    List<PCollection<KV<String, Integer>>> allPatientIds = Lists.newArrayList();
    for (Map.Entry<String, List<SearchSegmentDescriptor>> entry : segmentMap.entrySet()) {
      String resourceType = entry.getKey();
      PCollection<SearchSegmentDescriptor> inputSegments =
          pipeline.apply(Create.of(entry.getValue()));
      allPatientIds.add(fetchSegmentsAndReturnPatientIds(inputSegments, resourceType, options));
    }
    if (!options.getActivePeriod().isEmpty()) {
      Set<String> patientAssociatedResources =
          fhirSearchUtil.findPatientAssociatedResources(segmentMap.keySet());
      fetchPatientHistory(
          pipeline, allPatientIds, patientAssociatedResources, options, avroConversionUtil);
    }
    return Arrays.asList(pipeline);
  }

  private static DataSource createJdbcPooledDataSource(
      FhirEtlOptions options, DatabaseConfiguration dbConfig) {
    if (options.getJdbcInitialPoolSize() != JdbcConnectionPools.MIN_CONNECTIONS) {
      log.warn("Setting jdbcInitialPoolSize has no effect; it is deprecated and will be removed.");
    }
    return JdbcConnectionPools.getInstance()
        .getPooledDataSource(
            JdbcConnectionPools.dbConfigToDataSourceConfig(dbConfig), options.getJdbcMaxPoolSize());
  }

  private static List<Pipeline> buildOpenmrsJdbcPipeline(
      FhirEtlOptions options, AvroConversionUtil avroConversionUtil)
      throws IOException, SQLException, ProfileException {
    // TODO add incremental support.
    Preconditions.checkArgument(Strings.isNullOrEmpty(options.getSince()));
    FhirSearchUtil fhirSearchUtil =
        createFhirSearchUtil(options, avroConversionUtil.getFhirContext());
    List<Pipeline> pipelines = new ArrayList<>();
    Pipeline pipeline = Pipeline.create(options);
    pipelines.add(pipeline);
    DatabaseConfiguration dbConfig =
        DatabaseConfiguration.createConfigFromFile(options.getFhirDatabaseConfigPath());
    DataSource jdbcSource = createJdbcPooledDataSource(options, dbConfig);
    JdbcFetchOpenMrs jdbcUtil = new JdbcFetchOpenMrs(jdbcSource);
    int batchSize =
        Math.min(
            options.getBatchSize(), 170); // batch size > 200 will result in HTTP 400 Bad Request
    Map<String, List<String>> reverseMap =
        jdbcUtil.createFhirReverseMap(options.getResourceList(), dbConfig);
    // process each table-resource mappings
    Set<String> resourceTypes = new HashSet<>();
    List<PCollection<KV<String, Integer>>> allPatientIds = Lists.newArrayList();
    for (Map.Entry<String, List<String>> entry : reverseMap.entrySet()) {
      String tableName = entry.getKey();
      log.info(String.format("List of resources for table %s is %s", tableName, entry.getValue()));
      PCollection<String> uuids;
      if (options.getActivePeriod().isEmpty() || !tableDateColumn.containsKey(tableName)) {
        if (!options.getActivePeriod().isEmpty()) {
          log.warn(
              String.format(
                  "There is no date mapping for table %s; fetching all rows.", tableName));
        }
        uuids = jdbcUtil.fetchAllUuids(pipeline, tableName, options.getJdbcFetchSize());
      } else {
        try {
          uuids =
              jdbcUtil.fetchUuidsByDate(
                  pipeline, tableName, tableDateColumn.get(tableName), options.getActivePeriod());
        } catch (CannotProvideCoderException e) {
          // This should never happen!
          String error = "Cannot provide coder for String! " + e.getMessage();
          log.error("{} {}", error, e);
          throw new IllegalStateException(error);
        }
      }
      for (String resourceType : entry.getValue()) {
        resourceTypes.add(resourceType);
        String baseBundleUrl = options.getFhirServerUrl() + "/" + resourceType;
        PCollection<SearchSegmentDescriptor> inputSegments =
            uuids.apply(
                String.format("CreateSearchSegments_%s_table_%s", resourceType, tableName),
                new JdbcFetchOpenMrs.CreateSearchSegments(resourceType, baseBundleUrl, batchSize));
        allPatientIds.add(fetchSegmentsAndReturnPatientIds(inputSegments, resourceType, options));
      }
    }
    if (!options.getActivePeriod().isEmpty()) {
      Set<String> patientAssociatedResources =
          fhirSearchUtil.findPatientAssociatedResources(resourceTypes);
      fetchPatientHistory(
          pipeline, allPatientIds, patientAssociatedResources, options, avroConversionUtil);
    }
    return pipelines;
  }

  static void validateOptions(FhirEtlOptions options) {
    Preconditions.checkNotNull(options.getFhirFetchMode(), "--fhirFetchMode cannot be empty");
    switch (options.getFhirFetchMode()) {
      case FHIR_SEARCH -> Preconditions.checkState(
          !Strings.isNullOrEmpty(options.getFhirServerUrl()),
          "--fhirServerUrl cannot be empty for FHIR_SEARCH fetch mode");
      case PARQUET -> {
        Preconditions.checkState(
            !Strings.isNullOrEmpty(options.getParquetInputDwhRoot()),
            "--parquetInputDwhRoot cannot be empty for PARQUET fetch mode");
        // This constraint is to make the PipelineManager logic simpler and because there is
        // currently no use-case for both reading from Parquet files and also writing into Parquet.
        Preconditions.checkState(
            Strings.isNullOrEmpty(options.getOutputParquetPath()),
            "--parquetInputDwhRoot and --outputParquetPath cannot be used together!");
      }
      case JSON -> Preconditions.checkState(
          !Strings.isNullOrEmpty(options.getSourceJsonFilePatternList()),
          "--sourceJsonFilePattern cannot be empty for JSON fetch mode");
      case NDJSON -> Preconditions.checkState(
          !Strings.isNullOrEmpty(options.getSourceNdjsonFilePatternList()),
          "--sourceNdjsonFilePattern cannot be empty for NDJSON fetch mode");
      case BULK_EXPORT -> Preconditions.checkState(
          !Strings.isNullOrEmpty(options.getFhirServerUrl()),
          "--fhirServerUrl cannot be empty for BULK_EXPORT fetch mode");
      case HAPI_JDBC -> Preconditions.checkState(
          !Strings.isNullOrEmpty(options.getFhirDatabaseConfigPath()),
          "--fhirDatabaseConfigPath cannot be empty for HAPI_JDBC fetch mode");
      case OPENMRS_JDBC -> Preconditions.checkState(
          !Strings.isNullOrEmpty(options.getFhirDatabaseConfigPath())
              && !Strings.isNullOrEmpty(options.getFhirServerUrl()),
          "--fhirDatabaseConfigPath and -fhirServerUrl cannot be empty for OPENMRS_JDBC fetch"
              + " mode");
      case TOPIC_LISTENER -> {
        Preconditions.checkState(
            !Strings.isNullOrEmpty(options.getKafkaBootstrapServers()),
            "--kafkaBootstrapServers cannot be empty for TOPIC_LISTENER fetch mode");
        Preconditions.checkState(
            !Strings.isNullOrEmpty(options.getKafkaTopic()),
            "--kafkaTopic cannot be empty for TOPIC_LISTENER fetch mode");
        Preconditions.checkState(
            !Strings.isNullOrEmpty(options.getKafkaConsumerGroup()),
            "--kafkaConsumerGroup cannot be empty for TOPIC_LISTENER fetch mode");
      }
    }
    if (!options.getActivePeriod().isEmpty()) {
      Set<String> resourceSet = Sets.newHashSet(options.getResourceList().split(","));
      if (resourceSet.contains("Patient")) {
        throw new IllegalArgumentException(
            "When using --activePeriod feature, 'Patient' should not be in --resourceList got: "
                + options.getResourceList());
      }
      if (!resourceSet.contains("Encounter")) {
        throw new IllegalArgumentException(
            "When using --activePeriod feature, 'Encounter' should be in --resourceList got: "
                + options.getResourceList());
      }
    }
    if (options.isCreateParquetViews() && Strings.isNullOrEmpty(options.getViewDefinitionsDir())) {
      throw new IllegalArgumentException(
          "When using --createParquetViews, --viewDefinitionsDir cannot be empty");
    }
    if (options.getCacheBundleForParquetWrites()
        && !"DataflowRunner".equals(options.getRunner().getSimpleName())) {
      throw new IllegalArgumentException(
          "--cacheBundleForParquetWrites is intended to be used with Dataflow runner only!");
    }
  }

  // TODO: Implement active period feature for JDBC mode with a HAPI source server (issue #278).
  private static List<Pipeline> buildHapiJdbcPipeline(FhirEtlOptions options)
      throws SQLException, IOException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(options.getFhirDatabaseConfigPath()));
    DatabaseConfiguration dbConfig =
        DatabaseConfiguration.createConfigFromFile(options.getFhirDatabaseConfigPath());
    boolean foundResource = false;
    DataSource jdbcSource = createJdbcPooledDataSource(options, dbConfig);

    JdbcFetchHapi jdbcFetchHapi = new JdbcFetchHapi(jdbcSource);
    Map<String, Integer> resourceCount =
        jdbcFetchHapi.searchResourceCounts(options.getResourceList(), options.getSince());

    List<Pipeline> pipelines = new ArrayList<>();
    long totalNumOfResources = 0l;
    for (String resourceType : options.getResourceList().split(",")) {
      int numResources = resourceCount.get(resourceType);
      if (numResources == 0) {
        continue;
      }

      totalNumOfResources += numResources;
      foundResource = true;
      Pipeline pipeline = Pipeline.create(options);
      PCollection<QueryParameterDescriptor> queryParameters =
          pipeline.apply(
              "Generate query parameters for " + resourceType,
              Create.of(
                  new JdbcFetchHapi(jdbcSource)
                      .generateQueryParameters(options, resourceType, numResources)));
      PCollection<HapiRowDescriptor> payload =
          queryParameters.apply(
              "JdbcIO fetch for " + resourceType,
              new JdbcFetchHapi.FetchRowsJdbcIo(
                  options.getResourceList(),
                  JdbcIO.DataSourceConfiguration.create(jdbcSource),
                  options.getSince()));

      payload.apply(
          "Convert to parquet for " + resourceType,
          ParDo.of(new ConvertResourceFn(options, "ConvertResourceFn")));
      pipelines.add(pipeline);
    }

    if (foundResource) { // Otherwise, there is nothing to be done!
      PipelineMetrics pipelineMetrics =
          PipelineMetricsProvider.getPipelineMetrics(options.getRunner());
      if (pipelineMetrics != null) {
        pipelineMetrics.clearAllMetrics();
        pipelineMetrics.setTotalNumOfResources(totalNumOfResources);
      }
      return pipelines;
    }
    return null;
  }

  private static List<Pipeline> buildParquetReadPipeline(
      FhirEtlOptions options, AvroConversionUtil avroConversionUtil)
      throws IOException, ProfileException {
    Preconditions.checkArgument(!options.getParquetInputDwhRoot().isEmpty());
    DwhFiles dwhFiles =
        DwhFiles.forRoot(options.getParquetInputDwhRoot(), avroConversionUtil.getFhirContext());
    Set<String> resourceTypes = dwhFiles.findNonEmptyResourceDirs();
    log.info("Reading Parquet files for these resource types: {}", resourceTypes);
    List<Pipeline> pipelineList = new ArrayList<>();
    for (String resourceType : resourceTypes) {
      Pipeline pipeline = Pipeline.create(options);
      PCollection<ReadableFile> inputFiles =
          pipeline
              .apply(Create.of(dwhFiles.getFilePattern(resourceType)))
              .apply(FileIO.matchAll())
              .apply(FileIO.readMatches());

      PCollection<GenericRecord> records =
          inputFiles.apply(ParquetIO.readFiles(avroConversionUtil.getResourceSchema(resourceType)));

      records.apply(
          "Process Parquet records for " + resourceType,
          ParDo.of(new ProcessGenericRecords(options, resourceType)));
      pipelineList.add(pipeline);
    }
    return pipelineList;
  }

  private static List<Pipeline> buildMultiJsonReadPipeline(
      FhirEtlOptions options, boolean isFileNdjson) {
    String multiFilePattern =
        isFileNdjson
            ? options.getSourceNdjsonFilePatternList()
            : options.getSourceJsonFilePatternList();
    Preconditions.checkArgument(!multiFilePattern.isEmpty());

    Pipeline pipeline = Pipeline.create(options);
    pipeline
        .apply(Create.of(Arrays.asList(multiFilePattern.split(","))))
        .apply(FileIO.matchAll())
        .apply(FileIO.readMatches())
        .apply(
            String.format("Read Multi Json Files, isFileNdjson=%s", isFileNdjson),
            ParDo.of(new ReadJsonFn.FromFile(options, isFileNdjson)));
    return Arrays.asList(pipeline);
  }

  private static List<Pipeline> buildBulkExportReadPipeline(
      FhirEtlOptions options, AvroConversionUtil avroConversionUtil) throws IOException {
    BulkExportUtil bulkExportUtil =
        createBulkExportUtil(options, avroConversionUtil.getFhirContext());
    List<String> resourceTypes =
        Arrays.asList(options.getResourceList().split(",")).stream()
            .distinct()
            .collect(Collectors.toList());
    BulkExportResponse bulkExportResponse =
        bulkExportUtil.triggerBulkExport(
            resourceTypes, options.getSince(), options.getFhirVersion());

    Map<String, List<String>> typeToNdjsonFileMappings =
        validateAndFetchNdjsonFileMappings(bulkExportResponse);
    List<Pipeline> pipelines = new ArrayList<>();
    if (typeToNdjsonFileMappings != null && !typeToNdjsonFileMappings.isEmpty()) {
      // Update the transaction timestamp value from the bulkExportResponse. This value will be used
      // as the start timestamp for the next bulk export job.
      DwhFiles.forRoot(options.getOutputParquetPath(), avroConversionUtil.getFhirContext())
          .writeTimestampFile(
              bulkExportResponse.transactionTime().toInstant(),
              DwhFiles.TIMESTAMP_FILE_BULK_TRANSACTION_TIME);
      for (String type : typeToNdjsonFileMappings.keySet()) {
        Pipeline pipeline = Pipeline.create(options);
        pipeline
            .apply(Create.of(typeToNdjsonFileMappings.get(type)))
            .apply("Read Multi Ndjson Files", ParDo.of(new ReadJsonFn.FromUrl(options, true)));
        pipelines.add(pipeline);
      }
    }
    return pipelines;
  }

  private static Map<String, List<String>> validateAndFetchNdjsonFileMappings(
      BulkExportResponse bulkExportResponse) {
    if (!CollectionUtils.isEmpty(bulkExportResponse.error())) {
      log.error("Error occurred during bulk export, error={}", bulkExportResponse.error());
      throw new IllegalStateException("Error occurred during bulk export, please check logs");
    }

    if (bulkExportResponse.transactionTime() == null) {
      String errorMsg = "Transaction time cannot be empty for bulk export response";
      log.error(errorMsg);
      throw new IllegalStateException(errorMsg);
    }

    if (CollectionUtils.isEmpty(bulkExportResponse.output())
        && CollectionUtils.isEmpty(bulkExportResponse.deleted())) {
      log.warn("No resources found to be exported!");
      return Maps.newHashMap();
    }
    if (!CollectionUtils.isEmpty(bulkExportResponse.deleted())) {
      // TODO : Delete the FHIR resources
    }
    if (!CollectionUtils.isEmpty(bulkExportResponse.output())) {
      return bulkExportResponse.output().stream()
          .collect(
              Collectors.groupingBy(
                  Output::type, Collectors.mapping(Output::url, Collectors.toList())));
    }
    return Maps.newHashMap();
  }

  private static List<Pipeline> buildTopicListenerPipeline(
      FhirEtlOptions options, AvroConversionUtil avroConversionUtil) {

    Pipeline pipeline = Pipeline.create(options);

    // Read from Kafka topic
    PCollection<String> messages =
        pipeline
            .apply(
                "Read From Kafka",
                KafkaIO.<String, String>read()
                    .withBootstrapServers(options.getKafkaBootstrapServers())
                    .withTopic(options.getKafkaTopic())
                    .withConsumerConfigUpdates(
                        Map.of(
                            "group.id",
                            options.getKafkaConsumerGroup(),
                            "auto.offset.reset",
                            "earliest"))
                    .withKeyDeserializer(StringDeserializer.class)
                    .withValueDeserializer(StringDeserializer.class)
                    .withoutMetadata())
            .apply("Extract Values", Values.create());

    // Process the messages
    messages.apply("Process FHIR Bundles", ParDo.of(new ProcessFhirBundleFn(options)));

    return Arrays.asList(pipeline);
  }

  /**
   * Pipeline builder for fetching resources from a FHIR server. The mode of the pipeline is decided
   * based on the given `options`. There are currently four modes in this priority order:
   *
   * <p>1) Fetching directly from the HAPI database if `options.isJdbcModeHapi()` is set.
   *
   * <p>2) A combination of direct DB and OpenMRS FHIR API access if `options.isJdbcModeEnabled()`.
   *
   * <p>3) Reading from input JSON files if `options.getSourceJsonFilePattern()` is set.
   *
   * <p>4) Using FHIR search API if none of the above options are set.
   *
   * <p>Depending on the options, the created pipeline may write into Parquet files, push resources
   * to another FHIR server, write into another database, etc.
   *
   * @param options the pipeline options to be used.
   * @return the created Pipeline instance or null if nothing needs to be done.
   */
  static List<Pipeline> setupAndBuildPipelines(
      FhirEtlOptions options, AvroConversionUtil avroConversionUtil)
      throws IOException, SQLException, ViewDefinitionException, ProfileException {

    if (!options.getSinkDbConfigPath().isEmpty()) {
      JdbcResourceWriter.createTables(options);
    }
    FhirFetchMode fhirFetchMode = options.getFhirFetchMode();
    return switch (fhirFetchMode) {
      case HAPI_JDBC -> buildHapiJdbcPipeline(options);
      case OPENMRS_JDBC -> buildOpenmrsJdbcPipeline(options, avroConversionUtil);
      case PARQUET -> buildParquetReadPipeline(options, avroConversionUtil);
      case JSON -> buildMultiJsonReadPipeline(options, false);
      case NDJSON -> buildMultiJsonReadPipeline(options, true);
      case FHIR_SEARCH -> buildFhirSearchPipeline(options, avroConversionUtil);
      case BULK_EXPORT -> buildBulkExportReadPipeline(options, avroConversionUtil);
      case TOPIC_LISTENER -> buildTopicListenerPipeline(options, avroConversionUtil);
    };
  }

  public static void main(String[] args)
      throws IOException,
          SQLException,
          ViewDefinitionException,
          ProfileException,
          ExecutionException,
          InterruptedException {

    AvroConversionUtil.initializeAvroConverters();

    PipelineOptionsFactory.register(FhirEtlOptions.class);
    FhirEtlOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(FhirEtlOptions.class);
    log.info("Flags: " + options);
    validateOptions(options);
    AvroConversionUtil avroConversionUtil =
        AvroConversionUtil.getInstance(
            options.getFhirVersion(),
            options.getStructureDefinitionsPath(),
            options.getRecursiveDepth());
    List<Pipeline> pipelines = setupAndBuildPipelines(options, avroConversionUtil);
    EtlUtils.runMultiplePipelinesWithTimestamp(
        pipelines, options, avroConversionUtil.getFhirContext());

    log.info("DONE!");
  }
}
