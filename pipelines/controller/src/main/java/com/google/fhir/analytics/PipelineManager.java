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
import com.google.fhir.analytics.metrics.CumulativeMetrics;
import com.google.fhir.analytics.metrics.PipelineMetrics;
import com.google.fhir.analytics.metrics.PipelineMetricsProvider;
import com.google.fhir.analytics.model.DatabaseConfiguration;
import com.google.fhir.analytics.view.ViewDefinitionException;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.beans.PropertyVetoException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;
import lombok.Data;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.metrics.GaugeResult;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.stereotype.Component;

/**
 * This is the class responsible for managing everything pipeline related. It is supposed to be a
 * singleton (and thread-safe), hence the private constructor and builder method. We rely on Spring
 * for the singleton constraint as it is the default (instead of a private constructor and a static
 * instance which does not play with Spring's dependency injection).
 */
@EnableScheduling
@Component
public class PipelineManager implements ApplicationListener<ApplicationReadyEvent> {

  private static final Logger logger = LoggerFactory.getLogger(PipelineManager.class.getName());

  @Autowired private DataProperties dataProperties;

  @Autowired private DwhFilesManager dwhFilesManager;

  @Autowired private MeterRegistry meterRegistry;

  private HiveTableManager hiveTableManager;

  private PipelineThread currentPipeline;

  private DwhFiles currentDwh;

  private LocalDateTime lastRunEnd;

  private CronExpression cron;

  // TODO expose this in the web-UI
  private LastRunStatus lastRunStatus = LastRunStatus.NOT_RUN;

  private DwhRunDetails lastRunDetails;

  private FlinkConfiguration flinkConfiguration;

  private AvroConversionUtil avroConversionUtil;

  private static final String ERROR_FILE_NAME = "error.log";
  private static final String SUCCESS = "SUCCESS";
  private static final String FAILURE = "FAILURE";

  static enum RunMode {
    INCREMENTAL,
    FULL,
    VIEWS,
  }

  /**
   * This method publishes the beam pipeline metrics to the spring boot actuator. Previous metrics
   * are removed from the actuator before publishing the latest metrics
   *
   * @param metricResults The pipeline metrics that needs to be pushed
   */
  void publishPipelineMetrics(MetricResults metricResults) {
    MetricQueryResults metricQueryResults = EtlUtils.getMetrics(metricResults);
    for (MetricResult<Long> metricCounterResult : metricQueryResults.getCounters()) {
      Gauge.builder(
              metricCounterResult.getName().getNamespace()
                  + "_"
                  + metricCounterResult.getName().getName(),
              () -> metricCounterResult.getAttempted())
          // Make a strong reference so that the value is not immediately cleared after GC
          .strongReference(true)
          .register(meterRegistry);
    }
    for (MetricResult<GaugeResult> metricGaugeResult : metricQueryResults.getGauges()) {
      Gauge.builder(
              metricGaugeResult.getName().getNamespace()
                  + "_"
                  + metricGaugeResult.getName().getName(),
              () -> metricGaugeResult.getAttempted().getValue())
          .strongReference(true)
          .register(meterRegistry);
    }
  }

  void removePipelineMetrics() {
    meterRegistry
        .getMeters()
        .forEach(
            meter -> {
              if (meter.getId().getName().startsWith(MetricsConstants.METRICS_NAMESPACE)) {
                meterRegistry.remove(meter);
              }
            });
  }

  public CumulativeMetrics getCumulativeMetrics() {
    // TODO Generate metrics and stats even for incremental and recreate views run; incremental run
    //  has two pipelines running one after the other, come up with a strategy to aggregate the
    //  metrics and generate the stats.
    if (isBatchRun() && isRunning()) {
      PipelineMetrics pipelineMetrics =
          PipelineMetricsProvider.getPipelineMetrics(currentPipeline.pipelineRunnerClass);
      return pipelineMetrics != null ? pipelineMetrics.getCumulativeMetricsForOngoingBatch() : null;
    }
    return null;
  }

  private void setLastRunStatus(LastRunStatus status) {
    lastRunStatus = status;
    if (status == LastRunStatus.SUCCESS) {
      lastRunEnd = LocalDateTime.now();
    }
  }

  @PostConstruct
  private void initDwhStatus() throws ProfileException {

    // Initialise the Flink configurations for all the pipelines
    initialiseFlinkConfiguration();
    avroConversionUtil =
        AvroConversionUtil.getInstance(
            dataProperties.getFhirVersion(),
            dataProperties.getStructureDefinitionsPath(),
            dataProperties.getRecursiveDepth());

    PipelineConfig pipelineConfig = dataProperties.createBatchOptions();
    FileSystems.setDefaultPipelineOptions(pipelineConfig.getFhirEtlOptions());
    validateFhirSourceConfiguration(pipelineConfig.getFhirEtlOptions());

    cron = CronExpression.parse(dataProperties.getIncrementalSchedule());
    String rootPrefix = dataProperties.getDwhRootPrefix();
    Preconditions.checkState(rootPrefix != null && !rootPrefix.isEmpty());

    String lastCompletedDwh = "";
    String lastDwh = "";
    String baseDir = dwhFilesManager.getBaseDir(rootPrefix);
    try {
      String prefix = dwhFilesManager.getPrefix(rootPrefix);
      List<ResourceId> paths =
          DwhFiles.getAllChildDirectories(baseDir).stream()
              .filter(dir -> dir.getFilename().startsWith(prefix))
              .collect(Collectors.toList());

      for (ResourceId path : paths) {
        if (!path.getFilename().startsWith(prefix + DwhFiles.TIMESTAMP_PREFIX)) {
          // This is not necessarily an error; the user may want to bootstrap from an already
          // created DWH outside the control-panel framework, e.g., by running the batch pipeline
          // directly.
          logger.warn(
              "DWH directory {} does not start with {}{}",
              paths,
              prefix,
              DwhFiles.TIMESTAMP_PREFIX);
        }
        if (lastDwh.isEmpty() || lastDwh.compareTo(path.getFilename()) < 0) {
          logger.debug("Found a more recent DWH {}", path.getFilename());
          lastDwh = path.getFilename();
        }
        // Do not consider if the DWH is not completely created earlier.
        if (!dwhFilesManager.isDwhComplete(path)) {
          continue;
        }
        if (lastCompletedDwh.isEmpty() || lastCompletedDwh.compareTo(path.getFilename()) < 0) {
          logger.debug("Found a more recent completed DWH {}", path.getFilename());
          lastCompletedDwh = path.getFilename();
        }
      }
    } catch (IOException e) {
      logger.error("IOException while initializing DWH: ", e);
      throw new RuntimeException(e);
    }
    if (lastCompletedDwh.isEmpty()) {
      logger.info("No DWH found; it should be created by running a full pipeline");
      currentDwh = null;
      lastRunEnd = null;
    } else {
      logger.info("Initializing with most recent DWH {}", lastCompletedDwh);
      ResourceId resourceId =
          FileSystems.matchNewResource(baseDir, true)
              .resolve(lastCompletedDwh, StandardResolveOptions.RESOLVE_DIRECTORY);
      currentDwh = DwhFiles.forRoot(resourceId.toString(), avroConversionUtil.getFhirContext());
      // There exists a DWH from before, so we set the scheduler to continue updating the DWH.
      lastRunEnd = LocalDateTime.now();
    }
    if (!lastDwh.isEmpty()) {
      initialiseLastRunDetails(baseDir, lastDwh);
    }
  }

  FhirContext getFhirContext() {
    return avroConversionUtil.getFhirContext();
  }

  private void initialiseFlinkConfiguration() {
    flinkConfiguration = new FlinkConfiguration();
    try {
      flinkConfiguration.initialiseFlinkConfiguration(dataProperties);
    } catch (IOException e) {
      logger.error("IOException while initializing Flink Configuration: ", e);
      throw new RuntimeException(e);
    }
  }

  /**
   * This method initialises the lastRunDetails based on the dwh snapshot created recently. In case
   * an incremental run exists in the snapshot, the status is set based on the incremental run.
   */
  private void initialiseLastRunDetails(String baseDir, String dwhDirectory) {
    try {
      ResourceId dwhDirectoryPath =
          FileSystems.matchNewResource(baseDir, true)
              .resolve(dwhDirectory, StandardResolveOptions.RESOLVE_DIRECTORY);
      DwhFiles dwhFiles =
          DwhFiles.forRoot(dwhDirectoryPath.toString(), avroConversionUtil.getFhirContext());
      ResourceId incPath = dwhFiles.getLatestIncrementalRunPath();
      if (incPath != null) {
        updateLastRunDetails(incPath);
        return;
      }
      // In case of no incremental run, the status is set based on the dwhDirectory snapshot.
      updateLastRunDetails(dwhDirectoryPath);
    } catch (IOException e) {
      logger.error("IOException while initialising last run details ", e);
      throw new RuntimeException(e);
    }
  }

  private void updateLastRunDetails(ResourceId dwhDirectoryPath) throws IOException {
    if (dwhFilesManager.isDwhComplete(dwhDirectoryPath)) {
      setLastRunDetails(dwhDirectoryPath.toString(), SUCCESS);
    } else {
      setLastRunDetails(dwhDirectoryPath.toString(), FAILURE);
    }
  }

  /**
   * Validate the FHIR source configuration parameters during the launch of the application. This is
   * to detect any mis-configurations earlier enough and avoid failures during pipeline runs.
   */
  void validateFhirSourceConfiguration(FhirEtlOptions options) throws ProfileException {
    if (Boolean.TRUE.equals(options.isJdbcModeHapi())) {
      validateDbConfigParameters(options.getFhirDatabaseConfigPath());
    } else if (!Strings.isNullOrEmpty(options.getFhirServerUrl())) {
      validateFhirSearchParameters(options);
    }
  }

  private void validateDbConfigParameters(String dbConfigPath) {
    try {
      DatabaseConfiguration dbConfiguration =
          DatabaseConfiguration.createConfigFromFile(dbConfigPath);
      boolean isValid =
          JdbcConnectionUtil.validateJdbcDetails(
              dbConfiguration.getJdbcDriverClass(),
              dbConfiguration.makeJdbsUrlFromConfig(),
              dbConfiguration.getDatabaseUser(),
              dbConfiguration.getDatabasePassword());
      Preconditions.checkArgument(isValid);
    } catch (IOException | SQLException | ClassNotFoundException e) {
      logger.error("Error occurred while validating jdbc details", e);
      throw new RuntimeException(e);
    }
  }

  private void validateFhirSearchParameters(FhirEtlOptions options) throws ProfileException {
    FhirSearchUtil fhirSearchUtil = getFhirSearchUtil(options);
    fhirSearchUtil.testFhirConnection();
  }

  private FhirSearchUtil getFhirSearchUtil(FhirEtlOptions options) {
    return new FhirSearchUtil(
        new FetchUtil(
            options.getFhirServerUrl(),
            options.getFhirServerUserName(),
            options.getFhirServerPassword(),
            options.getFhirServerOAuthTokenEndpoint(),
            options.getFhirServerOAuthMechanism(),
            options.getFhirServerOAuthClientId(),
            options.getFhirServerOAuthClientSecret(),
            avroConversionUtil.getFhirContext()));
  }

  synchronized boolean isBatchRun() {
    return (currentPipeline != null) && (currentPipeline.runMode == RunMode.FULL);
  }

  synchronized boolean isRunning() {
    return currentPipeline != null && currentPipeline.isAlive();
  }

  synchronized String getCurrentDwhRoot() {
    if (currentDwh == null) {
      return "";
    }
    return currentDwh.getRoot();
  }

  /**
   * @return the next scheduled time to run the incremental pipeline or null iff a pipeline is
   *     currently running or no previous DWH exist.
   */
  LocalDateTime getNextIncrementalTime() {
    if (isRunning() || lastRunEnd == null) {
      return null;
    }
    return cron.next(lastRunEnd);
  }

  DwhRunDetails getLastRunDetails() {
    return lastRunDetails;
  }

  // Every 30 seconds, check for pipeline status and incremental pipeline schedule.
  @Scheduled(fixedDelay = 30000)
  private void checkSchedule()
      throws IOException,
          PropertyVetoException,
          SQLException,
          ViewDefinitionException,
          ProfileException {
    LocalDateTime next = getNextIncrementalTime();
    if (next == null) {
      return;
    }
    logger.info("Last run was at {} next run is at {}", lastRunEnd, next);
    if (next.compareTo(LocalDateTime.now()) < 0) {
      logger.info("Incremental run triggered at {}", LocalDateTime.now());
      runIncrementalPipeline();
    }
  }

  synchronized void runBatchPipeline(boolean isRecreateViews) {
    Preconditions.checkState(!isRunning(), "cannot start a pipeline while another one is running");
    Preconditions.checkState(
        !Strings.isNullOrEmpty(getCurrentDwhRoot()) || !isRecreateViews,
        "cannot recreate views because no DWH is found!");
    PipelineConfig pipelineConfig =
        (isRecreateViews
            ? dataProperties.createRecreateViewsOptions(getCurrentDwhRoot())
            : dataProperties.createBatchOptions());
    FhirEtlOptions options = pipelineConfig.getFhirEtlOptions();
    FlinkPipelineOptions flinkOptions = options.as(FlinkPipelineOptions.class);
    if (!Strings.isNullOrEmpty(flinkConfiguration.getFlinkConfDir())) {
      flinkOptions.setFlinkConfDir(flinkConfiguration.getFlinkConfDir());
    }
    // sanity check; should always pass!
    FhirEtl.validateOptions(options);

    currentPipeline =
        new PipelineThread(
            options,
            this,
            dwhFilesManager,
            dataProperties,
            pipelineConfig,
            isRecreateViews ? RunMode.VIEWS : RunMode.FULL,
            avroConversionUtil,
            FlinkRunner.class);
    if (isRecreateViews) {
      logger.info(
          "Running pipeline for recreating views from DWH {}", options.getParquetInputDwhRoot());
    } else {
      logger.info("Running full pipeline for DWH {}", options.getOutputParquetPath());
    }
    // We will only have one thread for running pipelines hence no need for a thread pool.
    currentPipeline.start();
  }

  synchronized void runIncrementalPipeline() throws IOException {
    // TODO do the same as above but read/set --since
    Preconditions.checkState(!isRunning(), "cannot start a pipeline while another one is running");
    Preconditions.checkState(
        currentDwh != null,
        "cannot start the incremental pipeline while there are no DWHs; run full pipeline");
    PipelineConfig pipelineConfig = dataProperties.createBatchOptions();
    FhirEtlOptions options = pipelineConfig.getFhirEtlOptions();
    String finalDwhRoot = options.getOutputParquetPath();
    String incrementalDwhRoot = currentDwh.newIncrementalRunPath().toString();
    options.setOutputParquetPath(incrementalDwhRoot);
    String since = fetchSinceTimestamp(options);
    options.setSince(since);
    FlinkPipelineOptions flinkOptionsForBatch = options.as(FlinkPipelineOptions.class);
    if (!Strings.isNullOrEmpty(flinkConfiguration.getFlinkConfDir())) {
      flinkOptionsForBatch.setFlinkConfDir(flinkConfiguration.getFlinkConfDir());
    }
    // sanity check; should always pass!
    FhirEtl.validateOptions(options);

    // The merger pipeline merges the original full DWH with the new incremental one.
    ParquetMergerOptions mergerOptions = PipelineOptionsFactory.as(ParquetMergerOptions.class);
    mergerOptions.setDwh1(currentDwh.getRoot());
    mergerOptions.setDwh2(incrementalDwhRoot);
    mergerOptions.setMergedDwh(finalDwhRoot);
    mergerOptions.setRunner(FlinkRunner.class);
    mergerOptions.setViewDefinitionsDir(options.getViewDefinitionsDir());
    // The number of shards is set based on the parallelism available for the FlinkRunner
    // Pipeline
    // TODO: For Flink non-local mode, refactor this to be not dependent on the
    // pipeline-controller
    //  machine https://github.com/google/fhir-data-pipes/issues/893
    int numShards =
        dataProperties.getNumThreads() > 0
            ? dataProperties.getNumThreads()
            : Runtime.getRuntime().availableProcessors();
    mergerOptions.setNumShards(numShards);
    mergerOptions.setStructureDefinitionsPath(
        Strings.nullToEmpty(dataProperties.getStructureDefinitionsPath()));
    mergerOptions.setFhirVersion(dataProperties.getFhirVersion());
    if (dataProperties.getRowGroupSizeForParquetFiles() > 0) {
      mergerOptions.setRowGroupSizeForParquetFiles(dataProperties.getRowGroupSizeForParquetFiles());
    }
    FlinkPipelineOptions flinkOptionsForMerge = mergerOptions.as(FlinkPipelineOptions.class);
    if (!Strings.isNullOrEmpty(flinkConfiguration.getFlinkConfDir())) {
      flinkOptionsForMerge.setFlinkConfDir(flinkConfiguration.getFlinkConfDir());
    }
    flinkOptionsForMerge.setFasterCopy(true);
    if (dataProperties.getNumThreads() > 0) {
      flinkOptionsForMerge.setParallelism(dataProperties.getNumThreads());
    }
    // Creating a thread for running both pipelines, one after the other.
    currentPipeline =
        new PipelineThread(
            options,
            mergerOptions,
            this,
            dwhFilesManager,
            dataProperties,
            pipelineConfig,
            avroConversionUtil,
            FlinkRunner.class);
    logger.info("Running incremental pipeline for DWH {} since {}", currentDwh.getRoot(), since);
    currentPipeline.start();
  }

  private String fetchSinceTimestamp(FhirEtlOptions options) throws IOException {
    Instant timestamp = null;
    if (FhirFetchMode.BULK_EXPORT.equals(options.getFhirFetchMode())) {
      try {
        timestamp = currentDwh.readTimestampFile(DwhFiles.TIMESTAMP_FILE_BULK_TRANSACTION_TIME);
      } catch (NoSuchFileException e) {
        logger.warn(
            "No bulk export timestamp file found for the previous run, will try to rely on the"
                + " start timestamp");
      }
    }

    if (timestamp == null) {
      timestamp = currentDwh.readTimestampFile(DwhFiles.TIMESTAMP_FILE_START);
    }
    if (timestamp == null) {
      throw new IllegalStateException(
          "Timestamp value is empty and hence cannot start the pipeline");
    }
    return timestamp.toString();
  }

  @Override
  public void onApplicationEvent(ApplicationReadyEvent applicationReadyEvent) {
    if (!dataProperties.isCreateHiveResourceTables()) {
      logger.info("createHiveResourceTables is false; skipping Hive table creation.");
      return;
    }
    DatabaseConfiguration dbConfig;
    try {
      dbConfig =
          DatabaseConfiguration.createConfigFromFile(dataProperties.getThriftserverHiveConfig());
    } catch (IOException e) {
      logger.error("Exception while reading thrift hive config.");
      throw new RuntimeException(e);
    }
    try {
      hiveTableManager = new HiveTableManager(dbConfig, dataProperties.getHiveResourceViewsDir());
      hiveTableManager.showTables();
    } catch (SQLException e) {
      logger.error("Exception while querying the thriftserver: ", e);
      throw new RuntimeException(e);
    }

    // Making sure that the tables for the current DWH files are created.
    createResourceTables();
  }

  /**
   * This method, upon Controller start, checks on Thrift sever to create resource tables if they
   * don't exist. There is a @PostConstruct method present in this class which is initDwhStatus and
   * the reason below code has not been added because dataProperties.getThriftserverHiveConfig()
   * turns out to be null there.
   */
  public void createResourceTables() {
    if (!dataProperties.isCreateHiveResourceTables()) {
      logger.info("createHiveResourceTables is false; skipping Hive table creation.");
      return;
    }
    String rootPrefix = dataProperties.getDwhRootPrefix();
    Preconditions.checkState(rootPrefix != null && !rootPrefix.isEmpty());

    String prefix = dwhFilesManager.getPrefix(rootPrefix);
    String baseDir = dwhFilesManager.getBaseDir(rootPrefix);

    try {
      List<ResourceId> paths =
          DwhFiles.getAllChildDirectories(baseDir).stream()
              .filter(dir -> dir.getFilename().startsWith(prefix + DwhFiles.TIMESTAMP_PREFIX))
              .filter(
                  dir -> {
                    try {
                      return dwhFilesManager.isDwhComplete(dir);
                    } catch (IOException e) {
                      logger.error("Error while accessing {}", dir, e);
                    }
                    return false;
                  })
              .collect(Collectors.toList());

      Preconditions.checkState(paths != null, "Make sure DWH prefix is a valid path!");

      // Sort snapshots directories such that the canonical view is created for the latest one.
      Collections.sort(paths, Comparator.comparing(ResourceId::toString));

      // TODO: Why are we creating these tables for all paths and not just the most recent? If all
      //  are needed, why are we doing the above `sort`?
      for (ResourceId path : paths) {
        String[] tokens = path.getFilename().split(prefix + DwhFiles.TIMESTAMP_PREFIX);
        if (tokens.length > 1) {
          String timestamp = tokens[1];
          logger.info("Creating resource tables for relative path {}", path.getFilename());
          String fileSeparator = DwhFiles.getFileSeparatorForDwhFiles(rootPrefix);
          List<String> existingResources =
              dwhFilesManager.findExistingResources(baseDir + fileSeparator + path.getFilename());
          try {
            hiveTableManager.createResourceAndCanonicalTables(
                existingResources, timestamp, path.getFilename());
          } catch (SQLException e) {
            logger.error(
                "Exception while creating resource table on thriftserver for path: {}",
                path.getFilename(),
                e);
          }
        }
      }
    } catch (IOException e) {
      // In case of exceptions at this stage, we just log the exception.
      logger.error("Exception while reading thriftserver parquet output directory: ", e);
    }
  }

  HiveTableManager getHiveTableManager() {
    return hiveTableManager;
  }

  private synchronized void updateDwh(String newRoot) throws ProfileException {
    currentDwh = DwhFiles.forRoot(newRoot, avroConversionUtil.getFhirContext());
  }

  private static class PipelineThread extends Thread {
    private FhirEtlOptions options;
    private final PipelineManager manager;
    private final DwhFilesManager dwhFilesManager;
    // This is used in the incremental mode only.
    private final ParquetMergerOptions mergerOptions;

    private final DataProperties dataProperties;

    private final PipelineConfig pipelineConfig;

    private final RunMode runMode;

    private AvroConversionUtil avroConversionUtil;

    private Class<? extends PipelineRunner> pipelineRunnerClass;

    PipelineThread(
        FhirEtlOptions options,
        PipelineManager manager,
        DwhFilesManager dwhFilesManager,
        DataProperties dataProperties,
        PipelineConfig pipelineConfig,
        RunMode runMode,
        AvroConversionUtil avroConversionUtil,
        Class<? extends PipelineRunner> pipelineRunnerClass) {
      Preconditions.checkArgument(options != null);
      this.options = options;
      this.manager = manager;
      this.dwhFilesManager = dwhFilesManager;
      this.dataProperties = dataProperties;
      this.pipelineConfig = pipelineConfig;
      this.runMode = runMode;
      this.mergerOptions = null;
      this.avroConversionUtil = avroConversionUtil;
      this.pipelineRunnerClass = pipelineRunnerClass;
    }

    // The constructor for the incremental pipeline (hence the `mergerOptions`).
    PipelineThread(
        FhirEtlOptions options,
        ParquetMergerOptions mergerOptions,
        PipelineManager manager,
        DwhFilesManager dwhFilesManager,
        DataProperties dataProperties,
        PipelineConfig pipelineConfig,
        AvroConversionUtil avroConversionUtil,
        Class<? extends PipelineRunner> pipelineRunnerClass) {
      Preconditions.checkArgument(options != null);
      this.options = options;
      this.manager = manager;
      this.dwhFilesManager = dwhFilesManager;
      this.mergerOptions = mergerOptions;
      this.dataProperties = dataProperties;
      this.pipelineConfig = pipelineConfig;
      this.avroConversionUtil = avroConversionUtil;
      this.pipelineRunnerClass = pipelineRunnerClass;
      this.runMode = RunMode.INCREMENTAL;
    }

    @Override
    public void run() {
      long start = System.currentTimeMillis();
      // The number of threads may increase after a few runs, but it should eventually go down.
      logger.info(
          "Pipelines execution started with a new thread; number of threads is {}",
          Thread.activeCount());
      String currentDwhRoot = null;
      FhirContext fhirContext = null;
      try {
        if (runMode != RunMode.VIEWS) {
          currentDwhRoot = options.getOutputParquetPath();
        } else {
          currentDwhRoot = options.getParquetInputDwhRoot();
        }
        fhirContext = avroConversionUtil.getFhirContext();

        List<Pipeline> pipelines = FhirEtl.setupAndBuildPipelines(options, avroConversionUtil);
        if (pipelines == null || pipelines.isEmpty()) {
          logger.warn("No resources found to be fetched!");
          manager.setLastRunStatus(LastRunStatus.SUCCESS);
          return;
        }

        List<PipelineResult> pipelineResults =
            EtlUtils.runMultiplePipelinesWithTimestamp(pipelines, options, fhirContext);
        // Remove the metrics of the previous pipeline and register the new metrics
        manager.removePipelineMetrics();
        pipelineResults.stream()
            .forEach(pipelineResult -> manager.publishPipelineMetrics(pipelineResult.metrics()));
        if (runMode == RunMode.VIEWS) {
          // Nothing more is needed to be done as we do not recreate a new DWH in this mode.
          // TODO record timing info and other details in this case.
          return;
        }
        if (mergerOptions == null) { // Do not update DWH yet if this was an incremental run.
          manager.updateDwh(currentDwhRoot);
        } else {
          currentDwhRoot = mergerOptions.getMergedDwh();
          List<Pipeline> mergerPipelines =
              ParquetMerger.createMergerPipelines(mergerOptions, avroConversionUtil);
          logger.info("Merger options are {}", mergerOptions);
          List<PipelineResult> mergerPipelineResults =
              EtlUtils.runMultipleMergerPipelinesWithTimestamp(
                  mergerPipelines, mergerOptions, fhirContext);
          mergerPipelineResults.stream()
              .forEach(pipelineResult -> manager.publishPipelineMetrics(pipelineResult.metrics()));
          manager.updateDwh(currentDwhRoot);
        }
        if (dataProperties.isCreateHiveResourceTables()) {
          List<String> existingResources = dwhFilesManager.findExistingResources(currentDwhRoot);
          createHiveResourceTables(
              existingResources,
              pipelineConfig.getTimestampSuffix(),
              pipelineConfig.getThriftServerParquetPath());
        }
        manager.setLastRunStatus(LastRunStatus.SUCCESS);
        manager.setLastRunDetails(currentDwhRoot, SUCCESS);
      } catch (Exception e) {
        logger.error("exception while running pipeline: ", e);
        manager.captureError(fhirContext, currentDwhRoot, e);
        manager.setLastRunDetails(currentDwhRoot, FAILURE);
        manager.setLastRunStatus(LastRunStatus.FAILURE);
      } finally {
        // See https://github.com/google/fhir-data-pipes/issues/777#issuecomment-1703142297
        System.gc();
        logger.info(
            "Total time taken for the pipelines = {} secs",
            (System.currentTimeMillis() - start) / 1000);
      }
    }

    private void createHiveResourceTables(
        List<String> resourceList, String timestampSuffix, String thriftServerParquetPath)
        throws IOException, SQLException {
      logger.info("Establishing connection to Thrift server Hive");
      DatabaseConfiguration dbConfig =
          DatabaseConfiguration.createConfigFromFile(dataProperties.getThriftserverHiveConfig());

      logger.info("Creating resources on Hive server for resources: {}", resourceList);
      manager
          .getHiveTableManager()
          .createResourceAndCanonicalTables(resourceList, timestampSuffix, thriftServerParquetPath);
      logger.info("Created resources on Thrift server Hive");
    }
  }

  /** This method captures the given exception into a file rooted at the dwhRoot location. */
  void captureError(FhirContext fhirContext, String dwhRoot, Exception e) {
    try {
      if (!Strings.isNullOrEmpty(dwhRoot)) {
        String stackTrace = ExceptionUtils.getStackTrace(e);
        DwhFiles.forRoot(dwhRoot, fhirContext)
            .overwriteFile(ERROR_FILE_NAME, stackTrace.getBytes(StandardCharsets.UTF_8));
      }
    } catch (IOException ex) {
      logger.error("Error while capturing error to a file", ex);
    }
  }

  /** Sets the details of the last pipeline run with the given dwhRoot as the snapshot location. */
  void setLastRunDetails(String dwhRoot, String status) {
    DwhRunDetails dwhRunDetails = new DwhRunDetails();
    try {
      DwhFiles dwhFiles = DwhFiles.forRoot(dwhRoot, avroConversionUtil.getFhirContext());
      String startTime = dwhFiles.readTimestampFile(DwhFiles.TIMESTAMP_FILE_START).toString();
      dwhRunDetails.setStartTime(startTime);
      if (!Strings.isNullOrEmpty(status) && status.equalsIgnoreCase(SUCCESS)) {
        String endTime = dwhFiles.readTimestampFile(DwhFiles.TIMESTAMP_FILE_END).toString();
        dwhRunDetails.setEndTime(endTime);
      } else {
        String fileSeparator = DwhFiles.getFileSeparatorForDwhFiles(dwhRoot);
        dwhRoot = dwhRoot.endsWith(fileSeparator) ? dwhRoot : dwhRoot + fileSeparator;
        ResourceId errorResource = FileSystems.matchNewResource(dwhRoot + ERROR_FILE_NAME, false);
        if (dwhFilesManager.doesFileExist(errorResource)) {
          dwhRunDetails.setErrorLogPath(dwhRoot + ERROR_FILE_NAME);
        }
      }
      dwhRunDetails.setStatus(status);
      this.lastRunDetails = dwhRunDetails;
    } catch (IOException e) {
      logger.error("Error while updating last run details", e);
      throw new RuntimeException(e);
    }
  }

  public enum LastRunStatus {
    NOT_RUN,
    SUCCESS,
    FAILURE
  }

  @Data
  public class DwhRunDetails {

    private String startTime;
    private String endTime;
    private String status;
    private String errorLogPath;
  }
}
