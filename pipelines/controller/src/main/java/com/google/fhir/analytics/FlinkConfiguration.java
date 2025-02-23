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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.fhir.analytics.metrics.FlinkJobListener;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.taskexecutor.TaskExecutorResourceUtils;
import org.apache.parquet.hadoop.ParquetWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.FileSystemUtils;

/** This class is responsible for the generation and validation of Flink configuration file. */
public class FlinkConfiguration {

  private static final Logger logger = LoggerFactory.getLogger(FlinkConfiguration.class.getName());
  static final String TEMP_FLINK_CONF_DIR = "tmp-flink-conf";

  /**
   * This value is arrived based on the maximum number of reshuffle/partition operations that can
   * occur in the pipelines. This has to be updated if any modifications are made to the pipelines
   * that impact the reshuffle/partition operations.
   *
   * <p>Currently, for incremental run a maximum of 7 reshuffle/partition operations take place.
   *
   * <ul>
   *   <li>2 reshuffle operations for matching and reading from Parquet files
   *   <li>1 reshuffle operation for GroupByKey
   *   <li>4 reshuffle operations for writing back to new parquet files (some kind of map-reduce
   *       logic is used).
   * </ul>
   *
   * <p>An extra number is allocated as a buffer
   */
  static final int NUMBER_OF_RESHUFFLES = 8;

  private static final int DEFAULT_PARALLELISM = Runtime.getRuntime().availableProcessors();
  static final String DEFAULT_MANAGED_MEMORY_SIZE = "256mb";

  /**
   * The factor of how much memory is needed when parquet row groups are read and decoded into
   * in-memory. 2 is a rough estimate *
   */
  private static final int PARQUET_ROW_GROUP_INFLATION_FACTOR = 2;

  private static final String KEY_VALUE_FORMAT = "%s: %s";
  private String flinkConfDir;

  public String getFlinkConfDir() {
    return flinkConfDir;
  }

  /**
   * This method auto-generates/validates the flink-conf.yaml file. The auto generated file contains
   * parameter values which are optimised for the pipelines to run without fail. In case of
   * auto-generated file, the file is placed in a temporary directory and will be deleted once the
   * application is stopped.
   *
   * <p>In particular the task manager network buffer size for each pipeline is inspired from this
   * <a
   * href="https://nightlies.apache.org/flink/flink-docs-release-1.8/ops/config.html#configuring-the-network-buffers">link</a>
   *
   * <p>This method also fails fast if the generated/passed flink configuration values might result
   * in failure based on the JVM Heap memory values.
   *
   * @param dataProperties the application properties
   * @throws IOException
   */
  void initialiseFlinkConfiguration(DataProperties dataProperties) throws IOException {
    boolean isFlinkModelLocal = isFlinkModeLocal(dataProperties);
    Preconditions.checkState(
        isFlinkModelLocal || !dataProperties.isAutoGenerateFlinkConfiguration(),
        "Auto-generation of Flink configuration is only applicable for Local execution mode");
    // Do not generate or validate the Flink configuration in case of non-local mode
    if (!isFlinkModelLocal) return;

    validateHeapMemoryConfig(dataProperties);
    if (dataProperties.isAutoGenerateFlinkConfiguration()) {
      Path confPath = Files.createTempDirectory(TEMP_FLINK_CONF_DIR);
      logger.info("Creating Flink temporary configuration directory at {}", confPath);
      Path filePath = Paths.get(confPath.toString(), GlobalConfiguration.FLINK_CONF_FILENAME);
      validateAndCreateConfFile(filePath, dataProperties);
      this.flinkConfDir = confPath.toString();
      deleteFilesRecursivelyOnExit(confPath);
    } else {
      String envFlinkConfDir = System.getenv(ConfigConstants.ENV_FLINK_CONF_DIR);
      Configuration configuration;
      if (Strings.isNullOrEmpty(envFlinkConfDir)) {
        logger.warn(
            "The environment variable {} is not set, hence default flink configuration will"
                + " be used.",
            ConfigConstants.ENV_FLINK_CONF_DIR);
        configuration = GlobalConfiguration.loadConfiguration();
      } else {
        logger.info(
            "Flink configuration will be initialised from the directory at {}", envFlinkConfDir);
        configuration = GlobalConfiguration.loadConfiguration(envFlinkConfDir);
      }
      configuration = TaskExecutorResourceUtils.adjustForLocalExecution(configuration);
      validateDirectMemoryMax(
          configuration.get(TaskManagerOptions.NETWORK_MEMORY_MAX).getBytes(),
          configuration.get(TaskManagerOptions.MANAGED_MEMORY_SIZE).getBytes());
    }
  }

  private void validateAndCreateConfFile(Path filePath, DataProperties dataProperties)
      throws IOException {
    long networkMemoryMax = calculateNetworkMemoryMax(dataProperties);
    validateDirectMemoryMax(networkMemoryMax, MemorySize.parseBytes(DEFAULT_MANAGED_MEMORY_SIZE));
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath.toString()))) {
      logger.info("Network memory : {}", networkMemoryMax);
      writer.write(
          String.format(
              KEY_VALUE_FORMAT, TaskManagerOptions.NETWORK_MEMORY_MAX.key(), networkMemoryMax));
      writer.newLine();
      writer.write(
          String.format(
              KEY_VALUE_FORMAT,
              TaskManagerOptions.MANAGED_MEMORY_SIZE.key(),
              DEFAULT_MANAGED_MEMORY_SIZE));
      writer.newLine();
      writer.write(
          String.format(KEY_VALUE_FORMAT, DeploymentOptions.ATTACHED.key(), Boolean.FALSE));
      writer.newLine();
      writer.write(
          String.format(
              KEY_VALUE_FORMAT,
              DeploymentOptions.JOB_LISTENERS.key(),
              FlinkJobListener.class.getName()));
    }
  }

  private void validateDirectMemoryMax(long networkMemoryInBytes, long managedMemoryInBytes) {
    /*
    long totalDirectMemory = networkMemoryInBytes + managedMemoryInBytes;
    long totalDirectMemoryOfAllParallelPipelines =
        totalDirectMemory * EtlUtils.NO_OF_PARALLEL_PIPELINES;

    long jvmMaxDirectMemory = jdk.internal.misc.VM.maxDirectMemory();
    if (jvmMaxDirectMemory <= totalDirectMemoryOfAllParallelPipelines) {
      String errorMessage =
          String.format(
              "Total Flink Off-Heap Memory param values should be less than the JVM Maximum Direct"
                  + " Memory. JVM maxDirectMemory=%s bytes, Total Flink Off-Heap Memory=%s bytes",
              jvmMaxDirectMemory, totalDirectMemoryOfAllParallelPipelines);
      logger.error(errorMessage);
      throw new IllegalConfigurationException(errorMessage);
    }
     */
  }

  private long calculateNetworkMemoryMax(DataProperties dataProperties) {
    int parallelism =
        dataProperties.getNumThreads() > 0 ? dataProperties.getNumThreads() : DEFAULT_PARALLELISM;
    int numOfNetworkBuffers = (int) Math.pow(parallelism + 1, 2) * NUMBER_OF_RESHUFFLES;
    return numOfNetworkBuffers * TaskManagerOptions.MEMORY_SEGMENT_SIZE.defaultValue().getBytes();
  }

  private void deleteFilesRecursivelyOnExit(Path path) {
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  try {
                    FileSystemUtils.deleteRecursively(path);
                  } catch (IOException e) {
                    logger.warn(
                        "Error occurred while deleting the files recursively for the path {}",
                        path.toString());
                  }
                }));
  }

  /**
   * Validates if the JVM memory is sufficient to run pipelines successfully. This validation helps
   * the application to fail-fast rather than failing during batch execution.
   */
  private void validateHeapMemoryConfig(DataProperties dataProperties) {
    long maxMemory = Runtime.getRuntime().maxMemory();
    long minJVMMemoryRequired = minJVMMemory(dataProperties);
    if (minJVMMemoryRequired > maxMemory) {
      throw new IllegalConfigurationException(
          String.format(
              "Insufficient max JVM memory, required %s MB but provided %s MB",
              memoryInMB(minJVMMemoryRequired), memoryInMB(maxMemory)));
    }
  }

  /**
   * The minimum JVM memory needed for the pipelines to run without fail. This is derived based on
   * the number of parquet row groups that can be read or written in parallel by the pipeline
   * threads. This is roughly derived by the below formula.
   *
   * <p>Minimum JVM Memory = Memory for JVM stack, perm files etc + (#Parallel Pipeline Threads *
   * #Parallel Pipelines * Parquet Row Group Size * Row Group Inflation factor)
   */
  private long minJVMMemory(DataProperties dataProperties) {
    // If the parquet files have already been created with a different row group size using the
    // pipelines earlier, then the below values won't be honored. The below values will be applied
    // only on the files which are written and read by the new pipelines.
    int rowGroupSize =
        dataProperties.getRowGroupSizeForParquetFiles() > 0
            ? dataProperties.getRowGroupSizeForParquetFiles()
            : ParquetWriter.DEFAULT_BLOCK_SIZE;
    int parallelism =
        dataProperties.getNumThreads() > 0 ? dataProperties.getNumThreads() : DEFAULT_PARALLELISM;
    // This is the minimum memory required for reading/writing parquet row groups in parallel by
    // pipelines. Row group size is multiplied by inflation factor to accommodate the memory needed
    // when the parquet rows are read and decoded in memory.
    long memoryNeededForParquetRowGroups =
        (long) parallelism
            * EtlUtils.NO_OF_PARALLEL_PIPELINES
            * rowGroupSize
            * PARQUET_ROW_GROUP_INFLATION_FACTOR;
    // This is required for stack, perm files etc.
    long minJVMMemoryMisc = 512 * 1024 * 1024;
    return memoryNeededForParquetRowGroups + minJVMMemoryMisc;
  }

  private long memoryInMB(long memoryInBytes) {
    return memoryInBytes / (1024 * 1024);
  }

  private boolean isFlinkModeLocal(DataProperties dataProperties) {
    // TODO: Enable the pipeline for Flink non-local modes as well
    // https://github.com/google/fhir-data-pipes/issues/893
    return true;
  }
}
