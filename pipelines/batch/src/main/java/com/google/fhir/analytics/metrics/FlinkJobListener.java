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
package com.google.fhir.analytics.metrics;

import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobListener;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlinkJobListener implements JobListener {

  private static final Logger logger = LoggerFactory.getLogger(FlinkJobListener.class);

  @Override
  public void onJobSubmitted(@Nullable JobClient jobClient, @Nullable Throwable throwable) {
    if (throwable != null) {
      logger.error("Error while submitting the job", throwable);
      return;
    }

    logger.info("Submitting the job with ID {} ", this);
    FlinkPipelineMetrics flinkPipelineMetrics =
        (FlinkPipelineMetrics) PipelineMetricsProvider.getPipelineMetrics(FlinkRunner.class);
    flinkPipelineMetrics.addJobClient(jobClient);
  }

  @Override
  public void onJobExecuted(
      @Nullable JobExecutionResult jobExecutionResult, @Nullable Throwable throwable) {

    // Exactly one of `jobExecutionResult` should be null according to the contract.
    if (throwable != null || jobExecutionResult == null) {
      logger.error("Error while executing the job", throwable);
      return;
    }

    logger.info(
        "Clearing the job with ID {}, jobExecutionResult={}",
        jobExecutionResult.getJobID(),
        jobExecutionResult);
    FlinkPipelineMetrics flinkPipelineMetrics =
        (FlinkPipelineMetrics) PipelineMetricsProvider.getPipelineMetrics(FlinkRunner.class);
    flinkPipelineMetrics.removeJobClient(jobExecutionResult.getJobID().toHexString());
  }
}
