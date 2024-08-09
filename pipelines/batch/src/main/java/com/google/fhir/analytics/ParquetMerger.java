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

import com.cerner.bunsen.exception.ProfileException;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.fhir.analytics.view.ViewDefinition;
import com.google.fhir.analytics.view.ViewDefinitionException;
import com.google.fhir.analytics.view.ViewManager;
import com.google.fhir.analytics.view.ViewSchema;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.io.parquet.ParquetIO.Sink;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.hl7.fhir.r4.model.codesystems.ActionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO add unit-tests for sub-pipelines.

/**
 * Pipelines for merging two FHIR data-warehouses (e.g., an old version and an incremental update)
 * which does deduplication of resources too.
 */
public class ParquetMerger {

  private static final Logger log = LoggerFactory.getLogger(ParquetMerger.class);

  private static String ID_KEY = "id";
  private static String META_KEY = "meta";
  private static String LAST_UPDATED_KEY = "lastUpdated";
  private static String TAG_KEY = "tag";
  private static String SYSTEM_KEY = "system";
  private static String CODE_KEY = "code";

  /**
   * This method reads all the Parquet files under the paths {@code dwhFilesList} for the given
   * {@code resourceType}. It then groups the records by ID key and returns the grouped PCollection.
   */
  private static PCollection<KV<String, Iterable<GenericRecord>>> readAndGroupById(
      Pipeline pipeline, List<DwhFiles> dwhFilesList, String resourceType, Schema schema) {

    // Reading all parquet files at once instead of one set at a time, reduces the number of Flink
    // reshuffle operations by one.
    PCollection<ReadableFile> inputFiles =
        pipeline
            .apply(Create.of(getParquetFilePaths(resourceType, dwhFilesList)))
            .apply(FileIO.matchAll())
            .apply(FileIO.readMatches());

    PCollection<GenericRecord> records = inputFiles.apply(ParquetIO.readFiles(schema));

    return records
        .apply(
            ParDo.of(
                new DoFn<GenericRecord, KV<String, GenericRecord>>() {
                  @ProcessElement
                  public void processElement(
                      @Element GenericRecord record,
                      OutputReceiver<KV<String, GenericRecord>> out) {
                    String id = record.get(ID_KEY).toString();
                    if (id == null) {
                      throw new IllegalArgumentException(
                          String.format("No %s key found in %s", ID_KEY, record));
                    }
                    out.output(KV.of(id, record));
                  }
                }))
        .apply(GroupByKey.create());
  }

  private static List<String> getParquetFilePaths(
      String resourceType, List<DwhFiles> dwhFilesList) {
    List<String> parquetFilePaths = new ArrayList<>();
    if (dwhFilesList != null && !dwhFilesList.isEmpty()) {
      for (DwhFiles dwhFiles : dwhFilesList) {
        parquetFilePaths.add(dwhFiles.getFilePattern(resourceType));
      }
    }
    return parquetFilePaths;
  }

  private static String getUpdateTime(GenericRecord record) {
    return ((GenericRecord) record.get(META_KEY)).get(LAST_UPDATED_KEY).toString();
  }

  /**
   * This method identifies if the record is a deleted record. For a deleted record, the meta.tag
   * would be updated with a ActionType.REMOVE during the incremental parquet file creation, the
   * same information is being reused to check if the record is deleted or not.
   *
   * @param record
   * @return
   */
  private static Boolean isRecordDeleted(GenericRecord record) {
    Object tag = ((GenericRecord) record.get(META_KEY)).get(TAG_KEY);
    if (tag != null && tag instanceof Collection) {
      Collection tagCollection = (Collection) tag;
      if (!tagCollection.isEmpty()) {
        Iterator iterator = tagCollection.iterator();
        while (iterator.hasNext()) {
          GenericRecord tagCoding = (GenericRecord) iterator.next();
          if (tagCoding.get(SYSTEM_KEY) != null
              && tagCoding.get(SYSTEM_KEY).toString().equals(ActionType.REMOVE.getSystem())
              && tagCoding.get(CODE_KEY) != null
              && tagCoding.get(CODE_KEY).toString().equals(ActionType.REMOVE.toCode())) {
            return Boolean.TRUE;
          }
        }
      }
    }
    return Boolean.FALSE;
  }

  private static GenericRecord findLastRecord(
      Iterable<GenericRecord> genericRecords, Counter numDuplicates) {
    // Note we are assuming all times have the same time-zone to avoid parsing date values.
    String lastUpdated = null;
    GenericRecord lastRecord = null;
    int numRec = 0;
    for (GenericRecord record : genericRecords) {
      numRec++;
      String updateTimeStr = getUpdateTime(record);
      if (lastUpdated == null || lastUpdated.compareTo(updateTimeStr) < 0) {
        lastUpdated = updateTimeStr;
        lastRecord = record;
      }
    }
    if (numRec > 1) {
      numDuplicates.inc();
    }
    if (numRec > 2) {
      log.warn("Record with ID {} repeated more than twice!", lastRecord.get(ID_KEY));
    }
    return lastRecord;
  }

  static List<Pipeline> createMergerPipelines(
      ParquetMergerOptions options, AvroConversionUtil avroConversionUtil)
      throws IOException, ProfileException {
    Preconditions.checkArgument(!options.getDwh1().isEmpty());
    Preconditions.checkArgument(!options.getDwh2().isEmpty());
    Preconditions.checkArgument(!options.getMergedDwh().isEmpty());

    Counter numOutputRecords =
        Metrics.counter(MetricsConstants.METRICS_NAMESPACE, MetricsConstants.NUM_OUTPUT_RECORDS);
    Counter numDuplicates =
        Metrics.counter(MetricsConstants.METRICS_NAMESPACE, MetricsConstants.NUM_DUPLICATES);

    String dwh1 = options.getDwh1();
    String dwh2 = options.getDwh2();
    String mergedDwh = options.getMergedDwh();
    DwhFiles dwhFiles1 = DwhFiles.forRoot(dwh1, avroConversionUtil.getFhirContext());
    DwhFiles dwhFiles2 = DwhFiles.forRoot(dwh2, avroConversionUtil.getFhirContext());
    DwhFiles mergedDwhFiles = DwhFiles.forRoot(mergedDwh, avroConversionUtil.getFhirContext());

    Set<String> resourceTypes1 = dwhFiles1.findNonEmptyFhirResourceTypes();
    Set<String> resourceTypes2 = dwhFiles2.findNonEmptyFhirResourceTypes();
    Set<String> dwhViews1 = new HashSet<>();
    Set<String> dwhViews2 = new HashSet<>();

    // View Definition Map to generate schema for ParquetIO
    Map<String, ViewDefinition> viewMap = new HashMap<>();

    if (options.isMergeParquetViews()) {
      dwhViews1 = dwhFiles1.findNonEmptyViewTypes();
      dwhViews2 = dwhFiles2.findNonEmptyViewTypes();

      Set<String> resourceList = new HashSet<>(resourceTypes2);
      resourceList.addAll(resourceTypes1);

      ViewManager viewManager;
      try {
        viewManager = ViewManager.createForDir(options.getViewDefinitionsDir());
      } catch (IOException | ViewDefinitionException e) {
        String errorMsg =
            String.format("Error while reading views from %s", options.getViewDefinitionsDir());
        log.error(errorMsg, e);
        throw new IllegalArgumentException(errorMsg);
      }

      viewMap =
          ViewSchema.createViewMap(
              new ArrayList<>(), new ArrayList<>(), resourceList.stream().toList(), viewManager);
      copyDistinctResources(dwhViews1, dwhViews2, dwhFiles1, dwhFiles2, mergedDwhFiles);
    }

    copyDistinctResources(resourceTypes1, resourceTypes2, dwhFiles1, dwhFiles2, mergedDwhFiles);
    List<Pipeline> pipelines = new ArrayList<>();
    for (String type : resourceTypes1) {
      if (!resourceTypes2.contains(type)) {
        continue;
      }
      Pipeline pipeline = Pipeline.create(options);
      pipelines.add(pipeline);
      log.info("Merging resource type {}", type);
      PCollection<KV<String, Iterable<GenericRecord>>> groupedRecords =
          readAndGroupById(
              pipeline,
              Arrays.asList(dwhFiles1, dwhFiles2),
              type,
              avroConversionUtil.getResourceSchema(type));

      writeMergedResources(
          groupedRecords,
          mergedDwhFiles,
          avroConversionUtil.getResourceSchema(type),
          options,
          type,
          numDuplicates,
          numOutputRecords);
    }
    if (!dwhViews1.isEmpty() || !dwhViews2.isEmpty()) {
      for (String viewName : dwhViews1) {
        if (!dwhViews2.contains(viewName)) {
          continue;
        }
        Pipeline pipeline = Pipeline.create(options);
        pipelines.add(pipeline);
        log.info("Merging materialized view {}", viewName);
        ViewDefinition viewDef = viewMap.get(viewName);
        Schema schema = ViewSchema.getAvroSchema(viewDef);
        PCollection<KV<String, Iterable<GenericRecord>>> groupedRecords =
            readAndGroupById(
                pipeline,
                Arrays.asList(dwhFiles1, dwhFiles2),
                viewName,
                ViewSchema.getAvroSchema(viewDef));

        writeMergedResources(
            groupedRecords,
            mergedDwhFiles,
            schema,
            options,
            viewName,
            numDuplicates,
            numOutputRecords);
      }
    }
    return pipelines;
  }

  public static void writeMergedResources(
      PCollection<KV<String, Iterable<GenericRecord>>> groupedRecords,
      DwhFiles mergedDwhFiles,
      Schema schema,
      ParquetMergerOptions options,
      String type,
      Counter numDuplicates,
      Counter numOutputRecords) {
    PCollection<GenericRecord> merged =
        groupedRecords
            .apply(
                ParDo.of(
                    new DoFn<KV<String, Iterable<GenericRecord>>, GenericRecord>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        KV<String, Iterable<GenericRecord>> e = c.element();
                        GenericRecord lastRecord = findLastRecord(e.getValue(), numDuplicates);
                        if (!isRecordDeleted(lastRecord)) {
                          numOutputRecords.inc();
                          c.output(lastRecord);
                        }
                      }
                    }))
            .setCoder(AvroCoder.of(schema));

    Sink parquetSink = ParquetIO.sink(schema).withCompressionCodec(CompressionCodecName.SNAPPY);
    if (options.getRowGroupSizeForParquetFiles() > 0) {
      parquetSink = parquetSink.withRowGroupSize(options.getRowGroupSizeForParquetFiles());
    }
    merged.apply(
        FileIO.<GenericRecord>write()
            .via(parquetSink)
            .to(mergedDwhFiles.getResourcePath(type).toString())
            .withSuffix(".parquet")
            // TODO if we don't set this, DirectRunner works fine but FlinkRunner only writes
            //   ~10% of the records. This is not specific to Parquet or GenericRecord; it even
            //   happens for TextIO. We should investigate this further and possibly file a bug.
            .withNumShards(options.getNumShards()));
  }

  public static void copyDistinctResources(
      Set<String> types1, Set<String> types2, DwhFiles dwh1, DwhFiles dwh2, DwhFiles mergedDwh)
      throws IOException {
    for (String type : Sets.difference(types1, types2)) {
      dwh1.copyResourcesToDwh(type, mergedDwh);
    }
    for (String type : Sets.difference(types2, types1)) {
      dwh2.copyResourcesToDwh(type, mergedDwh);
    }
  }

  public static void main(String[] args) throws IOException, ProfileException {

    AvroConversionUtil.initializeAvroConverters();
    PipelineOptionsFactory.register(ParquetMergerOptions.class);
    ParquetMergerOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(ParquetMergerOptions.class);
    log.info("Flags: " + options);
    AvroConversionUtil avroConversionUtil =
        AvroConversionUtil.getInstance(
            options.getFhirVersion(),
            options.getStructureDefinitionsPath(),
            options.getRecursiveDepth());
    if (options.getDwh1().isEmpty()
        || options.getDwh2().isEmpty()
        || options.getMergedDwh().isEmpty()) {
      throw new IllegalArgumentException("All of --dwh1, --dwh2, and --mergedDwh should be set!");
    }
    if (options.isMergeParquetViews() && options.getViewDefinitionsDir().isEmpty()) {
      throw new IllegalArgumentException(
          "When using --mergeParquetViews, --viewDefinitionsDir cannot be empty");
    }

    List<Pipeline> pipelines = createMergerPipelines(options, avroConversionUtil);
    EtlUtils.runMultipleMergerPipelinesWithTimestamp(
        pipelines, options, avroConversionUtil.getFhirContext());
    log.info("DONE!");
  }
}
