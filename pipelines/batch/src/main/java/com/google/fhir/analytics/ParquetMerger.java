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
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.google.fhir.analytics.view.ViewDefinition;
import com.google.fhir.analytics.view.ViewDefinitionException;
import com.google.fhir.analytics.view.ViewManager;
import com.google.fhir.analytics.view.ViewSchema;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
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
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.hl7.fhir.r4.model.codesystems.ActionType;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO add unit-tests for sub-pipelines.

/**
 * Pipelines for merging two FHIR data-warehouses (e.g., an old version and an incremental update)
 * which does deduplication of resources too.
 */
public class ParquetMerger {

  private static final Logger log = LoggerFactory.getLogger(ParquetMerger.class);

  private static final TupleTag<GenericRecord> oldDwh = new TupleTag<>();

  private static final TupleTag<GenericRecord> newDwh = new TupleTag<>();

  private static String ID_KEY = "id";
  private static String META_KEY = "meta";
  private static String LAST_UPDATED_KEY = "lastUpdated";
  private static String TAG_KEY = "tag";
  private static String SYSTEM_KEY = "system";
  private static String CODE_KEY = "code";

  /**
   * This method reads all the Parquet files under the paths {@code dwhFilesList} for the given
   * {@code viewName}. It then groups the records by ID key, tags the records according to their
   * creation timestamp and returns the grouped PCollection.
   */
  private static PCollection<KV<String, CoGbkResult>> readViewAndGroupById(
      Pipeline pipeline, DwhFiles dwh1, DwhFiles dwh2, String viewName, Schema schema)
      throws IOException {

    // TODO:The extra reshuffle may be worth it to keep the logic simple and due to the smaller view
    // schemas. Ask Chandra is the performance hit (memory-wise) won't be as large

    // Reading all parquet files at once instead of one set at a time, reduces the number of Flink
    // reshuffle operations by one.
    PCollection<ReadableFile> inputFiles1 =
        pipeline
            .apply(Create.of(getParquetFilePaths(viewName, Arrays.asList(dwh1))))
            .apply(FileIO.matchAll())
            .apply(FileIO.readMatches());

    PCollection<ReadableFile> inputFiles2 =
        pipeline
            .apply(Create.of(getParquetFilePaths(viewName, Arrays.asList(dwh2))))
            .apply(FileIO.matchAll())
            .apply(FileIO.readMatches());

    PCollection<GenericRecord> records1 = inputFiles1.apply(ParquetIO.readFiles(schema));
    PCollection<GenericRecord> records2 = inputFiles2.apply(ParquetIO.readFiles(schema));

    Instant timestamp1 = dwh1.readTimestampFile(DwhFiles.TIMESTAMP_FILE_START);
    Instant timestamp2 = dwh2.readTimestampFile(DwhFiles.TIMESTAMP_FILE_START);

    PCollection<KV<String, GenericRecord>> dwhIdGroup1 =
        records1.apply(ParDo.of(new GroupViewIds()));

    PCollection<KV<String, GenericRecord>> dwhIdGroup2 =
        records2.apply(ParDo.of(new GroupViewIds()));

    PCollection<KV<String, CoGbkResult>> results;
    if (timestamp1.isAfter(timestamp2)) {
      results =
          KeyedPCollectionTuple.of(oldDwh, dwhIdGroup2)
              .and(newDwh, dwhIdGroup1)
              .apply(CoGroupByKey.create());
    } else {
      results =
          KeyedPCollectionTuple.of(newDwh, dwhIdGroup2)
              .and(oldDwh, dwhIdGroup1)
              .apply(CoGroupByKey.create());
    }
    return results;
  }

  /**
   * This method reads all the Parquet files under the paths {@code dwhFilesList} for the given
   * {@code resourceType}. It then groups the records by ID key and returns the grouped PCollection.
   */
  private static PCollection<KV<String, Iterable<GenericRecord>>> readAndGroupById(
      Pipeline pipeline,
      List<DwhFiles> dwhFilesList,
      String resourceType,
      AvroConversionUtil avroConversionUtil)
      throws ProfileException {

    // Reading all parquet files at once instead of one set at a time, reduces the number of Flink
    // reshuffle operations by one.
    PCollection<ReadableFile> inputFiles =
        pipeline
            .apply(Create.of(getParquetFilePaths(resourceType, dwhFilesList)))
            .apply(FileIO.matchAll())
            .apply(FileIO.readMatches());

    // The assumption here is that the schema of the old parquet files is same as the current
    // schema, otherwise reading of the older records fail even if the new Schema is just an
    // extension. In general, if the schema changes due to addition of new extensions, then the
    // merging process fail. It is recommended to recreate the entire parquet files using the new
    // schema again using the batch run.
    PCollection<GenericRecord> records =
        inputFiles.apply(ParquetIO.readFiles(avroConversionUtil.getResourceSchema(resourceType)));

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

    Set<String> resourceTypes1 = dwhFiles1.findNonEmptyResourceDirs();
    Set<String> resourceTypes2 = dwhFiles2.findNonEmptyResourceDirs();
    Set<String> dwhViews1 = new HashSet<>();
    Set<String> dwhViews2 = new HashSet<>();

    ViewManager viewManager = null;
    if (!Strings.isNullOrEmpty(options.getViewDefinitionsDir())) {
      try {
        viewManager = ViewManager.createForDir(options.getViewDefinitionsDir());
      } catch (IOException | ViewDefinitionException e) {
        String errorMsg =
            String.format("Error while reading views from %s", options.getViewDefinitionsDir());
        log.error(errorMsg, e);
        throw new IllegalArgumentException(errorMsg);
      }
      dwhViews1 = dwhFiles1.findNonEmptyViewDirs(viewManager);
      dwhViews2 = dwhFiles2.findNonEmptyViewDirs(viewManager);
      copyDistinctResources(dwhViews1, dwhViews2, dwhFiles1, dwhFiles2, mergedDwhFiles);
    }

    copyDistinctResources(resourceTypes1, resourceTypes2, dwhFiles1, dwhFiles2, mergedDwhFiles);
    List<Pipeline> pipelines = new ArrayList<>();
    for (String type : resourceTypes1) {
      if (!resourceTypes2.contains(type)) {
        continue;
      }
      pipelines.add(
          writeMergedResources(
              options,
              avroConversionUtil,
              dwhFiles1,
              dwhFiles2,
              mergedDwhFiles,
              avroConversionUtil.getResourceSchema(type),
              type,
              numDuplicates,
              numOutputRecords));
    }
    if ((!dwhViews1.isEmpty() || !dwhViews2.isEmpty())
        && !Strings.isNullOrEmpty(options.getViewDefinitionsDir())) {
      for (String viewName : dwhViews1) {
        if (!dwhViews2.contains(viewName)) {
          continue;
        }
        pipelines.add(
            writeMergedViewsPipeline(
                options, dwhFiles1, dwhFiles2, mergedDwhFiles, viewName, viewManager));
      }
    }
    return pipelines;
  }

  public static Pipeline writeMergedViewsPipeline(
      ParquetMergerOptions options,
      DwhFiles dwhFiles1,
      DwhFiles dwhFiles2,
      DwhFiles mergedDwhFiles,
      String viewName,
      @Nullable ViewManager viewManager)
      throws IOException {
    Pipeline pipeline = Pipeline.create(options);
    log.info("Merging materialized view {}", viewName);
    ViewDefinition viewDef = viewManager.getViewDefinition(viewName);
    if (viewDef != null) {
      Schema schema = ViewSchema.getAvroSchema(viewDef);
      PCollection<KV<String, CoGbkResult>> groupedRecords =
          readViewAndGroupById(pipeline, dwhFiles1, dwhFiles2, viewName, schema);
      PCollection<GenericRecord> merged =
          groupedRecords
              .apply(
                  ParDo.of(
                      new DoFn<KV<String, CoGbkResult>, GenericRecord>() {
                        // Because of ViewDefinition flattening we might need to output multiple
                        // GenericRecords
                        @ProcessElement
                        public void processElement(ProcessContext c) {
                          KV<String, CoGbkResult> e = c.element();
                          List<GenericRecord> lastRecords = new ArrayList<>();
                          e.getValue().getAll(newDwh).forEach(lastRecords::add);
                          for (GenericRecord r : lastRecords) {
                            c.output(r);
                          }
                        }
                      }))
              .setCoder(AvroCoder.of(schema));
      writeToParquetSink(
          options, merged, schema, mergedDwhFiles.getResourcePath(viewName).toString());
    }
    return pipeline;
  }

  public static Pipeline writeMergedResources(
      ParquetMergerOptions options,
      AvroConversionUtil avroConversionUtil,
      DwhFiles dwhFiles1,
      DwhFiles dwhFiles2,
      DwhFiles mergedDwhFiles,
      Schema schema,
      String type,
      Counter numDuplicates,
      Counter numOutputRecords)
      throws ProfileException {
    Pipeline pipeline = Pipeline.create(options);
    log.info("Merging resource type {}", type);
    PCollection<KV<String, Iterable<GenericRecord>>> groupedRecords =
        readAndGroupById(pipeline, Arrays.asList(dwhFiles1, dwhFiles2), type, avroConversionUtil);
    PCollection<GenericRecord> merged =
        groupedRecords.apply(
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
                }));
    merged.setCoder(AvroCoder.of(schema));
    writeToParquetSink(options, merged, schema, mergedDwhFiles.getResourcePath(type).toString());
    return pipeline;
  }

  public static void writeToParquetSink(
      ParquetMergerOptions options,
      PCollection<GenericRecord> mergedRecords,
      Schema schema,
      String path) {
    Sink parquetSink = ParquetIO.sink(schema).withCompressionCodec(CompressionCodecName.SNAPPY);
    if (options.getRowGroupSizeForParquetFiles() > 0) {
      parquetSink = parquetSink.withRowGroupSize(options.getRowGroupSizeForParquetFiles());
    }
    mergedRecords.apply(
        FileIO.<GenericRecord>write()
            .via(parquetSink)
            .to(path)
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

  /** DoFn to group GenericRecords by their IDs in a Key Value Mapping. */
  private static class GroupViewIds extends DoFn<GenericRecord, KV<String, GenericRecord>> {
    @ProcessElement
    public void processElement(
        @Element GenericRecord record, OutputReceiver<KV<String, GenericRecord>> out) {
      String id = record.get(ID_KEY).toString();
      if (id == null) {
        throw new IllegalArgumentException(String.format("No %s key found in %s", ID_KEY, record));
      }
      out.output(KV.of(id, record));
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

    List<Pipeline> pipelines = createMergerPipelines(options, avroConversionUtil);
    EtlUtils.runMultipleMergerPipelinesWithTimestamp(
        pipelines, options, avroConversionUtil.getFhirContext());
    log.info("DONE!");
  }
}
