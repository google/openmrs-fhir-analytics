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

import ca.uhn.fhir.context.FhirVersionEnum;
import com.cerner.bunsen.exception.ProfileException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.fhir.analytics.view.ViewApplicationException;
import com.google.fhir.analytics.view.ViewApplicator;
import com.google.fhir.analytics.view.ViewApplicator.RowList;
import com.google.fhir.analytics.view.ViewDefinition;
import com.google.fhir.analytics.view.ViewDefinitionException;
import com.google.fhir.analytics.view.ViewManager;
import com.google.fhir.analytics.view.ViewSchema;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import javax.annotation.Nullable;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;
import org.hl7.fhir.r4.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Implementing AutoCloseable does not help much in the usage pattern we have with DoFns. This is
// because we cannot wrap single resource writes into a "try-with-resources" block, otherwise each
// Parquet row-group will include only one record.
public class ParquetUtil {

  private static final Logger log = LoggerFactory.getLogger(ParquetUtil.class);
  public static String PARQUET_EXTENSION = ".parquet";
  private final AvroConversionUtil conversionUtil;

  private Map<String, ViewDefinition> viewMap;

  private final Map<String, ParquetWriter<GenericRecord>> viewWriterMap;
  private final Map<String, WriterWithCache> writerMap;

  private final int rowGroupSize;
  private final boolean cacheBundle;
  private final DwhFiles dwhFiles;
  private final Timer timer;
  private final Random random;
  private final String namePrefix;
  private boolean flushedInCurrentPeriod;

  private ViewManager viewManager;

  private final boolean createParquetViews;

  private synchronized void setFlushedInCurrentPeriod(boolean value) {
    flushedInCurrentPeriod = value;
  }

  public String getParquetPath() {
    return dwhFiles.getRoot();
  }

  // TODO remove this constructor and only expose a similar one in `DwhFiles` (for testing).

  /**
   * @param fhirVersionEnum This should match the resources intended to be converted.
   * @param structureDefinitionsPath Directory path containing the structure definitions for custom
   *     fhir profiles; if it starts with `classpath:` then classpath will be searched instead.
   * @param parquetFilePath The directory under which the Parquet files are written.
   * @param secondsToFlush The interval after which the content of Parquet writers is flushed to
   *     disk.
   * @param rowGroupSize The approximate size of row-groups in the Parquet files (0 means use
   *     default).
   * @param namePrefix The prefix directory at which the Parquet files are written
   * @param recursiveDepth The maximum recursive depth for FHIR resources when converting to Avro.
   * @param cacheBundle Whether to cache output records or directly send them to the Parquet writer.
   *     If this is enabled, then it is the responsibility of the user code to call emptyCache. This
   *     is used when we want to make sure a DoFn is idempotent.
   * @throws ProfileException if any errors are encountered during initialisation of FhirContext
   */
  @VisibleForTesting
  ParquetUtil(
      FhirVersionEnum fhirVersionEnum,
      String structureDefinitionsPath,
      String parquetFilePath,
      String viewDefinitionsDir,
      boolean createParquetViews,
      List<String> resourceList,
      int secondsToFlush,
      int rowGroupSize,
      String namePrefix,
      int recursiveDepth,
      boolean cacheBundle)
      throws ProfileException {
    if (fhirVersionEnum == FhirVersionEnum.DSTU3 || fhirVersionEnum == FhirVersionEnum.R4) {
      this.conversionUtil =
          AvroConversionUtil.getInstance(fhirVersionEnum, structureDefinitionsPath, recursiveDepth);
    } else {
      throw new IllegalArgumentException("Only versions 3 and 4 of FHIR are supported!");
    }
    this.dwhFiles = new DwhFiles(parquetFilePath, conversionUtil.getFhirContext());
    this.writerMap = new HashMap<>();
    this.rowGroupSize = rowGroupSize;
    this.cacheBundle = cacheBundle;
    this.namePrefix = namePrefix;
    this.createParquetViews = createParquetViews;
    this.viewWriterMap = new HashMap<>();
    this.viewMap = null;
    this.viewManager = null;
    setFlushedInCurrentPeriod(false);
    if (createParquetViews) {
      try {
        this.viewManager = ViewManager.createForDir(viewDefinitionsDir);
      } catch (IOException | ViewDefinitionException e) {
        String errorMsg = String.format("Error while reading views from %s", viewDefinitionsDir);
        log.error(errorMsg, e);
        throw new IllegalArgumentException(errorMsg);
      }
      List<String> names = new ArrayList<>();
      this.viewMap =
          ViewSchema.createViewMap(names, new ArrayList<>(), resourceList, this.viewManager);

      Map<String, Integer> frequencyMap = new HashMap<>();
      Set<String> dupViews = new HashSet<>();
      for (String name : names) {
        frequencyMap.put(name, frequencyMap.getOrDefault(name, 0) + 1);
      }
      for (String name : frequencyMap.keySet()) {
        if (frequencyMap.get(name) > 1) {
          dupViews.add(name);
        }
      }
      if (!dupViews.isEmpty()) {
        String errorMsg =
            "Duplicate ViewDefinition names found: "
                + Arrays.toString(dupViews.toArray())
                + ". Ensure each view has a distinct name!";
        log.error(errorMsg);
        throw new IllegalArgumentException(errorMsg);
      }
    }
    if (secondsToFlush > 0) {
      TimerTask task =
          new TimerTask() {

            @Override
            public void run() {
              try {
                // If a flush has happened recently, e.g., through a `FinishBundle` call, we don't
                // need to do that again; the timer here is just to make sure the data is flushed
                // into Parquet files _at least_ once in every `secondsToFlush`.
                if (!flushedInCurrentPeriod) {
                  log.info("Flush timed out for thread " + Thread.currentThread().getId());
                  flushAll();
                }
                setFlushedInCurrentPeriod(false);
              } catch (IOException | ProfileException e) {
                log.error("Could not flush Parquet files: " + e);
              }
            }
          };
      this.timer = new Timer();
      timer.scheduleAtFixedRate(task, secondsToFlush * 1000, secondsToFlush * 1000);
    } else {
      timer = null;
    }
    this.random = new Random(System.currentTimeMillis());
  }

  @VisibleForTesting
  synchronized ResourceId getUniqueOutputFilePath(String resourceType) throws IOException {
    ResourceId resourceId = dwhFiles.getResourcePath(resourceType);
    String uniqueFileName =
        String.format(
            "%s%s_output-parquet-th-%d-ts-%d-r-%d%s",
            namePrefix,
            resourceType,
            Thread.currentThread().getId(),
            System.currentTimeMillis(),
            random.nextInt(1000000),
            PARQUET_EXTENSION);
    return resourceId.resolve(uniqueFileName, StandardResolveOptions.RESOLVE_FILE);
  }

  @VisibleForTesting
  synchronized ResourceId getUniqueOutputFilePathView(String viewName) throws IOException {
    ResourceId resourceId = dwhFiles.getResourcePath(viewName.toLowerCase(Locale.ENGLISH));
    String uniqueFileName =
        String.format(
            "%s%s_output-parquet-th-%d-ts-%d-r-%d%s",
            namePrefix,
            viewName,
            Thread.currentThread().getId(),
            System.currentTimeMillis(),
            random.nextInt(1000000),
            PARQUET_EXTENSION);
    return resourceId.resolve(uniqueFileName, StandardResolveOptions.RESOLVE_FILE);
  }

  private synchronized void createWriter(String resourceType, @Nullable ViewDefinition vDef)
      throws IOException, ProfileException {
    boolean noView = vDef == null;
    ResourceId resourceId =
        noView
            ? getUniqueOutputFilePath(resourceType)
            : getUniqueOutputFilePathView(vDef.getName());

    WritableByteChannel writableByteChannel =
        org.apache.beam.sdk.io.FileSystems.create(resourceId, MimeTypes.BINARY);
    OutputStream outputStream = Channels.newOutputStream(writableByteChannel);
    FhirOutputFile fhirOutputFile = new FhirOutputFile(outputStream);

    AvroParquetWriter.Builder<GenericRecord> builder = AvroParquetWriter.builder(fhirOutputFile);
    // TODO: Adjust other parquet file parameters for our needs or make them configurable.
    if (rowGroupSize > 0) {
      builder.withRowGroupSize(rowGroupSize);
    }
    ParquetWriter<GenericRecord> writer;
    if (noView) {
      writer = builder.withSchema(conversionUtil.getResourceSchema(resourceType)).build();
      writerMap.put(resourceType, new WriterWithCache(writer, this.cacheBundle));
    } else {
      writer = builder.withSchema(ViewSchema.getAvroSchema(vDef)).build();
      viewWriterMap.put(vDef.getName(), writer);
    }
  }

  /**
   * This is to write a FHIR resource to a Parquet file. Automatically handles file creation in a
   * directory named after the resource type (e.g., `Patient`) with names following the
   * "output-parquet-th-T-ts-TS-r-R.parquet" pattern where T is the thread identifier, TS is a
   * timestamp and R is a random number
   */
  public synchronized void write(Resource resource) throws IOException, ProfileException {
    Preconditions.checkNotNull(resource.fhirType());
    String resourceType = resource.fhirType();
    if (!writerMap.containsKey(resourceType)) {
      createWriter(resourceType, null);
    }
    final WriterWithCache writer = writerMap.get(resourceType);
    GenericRecord record = conversionUtil.convertToAvro(resource);
    if (record != null) {
      writer.write(record);
    }
  }

  public synchronized void emptyCache() throws IOException {
    for (WriterWithCache writer : writerMap.values()) {
      writer.flushCache();
    }
  }

  /**
   * This is to write a materialized View Definition to a Parquet file.
   *
   * @see #write(Resource)
   */
  public synchronized void write(Resource resource, ViewDefinition vDef)
      throws IOException, ProfileException, ViewApplicationException {
    Preconditions.checkNotNull(resource.fhirType());
    if (!viewWriterMap.containsKey(vDef.getName())) {
      createWriter("", vDef);
    }
    final ParquetWriter<GenericRecord> parquetWriter = viewWriterMap.get(vDef.getName());
    ViewApplicator applicator = new ViewApplicator(vDef);
    RowList rows = applicator.apply(resource);
    List<GenericRecord> result = ViewSchema.setValueInRecord(rows, vDef);
    for (GenericRecord record : result) {
      parquetWriter.write(record);
    }
  }

  private synchronized void flush(String resourceType) throws IOException, ProfileException {
    WriterWithCache writer = writerMap.get(resourceType);
    if (writer != null && writer.getDataSize() > 0) {
      writer.close();
      createWriter(resourceType, null);
    }
  }

  private synchronized void flushViewWriter(String viewName) throws IOException, ProfileException {
    ParquetWriter<GenericRecord> writer = viewWriterMap.get(viewName);
    if (writer != null && writer.getDataSize() > 0) {
      writer.close();
      createWriter(viewName, viewMap.get(viewName));
    }
  }

  synchronized void flushAll() throws IOException, ProfileException {
    log.info("Flushing all Parquet writers for thread " + Thread.currentThread().getId());
    for (String viewName : viewWriterMap.keySet()) {
      flushViewWriter(viewName);
    }
    for (String resourceType : writerMap.keySet()) {
      flush(resourceType);
    }
    setFlushedInCurrentPeriod(true);
  }

  public synchronized void closeAllWriters() throws IOException {
    if (timer != null) {
      timer.cancel();
    }
    for (Map.Entry<String, ParquetWriter<GenericRecord>> entry : viewWriterMap.entrySet()) {
      entry.getValue().close();
      viewWriterMap.put(entry.getKey(), null);
    }
    for (Map.Entry<String, WriterWithCache> entry : writerMap.entrySet()) {
      entry.getValue().close();
      writerMap.put(entry.getKey(), null);
    }
  }

  public void writeRecords(Bundle bundle, Set<String> resourceTypes)
      throws IOException, ProfileException, ViewApplicationException {
    if (bundle.getEntry() == null) {
      return;
    }
    for (BundleEntryComponent entry : bundle.getEntry()) {
      Resource resource = entry.getResource();
      if (resourceTypes == null || resourceTypes.contains(resource.getResourceType().name())) {
        write(resource);
        if (createParquetViews) {
          ImmutableList<ViewDefinition> views = viewManager.getViewsForType(resource.fhirType());
          if (views != null) {
            for (ViewDefinition vDef : views) {
              write(resource, vDef);
            }
          }
        }
      }
    }
  }

  private static class WriterWithCache {

    private final ParquetWriter<GenericRecord> writer;
    private final List<GenericRecord> cache;
    private final boolean useCache;

    WriterWithCache(ParquetWriter<GenericRecord> writer, boolean useCache) {
      this.writer = writer;
      this.cache = new ArrayList<>();
      this.useCache = useCache;
    }

    void flushCache() throws IOException {
      if (useCache && !cache.isEmpty()) {
        log.info("Parquet cache size= {}", cache.size());
        for (GenericRecord record : cache) {
          writer.write(record);
        }
        cache.clear();
      }
    }

    void write(GenericRecord record) throws IOException {
      if (useCache) {
        cache.add(record);
      } else {
        writer.write(record);
      }
    }

    void close() throws IOException {
      flushCache();
      writer.close();
    }

    long getDataSize() {
      return writer.getDataSize();
    }
  }
}
