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
import ca.uhn.fhir.parser.DataFormatException;
import com.google.api.client.util.Sets;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.fhir.analytics.view.ViewManager;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.MatchResult.Status;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.util.MimeTypes;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The interface for working with the data-warehouse files. This is where all file-structure logic
 * should be implemented. This should support different file-systems, including distributed ones.
 */
public class DwhFiles {

  private static final Logger log = LoggerFactory.getLogger(DwhFiles.class);

  public static final String TIMESTAMP_FILE_START = "timestamp_start.txt";

  public static final String TIMESTAMP_FILE_END = "timestamp_end.txt";

  public static final String TIMESTAMP_FILE_BULK_TRANSACTION_TIME =
      "timestamp_bulk_transaction_time.txt";

  private static final String INCREMENTAL_DIR = "incremental_run";

  static final String TIMESTAMP_PREFIX = "_TIMESTAMP_";

  // TODO: It is probably better if we build all DWH files related operations using Beam's
  //  filesystem API such that when a new filesystem is registered, it automatically works
  //  everywhere in our code. Note that currently we have hardcoded the valid schema in some places,
  //  hence a newly registered filesystem is not automatically handled everywhere.
  static final String LOCAL_SCHEME = "file";
  static final String GCS_SCHEME = "gs";
  static final String S3_SCHEME = "s3";

  private final String dwhRoot;

  private final FhirContext fhirContext;

  /**
   * In order to interpret the file paths correctly the {@code dwhRoot} has to be represented in one
   * of the below formats according to the filesystem platform:
   *
   * <p>Linux/Mac:
   *
   * <ul>
   *   <li>TestRoot/SamplePrefix
   *   <li>/Users/beam/TestRoot/SamplePrefix
   * </ul>
   *
   * <p>Windows OS:
   *
   * <ul>
   *   <li>TestRoot\SamplePrefix
   *   <li>C:\Users\beam\TestRoot\SamplePrefix
   * </ul>
   *
   * <p>GCS filesystem
   *
   * <ul>
   *   <li>gs://TestBucket/TestRoot/SamplePrefix
   * </ul>
   *
   * @param dwhRoot
   * @param fhirContext
   */
  DwhFiles(String dwhRoot, FhirContext fhirContext) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(dwhRoot));
    this.dwhRoot = dwhRoot;
    this.fhirContext = fhirContext;
  }

  static DwhFiles forRoot(String dwhRoot, FhirContext fhirContext) {
    return new DwhFiles(dwhRoot, fhirContext);
  }

  public static String safeTimestampSuffix() {
    return Instant.now().toString().replace(":", "-").replace("-", "_").replace(".", "_");
  }

  public String getRoot() {
    return dwhRoot;
  }

  /**
   * This returns the path to the directory which contains resources of a specific type.
   *
   * @param resourceType the type of the FHIR resources
   * @return The path to the resource type dir
   */
  public ResourceId getResourcePath(String resourceType) {
    return FileSystems.matchNewResource(getRoot(), true)
        .resolve(resourceType, StandardResolveOptions.RESOLVE_DIRECTORY);
  }

  /**
   * @param resourceType the type of the FHIR resources
   * @return The file pattern for Parquet files of `resourceType` in this DWH.
   */
  public String getFilePattern(String resourceType) {
    return String.format(
        "%s*%s", getResourcePath(resourceType).toString(), ParquetUtil.PARQUET_EXTENSION);
  }

  // TODO: Move this to a util class and make it non-static.
  /**
   * Returns all the child directories under the given base directory which are 1-level deep. Note
   * in many cloud/distributed file-systems, we do not have "directories"; there are only buckets
   * and files in those buckets. We use file-seprators (e.g., `/`) to simulate the concept of
   * directories. So for example, this method returns an empty set if `baseDir` is `bucket/test` and
   * the only file in that bucket is `bucket/test/dir1/dir2/file.txt`. If `baseDir` is
   * `bucket/test/dir1`, in the above example, `dir2` is returned.
   *
   * @param baseDir the path under which "directories" are looked for.
   * @return The list of all child directories under the base directory
   * @throws IOException
   */
  static Set<ResourceId> getAllChildDirectories(String baseDir) throws IOException {
    String fileSeparator = getFileSeparatorForDwhFiles(baseDir);
    // Avoid using ResourceId.resolve(..) method to resolve the files when the path contains glob
    // expressions with multiple special characters like **, */* etc as this api only supports
    // single special characters like `*` or `..`. Rather use the FileSystems.match(..) if the path
    // contains glob expressions.
    List<MatchResult> matchResultList =
        FileSystems.match(
            List.of(
                getPathEndingWithFileSeparator(baseDir, fileSeparator)
                    + "*"
                    + fileSeparator
                    + "*"));
    Set<ResourceId> childDirectories = new HashSet<>();
    for (MatchResult matchResult : matchResultList) {
      if (matchResult.status() == Status.OK && !matchResult.metadata().isEmpty()) {
        for (Metadata metadata : matchResult.metadata()) {
          childDirectories.add(metadata.resourceId().getCurrentDirectory());
        }
      } else if (matchResult.status() == Status.ERROR) {
        String errorMessage = String.format("Error matching files under directory %s", baseDir);
        log.error(errorMessage);
        throw new IOException(errorMessage);
      }
    }
    log.info("Child directories of {} are {}", baseDir, childDirectories);
    return childDirectories;
  }

  /**
   * Also see {@link #newIncrementalRunPath()}
   *
   * @return the current incremental run path if one found; null otherwise.
   */
  @Nullable
  public ResourceId getLatestIncrementalRunPath() throws IOException {
    List<ResourceId> dirs =
        getAllChildDirectories(getRoot()).stream()
            .filter(dir -> dir.getFilename().contains(INCREMENTAL_DIR + TIMESTAMP_PREFIX))
            .collect(Collectors.toList());
    if (dirs.isEmpty()) return null;

    Collections.sort(dirs, Comparator.comparing(ResourceId::toString));
    return dirs.get(dirs.size() - 1);
  }

  /**
   * This returns a new incremental-run path based on the current timestamp. Note that each
   * incremental-run is relative to a full-run, hence we put this directory under the full-run root.
   *
   * @return a new incremental run path based on the current timestamp.
   */
  public ResourceId newIncrementalRunPath() {
    return FileSystems.matchNewResource(getRoot(), true)
        .resolve(
            String.format("%s%s%s", INCREMENTAL_DIR, TIMESTAMP_PREFIX, safeTimestampSuffix()),
            StandardResolveOptions.RESOLVE_DIRECTORY);
  }

  public Set<String> findNonEmptyResourceDirs() throws IOException {
    return findNonEmptyDirs(null);
  }

  public Set<String> findNonEmptyViewDirs(ViewManager viewManager) throws IOException {
    return findNonEmptyDirs(viewManager);
  }

  /**
   * This method returns the list of non-empty directories which contains at least one file under
   * it.
   *
   * @param viewManager Used to verify if non-empty directory is a valid View Definition. If null,
   *     the views returned will be valid FHIR Resource Types. Otherwise, the views will be valid
   *     View Definitions
   * @return the set of non-empty directories
   * @throws IOException if the directory does not contain a valid file type
   */
  private Set<String> findNonEmptyDirs(@Nullable ViewManager viewManager) throws IOException {
    // TODO : If the list of files under the dwhRoot is huge then there can be a lag in the api
    //  response. This issue https://github.com/google/fhir-data-pipes/issues/288 helps in
    //  maintaining the number of file to an optimum value.
    String fileType = viewManager == null ? "Resource types" : "Views";
    String fileSeparator = getFileSeparatorForDwhFiles(dwhRoot);
    List<MatchResult> matchedChildResultList =
        FileSystems.match(
            List.of(
                getPathEndingWithFileSeparator(dwhRoot, fileSeparator)
                    + "*"
                    + fileSeparator
                    + "*"));
    Set<String> fileSet = new HashSet<>();
    for (MatchResult matchResult : matchedChildResultList) {
      if (matchResult.status() == Status.OK && !matchResult.metadata().isEmpty()) {
        for (Metadata metadata : matchResult.metadata()) {
          fileSet.add(metadata.resourceId().getCurrentDirectory().getFilename());
        }
      } else if (matchResult.status() == Status.ERROR) {
        log.error("Error matching {} under {} ", fileType, dwhRoot);
        throw new IOException(String.format("Error matching %s under %s", fileType, dwhRoot));
      }
    }

    Set<String> typeSet = Sets.newHashSet();
    for (String file : fileSet) {
      if (viewManager == null) {
        try {
          fhirContext.getResourceType(file);
          typeSet.add(file);
        } catch (DataFormatException e) {
          log.debug("Ignoring file {} which is not a FHIR resource.", file);
        }
      } else {
        if (viewManager.getViewDefinition(file) != null) {
          typeSet.add(file);
        }
      }
    }
    log.info("{} under {} are {}", fileType, dwhRoot, typeSet);
    return typeSet;
  }

  /**
   * This method copies all the files under the directory (getRoot() + resourceType) into the
   * destination DwhFiles under the similar directory.
   *
   * @param resourceType The resource or view directory to be copied
   * @param destDwh The output Dwh to copy files to
   */
  public void copyResourcesToDwh(String resourceType, DwhFiles destDwh) throws IOException {
    String fileSeparator = getFileSeparatorForDwhFiles(dwhRoot);
    List<MatchResult> sourceMatchResultList =
        FileSystems.match(
            List.of(
                getPathEndingWithFileSeparator(dwhRoot, fileSeparator)
                    + resourceType
                    + fileSeparator
                    + "*"));
    List<ResourceId> sourceResourceIdList = new ArrayList<>();

    // The sourceMatchResultList should only contain 1 element as the matching list
    // (List.of(sourceFiles)) used for it was also one. The actual matched files should be
    // retrieved from the matchResult.metadata element which could be more than one
    for (MatchResult matchResult : sourceMatchResultList) {
      if (matchResult.status() == Status.OK) {
        List<ResourceId> resourceIds =
            matchResult.metadata().stream()
                .map(metadata -> metadata.resourceId())
                .collect(Collectors.toList());
        sourceResourceIdList.addAll(resourceIds);
      }
    }

    List<ResourceId> destResourceIdList = new ArrayList<>();
    sourceResourceIdList.stream()
        .forEach(
            resourceId -> {
              ResourceId destResourceId =
                  FileSystems.matchNewResource(destDwh.getRoot(), true)
                      .resolve(resourceType, StandardResolveOptions.RESOLVE_DIRECTORY)
                      .resolve(resourceId.getFilename(), StandardResolveOptions.RESOLVE_FILE);
              destResourceIdList.add(destResourceId);
            });
    FileSystems.copy(sourceResourceIdList, destResourceIdList);
  }

  public void writeTimestampFile(String fileName) throws IOException {
    writeTimestampFile(Instant.now(), fileName);
  }

  public void writeTimestampFile(Instant instant, String fileName) throws IOException {
    ResourceId resourceId =
        FileSystems.matchNewResource(getRoot(), true)
            .resolve(fileName, StandardResolveOptions.RESOLVE_FILE);
    List<MatchResult> matches = FileSystems.matchResources(Collections.singletonList(resourceId));
    MatchResult matchResult = Iterables.getOnlyElement(matches);

    if (matchResult.status() == Status.OK) {
      String errorMessage =
          String.format(
              "Attempting to write to the timestamp file %s which already exists",
              getRoot() + "/" + fileName);
      log.error(errorMessage);
      throw new FileAlreadyExistsException(errorMessage);
    } else if (matchResult.status() == Status.NOT_FOUND) {
      WritableByteChannel writableByteChannel = FileSystems.create(resourceId, MimeTypes.BINARY);
      writableByteChannel.write(
          ByteBuffer.wrap(instant.toString().getBytes(StandardCharsets.UTF_8)));
      writableByteChannel.close();
    } else {
      String errorMessage =
          String.format(
              "Error matching file spec %s: status %s",
              getRoot() + "/" + fileName, matchResult.status());
      log.error(errorMessage);
      throw new IOException(errorMessage);
    }
  }

  public Instant readTimestampFile(String fileName) throws IOException {
    ResourceId resourceId =
        FileSystems.matchNewResource(getRoot(), true)
            .resolve(fileName, StandardResolveOptions.RESOLVE_FILE);
    List<String> result = new ArrayList<>();
    ReadableByteChannel channel = FileSystems.open(resourceId);
    try (InputStream stream = Channels.newInputStream(channel)) {
      BufferedReader streamReader =
          new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
      String line;
      while ((line = streamReader.readLine()) != null) {
        result.add(line);
      }
    }
    if (result.isEmpty()) {
      String errorMessage =
          String.format("The timestamp file %s is empty", getRoot() + "/" + fileName);
      log.error(errorMessage);
      throw new IllegalStateException(errorMessage);
    }
    return Instant.parse(result.get(0));
  }

  /** Checks if the given file exists in the data-warehouse at the root location */
  public boolean doesTimestampFileExist(String fileName) throws IOException {
    ResourceId resourceId =
        FileSystems.matchNewResource(getRoot(), true)
            .resolve(fileName, StandardResolveOptions.RESOLVE_FILE);
    List<MatchResult> matchResultList =
        FileSystems.matchResources(Collections.singletonList(resourceId));
    MatchResult matchResult = Iterables.getOnlyElement(matchResultList);
    if (matchResult.status() == Status.OK) {
      return true;
    } else if (matchResult.status() == Status.NOT_FOUND) {
      return false;
    } else {
      String errorMessage =
          String.format("Error matching file spec %s: status %s", resourceId, matchResult.status());
      log.error(errorMessage);
      throw new IOException(errorMessage);
    }
  }

  public void overwriteFile(String fileName, byte[] content) throws IOException {
    ResourceId resourceId =
        FileSystems.matchNewResource(getRoot(), true)
            .resolve(fileName, StandardResolveOptions.RESOLVE_FILE);

    List<MatchResult> matches = FileSystems.matchResources(Collections.singletonList(resourceId));
    MatchResult matchResult = Iterables.getOnlyElement(matches);

    if (matchResult.status() == Status.OK) {
      String warnMessage =
          String.format("Overwriting the existing file %s", getRoot() + "/" + fileName);
      log.warn(warnMessage);
      FileSystems.delete(List.of(resourceId));
    }

    if (matchResult.status() == Status.NOT_FOUND || matchResult.status() == Status.OK) {
      WritableByteChannel writableByteChannel = FileSystems.create(resourceId, MimeTypes.BINARY);
      writableByteChannel.write(ByteBuffer.wrap(content));
      writableByteChannel.close();
    } else {
      String errorMessage =
          String.format(
              "Error matching file spec %s: status %s",
              getRoot() + "/" + fileName, matchResult.status());
      log.error(errorMessage);
      throw new IOException(errorMessage);
    }
  }

  /**
   * This method returns the file separator for the filesystem where the data-warehouse is hosted.
   *
   * @param dwhRootPrefix the root directory prefix of the filesystem.
   * @return the file separator
   */
  public static String getFileSeparatorForDwhFiles(String dwhRootPrefix) {
    CloudPath cloudPath = parsePath(dwhRootPrefix);
    switch (cloudPath.getScheme()) {
      case LOCAL_SCHEME:
        return File.separator;
      case GCS_SCHEME:
      case S3_SCHEME:
        return "/";
      default:
        String errorMessage =
            String.format("File system scheme=%s is not yet supported", cloudPath.getScheme());
        log.error(errorMessage);
        throw new IllegalArgumentException(errorMessage);
    }
  }

  /** This method returns the {@code path} by appending the {@code fileseparator} if required. */
  public static String getPathEndingWithFileSeparator(String path, String fileSeparator) {
    Preconditions.checkNotNull(path);
    Preconditions.checkNotNull(fileSeparator);
    return path.endsWith(fileSeparator) ? path : path + fileSeparator;
  }

  /**
   * This method returns the schema of the passed in spec. This is inspired from the parsing logic
   * in the repo <a href="https://github.com/apache/beam">Beam File System</a>
   *
   * @param path the path for which the schema needs to be parsed, e.g., gs://my-bucket/file or
   *     basedir/file or /rootbasedir/file
   * @return the schema
   */
  public static CloudPath parsePath(String path) {
    // The `path` is almost, but not quite, a URI. In particular,
    // the reserved characters '[', ']', and '?' have meanings that differ
    // from their use in the URI spec. ('*' is not reserved).
    // Here, we just need the scheme, bucket, and the rest of the path.
    return new CloudPath(path);
  }

  /**
   * This is based on a minimal part of {@link GcsPath} which is generalized to work for S3 buckets
   * as well (and other distributed file-systesm in future).
   */
  @Getter
  static class CloudPath {
    private final String scheme;
    private final String bucket;
    private final String object;
    static final Pattern CLOUD_URI =
        Pattern.compile("(?<SCHEME>[^:]+)://(?<BUCKET>[^/]+)(/(?<OBJECT>.*))?");

    private CloudPath(String uri) {
      Matcher m = CLOUD_URI.matcher(uri);
      if (m.matches()) {
        scheme = m.group("SCHEME");
        bucket = m.group("BUCKET");
        object = m.group("OBJECT");
      } else {
        scheme = LOCAL_SCHEME;
        bucket = "";
        object = uri;
      }
    }
  }
}
