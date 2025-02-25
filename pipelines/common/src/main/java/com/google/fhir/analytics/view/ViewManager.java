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
package com.google.fhir.analytics.view;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.jspecify.annotations.Nullable;

/**
 * This class is responsible for reading ViewDefinition files and producing the relevant row for
 * each input resource based on the views that apply to that resource.
 */
public class ViewManager {

  private static final String JSON_EXT = ".json";
  private final Map<String, ViewDefinition> viewNameMap = new HashMap<>();

  private final Multimap<String, ViewDefinition> viewMap = HashMultimap.create();

  // This should only be instantiated with factory methods.
  private ViewManager() {}

  // Note we rely on JAVA NIO API for file access; this may require additional dependencies to work
  // on distributed file systems, e.g., https://github.com/googleapis/java-storage-nio
  public static ViewManager createForDir(String viewDefinitionsDir)
      throws IOException, ViewDefinitionException {
    Preconditions.checkNotNull(viewDefinitionsDir);
    Preconditions.checkArgument(!viewDefinitionsDir.isEmpty());
    ViewManager viewManager = new ViewManager();
    List<Path> viewPaths = new ArrayList<>();
    try (Stream<Path> paths =
        Files.walk(Paths.get(viewDefinitionsDir), FileVisitOption.FOLLOW_LINKS)) {
      paths
          .filter(f -> f.toString().endsWith(JSON_EXT))
          .forEach(
              f -> {
                viewPaths.add(f);
              });
    }
    for (Path p : viewPaths) {
      ViewDefinition vDef = ViewDefinition.createFromFile(p);
      viewManager.viewMap.put(vDef.getResource(), vDef);
    }
    // Checking for Duplicate View Definitions and returning the names of any duplicates found
    Collection<ViewDefinition> viewDefinitions = viewManager.viewMap.values();
    Set<String> dupViews = new HashSet<>();
    for (ViewDefinition vDef : viewDefinitions) {
      String viewName = vDef.getName();
      if (viewManager.viewNameMap.containsKey(viewName)) {
        dupViews.add(viewName);
      }
      viewManager.viewNameMap.put(viewName, vDef);
    }

    if (!dupViews.isEmpty()) {
      String errorMsg =
          "Duplicate ViewDefinition names found: "
              + Arrays.toString(dupViews.toArray())
              + ". Ensure each view has a distinct name!";
      throw new IllegalArgumentException(errorMsg);
    }
    return viewManager;
  }

  @Nullable
  public ViewDefinition getViewDefinition(String viewName) {
    if (!viewNameMap.containsKey(viewName)) {
      return null;
    }
    return viewNameMap.get(viewName);
  }

  @Nullable
  public ImmutableList<ViewDefinition> getViewsForType(String resourceType) {
    if (!viewMap.containsKey(resourceType)) {
      return null;
    }
    return ImmutableList.copyOf(viewMap.get(resourceType));
  }
}
