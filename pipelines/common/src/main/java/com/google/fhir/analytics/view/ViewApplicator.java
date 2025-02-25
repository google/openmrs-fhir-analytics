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

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.fhirpath.FhirPathExecutionException;
import ca.uhn.fhir.fhirpath.IFhirPath;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.fhir.analytics.view.ViewDefinition.Column;
import com.google.fhir.analytics.view.ViewDefinition.Select;
import com.google.fhir.analytics.view.ViewDefinition.Where;
import com.google.gson.Gson;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Getter;
import org.hl7.fhir.dstu3.hapi.fluentpath.FhirPathDstu3;
import org.hl7.fhir.instance.model.api.IBase;
import org.hl7.fhir.instance.model.api.IBaseReference;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.instance.model.api.IIdType;
import org.hl7.fhir.instance.model.api.IPrimitiveType;
import org.hl7.fhir.r4.hapi.fluentpath.FhirPathR4;
import org.hl7.fhir.r4b.hapi.fhirpath.FhirPathR4B;
import org.hl7.fhir.r5.hapi.fhirpath.FhirPathR5;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Given a {@link ViewDefinition}, this is to apply it on FHIR resources of appropriate type. */
public class ViewApplicator {
  private static final Logger log = LoggerFactory.getLogger(ViewApplicator.class);
  public static String ID_TYPE = "id";
  public static final String GET_RESOURCE_KEY = "getResourceKey()";
  private static final Pattern GET_REF_KEY_PATTERN =
      Pattern.compile("(?<fhirPath>.*)getReferenceKey\\(((?<resourceType>[a-zA-Z]*))?\\)");
  private static final RowList EMPTY_LIST = RowList.builder().build();
  private final ViewDefinition viewDef;
  private final IFhirPath fhirPath;

  public ViewApplicator(ViewDefinition viewDefinition) {
    this.viewDef = viewDefinition;
    Set<FhirVersionEnum> fhirVersions = new HashSet<>();
    if (viewDefinition.getFhirVersion() != null) {
      for (String version : viewDefinition.getFhirVersion()) {
        fhirVersions.add(ViewDefinition.convertFhirVersion(version));
      }
    } else {
      fhirVersions.add(FhirVersionEnum.R4);
    }
    // This order is kind of arbitrary; going with this as we usually pick R4 as the default!
    if (fhirVersions.contains(FhirVersionEnum.R4)) {
      this.fhirPath = new FhirPathR4(FhirContext.forCached(FhirVersionEnum.R4));
    } else if (fhirVersions.contains(FhirVersionEnum.R4B)) {
      this.fhirPath = new FhirPathR4B(FhirContext.forCached(FhirVersionEnum.R4B));
    } else if (fhirVersions.contains(FhirVersionEnum.R5)) {
      this.fhirPath = new FhirPathR5(FhirContext.forCached(FhirVersionEnum.R5));
    } else if (fhirVersions.contains(FhirVersionEnum.DSTU3)) {
      this.fhirPath = new FhirPathDstu3(FhirContext.forCached(FhirVersionEnum.DSTU3));
    } else {
      throw new IllegalArgumentException(
          "ViewDefinition version not supported: " + viewDefinition.getFhirVersion());
    }
  }

  /**
   * Applies all the `select` fields of this {@link ViewDefinition} to the given `resource` and
   * returns the result list of rows. For each `select` the steps are: 1) Extract the elements
   * corresponding to `forEach` (or `forEachOrNull`) OR start with the root element if there is no
   * `forEach`. Call these the "target elements". 2) Extract the `column`s for each of the target
   * elements. 3) Apply sub-selects to each of the target elements. 4) Apply `unionAll` selects and
   * union the created row sets. 5) Create the cross-join of rows from steps 2, 3, and 4.
   *
   * @param resource the resource on which the above logic is applied.
   * @return the result list of rows. If there is no `forEachOrNull`, this method returns an empty
   *     RowList when one of the following cases are true: 1) There is a `forEach` element which
   *     does not match any elements. 2) There is a sub-select which returns an empty list. If there
   *     is a `forEachOrNull` but no element matches its FHIRPath or one of the sub-selects returns
   *     no rows, a single row is returned with the name of columns and null values.
   * @throws ViewApplicationException if there are any errors applying the view to resource. Most of
   *     the errors come from errors in the ViewDefinition.
   */
  public RowList apply(IBaseResource resource) throws ViewApplicationException {
    Preconditions.checkState(
        viewDef.getResource().equals(resource.fhirType()),
        String.format(
            "expected resource type %s got %s",
            viewDef.getResource(), Strings.nullToEmpty(resource.fhirType())));
    if (satisfiesWhere(resource)) {
      return applyAllSelects(resource, viewDef.getSelect());
    } else {
      return EMPTY_LIST;
    }
  }

  private List<IBase> evaluateFhirPath(IBase resource, String path)
      throws ViewApplicationException {
    try {
      return fhirPath.evaluate(resource, path, IBase.class);
    } catch (FhirPathExecutionException e) {
      log.error("Error while evaluating path {}:", path, e);
      throw new ViewApplicationException(
          String.format("Error while evaluating path %s: %s", path, e.getMessage()));
    }
  }

  private boolean satisfiesWhere(IBaseResource resource) throws ViewApplicationException {
    if (viewDef.getWhere() == null) {
      return true;
    }
    for (Where w : viewDef.getWhere()) {
      List<IBase> results = evaluateFhirPath(resource, w.getPath());
      // Empty list is treated as false; see logic operators https://hl7.org/fhirpath/#boolean-logic
      if (results != null && results.isEmpty()) {
        return false;
      }
      if (results == null || results.size() != 1 || !results.get(0).fhirType().equals("boolean")) {
        String error =
            String.format("The `where` FHIRPath %s did not return one boolean!", w.getPath());
        log.error(error);
        throw new ViewApplicationException(error);
      }
      IBase r = results.get(0);
      try {
        IPrimitiveType<Boolean> booleanBase = (IPrimitiveType<Boolean>) r;
        if (booleanBase.getValue() != Boolean.TRUE) {
          return false;
        }
      } catch (ClassCastException e) {
        // This should never happen because `r.fhirType()` is "boolean".
        throw new ViewApplicationException("Error casting to IPrimitiveType<Boolean>!");
      }
    }
    return true;
  }

  /**
   * Applies all given `select`s to the given element and return the cross join of returned sets.
   * Note `element` can be null in which case the output would have a single row with the name of
   * all columns and all values being null.
   */
  private RowList applyAllSelects(@Nullable IBase element, List<Select> selectList)
      throws ViewApplicationException {
    List<RowList> rowsPerSelect = new ArrayList<>();
    for (Select s : selectList) {
      rowsPerSelect.add(applySelect(element, s));
    }
    // Now doing the cross join between selects.
    return crossJoinAll(rowsPerSelect);
  }

  private RowList crossJoinAll(List<RowList> rowsPerSelect) throws ViewApplicationException {
    RowList currentList = null;
    for (RowList rows : rowsPerSelect) {
      if (rows == null || rows.getRows().isEmpty()) {
        // One of the sub-lists is empty hence the whole cross-join will be empty.
        return EMPTY_LIST;
      }
      if (currentList == null) {
        currentList = rows;
      } else {
        currentList = currentList.crossJoin(rows);
      }
    }
    return currentList;
  }

  /** See `apply` javadoc. */
  private RowList applySelect(@Nullable IBase element, Select select)
      throws ViewApplicationException {
    String forEach = Strings.nullToEmpty(select.getForEach());
    String forEachOrNull = Strings.nullToEmpty(select.getForEachOrNull());
    Preconditions.checkState(
        forEachOrNull.isEmpty() || forEach.isEmpty(),
        "At most one of forEach and forEachOrNull can be set");
    String forEachPath = "";
    boolean keepNull = false;
    if (!forEach.isEmpty()) {
      forEachPath = forEach;
    }
    if (!forEachOrNull.isEmpty()) {
      forEachPath = forEachOrNull;
      keepNull = true;
    }
    List<IBase> refElements = new ArrayList<>();
    refElements.add(element);
    if (element != null && !forEachPath.isEmpty()) {
      refElements = evaluateFhirPath(element, forEachPath);
    }

    RowList.Builder allRowsBuilder = RowList.builder();
    for (IBase elem : refElements) {
      allRowsBuilder.addRows(applyColumnSelectUnion(elem, select));
    }
    if (allRowsBuilder.isEmpty() && keepNull) { // Add a row with names and null values.
      RowList nullRow = applyColumnSelectUnion(null, select);
      allRowsBuilder.addRows(nullRow);
      Preconditions.checkState(nullRow.getRows().size() == 1);
    }

    return allRowsBuilder.build();
  }

  private RowList applyColumnSelectUnion(@Nullable IBase elem, Select select)
      throws ViewApplicationException {
    RowList aggregteRowList = EMPTY_LIST;
    // First apply the `column` fields.
    List<Column> columns = select.getColumn();
    if (columns != null && !columns.isEmpty()) {
      FlatRow columnRow = applyColumns(elem, columns);
      aggregteRowList = RowList.builder().addRow(columnRow).build();
    }

    // Then apply nested `select` fields.
    if (select.getSelect() != null && !select.getSelect().isEmpty()) {
      RowList subSelectRows = applyAllSelects(elem, select.getSelect());
      if (subSelectRows.isEmpty()) { // We can end early in this case!
        return EMPTY_LIST;
      }
      if (aggregteRowList.isEmpty()) {
        aggregteRowList = subSelectRows;
      } else {
        aggregteRowList = aggregteRowList.crossJoin(subSelectRows);
      }
    }

    // Then apply `unionAll`.
    if (select.getUnionAll() != null && !select.getUnionAll().isEmpty()) {
      RowList.Builder rowListBuilder = RowList.builder();
      if (elem == null) {
        // Here we can pick any one of the union selects as all should have the same schema.
        rowListBuilder.addRows(applySelect(null, select.getUnionAll().get(0)));
      } else {
        for (Select unionSelect : select.getUnionAll()) {
          RowList unionSelectRows = applySelect(elem, unionSelect);
          rowListBuilder.addRows(unionSelectRows);
        }
      }
      RowList unionAllRows = rowListBuilder.build();
      if (unionAllRows.isEmpty()) { // We can end early in this case!
        return EMPTY_LIST;
      }
      if (aggregteRowList.isEmpty()) {
        aggregteRowList = unionAllRows;
      } else {
        aggregteRowList = aggregteRowList.crossJoin(unionAllRows);
      }
    }
    Preconditions.checkState(elem != null || aggregteRowList.getRows().size() == 1);
    return aggregteRowList;
  }

  /**
   * Applies the given column FHIRPaths to the given element and return one single row. Note element
   * can be null in which case the created row will have all the column names with null values.
   */
  FlatRow applyColumns(@Nullable IBase element, List<Column> columns)
      throws ViewApplicationException {
    List<RowElement> rowElements = new ArrayList<>();
    for (Column col : columns) {
      if (GET_RESOURCE_KEY.equals(col.getPath())) {
        if (element == null || !(element instanceof IBaseResource)) {
          throw new ViewApplicationException(
              GET_RESOURCE_KEY + " can only be applied at the root!");
        }
        IBaseResource baseResource = (IBaseResource) element;
        rowElements.add(
            new RowElement(
                // TODO move all type inference to a single place outside View application.
                col.toBuilder().inferredType(ID_TYPE).inferredCollection(false).build(),
                Lists.newArrayList(baseResource.getIdElement())));
        continue;
      }
      Matcher refMatcher = GET_REF_KEY_PATTERN.matcher(col.getPath());
      if (refMatcher.matches()) {
        // TODO add a unit-test for when element can be null here, e.g., forEachOrNull.
        if (element == null) {
          rowElements.add(new RowElement(col, null));
          continue;
        }
        // The elements would all be IIdType, but we need IBase for creating RowElement.
        List<IBase> refs = new ArrayList<>();
        String resType = Strings.nullToEmpty(refMatcher.group("resourceType"));
        String fhirPathForRef = Strings.nullToEmpty(refMatcher.group("fhirPath"));
        if (fhirPathForRef.endsWith(".")) {
          fhirPathForRef = fhirPathForRef.substring(0, fhirPathForRef.length() - 1);
        }
        List<IBase> eval = List.of(element);
        if (!fhirPathForRef.isEmpty()) {
          eval = evaluateFhirPath(element, fhirPathForRef);
        }
        for (IBase refElem : eval) {
          if (!(refElem instanceof IBaseReference)) {
            throw new ViewApplicationException(
                "getReferenceKey can only be applied to Reference elements; got " + fhirPathForRef);
          }
          IIdType ref = ((IBaseReference) refElem).getReferenceElement();
          if (resType.isEmpty()) {
            refs.add(ref);
          } else {
            if (resType.equals(ref.getResourceType())) {
              refs.add(ref);
            }
          }
        }
        rowElements.add(
            // TODO move all type inference to a single place outside View application.
            new RowElement(col.toBuilder().inferredType(ID_TYPE).build(), refs));
        continue;
      }
      List<IBase> eval = null;
      if (element != null) {
        eval = evaluateFhirPath(element, col.getPath());
      }
      rowElements.add(new RowElement(col, eval));
    }
    Preconditions.checkState(columns.isEmpty() || !rowElements.isEmpty());
    return FlatRow.builder().elements(ImmutableList.copyOf(rowElements)).build();
  }

  // The rest are data objects used for representing rows; it is important that these are immutable.

  /**
   * This is a representation of a view, i.e., a list of rows. It also keeps information about the
   * schema, i.e., column names and their types. The instances of this are immutable after creation
   * in the sense that rows and columns cannot be changed. To create new instances, the helper
   * methods or the {@link Builder} can be used.
   */
  @Getter
  public static class RowList {
    // The keys of this map are the column names; the values are full Column structs.
    private final ImmutableMap<String, Column> columnInfos;

    private final ImmutableList<FlatRow> rows;

    private RowList(List<FlatRow> rows, LinkedHashMap<String, Column> columnInfos) {
      this.rows = ImmutableList.copyOf(rows);
      this.columnInfos = ImmutableMap.copyOf(columnInfos);
    }

    /**
     * Converts the view into CSV format; this is not production ready!
     *
     * @return the CSV representation of this.
     */
    @VisibleForTesting
    String toCsv() {
      // TODO escape comma and new-line in fields!
      final String sep = ",";
      StringBuilder builder = new StringBuilder();
      builder.append(String.join(sep, columnInfos.keySet()));
      for (FlatRow row : rows) {
        builder.append('\n');
        builder.append(
            String.join(
                sep,
                row.getElements().stream().map(e -> e.getString()).collect(Collectors.toList())));
      }
      return builder.toString();
    }

    /**
     * This method is good for exchanging the view with other systems, e.g., a frontend. It is
     * currently not used as the simpler forms like `toCsv()` or `toHtml()` are preferred.
     *
     * @return the JSON representation of this.
     */
    public String toJson() {
      Gson gson = new Gson();
      return gson.toJson(this);
    }

    /**
     * Create an HTML table from this view.
     *
     * @return the HTML content without enclosing {@code <table></table>} tags such that the table
     *     can be easily decorated by the frontend.
     */
    public String toHtml() {
      final String sep = "</td><td>";
      StringBuilder builder = new StringBuilder();
      builder
          .append("<thead><tr><td>")
          .append(String.join(sep, columnInfos.keySet()))
          .append("</td></tr></thead>")
          .append("<tbody>");
      for (FlatRow row : rows) {
        builder.append("<tr><td>");
        builder.append(
            String.join(
                sep,
                row.getElements().stream().map(e -> e.getString()).collect(Collectors.toList())));
        builder.append("</td></tr>");
      }
      builder.append("</tbody>");
      return builder.toString();
    }

    public boolean isEmpty() {
      return columnInfos.isEmpty();
    }

    /**
     * A helper method to create the cross product (join) of this RowList and the `other`.
     *
     * @param other the RowList to be cross-joined.
     * @return a new RowList which is the cross product of the two.
     * @throws ViewApplicationException if the two RowList share a column.
     */
    public RowList crossJoin(RowList other) throws ViewApplicationException {
      if (isEmpty() || other.isEmpty()) {
        return EMPTY_LIST;
      }
      LinkedHashMap<String, Column> resultColInfos = new LinkedHashMap<>(this.getColumnInfos());
      for (Entry<String, Column> entry : other.getColumnInfos().entrySet()) {
        if (resultColInfos.get(entry.getKey()) != null) {
          throw new ViewApplicationException(
              "Repeated column in the cross joined RowList: " + entry.getKey());
        }
        resultColInfos.put(entry.getKey(), entry.getValue());
      }
      List<FlatRow> resultList = new ArrayList<>();
      for (FlatRow myRow : this.rows) {
        for (FlatRow otherRow : other.getRows()) {
          FlatRow combinedRow = myRow.addColumns(otherRow);
          resultList.add(combinedRow);
        }
      }
      return new RowList(resultList, resultColInfos);
    }

    private static Builder builder() {
      return new Builder();
    }

    // This can eventually be public, but currently it is not used outside this file.
    private static class Builder {
      private final LinkedHashMap<String, Column> columnInfos = new LinkedHashMap<>();
      private final List<FlatRow> rows = new ArrayList<>();

      private Builder() {}

      public Builder addRow(FlatRow row) throws ViewApplicationException {
        // Ignore empty rows; this is just to simplify the client's code.
        if (row.isEmpty()) {
          return this;
        }

        if (columnInfos.isEmpty()) {
          for (RowElement e : row.getElements()) {
            columnInfos.put(e.getName(), e.getColumnInfo());
          }
        }
        if (row.getElements().size() != columnInfos.size()) {
          throw new ViewApplicationException(
              String.format(
                  "New row size does not match schema: %d vs %d",
                  row.getElements().size(), columnInfos.size()));
        }
        // Checking that the new row has the same columns, in the same order. We could check extra
        // fields like type, collection, etc. but this is assumed to be done in ViewDefinition.
        int ind = 0;
        for (Map.Entry<String, ViewDefinition.Column> entry : columnInfos.entrySet()) {
          RowElement e = row.getElements().get(ind++);
          if (!entry.getKey().equals(e.getName())) {
            throw new ViewApplicationException("Unexpected column " + e.getName());
          }
        }
        // Sanity checks are done; add the new row ...
        this.rows.add(row);
        return this;
      }

      public Builder addRows(RowList rows) throws ViewApplicationException {
        if (rows.isEmpty()) {
          return this;
        }
        for (FlatRow row : rows.getRows()) {
          this.addRow(row);
        }
        return this;
      }

      public boolean isEmpty() {
        return rows.isEmpty();
      }

      public RowList build() {
        return new RowList(rows, columnInfos);
      }
    }
  }

  /** Representation of a single row. Each element has a name, value, and type. */
  @Getter
  @Builder
  public static class FlatRow {
    private final ImmutableList<RowElement> elements;

    @Override
    public String toString() {
      return Arrays.toString(elements.toArray());
    }

    boolean isEmpty() {
      return elements.isEmpty();
    }

    public FlatRow addColumns(FlatRow other) {
      List<RowElement> resultRow = new ArrayList<>(elements);
      resultRow.addAll(other.getElements());
      return FlatRow.builder().elements(ImmutableList.copyOf(resultRow)).build();
    }
  }

  public static String getIdString(IIdType id) {
    return id.getIdPart();
  }

  @Getter
  public static class RowElement {
    private final List<IBase> values;
    private final Column columnInfo;

    @Override
    public String toString() {
      return columnInfo.getName() + ":" + Arrays.toString(values.toArray());
    }

    public RowElement(Column columnInfo, List<IBase> values) throws ViewApplicationException {
      if (!columnInfo.isCollection() && values != null && values.size() > 1) {
        throw new ViewApplicationException(
            "A list provided for the non-collection column " + columnInfo.getName());
      }
      this.values = values;
      this.columnInfo = columnInfo;
    }

    // Convenience function
    public String getName() {
      return columnInfo.getName();
    }

    public boolean isCollection() {
      return columnInfo.isCollection()
          || columnInfo.isInferredCollection()
          || (values != null && values.size() > 1);
    }

    @Nullable
    public IBase getSingleValue() {
      if (values == null || values.isEmpty() || isCollection()) {
        return null;
      }
      return values.get(0);
    }

    // Note the following methods are currently static, but it is possible that in the future we may
    // need to attach them to the ViewApplicator instance that has generated the RowElement. That
    // will be the case if we need to cast to the actual type of the value (which is FHIR version
    // dependent) and not just use the interfaces (which are version independent).

    @Nullable
    public String getString() {
      IPrimitiveType primitiveType = getPrimitiveType();
      if (primitiveType != null) {
        // TODO: This is a temporary solution to handle IDs properly. We should implement proper
        //  polymorphism to properly handle primitive types when we add type inference. This can
        //  be similar to how we convert primitives to Avro fields in Bunsen.
        String maybeId = getSingleIdPart();
        return maybeId != null ? maybeId : primitiveType.getValueAsString();
      }
      return getSingleValue() == null ? null : getSingleValue().toString();
    }

    @Nullable
    public IPrimitiveType getPrimitiveType() {
      IBase val = getSingleValue();
      if (val == null) {
        return null;
      }
      if (!(val instanceof IPrimitiveType<?>)) {
        return null;
      }
      return (IPrimitiveType) val;
    }

    @Nullable
    public <T> T getPrimitive() {
      IBase val = getSingleValue();
      if (val == null) {
        return null;
      }
      if (!(val instanceof IPrimitiveType<?>)) {
        return null;
      }
      IPrimitiveType<T> primitive = (IPrimitiveType<T>) val;
      if (primitive == null) {
        return null;
      }
      return primitive.getValue();
    }

    @Nullable
    private String getSingleIdPart() {
      Preconditions.checkState(!isCollection());
      if (values != null && !values.isEmpty() && ID_TYPE.equals(columnInfo.getInferredType())) {
        IBase elem = values.get(0);
        return getIdString((IIdType) elem);
      }
      return null;
    }

    public List<String> getIdParts() {
      List<String> idParts = new ArrayList<>();
      if (ID_TYPE.equals(columnInfo.getInferredType())) {
        for (IBase elem : values) {
          idParts.add(getIdString((IIdType) elem));
        }
      }
      return idParts;
    }

    public boolean isIdType() {
      return (ID_TYPE.equals(columnInfo.getInferredType()))
          || (ID_TYPE.equals(columnInfo.getType()));
    }
  }
}
