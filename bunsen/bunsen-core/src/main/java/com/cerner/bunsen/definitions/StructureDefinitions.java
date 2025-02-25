package com.cerner.bunsen.definitions;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableSet;
import java.lang.reflect.Constructor;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Abstract base class to visit FHIR structure definitions. */
public abstract class StructureDefinitions {

  // TODO make this FHIR version specific and hide it under an abstract method.
  @VisibleForTesting
  public static final Set<String> PRIMITIVE_TYPES =
      ImmutableSet.<String>builder()
          .add("id")
          .add("boolean")
          .add("code")
          .add("markdown")
          .add("date")
          .add("instant")
          .add("dateTime")
          .add("time")
          .add("string")
          .add("oid")
          // Note `xhtml` types are currently skipped because of:
          // https://github.com/google/fhir-data-pipes/issues/1014
          .add("xhtml")
          .add("decimal")
          .add("integer")
          .add("unsignedInt")
          .add("positiveInt")
          .add("base64Binary")
          .add("uri")
          // TODO: Figure out why these are added to R4 resource definitions.
          .add("http://hl7.org/fhirpath/System.String")
          .add("http://hl7.org/fhirpath/System.Boolean")
          .add("http://hl7.org/fhirpath/System.Integer")
          .add("http://hl7.org/fhirpath/System.Long")
          .add("http://hl7.org/fhirpath/System.Decimal")
          .add("http://hl7.org/fhirpath/System.DateTime")
          .add("http://hl7.org/fhirpath/System.Time")
          .add("canonical")
          .add("url")
          .add("uuid")
          .build();

  private static final String STU3_DEFINITIONS_CLASS =
      "com.cerner.bunsen.definitions.stu3.Stu3StructureDefinitions";

  private static final String R4_DEFINITIONS_CLASS =
      "com.cerner.bunsen.definitions.r4.R4StructureDefinitions";

  protected final FhirContext context;

  /**
   * Creates a new instance with the given context.
   *
   * @param context the FHIR context.
   */
  public StructureDefinitions(FhirContext context) {
    this.context = context;
  }

  // TODO the current order of methods reflects previous refactored files to make code review
  //  process simpler; we should probably change this in future and move public methods to the top.
  private List<IElementDefinition> getChildren(
      IElementDefinition parent, List<IElementDefinition> definitions) {

    String contentReference = parent.getContentReference();
    if (contentReference != null) {
      if (!contentReference.startsWith("#")) {
        // For Non-local references check if there is any existing local reference, otherwise fail.
        // This is a temporary fix so that the changes work for US Core Profile, the original issue
        // where in to support Non-local references is being tracked in the below ticket.
        // TODO: https://github.com/google/fhir-data-pipes/issues/961
        parent = null;
        if (contentReference.indexOf("#") > 0) {
          String referencedType = contentReference.substring(contentReference.indexOf("#") + 1);
          parent = getParentDefinition(referencedType, definitions);
        }
        if (parent == null) {
          throw new IllegalStateException("Non-local references are not yet supported");
        }
      } else {
        // Remove the leading hash (#) to get the referenced type.
        String referencedType = parent.getContentReference().substring(1);
        parent = getParentDefinition(referencedType, definitions);
        if (parent == null) {
          throw new IllegalArgumentException("Expected a reference type");
        }
      }
    }

    String startsWith = parent.getId() + ".";

    return definitions.stream()
        .filter(
            definition ->
                definition.getId().startsWith(startsWith)
                    && definition.getId().indexOf('.', startsWith.length()) < 0)
        .collect(Collectors.toList());
  }

  /**
   * Find the definition from the definitions list, whose path starts with the given referencedType
   */
  private IElementDefinition getParentDefinition(
      String referencedType, List<IElementDefinition> definitions) {
    return definitions.stream()
        .filter(definition -> definition.getPath().equals(referencedType))
        .findFirst()
        .orElse(null);
  }

  private <T> List<StructureField<T>> singleField(String elementName, T result) {
    if (result == null) {
      return Collections.emptyList();
    }
    return Collections.singletonList(StructureField.property(elementName, result));
  }

  private boolean shouldTerminateRecursive(
      DefinitionVisitor visitor, QualifiedPath newPath, Deque<QualifiedPath> stack) {

    // TODO we should add configuration parameters for exceptional paths
    //  that require deeper recursion; this is where to apply that logic:
    // String elementPath = DefinitionVisitorsUtil.pathFromStack(
    //    DefinitionVisitorsUtil.elementName(newPath.getElementPath()), stack);
    // if ("QuestionnaireResponse.item.item.item.item.item.item".startsWith(elementPath)) {
    //   return false;
    // }

    int maxDepth = visitor.getMaxDepth(newPath.getParentTypeUrl(), newPath.getElementPath());

    return stack.stream().filter(path -> path.equals(newPath)).count() > maxDepth;
  }

  private <T> List<StructureField<T>> extensionElementToFields(
      DefinitionVisitor<T> visitor,
      IStructureDefinition rootDefinition,
      IElementDefinition element,
      List<IElementDefinition> snapshotDefinitions,
      Deque<QualifiedPath> stack) {

    // FIXME: extension is a type rather than an external structure....
    IStructureDefinition definition = null;
    String profileUrl = element.getFirstTypeProfile();
    if (profileUrl != null) {
      definition = getStructureDefinition(profileUrl);
    }

    List<StructureField<T>> extensions;

    if (definition != null) {
      List<IElementDefinition> extensionDefinitions = definition.getSnapshotDefinitions();
      IElementDefinition extensionRoot = extensionDefinitions.get(0);
      extensions =
          visitExtensionDefinition(
              visitor,
              rootDefinition,
              element.getSliceName(),
              stack,
              definition.getUrl(),
              extensionDefinitions,
              extensionRoot);
    } else {
      if (element.getSliceName() == null) {
        return Collections.emptyList();
      }
      extensions =
          visitExtensionDefinition(
              visitor,
              rootDefinition,
              element.getSliceName(),
              stack,
              element.getFirstTypeProfile(),
              snapshotDefinitions,
              element);
    }

    if (!element.getMax().equals("1") && extensions.size() > 0) {
      // the nested extension element has max: *
      return Collections.singletonList(
          StructureField.extension(
              extensions.get(0).fieldName(),
              extensions.get(0).extensionUrl(),
              extensions.get(0).isModifier(),
              visitor.visitMultiValued(extensions.get(0).fieldName(), extensions.get(0).result())));

    } else {
      return extensions;
    }
  }

  private <T> List<StructureField<T>> visitExtensionDefinition(
      DefinitionVisitor<T> visitor,
      IStructureDefinition rootDefinition,
      String sliceName,
      Deque<QualifiedPath> stack,
      String url,
      List<IElementDefinition> extensionDefinitions,
      IElementDefinition extensionRoot) {

    // For extensions, we need to process slices to separate them into individual fields.
    List<IElementDefinition> children = getChildren(extensionRoot, extensionDefinitions);

    // Extensions may contain either additional extensions or a value field, but not both.
    // So if it has a child element with a slice name, it has additional extensions.
    List<IElementDefinition> childExtensions =
        children.stream()
            .filter(element -> element.getSliceName() != null)
            .collect(Collectors.toList());

    if (!childExtensions.isEmpty()) {
      List<StructureField<T>> childFields = new ArrayList<>();

      for (IElementDefinition childExtension : childExtensions) {
        List<StructureField<T>> childField =
            extensionElementToFields(
                visitor, rootDefinition, childExtension, extensionDefinitions, stack);
        childFields.addAll(childField);
      }

      T result = visitor.visitParentExtension(sliceName, url, childFields);

      if (result == null) {
        return Collections.emptyList();
      } else {
        return Collections.singletonList(
            StructureField.extension(sliceName, url, extensionRoot.getIsModifier(), result));
      }
    } else {
      // The extension has no children, so produce its value.
      Optional<IElementDefinition> valueElement =
          children.stream().filter(e -> e.getPath().contains("value")).findFirst();
      // FIXME: get the extension URL.
      Optional<IElementDefinition> urlElement =
          children.stream().filter(e -> e.getPath().endsWith("url")).findFirst();
      String extensionUrl = urlElement.get().getFixedPrimitiveValue();
      List<StructureField<T>> childField =
          elementToFields(visitor, rootDefinition, valueElement.get(), extensionDefinitions, stack);
      T result =
          visitor.visitLeafExtension(
              sliceName, extensionUrl, childField.iterator().next().result());
      return Collections.singletonList(
          StructureField.extension(sliceName, extensionUrl, extensionRoot.getIsModifier(), result));
    }
  }

  /**
   * Returns the fields for the given element. The returned stream can be empty (e.g., for elements
   * with max of zero), or have multiple values (for elements that generate fields with additional
   * data in siblings.)
   */
  private <T> List<StructureField<T>> elementToFields(
      DefinitionVisitor<T> visitor,
      IStructureDefinition rootDefinition,
      IElementDefinition element,
      List<IElementDefinition> snapshotDefinitions,
      Deque<QualifiedPath> stack) {

    String elementName = DefinitionVisitorsUtil.elementName(element.getPath());

    if (element.getMax().equals("0")) {
      // Fields with max of zero are omitted.
      return Collections.emptyList();
    } else if ("Extension".equals(element.getFirstTypeCode())) {
      return extensionElementToFields(visitor, rootDefinition, element, snapshotDefinitions, stack);
    } else if (element.getSliceName() != null) {
      // Drop slices for non-extension fields; otherwise we will end up with duplicated fields.
      return Collections.emptyList();
    } else if (element.hasSingleType()
        && PRIMITIVE_TYPES.contains(element.getFirstTypeCode())
        // The below condition is added so that cases like - elements with multiple choice types in
        // the base resource, that are restricted to include only one data type in the profiled
        // version are not matched. One such example is the R4, US Core Profile Observation resource
        // i.e. in the classpath, the resource
        // /r4-us-core-definitions/StructureDefinition-us-core-smokingstatus.json has element with
        // path `Observation.effective[x]` and is restricted to contain only one data type, the
        // below condition makes sure such fields are not matched here. However, the Extension
        // fields even though containing single type are defined with paths ending with [x]. Refer
        // /r4-us-core-definitions/StructureDefinition-us-core-race.json which contains a field
        // Extension.extension:text.value[x] that has a single type and the below condition
        // matches this case.
        && (element.getPath().startsWith("Extension") || !element.getPath().endsWith("[x]"))) {
      T primitiveConverter = visitor.visitPrimitive(elementName, element.getFirstTypeCode());
      if (!element.getMax().equals("1")) {
        return singleField(elementName, visitor.visitMultiValued(elementName, primitiveConverter));
      } else {
        return singleField(elementName, primitiveConverter);
      }

    } else if (element.getPath().endsWith("[x]") && !element.getPath().startsWith("Extension")) {
      // TODO fix for "Extension": https://github.com/google/fhir-data-pipes/issues/559

      // Use a linked hash map to preserve the order of the fields
      // for iteration.
      Map<String, T> choiceTypes = new LinkedHashMap<>();

      for (String typeCode : element.getAllTypeCodes()) {
        if (PRIMITIVE_TYPES.contains(typeCode)) {
          T child = visitor.visitPrimitive(elementName, typeCode);
          choiceTypes.put(typeCode, child);
        } else {
          IStructureDefinition structureDefinition = getStructureDefinition(typeCode);

          // TODO document why we are resetting the stack here; it is not clear
          //  why this cannot lead to infinite recursion for choice types. If
          //  we don't reset the stack, then we should handle null returns.
          T child = transform(visitor, element, structureDefinition, new ArrayDeque<>());
          Verify.verify(
              child != null, "Unexpected null choice type %s for element %s", typeCode, element);
          choiceTypes.put(typeCode, child);
        }
      }

      StructureField<T> field =
          new StructureField<>(
              elementName,
              elementName,
              null,
              false,
              true,
              visitor.visitChoice(elementName, choiceTypes));
      return Collections.singletonList(field);
    } else if (!element.getMax().equals("1")) {
      if (getDefinition(element) != null) {
        // Handle defined data types.
        IStructureDefinition definition = getDefinition(element);
        T type = transform(visitor, element, definition, stack);
        return singleField(elementName, visitor.visitMultiValued(elementName, type));
      } else {
        List<StructureField<T>> childElements =
            transformChildren(visitor, rootDefinition, snapshotDefinitions, stack, element);
        if (childElements.isEmpty()) {
          // All children were dropped because of recursion depth limit.
          return Collections.emptyList();
        }
        T result =
            visitor.visitComposite(
                elementName,
                DefinitionVisitorsUtil.pathFromStack(elementName, stack),
                elementName,
                rootDefinition.getUrl(),
                childElements);
        List<StructureField<T>> composite = singleField(elementName, result);
        // Array types should produce only a single element.
        if (composite.size() != 1) {
          throw new IllegalStateException(
              "Array type in " + element.getPath() + " must map to a single structure.");
        }

        // Wrap the item in the corresponding multi-valued type.
        return singleField(
            elementName, visitor.visitMultiValued(elementName, composite.get(0).result()));
      }

    } else if (getDefinition(element) != null) {

      // TODO refactor this and the similar block above for handling defined data types.
      // Handle defined data types.
      IStructureDefinition definition = getDefinition(element);
      T type = transform(visitor, element, definition, stack);
      return singleField(DefinitionVisitorsUtil.elementName(element.getPath()), type);
    } else {

      // Handle composite type
      List<StructureField<T>> childElements =
          transformChildren(visitor, rootDefinition, snapshotDefinitions, stack, element);
      if (childElements.isEmpty()) {
        // All children were dropped because of recursion depth limit.
        return Collections.emptyList();
      }

      T result =
          visitor.visitComposite(
              elementName,
              DefinitionVisitorsUtil.pathFromStack(elementName, stack),
              elementName,
              rootDefinition.getUrl(),
              childElements);

      return singleField(elementName, result);
    }
  }

  /**
   * Goes through the list of children of the given `element` and convert each of those
   * `ElementDefinision`s to `StructureField`s. NOTE: This is the only place where the traversal
   * stack can grow. It is also best if this is the only place where `shouldTerminateRecursive` is
   * called.
   */
  private <T> List<StructureField<T>> transformChildren(
      DefinitionVisitor<T> visitor,
      IStructureDefinition rootDefinition,
      List<IElementDefinition> snapshotDefinitions,
      Deque<QualifiedPath> stack,
      IElementDefinition element) {

    QualifiedPath qualifiedPath = new QualifiedPath(rootDefinition.getUrl(), element.getPath());

    if (shouldTerminateRecursive(visitor, qualifiedPath, stack)) {
      return Collections.emptyList();
    } else {
      stack.push(qualifiedPath);

      // Handle composite type
      List<StructureField<T>> childElements = new ArrayList<>();

      for (IElementDefinition child : getChildren(element, snapshotDefinitions)) {
        List<StructureField<T>> childFields =
            elementToFields(visitor, rootDefinition, child, snapshotDefinitions, stack);
        childElements.addAll(childFields);
      }

      stack.pop();

      return childElements;
    }
  }

  private <T> StructureField<T> transformContained(
      DefinitionVisitor<T> visitor,
      IStructureDefinition rootDefinition,
      List<IStructureDefinition> containedDefinitions,
      Deque<QualifiedPath> stack,
      IElementDefinition element) {

    Map<String, StructureField<T>> containedElements = new LinkedHashMap<>();

    for (IStructureDefinition containedDefinition : containedDefinitions) {
      IElementDefinition containedRootElement = containedDefinition.getRootDefinition();
      List<IElementDefinition> snapshotDefinitions = containedDefinition.getSnapshotDefinitions();
      List<StructureField<T>> childElements =
          transformChildren(
              visitor, containedDefinition, snapshotDefinitions, stack, containedRootElement);
      // At this level no child should be dropped because of recursion limit.
      Verify.verify(!childElements.isEmpty());
      String rootName = DefinitionVisitorsUtil.elementName(containedRootElement.getPath());
      T result =
          visitor.visitComposite(
              rootName,
              containedRootElement.getPath(),
              rootName,
              containedDefinition.getUrl(),
              childElements);
      containedElements.put(rootName, StructureField.property(rootName, result));
    }

    T result =
        visitor.visitContained(
            element.getPath() + ".contained", rootDefinition.getUrl(), containedElements);

    return StructureField.property("contained", result);
  }

  /**
   * Transforms a FHIR resource to a type defined by the visitor.
   *
   * @param visitor a visitor class to recursively transform the structure.
   * @param resourceTypeUrl the URL defining the resource type or profile.
   * @param <T> the return type of the visitor.
   * @return the transformed result.
   */
  public <T> T transform(DefinitionVisitor<T> visitor, String resourceTypeUrl) {
    return transform(visitor, resourceTypeUrl, Collections.emptyList());
  }

  /**
   * Transforms a FHIR resource to a type defined by the visitor.
   *
   * @param visitor a visitor class to recursively transform the structure.
   * @param resourceTypeUrl the URL defining the resource type or profile.
   * @param containedResourceTypeUrls the URLs defining the resource types or profiles to be
   *     contained to the given resource.
   * @param <T> the return type of the visitor.
   * @return the transformed result.
   */
  public <T> T transform(
      DefinitionVisitor<T> visitor,
      String resourceTypeUrl,
      List<String> containedResourceTypeUrls) {

    IStructureDefinition definition = getStructureDefinition(resourceTypeUrl);

    List<IStructureDefinition> containedDefinitions =
        containedResourceTypeUrls.stream()
            .map(containedResourceTypeUrl -> getStructureDefinition(containedResourceTypeUrl))
            .collect(Collectors.toList());

    return transformRoot(visitor, definition, containedDefinitions);
  }

  // TODO make the separation between this and `elementToFields` more clear.
  /**
   * Transforms the given FHIR structure definition.
   *
   * @param visitor the visitor performing the transformation
   * @param parentElement the element containing this definition for additional type information, or
   *     null if it is not contained in a parent element.
   * @param definition the FHIR structure definition to be converted
   * @param stack a stack of FHIR type URLs to detect recursive definitions.
   * @return the transformed structure, or null if it should not be included in the parent.
   */
  @Nullable
  private <T> T transform(
      DefinitionVisitor<T> visitor,
      IElementDefinition parentElement,
      IStructureDefinition definition,
      Deque<QualifiedPath> stack) {

    List<IElementDefinition> snapshotDefinitions = definition.getSnapshotDefinitions();
    IElementDefinition root = definition.getRootDefinition();

    List<StructureField<T>> childElements =
        transformChildren(visitor, definition, snapshotDefinitions, stack, root);

    if ("Reference".equals(definition.getType())) {
      List<String> referenceProfiles = parentElement.getReferenceTargetProfiles();
      List<String> referenceTypes =
          referenceProfiles.stream()
              .map(profile -> getStructureDefinition(profile).getType())
              .sorted()
              // Retrieve only the unique reference types
              .distinct()
              .collect(Collectors.toList());

      String elementName = DefinitionVisitorsUtil.elementName(parentElement.getPath());
      String elementFullPath = DefinitionVisitorsUtil.pathFromStack(elementName, stack);
      return visitor.visitReference(
          DefinitionVisitorsUtil.recordNameFor(elementFullPath), referenceTypes, childElements);
    } else {
      String rootName = DefinitionVisitorsUtil.elementName(root.getPath());

      // We don't want 'id' to be present in nested fields to make it consistent with SQL-on-FHIR.
      // https://github.com/FHIR/sql-on-fhir/blob/master/sql-on-fhir.md#id-fields-omitted
      childElements.removeIf(field -> field.fieldName().equals("id"));

      if (childElements.isEmpty()) {
        // All children were dropped because of recursion depth limit.
        return null;
      }
      return visitor.visitComposite(
          rootName,
          DefinitionVisitorsUtil.pathFromStack(root.getPath(), stack),
          rootName,
          definition.getUrl(),
          childElements);
    }
  }

  private <T> T transformRoot(
      DefinitionVisitor<T> visitor,
      IStructureDefinition definition,
      List<IStructureDefinition> containedDefinitions) {

    IElementDefinition rootElement = definition.getRootDefinition();
    List<IElementDefinition> snapshotDefinitions = definition.getSnapshotDefinitions();
    Deque<QualifiedPath> stack = new ArrayDeque<>();
    List<StructureField<T>> childElements =
        transformChildren(visitor, definition, snapshotDefinitions, stack, rootElement);
    // At this level no child should be dropped because of recursion limit.
    Verify.verify(!childElements.isEmpty());
    Verify.verify(stack.isEmpty());

    // If there are contained definitions, create a Resource Container StructureField
    if (containedDefinitions.size() > 0) {
      StructureField<T> containedElement =
          transformContained(visitor, definition, containedDefinitions, stack, rootElement);
      // Replace default StructureField with constructed Resource Container StructureField
      // TODO make this future proof instead of using a hard-coded index for `contained`.
      childElements.set(5, containedElement);
    }

    String rootName = DefinitionVisitorsUtil.elementName(rootElement.getPath());

    return visitor.visitComposite(rootName, rootName, rootName, definition.getUrl(), childElements);
  }

  /**
   * Returns the structure definition interface corresponding to the given resourceUrl.
   *
   * @param resourceUrl it can be a resource type like `Patient` or a profile URL.
   * @return the {@link IStructureDefinition} corresponding to the `resourceUrl`
   * @throws IllegalArgumentException if the structure definition cannot be found for the given
   *     resourceUrl.
   */
  @Nonnull
  protected abstract IStructureDefinition getStructureDefinition(String resourceUrl)
      throws IllegalArgumentException;

  /**
   * Returns the structure definition interface corresponding to the given element.
   *
   * @param element the target element
   * @return the structure definition or null if the given element has no type code.
   */
  @Nullable
  private IStructureDefinition getDefinition(IElementDefinition element) {
    String typeCode = element.getFirstTypeCode();
    return typeCode == null || typeCode.equals("BackboneElement") || typeCode.equals("Element")
        ? null
        : getStructureDefinition(typeCode);
  }

  /**
   * Returns supporting functions to make FHIR conversion work independent of version.
   *
   * @return functions supporting FHIR conversion.
   */
  public abstract FhirConversionSupport conversionSupport();

  /**
   * Create a new instance of this class for the given version of FHIR.
   *
   * @param context The FHIR context
   * @return a StructureDefinitions instance.
   */
  public static StructureDefinitions create(FhirContext context) {

    Class structureDefinitionsClass;

    FhirVersionEnum versionEnum = context.getVersion().getVersion();
    String className = null;

    if (FhirVersionEnum.DSTU3.equals(versionEnum)) {
      className = STU3_DEFINITIONS_CLASS;
    } else if (FhirVersionEnum.R4.equals(versionEnum)) {
      className = R4_DEFINITIONS_CLASS;
    } else {
      throw new IllegalArgumentException("Unsupported FHIR version: " + versionEnum);
    }

    try {
      structureDefinitionsClass = Class.forName(className);
    } catch (ClassNotFoundException exception) {
      throw new IllegalStateException(exception);
    }

    try {
      Constructor constructor = structureDefinitionsClass.getConstructor(FhirContext.class);
      return (StructureDefinitions) constructor.newInstance(context);
    } catch (Exception exception) {
      throw new IllegalStateException(exception);
    }
  }
}
