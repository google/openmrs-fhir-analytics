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
import ca.uhn.fhir.context.FhirVersionEnum;
import com.cerner.bunsen.ProfileMapperFhirContexts;
import com.cerner.bunsen.avro.AvroConverter;
import com.cerner.bunsen.exception.ProfileException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.avro.Conversions.DecimalConversion;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;
import org.hl7.fhir.r4.model.Resource;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is supposed to hold all AvroConverter objects, i.e., for all resource types. It is
 * also intended to be a singleton because creation of these objects can be expensive. So any user
 * that needs an AvroConverter gets the single instance of this class (per JVM) using `getInstance`.
 */
public class AvroConversionUtil {

  private static final Logger log = LoggerFactory.getLogger(AvroConversionUtil.class);

  // This is the singleton instance.
  @Nullable private static AvroConversionUtil instance;

  private final Map<String, AvroConverter> converterMap;

  private final ProfileMapperFhirContexts profileMapperFhirContexts;

  private final FhirContext fhirContext;

  private final FhirVersionEnum fhirVersionEnum;

  @Nullable private final String structureDefinitionsPath;

  private final int recursiveDepth;

  /**
   * This is to fix the logical type conversions for BigDecimal. This should be called once before
   * any FHIR resource conversion to Avro.
   */
  public static void initializeAvroConverters() {
    // For more context on the next two conversions, see this thread: https://bit.ly/3iE4rwS
    // Add BigDecimal conversion to the singleton instance to fix "Unknown datum type" Avro
    // exception.
    GenericData.get().addLogicalTypeConversion(new DecimalConversion());
    // This is for a similar error in the ParquetWriter.write which uses SpecificData.get() as its
    // model.
    SpecificData.get().addLogicalTypeConversion(new DecimalConversion());
  }

  private AvroConversionUtil(
      FhirVersionEnum fhirVersionEnum,
      @Nullable String structureDefinitionsPath,
      int recursiveDepth)
      throws ProfileException {
    this.fhirVersionEnum = fhirVersionEnum;
    this.structureDefinitionsPath = structureDefinitionsPath;
    this.converterMap = Maps.newHashMap();
    this.profileMapperFhirContexts = ProfileMapperFhirContexts.getInstance();
    this.fhirContext =
        profileMapperFhirContexts.contextFor(fhirVersionEnum, structureDefinitionsPath);
    this.recursiveDepth = recursiveDepth;
  }

  static synchronized AvroConversionUtil getInstance(
      FhirVersionEnum fhirVersionEnum,
      @Nullable String structureDefinitionsPath,
      int recursiveDepth)
      throws ProfileException {
    Preconditions.checkNotNull(fhirVersionEnum, "fhirVersionEnum cannot be null");
    structureDefinitionsPath = Strings.nullToEmpty(structureDefinitionsPath);

    if (instance == null) {
      instance = new AvroConversionUtil(fhirVersionEnum, structureDefinitionsPath, recursiveDepth);
    } else if (!fhirVersionEnum.equals(instance.fhirVersionEnum)
        || !structureDefinitionsPath.equals(instance.structureDefinitionsPath)
        || recursiveDepth != instance.recursiveDepth) {
      String errorMsg =
          String.format(
              "AvroConversionUtil has been initialised with different set of parameters earlier"
                  + " with fhirVersionEnum=%s, structureDefinitionsPath=%s"
                  + " compared to what is being passed now with"
                  + " fhirVersionEnum=%s, structureDefinitionsPath=%s",
              instance.fhirVersionEnum,
              instance.structureDefinitionsPath,
              fhirVersionEnum,
              structureDefinitionsPath);
      log.error(errorMsg);
      throw new ProfileException(errorMsg);
    }
    return instance;
  }

  public synchronized FhirContext getFhirContext() {
    // This should never be the case as creation of new instance makes sure the FhirContext is
    // initialised properly.
    Preconditions.checkNotNull(
        fhirContext, "The fhirContext should have been initialised in the constructor!");
    return fhirContext;
  }

  /**
   * Returns the Avro converter for the given resource type, which is the union of all converters
   * configured via the fhir profiles for the given resource type.
   */
  synchronized AvroConverter getConverter(String resourceType) throws ProfileException {
    if (!converterMap.containsKey(resourceType)) {
      FhirContext fhirContext = getFhirContext();
      List<String> profiles =
          profileMapperFhirContexts.getMappedProfilesForResource(
              fhirContext.getVersion().getVersion(), resourceType);
      if (profiles == null || profiles.isEmpty()) {
        String errorMsg =
            String.format("No mapped profiles found for resourceType=%s", resourceType);
        log.error(errorMsg);
        throw new ProfileException(errorMsg);
      }
      AvroConverter converter = AvroConverter.forResources(fhirContext, profiles, recursiveDepth);
      converterMap.put(resourceType, converter);
    }
    return converterMap.get(resourceType);
  }

  @VisibleForTesting
  @Nullable GenericRecord convertToAvro(Resource resource) throws ProfileException {
    AvroConverter converter = getConverter(resource.getResourceType().name());
    // TODO: Check why Bunsen returns IndexedRecord instead of GenericRecord.
    return (GenericRecord) converter.resourceToAvro(resource);
  }

  @VisibleForTesting
  Resource convertToHapi(GenericRecord record, String resourceType) throws ProfileException {
    // Note resourceType can also be inferred from the record (through fhirType).
    AvroConverter converter = getConverter(resourceType);
    IBaseResource resource = converter.avroToResource(record);
    // TODO: fix this for other FHIR versions: https://github.com/google/fhir-data-pipes/issues/400
    if (!(resource instanceof Resource)) {
      throw new IllegalArgumentException("Cannot convert input record to resource!");
    }
    return (Resource) resource;
  }

  /**
   * Returns the Avro schema for the given resource type. The avro schema is the union of all the
   * schemas configured for the given resource type (i.e. for all the profiles/extensions
   * configured). If none are configured then the default base schema is returned.
   */
  public Schema getResourceSchema(String resourceType) throws ProfileException {
    AvroConverter converter = getConverter(resourceType);
    Schema schema = converter.getSchema();
    log.debug(String.format("Schema for resource type %s is %s", resourceType, schema));
    return schema;
  }

  public List<GenericRecord> generateRecords(Bundle bundle) throws ProfileException {
    List<GenericRecord> records = new ArrayList<>();
    if (bundle.getTotal() == 0) {
      return records;
    }
    for (BundleEntryComponent entry : bundle.getEntry()) {
      Resource resource = entry.getResource();
      GenericRecord record = convertToAvro(resource);
      if (record != null) {
        records.add(record);
      }
    }
    return records;
  }

  @VisibleForTesting
  static synchronized void deRegisterMappingsFor(FhirVersionEnum fhirVersionEnum) {
    instance = null;
    ProfileMapperFhirContexts.getInstance().deRegisterFhirContexts(fhirVersionEnum);
  }
}
