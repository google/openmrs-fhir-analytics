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
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.cerner.bunsen.ProfileMapperFhirContexts;
import com.google.common.annotations.VisibleForTesting;
import com.google.fhir.analytics.JdbcConnectionPools.DataSourceConfig;
import com.google.fhir.analytics.model.DatabaseConfiguration;
import java.io.IOException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.camel.CamelContext;
import org.apache.camel.LoggingLevel;
import org.apache.camel.Service;
import org.apache.camel.builder.RouteBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Debezium change data capture / Listener
public class DebeziumListener extends RouteBuilder {

  private static final Logger log = LoggerFactory.getLogger(DebeziumListener.class);

  private DatabaseConfiguration databaseConfiguration;

  private DebeziumArgs params;

  public DebeziumListener(String[] args) throws IOException {
    this.params = new DebeziumArgs();
    JCommander.newBuilder().addObject(params).build().parse(args);
    this.databaseConfiguration =
        DatabaseConfiguration.createConfigFromFile(params.fhirDebeziumConfigPath);
  }

  @VisibleForTesting
  static final String DEBEZIUM_ROUTE_ID = DebeziumListener.class.getName() + ".MysqlDatabaseCDC";

  @Override
  public void configure() throws IOException, Exception {
    log.info("Debezium Listener Started... ");

    // Main Change Data Capture (DBZ) entrypoint
    from(getDebeziumConfig())
        .routeId(DEBEZIUM_ROUTE_ID)
        .log(LoggingLevel.TRACE, "Incoming Events: ${body} with headers ${headers}")
        .process(createFhirConverter(getContext()));
  }

  @VisibleForTesting
  FhirConverter createFhirConverter(CamelContext camelContext) throws Exception {
    FhirContext fhirContext =
        ProfileMapperFhirContexts.getInstance().contextFor(FhirVersionEnum.R4, null);
    String fhirBaseUrl = params.fhirServerUrl;
    // TODO add OAuth support if we want to continue maintaining the streaming pipeline.
    FetchUtil fetchUtil =
        new FetchUtil(
            fhirBaseUrl,
            params.fhirServerUserName,
            params.fhirServerPassword,
            "",
            "",
            "",
            fhirContext);
    FhirStoreUtil fhirStoreUtil =
        FhirStoreUtil.createFhirStoreUtil(
            params.fhirSinkPath,
            params.sinkUserName,
            params.sinkPassword,
            fhirContext.getRestfulClientFactory());
    ParquetUtil parquetUtil =
        new ParquetUtil(
            fhirContext.getVersion().getVersion(),
            "",
            params.outputParquetPath,
            "",
            false,
            params.secondsToFlushParquetFiles,
            params.rowGroupSizeForParquetFiles,
            "streaming_",
            1,
            false);
    DataSource jdbcSource =
        JdbcConnectionPools.getInstance()
            .getPooledDataSource(
                DataSourceConfig.create(
                    databaseConfiguration.getJdbcDriverClass(),
                    databaseConfiguration.makeJdbsUrlFromConfig(),
                    databaseConfiguration.getDatabaseUser(),
                    databaseConfiguration.getDatabasePassword()),
                params.jdbcMaxPoolSize);
    UuidUtil uuidUtil = new UuidUtil(jdbcSource);
    camelContext.addService(new ParquetService(parquetUtil), true);
    StatusServer statusServer = new StatusServer(params.statusPort);
    // TODO: Improve this `start` signal to make sure every resource after this time are fetched;
    // also handle the case with pre-existing offset file.
    // Note we use UTC because otherwise SQL queries comparing this with DB dates, do not pick the
    // right range.
    statusServer.setVar(
        "start", ZonedDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
    return new FhirConverter(
        fetchUtil,
        fhirStoreUtil,
        parquetUtil,
        params.fhirDebeziumConfigPath,
        uuidUtil,
        statusServer);
  }

  private String getDebeziumConfig() {
    Map<String, String> debeziumConfigs = this.databaseConfiguration.getDebeziumConfigurations();
    return "debezium-mysql:"
        + debeziumConfigs.get("databaseServerName")
        + "?"
        + "databaseHostname="
        + databaseConfiguration.getDatabaseHostName()
        + "&databaseServerId="
        + debeziumConfigs.get("databaseServerId")
        + "&databasePort="
        + databaseConfiguration.getDatabasePort()
        + "&databaseUser="
        + databaseConfiguration.getDatabaseUser()
        + "&databasePassword="
        + databaseConfiguration.getDatabasePassword()
        + "&databaseServerName="
        + debeziumConfigs.get("databaseServerName")
        + "&databaseWhitelist="
        + databaseConfiguration.getDatabaseName()
        + "&offsetStorage=org.apache.kafka.connect.storage.FileOffsetBackingStore"
        + "&offsetStorageFileName="
        + debeziumConfigs.get("databaseOffsetStorage")
        + "&databaseHistoryFileFilename="
        + debeziumConfigs.get("databaseHistory")
        + "&snapshotMode="
        + debeziumConfigs.get("snapshotMode");
  }

  /**
   * The only purpose for this service is to properly close ParquetWriter objects when the pipeline
   * is stopped.
   */
  private static class ParquetService implements Service {

    private ParquetUtil parquetUtil;

    ParquetService(ParquetUtil parquetUtil) {
      this.parquetUtil = parquetUtil;
    }

    @Override
    public void start() {}

    @Override
    public void stop() {
      try {
        parquetUtil.flushAllWritersAndStopTimer();
      } catch (IOException e) {
        log.error("Could not close Parquet file writers properly!");
      }
    }

    @Override
    public void close() {
      stop();
    }
  }

  @Parameters(separators = "=")
  static class DebeziumArgs extends BaseArgs {

    public DebeziumArgs() {}
    ;

    @Parameter(
        names = {"--fhirDebeziumConfigPath"},
        description = "Google cloud FHIR store")
    public String fhirDebeziumConfigPath = "../utils/dbz_event_to_fhir_config.json";

    @Parameter(
        names = {"--jdbcMaxPoolSize"},
        description = "JDBC maximum pool size")
    public int jdbcMaxPoolSize = 50;

    @Parameter(
        names = {"--jdbcInitialPoolSize"},
        description = "JDBC initial pool size")
    public int initialPoolSize = 10;

    @Parameter(
        names = {"--statusPort"},
        description = "The port on which the status server listens.")
    public int statusPort = 9033;
  }
}
