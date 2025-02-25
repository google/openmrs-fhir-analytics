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

import com.google.auto.value.AutoValue;
import com.google.fhir.analytics.model.DatabaseConfiguration;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import java.beans.PropertyVetoException;
import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.sql.DataSource;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a Singleton that is intended for managing all JDBC connection pools. It guarantees that
 * for each DB config, we only have one pool per JVM.
 */
public class JdbcConnectionPools {
  private static final Logger log = LoggerFactory.getLogger(JdbcConnectionPools.class);

  static final int MIN_CONNECTIONS = 3;

  @Nullable private static JdbcConnectionPools instance = null;

  private final ConcurrentMap<DataSourceConfig, DataSource> dataSources = new ConcurrentHashMap<>();

  // This class should not be instantiated!
  private JdbcConnectionPools() {}

  public static synchronized JdbcConnectionPools getInstance() {
    if (instance == null) {
      instance = new JdbcConnectionPools();
    }
    return instance;
  }

  /**
   * Creates a new connection pool for the given config or return an already created one if one
   * exists. The pool size parameters are just hints and won't be used if a pool already exists for
   * the given configuration. This method is to impose a Singleton pattern for each config.
   *
   * <p>Note in the context of our Beam pipelines, we only serialize `DataSourceConfig` objects and
   * create `DataSource` from them when necessary; i.e., do not serialize the returned `DataSource`
   * in our code. Some Beam libraries like JdbcIO may serialize `DataSource` but they should
   * properly handle the singleton pattern needed for pooled data-sources.
   *
   * @param config the JDBC connection information
   * @param jdbcMaxPoolSize maximum pool size if a new pool is created.
   * @return a pooling DataSource for the given DB config
   */
  public DataSource getPooledDataSource(DataSourceConfig config, int jdbcMaxPoolSize) {
    dataSources.computeIfAbsent(config, c -> createNewPool(c, jdbcMaxPoolSize));
    return dataSources.get(config);
  }

  private static DataSource createNewPool(DataSourceConfig config, int jdbcMaxPoolSize) {
    log.info(
        "Creating a JDBC connection pool for "
            + config.jdbcUrl()
            + " with driver class "
            + config.jdbcDriverClass()
            + " and max pool size "
            + jdbcMaxPoolSize);
    // Note caching of these connection-pools is important beyond just performance benefits. If a
    // `ComboPooledDataSource` goes out of scope without calling `close()` on it, then it can leak
    // connections (and memory) as its threads are not killed and can hold those objects; this was
    // the case even with `setNumHelperThreads(0)`.
    ComboPooledDataSource comboPooledDataSource = new ComboPooledDataSource();
    try {
      comboPooledDataSource.setDriverClass(config.jdbcDriverClass());
    } catch (PropertyVetoException e) {
      String errorMes = "Error in setting the JDBC driver class " + config.jdbcDriverClass();
      log.error(errorMes);
      throw new IllegalArgumentException(errorMes, e);
    }
    comboPooledDataSource.setJdbcUrl(config.jdbcUrl());
    comboPooledDataSource.setUser(config.dbUser());
    comboPooledDataSource.setPassword(config.dbPassword());
    comboPooledDataSource.setMaxPoolSize(Math.max(MIN_CONNECTIONS, jdbcMaxPoolSize));
    if (MIN_CONNECTIONS > jdbcMaxPoolSize) {
      log.warn(
          "The requested max pool size {} is less than {}; adjusting max pool size to {}.",
          jdbcMaxPoolSize,
          MIN_CONNECTIONS,
          MIN_CONNECTIONS);
    }
    comboPooledDataSource.setMinPoolSize(MIN_CONNECTIONS);
    // Setting an idle time to reduce the number of connections when idle; avoid setting
    // maxIdleTime! Instead, use connection testing as done below; see:
    // https://www.mchange.com/projects/c3p0/#managing_pool_size
    comboPooledDataSource.setMaxIdleTimeExcessConnections(30);
    // See https://www.mchange.com/projects/c3p0/#configuring_connection_testing
    comboPooledDataSource.setIdleConnectionTestPeriod(40);
    comboPooledDataSource.setTestConnectionOnCheckin(true);
    return comboPooledDataSource;
  }

  // TODO we should `close()` connection pools on application shutdown.

  public static DataSourceConfig dbConfigToDataSourceConfig(DatabaseConfiguration config) {
    return DataSourceConfig.create(
        config.getJdbcDriverClass(),
        config.makeJdbsUrlFromConfig(),
        config.getDatabaseUser(),
        config.getDatabasePassword());
  }

  /**
   * This is to identify a database connection pool. We use instances of these to cache connection
   * pools and to impose a Singleton pattern per connection config (hence AutoValue).
   */
  @AutoValue
  public abstract static class DataSourceConfig implements Serializable {
    abstract String jdbcDriverClass();

    abstract String jdbcUrl();

    abstract String dbUser();

    abstract String dbPassword();

    static DataSourceConfig create(
        String jdbcDriverClass, String jdbcUrl, String dbUser, String dbPassword) {
      return new AutoValue_JdbcConnectionPools_DataSourceConfig(
          jdbcDriverClass, jdbcUrl, dbUser, dbPassword);
    }
  }
}
