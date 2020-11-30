package io.raven.db.utils;

import com.google.common.base.Strings;
import io.raven.db.config.JanusConfig;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.SessionFactory;
import org.hibernate.boot.Metadata;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Environment;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;


/**
 * Database helper to initialize hibernate session factory.
 *
 * @author phaneesh
 */
@Slf4j
public class SessionFactoryUtil {

  private SessionFactory sessionFactory;

  private final JanusConfig janusConfig;

  private final List<Class<?>> entities;

  private SessionFactoryUtil(final JanusConfig janusConfig, final List<Class<?>> entities) {
    this.janusConfig = janusConfig;
    this.entities = entities;
  }

  public static SessionFactoryUtil getInstance(JanusConfig janusConfig, List<Class<?>> entities) {
    return new SessionFactoryUtil(janusConfig, entities);
  }

  /**
   * Get the list of session factories sharded database.
   *
   * @return list of {@link SessionFactory}
   */
  public synchronized SessionFactory getSessionFactory() {
    final StandardServiceRegistryBuilder registryBuilder = new StandardServiceRegistryBuilder();
    Map<String, Object> settings = new HashMap<>();
    settings.put(Environment.HBM2DDL_AUTO,
        janusConfig.isCreateSchema() ? "create-drop" : "none");
    settings.put(Environment.SHOW_SQL, janusConfig.isShowSql());
    settings.put(Environment.CURRENT_SESSION_CONTEXT_CLASS, "managed");
    settings.put(Environment.DIALECT, janusConfig.getDialect());
    settings.put(Environment.URL, janusConfig.getUrl());
    if (!Strings.isNullOrEmpty(janusConfig.getUser())) {
      settings.put(Environment.USER, janusConfig.getUser());
    }
    if (!Strings.isNullOrEmpty(janusConfig.getPassword())) {
      settings.put(Environment.PASS, janusConfig.getPassword());
    }
    if (Objects.nonNull(janusConfig.getCatalog())) {
      settings.put(Environment.DEFAULT_CATALOG, janusConfig.getCatalog());
    }
    settings.put("hibernate.connection.schema", janusConfig.getDatabase());
    settings.put(Environment.DRIVER, janusConfig.getDriverClass());
    // HikariCP settings
    // Maximum waiting time for a connection from the pool
    settings.put("hibernate.hikari.connectionTimeout", "20000");
    // Minimum number of ideal connections in the pool
    settings.put("hibernate.hikari.minimumIdle", String.valueOf(janusConfig.getMinPoolSize()));
    // Maximum number of actual connection in the pool
    settings.put("hibernate.hikari.maximumPoolSize", String.valueOf(janusConfig.getMaxPoolSize()));
    // Maximum time that a connection is allowed to sit ideal in the pool
    settings.put("hibernate.hikari.idleTimeout", String.valueOf(janusConfig.getIdleTimeout()));
    //Test query to validate the connection
    if (Objects.nonNull(janusConfig.getTestQuery())) {
      settings.put("hibernate.hikari.connectionTestQuery", janusConfig.getTestQuery());
    }
    //Turn off auto commit
    settings.put("hibernate.hikari.autoCommit", "false");
    //Set Max age for connections
    settings.put("hibernate.hikari.maxLifetime", String.valueOf(janusConfig.getMaxAge()));
    settings.put("hibernate.temp.use_jdbc_metadata_defaults", "false");
    settings.put(Environment.USE_QUERY_CACHE, "false");
    settings.put("hibernate.cache.provider_class", "org.hibernate.cache.NoCacheProvider");
    settings.put("hibernate.cache.use_minimal_puts", "false");
    settings.put("max_fetch_depth", "3");

    registryBuilder.applySettings(settings);
    StandardServiceRegistry registry = registryBuilder.build();
    MetadataSources sources = new MetadataSources(registry);
    entities.forEach(sources::addAnnotatedClass);
    Metadata metadata = sources.getMetadataBuilder()
        .build();
    sessionFactory = metadata.getSessionFactoryBuilder()
        .applyStatisticsSupport(false)
        .build();
    return sessionFactory;
  }

  public void close() {
    sessionFactory.close();
  }

}
