package io.raven.db.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.Generated;
import lombok.NoArgsConstructor;

import java.io.Serializable;


/**
 * Database config for each shard.
 *
 * @author phaneesh
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@Generated
public class JanusConfig implements Serializable {

  private String url;

  private String database;

  private String catalog;

  private String driverClass;

  private String user;

  private String password;

  private String dialect;

  private boolean createSchema;

  @Builder.Default
  private boolean showSql = false;

  @Builder.Default
  private int maxPoolSize = 4;

  @Builder.Default
  private int minPoolSize = 2;

  @Builder.Default
  private int idleTimeout = 55000;

  @Builder.Default
  private String testQuery = "SELECT 1;";

  @Builder.Default
  private int maxAge = 60000;

}