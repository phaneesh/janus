package io.raven.db;

import io.raven.db.config.JanusConfig;
import io.raven.db.dao.LookupDao;
import io.raven.db.dao.testdata.entities.TestEntity;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class JanusTest {

  private Janus dbManager;

  @Before
  public void before() {
    JanusConfig janusConfig = JanusConfig.builder()
        .createSchema(true)
        .showSql(true)
        .driverClass("org.h2.Driver")
        .dialect("org.hibernate.dialect.H2Dialect")
        .user("sa")
        .password("")
        .database("db_1")
        .url("jdbc:h2:mem:db_1")
        .build();

    dbManager = new Janus(janusConfig, TestEntity.class);
  }

  @Test
  public void testInit() {
    assertNotNull(dbManager.getSessionFactory());
  }

  @Test
  public void testCreateParentObjectDao() {
    LookupDao<TestEntity> lookupDao = dbManager.createParentObjectDao(TestEntity.class);
    assertNotNull(lookupDao);
  }

  @Test
  public void testClose() {
    assertTrue(dbManager.close());
  }
}
