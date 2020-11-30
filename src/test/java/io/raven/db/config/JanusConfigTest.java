package io.raven.db.config;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class JanusConfigTest {

  @Test
  public void testDefaultBuilder() {
    JanusConfig factory = JanusConfig.builder()
        .dialect("Test")
        .driverClass("Test")
        .user("sa")
        .database("test")
        .url("test")
        .build();
    assertNotNull(factory.getDatabase());
    assertNotNull(factory.getUrl());
    assertNotNull(factory.getDialect());
    assertNotNull(factory.getDriverClass());
    assertNotNull(factory.getUser());
    assertEquals(2, factory.getMinPoolSize());
    assertEquals(4, factory.getMaxPoolSize());
    assertFalse(factory.isCreateSchema());
    assertFalse(factory.isShowSql());
    assertEquals(55000, factory.getIdleTimeout());
    assertEquals(60000, factory.getMaxAge());
    assertEquals("SELECT 1;", factory.getTestQuery());
    assertNull(factory.getPassword());
    assertNotNull(factory.toString());
    assertTrue(factory.hashCode() != 0);
  }

  @Test
  public void testDefaultConstructor() {
    JanusConfig factory = new JanusConfig();
    assertNull(factory.getDriverClass());
    assertNull(factory.getDialect());
    assertNull(factory.getUser());
    assertEquals(2, factory.getMinPoolSize());
    assertEquals(4, factory.getMaxPoolSize());
    assertFalse(factory.isCreateSchema());
    assertFalse(factory.isShowSql());
    assertEquals(55000, factory.getIdleTimeout());
    assertEquals(60000, factory.getMaxAge());
    assertEquals("SELECT 1;", factory.getTestQuery());
    assertNull(factory.getPassword());
    assertNotNull(factory.toString());
    assertTrue(factory.hashCode() != 0);
  }

}
