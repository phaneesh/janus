package io.raven.db.dao;

import io.raven.db.config.JanusConfig;
import io.raven.db.utils.SessionFactoryUtil;
import org.hibernate.SessionFactory;
import org.junit.After;

import java.util.List;

public class BaseDaoTest {

  protected SessionFactory sessionFactory;

  private SessionFactoryUtil sessionFactoryUtil;

  protected void setup(List<Class<?>> entities) {
    JanusConfig janusConfig = JanusConfig.builder()
        .createSchema(true)
        .showSql(true)
        .driverClass("org.h2.Driver")
        .dialect("org.hibernate.dialect.H2Dialect")
        .database("db_1")
        .url("jdbc:h2:mem:db_1")
        .user("sa")
        .password("")
        .build();
    sessionFactoryUtil = SessionFactoryUtil.getInstance(janusConfig, entities);
    sessionFactory = sessionFactoryUtil.getSessionFactory();
  }

  @After
  public void after() {
    sessionFactoryUtil.close();
  }

}
