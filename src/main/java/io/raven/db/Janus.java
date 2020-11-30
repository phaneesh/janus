package io.raven.db;

import com.google.common.collect.ImmutableList;
import io.raven.db.config.JanusConfig;
import io.raven.db.dao.LookupDao;
import io.raven.db.utils.SessionFactoryUtil;
import lombok.Generated;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.hibernate.SessionFactory;

import java.util.List;


/**
 * A manager that provides wrapper over hibernate.
 */
@Slf4j
public class Janus {

  @Getter
  @Generated
  private SessionFactory sessionFactory;

  private SessionFactoryUtil sessionFactoryUtil;

  /**
   * Default constructor for creating new DbShardingManager.
   * @param janusConfig Sharded database configuration.
   * @param entities Entities to register
   */
  public Janus(JanusConfig janusConfig, Class<?>... entities) {
    val inEntities = ImmutableList.<Class<?>>builder().add(entities).build();
    init(janusConfig, inEntities);
  }

  private void init(final JanusConfig config, final List<Class<?>> inEntities) {
    sessionFactoryUtil = SessionFactoryUtil.getInstance(config, inEntities);
    sessionFactory = sessionFactoryUtil.getSessionFactory();
  }

  public boolean close() {
    sessionFactoryUtil.close();
    return true;
  }

  public <E> LookupDao<E> createParentObjectDao(Class<E> clazz) {
    return new LookupDao<>(sessionFactory, clazz);
  }

}
