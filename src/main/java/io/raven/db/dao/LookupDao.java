package io.raven.db.dao;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import io.raven.db.annotations.LookupKey;
import io.raven.db.utils.TransactionHandler;
import io.raven.db.utils.Transactions;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.hibernate.Criteria;
import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.MultiIdentifierLoadAccess;
import org.hibernate.SessionFactory;
import org.hibernate.criterion.DetachedCriteria;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Restrictions;
import org.hibernate.query.Query;

import javax.persistence.Id;
import javax.persistence.criteria.CriteriaUpdate;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

@Slf4j
public class LookupDao<T> {


  private final Class<T> entityClass;
  private final Field idField;
  private final Field keyField;

  @VisibleForTesting
  private final InternalDao dao;

  /**
   * Creates a new Lookup DAO.
   *
   * @param sessionFactory a session provider for each shard
   */
  public LookupDao(SessionFactory sessionFactory, Class<T> entityClass) {
    this.dao = new InternalDao(sessionFactory);
    this.entityClass = entityClass;
    Field[] lookupFields = FieldUtils.getFieldsWithAnnotation(entityClass, LookupKey.class);
    Field[] idFields = FieldUtils.getFieldsWithAnnotation(entityClass, Id.class);
    Preconditions.checkArgument(lookupFields.length != 0, "At least one field needs to be lookup keys");
    Preconditions.checkArgument(lookupFields.length == 1, "Only one field can be lookup keys");
    Preconditions.checkArgument(idFields.length != 0, "At least one field needs to be a key");
    Preconditions.checkArgument(idFields.length == 1, "Only one field can be a key");
    keyField = lookupFields[0];
    Preconditions.checkArgument(ClassUtils.isAssignable(keyField.getType(), String.class), "Lookup Key field must be a string");
    if (!keyField.trySetAccessible()) {
      log.error("Error making LookupKey field accessible please use a public method and mark that as LookupKey");
      throw new IllegalArgumentException("Invalid class, DAO cannot be created. LookupKey is not accessible");
    }
    idField = idFields[0];
    Preconditions.checkArgument(ClassUtils.isAssignable(idField.getType(), Long.class), "Key field must be a Long");
    if (!keyField.trySetAccessible()) {
      log.error("Error making Key field accessible please use a public method and mark that as Key");
      throw new IllegalArgumentException("Invalid class, DAO cannot be created. Key is not accessible");
    }
  }

  public Class<T> getEntityClass() {
    return dao.getEntityClass();
  }

  public Optional<T> get(Long id) throws Exception {
    return get(id, t -> t);
  }

  public <U> Optional<U> get(Long id, Function<T, U> handler) throws Exception {
    U result = Transactions.execute(dao.sessionFactory, true, dao::get, id, handler);
    return Optional.ofNullable(result);
  }

  public List<T> get(List<Long> ids) throws Exception {
    return get(ids, e -> e);
  }

  public <U> List<U> get(List<Long> ids, Function<List<T>, List<U>> handler) throws Exception {
    return Transactions.execute(dao.sessionFactory, true, dao::get, ids, handler);
  }

  public Optional<T> lookup(String key) throws Exception {
    return lookup(key, e -> e);
  }

  public <U> Optional<U> lookup(String key, Function<T, U> handler) throws Exception {
    DetachedCriteria criteria = DetachedCriteria.forClass(entityClass)
        .add(Restrictions.eq(keyField.getName(), key));
    U result = Transactions.execute(dao.sessionFactory, true, dao::selectSingle, criteria, handler);
    return Optional.ofNullable(result);
  }

  public <U> List<U> lookupMulti(String key, Function<List<T>, List<U>> handler) throws Exception {
    DetachedCriteria criteria = DetachedCriteria.forClass(entityClass)
        .add(Restrictions.eq(keyField.getName(), key));
    return Transactions.execute(dao.sessionFactory, true, dao::select, criteria, handler);
  }

  public List<T> lookupMulti(String key) throws Exception {
    return lookupMulti(key, t -> t);
  }

  public List<T> get(DetachedCriteria criteria) throws Exception {
    return Transactions.execute(dao.sessionFactory, true, dao::select, criteria, e -> e);
  }

  public <U> List<U> get(DetachedCriteria criteria, Function<List<T>, List<U>> handler) throws Exception {
    return Transactions.execute(dao.sessionFactory, true, dao::select, criteria, handler);
  }

  public boolean exists(Long id) throws Exception {
    DetachedCriteria criteria = DetachedCriteria.forClass(entityClass)
        .add(Restrictions.eq(idField.getName(), id))
        .setProjection(Projections.property(idField.getName()));
    T result = Transactions.execute(dao.sessionFactory, true, dao::selectSingle, criteria, e -> e);
    return Objects.nonNull(result);
  }

  public boolean exists(String key) throws Exception {
    DetachedCriteria criteria = DetachedCriteria.forClass(entityClass)
        .add(Restrictions.eq(keyField.getName(), key))
        .setProjection(Projections.property(idField.getName()));
    T result = Transactions.execute(dao.sessionFactory, true, dao::selectSingle, criteria, e -> e);
    return Objects.nonNull(result);
  }

  public <N extends Number> N max(final DetachedCriteria criteria, final String propertyName) throws Exception {
    return Transactions.execute(dao.sessionFactory, true, dao::max, AggregateParams.builder()
        .criteria(criteria)
        .propertyName(propertyName).build());
  }

  public <N extends Number> N min(final DetachedCriteria criteria, final String propertyName) throws Exception {
    return Transactions.execute(dao.sessionFactory, true, dao::min, AggregateParams.builder()
        .criteria(criteria)
        .propertyName(propertyName).build());
  }

  public <U> U save(T entity, Function<T, U> handler) throws Exception {
    return Transactions.execute(dao.sessionFactory, false, dao::save, entity, handler);
  }

  public Optional<T> save(T entity) throws Exception {
    return Optional.ofNullable(save(entity, t -> t));
  }

  public <U> List<U> save(List<T> entities, Function<List<T>, List<U>> handler) throws Exception {
    return Transactions.execute(dao.sessionFactory, false, dao::save, entities, handler);
  }

  public List<T> save(List<T> entities) throws Exception {
    return save(entities, t -> t);
  }

  public boolean updateInLock(Long id, Function<Optional<T>, T> updater) {
    return updateImpl(id, dao::getLockedForWrite, updater, dao);
  }

  private boolean updateImpl(Long id, Function<Long, T> getter, Function<Optional<T>, T> updater, InternalDao dao) {
    try {
      return Transactions.<T, Long, Boolean>execute(dao.sessionFactory, false, getter, id, entity -> {
        if (null == entity) {
          return false;
        }
        T newEntity = updater.apply(Optional.of(entity));
        if (null == newEntity) {
          return false;
        }
        dao.update(newEntity);
        return true;
      });
    } catch (Exception e) {
      throw new DaoException("Error updating entity: " + id, e);
    }
  }

  public boolean update(Long id, Function<Optional<T>, T> updater) {
    return updateImpl(id, dao::get, updater, dao);
  }

  public int update(String query, Map<String, Object> params) throws Exception {
    return Transactions.execute(dao.sessionFactory, false, dao::update, QueryParams.builder()
        .params(params)
        .query(query)
        .nativeQuery(false)
        .build());
  }

  public int updateNative(String query, Map<String, Object> params) throws Exception {
    return Transactions.execute(dao.sessionFactory, false, dao::update, QueryParams.builder()
        .params(params)
        .query(query)
        .nativeQuery(true)
        .build());
  }

  public LockedContext<T> lockAndGetExecutor(Long id) {
    return new LockedContext<T>(dao.sessionFactory, dao::getLockedForWrite, id);
  }

  public BatchLockedContext<T> lockAndGetExecutor(List<Long> ids) {
    return new BatchLockedContext<>(dao.sessionFactory, dao::getLockedForWrite, ids, true);
  }

  public BatchLockedContext<T> lockAndGetExecutor(Supplier<List<Long>> supplier) {
    return lockAndGetExecutor(supplier.get());
  }

  public BatchLockedContext<T> saveAndGetExecutor(List<T> entities) {
    return new BatchLockedContext<>(dao.sessionFactory, dao::save, entities);
  }

  public LockedContext<T> saveAndGetExecutor(T entity) {
    return new LockedContext<>(dao.sessionFactory, dao::save, entity);
  }

  public <N extends Number> N sum(final DetachedCriteria criteria, final String propertyName) throws Exception {
    return Transactions.execute(dao.sessionFactory, true, dao::sum, AggregateParams
    .builder()
    .criteria(criteria)
    .propertyName(propertyName)
    .build());
  }

  public long count(DetachedCriteria criteria) {
    try {
      criteria.setProjection(Projections.property(idField.getName()));
      return Transactions.execute(dao.sessionFactory, true, dao::count, criteria);
    } catch (Exception e) {
      throw new DaoException(e);
    }
  }

  public List<T> select(DetachedCriteria detachedCriteria, int limit, int offset) throws Exception {
    return select(detachedCriteria, limit, offset, ts -> ts);
  }

  public List<T> select(final String query, final Map<String, Object> params, final boolean nativeQuery) throws Exception {
    QueryParams queryParams = QueryParams.builder()
        .query(query)
        .params(params)
        .nativeQuery(nativeQuery)
        .build();
    return Transactions.execute(dao.sessionFactory, true, dao::select, queryParams);
  }

  public <U> List<U> select(DetachedCriteria detachedCriteria, int limit, int offset, Function<List<T>, List<U>> handler) throws Exception {
    CriteriaParams params = CriteriaParams.builder()
        .criteria(detachedCriteria)
        .limit(limit)
        .offset(offset)
        .build();
    try {
      return Transactions.execute(dao.sessionFactory, true, dao::select, params, handler);
    } catch (Exception e) {
      throw new DaoException(e);
    }
  }

  public <U> List<U> selectPaginated(DetachedCriteria criteria, Function<List<T>, List<U>> handler, int pageSize) {
    try {
      CriteriaParams params = CriteriaParams.builder()
          .criteria(criteria)
          .limit(pageSize)
          .build();
      return Transactions.execute(dao.sessionFactory, true, dao::selectPaginated, params, handler);
    } catch (Exception e) {
      throw new DaoException(e);
    }
  }

  public List<T> select(DetachedCriteria detachedCriteria) throws Exception {
    try {
      return Transactions.execute(dao.sessionFactory, true, dao::select, detachedCriteria, t -> t);
    } catch (Exception e) {
      throw new DaoException(e);
    }
  }

  public <U> List<U> select(DetachedCriteria detachedCriteria, Function<List<T>, List<U>> handler) throws Exception {
    try {
      return Transactions.execute(dao.sessionFactory, true, dao::select, detachedCriteria, handler);
    } catch (Exception e) {
      throw new DaoException(e);
    }
  }

  public <U> Optional<U> selectSingle(DetachedCriteria detachedCriteria, Function<T, U> handler) throws Exception {
    U result = Transactions.execute(dao.sessionFactory, true, dao::selectSingle, detachedCriteria, handler);
    return Optional.ofNullable(result);
  }

  protected Field getKeyField() {
    return this.keyField;
  }

  protected Field getIdField() {
    return this.idField;
  }

  @Data
  @Builder
  private static class CriteriaParams {

    private DetachedCriteria criteria;

    @Builder.Default
    private int limit = -1;

    @Builder.Default
    private int offset = -1;

    private String fetchProfile;
  }

  @Data
  @Builder
  private static class QueryParams {

    private String query;

    private Map<String, Object> params;

    private boolean nativeQuery;
  }

  @Data
  @Builder
  private static class AggregateParams {

    private DetachedCriteria criteria;

    private String propertyName;
  }

  @Getter
  public static class LockedContext<T> {

    private final SessionFactory sessionFactory;
    private final Mode mode;
    private Function<Long, T> function;
    private Function<T, T> saver;
    private T entity;
    private Long key;
    private List<Function<T, Void>> operations = Lists.newArrayList();

    public LockedContext(SessionFactory sessionFactory, Function<Long, T> getter, Long key) {
      this.sessionFactory = sessionFactory;
      this.function = getter;
      this.key = key;
      this.mode = Mode.READ;
    }

    public LockedContext(SessionFactory sessionFactory, Function<T, T> saver, T entity) {
      this.sessionFactory = sessionFactory;
      this.saver = saver;
      this.entity = entity;
      this.mode = Mode.INSERT;
    }

    public LockedContext<T> mutate(Mutator<T> mutator) {
      return apply(parent -> {
        mutator.mutator(parent);
        return null;
      });
    }

    public LockedContext<T> apply(Function<T, Void> handler) {
      this.operations.add(handler);
      return this;
    }

    public <U> LockedContext<T> save(LookupDao<U> lookupDao, Function<T, U> entityGenerator) {
      return apply(parent -> {
        try {
          U applied = entityGenerator.apply(parent);
          lookupDao.save(applied);
        } catch (Exception e) {
          throw new DaoException(e);
        }
        return null;
      });
    }

    public <U> LockedContext<T> saveAll(LookupDao<U> lookupDao, Function<T, List<U>> entityGenerator) {
      return apply(parent -> {
        try {
          List<U> entities = entityGenerator.apply(parent);
          for (U e : entities) {
            lookupDao.save(e);
          }
        } catch (Exception e) {
          throw new DaoException(e);
        }
        return null;
      });
    }

    public <U> LockedContext<T> update(LookupDao<U> lookupDao, Long id, Function<Optional<U>, U> handler) {
      return apply(parent -> {
        try {
          lookupDao.update(id, handler);
        } catch (Exception e) {
          throw new DaoException(e);
        }
        return null;
      });
    }

    public <U> LockedContext<T> update(LookupDao<U> lookupDao, String query, Map<String, Object> params) {
      return apply(parent -> {
        try {
          int result = lookupDao.update(query, params);
          if (result < 1)
            throw new DaoException("Update operation returned result " + result);
        } catch (Exception e) {
          throw new DaoException(e);
        }
        return null;
      });
    }

    public LockedContext<T> filter(Predicate<T> predicate) {
      return filter(predicate, new IllegalArgumentException("Predicate check failed"));
    }

    public LockedContext<T> filter(Predicate<T> predicate, RuntimeException failureException) {
      return apply(parent -> {
        boolean result = predicate.test(parent);
        if (!result) {
          throw failureException;
        }
        return null;
      });
    }

    public T execute() {
      TransactionHandler transactionHandler = new TransactionHandler(sessionFactory, false);
      transactionHandler.beforeStart();
      try {
        T result = generateEntity();
        operations
            .forEach(operation -> operation.apply(result));
        return result;
      } catch (Exception e) {
        transactionHandler.onError(e);
        throw e;
      } finally {
        transactionHandler.afterEnd();
      }
    }

    private T generateEntity() {
      T result = null;
      switch (mode) {
        case READ:
          result = function.apply(key);
          if (result == null) {
            throw new DaoException("Entity doesn't exist for keys: " + key);
          }
          break;
        case INSERT:
          result = saver.apply(entity);
          break;
        default:
          break;
      }
      return result;
    }

    enum Mode {READ, INSERT}

    @FunctionalInterface
    public interface Mutator<T> {
      void mutator(T parent);
    }
  }

  @Getter
  public static class BatchLockedContext<T> {
    private final SessionFactory sessionFactory;
    private final Mode mode;
    private Function<List<Long>, List<T>> function;
    private Function<List<T>, List<T>> saver;
    private List<T> entity;
    private List<Long> keys;
    private List<Function<List<T>, Void>> operations = Lists.newArrayList();

    public BatchLockedContext(SessionFactory sessionFactory, Function<List<Long>, List<T>> getter, List<Long> keys, boolean read) {
      this.sessionFactory = sessionFactory;
      this.function = getter;
      this.keys = keys;
      this.mode = Mode.READ;
    }

    public BatchLockedContext(SessionFactory sessionFactory, Function<List<T>, List<T>> saver, List<T> entity) {
      this.sessionFactory = sessionFactory;
      this.saver = saver;
      this.entity = entity;
      this.mode = Mode.INSERT;
    }

    public BatchLockedContext<T> mutate(Mutator<T> mutator) {
      return apply(parent -> {
        mutator.mutator(parent);
        return null;
      });
    }

    public BatchLockedContext<T> apply(Function<List<T>, Void> handler) {
      this.operations.add(handler);
      return this;
    }

    public <U> BatchLockedContext<T> saveAll(LookupDao<U> lookupDao, Function<List<T>, List<U>> entityGenerator, BiFunction<List<U>, List<T>, Void> postPersistHandler) {
      return apply(parent -> {
        try {
          List<U> entities = entityGenerator.apply(parent);
          for (U entity : entities) {
            lookupDao.save(entity);
          }
          postPersistHandler.apply(entities, parent);
        } catch (Exception e) {
          throw new DaoException(e);
        }
        return null;
      });
    }

    public <U> BatchLockedContext<T> saveSingle(LookupDao<U> lookupDao, Function<List<T>, Optional<U>> entityGenerator, BiFunction<Optional<U>, List<T>, Void> postPersistHandler) {
      return apply(parent -> {
        try {
          Optional<U> entity = entityGenerator.apply(parent);
          if (entity.isPresent()) {
            lookupDao.save(entity.get());
          }
          postPersistHandler.apply(entity, parent);
        } catch (Exception e) {
          throw new DaoException(e);
        }
        return null;
      });
    }

    public List<T> execute() {
      TransactionHandler transactionHandler = new TransactionHandler(sessionFactory, false);
      transactionHandler.beforeStart();
      try {
        List<T> result = generateEntity();
        operations
            .forEach(operation -> operation.apply(result));
        return result;
      } catch (Exception e) {
        transactionHandler.onError(e);
        throw e;
      } finally {
        transactionHandler.afterEnd();
      }
    }

    private List<T> generateEntity() {
      List<T> result = null;
      switch (mode) {
        case READ:
          result = function.apply(keys);
          if (result == null) {
            throw new DaoException("Entity doesn't exist for keys: " + keys);
          }
          break;
        case INSERT:
          result = saver.apply(entity);
          break;
        default:
          break;

      }
      return result;
    }

    enum Mode {READ, INSERT}

    @FunctionalInterface
    public interface Mutator<T> {
      void mutator(List<T> parent);
    }
  }

  private final class InternalDao extends AbstractDao<T> {

    private final SessionFactory sessionFactory;

    public InternalDao(SessionFactory sessionFactory) {
      super(sessionFactory);
      this.sessionFactory = sessionFactory;
    }

    T get(Long id) {
      return getLocked(id, LockMode.READ);
    }

    T getLocked(Long id, LockMode lockMode) {
      return currentSession().get(entityClass, id, lockMode);
    }

    List<T> get(List<Long> ids) {
      return getLocked(ids, LockMode.READ);
    }

    List<T> getLocked(List<Long> ids, LockMode lockMode) {
      MultiIdentifierLoadAccess<T> multiGet = currentSession().byMultipleIds(entityClass);
      return multiGet.with(new LockOptions(lockMode)).multiLoad(ids);
    }

    T getLockedForWrite(Long id) {
      return getLocked(id, LockMode.UPGRADE_NOWAIT);
    }

    List<T> getLockedForWrite(List<Long> ids) {
      return getLocked(ids, LockMode.UPGRADE_NOWAIT);
    }

    T save(T entity) {
      return persist(entity);
    }

    List<T> save(List<T> entities) {
      List<T> saved = new ArrayList<>();
      for (T e : entities) {
        saved.add(persist(e));
      }
      return saved;
    }

    void update(T entity) {
      currentSession().evict(entity); //Detach .. otherwise update is a no-op
      currentSession().update(entity);
    }

    long update(CriteriaUpdate<T> criteriaUpdate) {
      return currentSession().createQuery(criteriaUpdate).executeUpdate();
    }

    List<T> select(DetachedCriteria criteria) {
      return list(criteria.getExecutableCriteria(currentSession()));
    }

    public List<T> select(final QueryParams queryParams) {
      Query<T> tQuery = currentSession().createQuery(queryParams.query, entityClass);
      if (queryParams.params != null)
        queryParams.params.forEach(tQuery::setParameter);
      return tQuery.getResultList();
    }

    public List<T> select(CriteriaParams criteriaParams) {
      Criteria exeCriteria = criteriaParams.criteria.getExecutableCriteria(currentSession());
      if (criteriaParams.limit != -1)
        exeCriteria.setMaxResults(criteriaParams.limit);
      if (criteriaParams.offset != -1)
        exeCriteria.setFirstResult(criteriaParams.offset);
      if (!Strings.isNullOrEmpty(criteriaParams.fetchProfile)) {
        currentSession().enableFetchProfile(criteriaParams.fetchProfile);
      }
      return list(exeCriteria);
    }

    public List<T> selectPaginated(CriteriaParams criteriaParams) {
      List<T> result = new ArrayList<>();
      boolean run = true;
      int offset = 0;
      while (run) {
        Criteria exeCriteria = criteriaParams.criteria.getExecutableCriteria(currentSession());
        exeCriteria.setMaxResults(criteriaParams.limit);
        exeCriteria.setFirstResult(offset);
        List<T> batchResult = list(exeCriteria);
        result.addAll(batchResult);
        offset += criteriaParams.limit;
        run = !batchResult.isEmpty();
      }
      return result;
    }

    public T selectSingle(DetachedCriteria criteria) {
      return uniqueResult(criteria.getExecutableCriteria(currentSession()));
    }

    long count(DetachedCriteria criteria) {
      return (long) criteria.getExecutableCriteria(currentSession())
          .setProjection(Projections.rowCount())
          .uniqueResult();
    }

    <N extends Number> N sum(AggregateParams aggregateParams) {
      return (N) aggregateParams.criteria.getExecutableCriteria(currentSession())
          .setProjection(Projections.sum(aggregateParams.propertyName))
          .uniqueResult();
    }

    <N extends Number> N max(AggregateParams aggregateParams) {
      return (N) aggregateParams.criteria.getExecutableCriteria(currentSession())
          .setProjection(Projections.max(aggregateParams.propertyName))
          .uniqueResult();
    }

    <N extends Number> N min(AggregateParams aggregateParams) {
      return (N) aggregateParams.criteria.getExecutableCriteria(currentSession())
          .setProjection(Projections.min(aggregateParams.propertyName))
          .uniqueResult();
    }

    public int update(QueryParams updateParams) {
      Query<T> tQuery;
      if (updateParams.nativeQuery) {
        tQuery = currentSession().createSQLQuery(updateParams.query);
      } else {
        tQuery = currentSession().createQuery(updateParams.query);
      }
      updateParams.params.forEach(tQuery::setParameter);
      return tQuery.executeUpdate();
    }
  }
}
