package io.raven.db.dao;

import io.raven.db.utils.Generics;
import org.hibernate.Criteria;
import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;

import java.util.List;

import static java.util.Objects.requireNonNull;


/**
 * An abstract base class for Hibernate DAO classes.
 *
 * @param <E> the class which this DAO manages
 */
public class AbstractDao<E> {

  private final SessionFactory sessionFactory;
  private final Class<?> entityClass;

  /**
   * Creates a new DAO with a given session provider.
   *
   * @param sessionFactory a session provider
   */
  public AbstractDao(SessionFactory sessionFactory) {
    this.sessionFactory = requireNonNull(sessionFactory);
    this.entityClass = Generics.getTypeParameter(getClass());
  }

  /**
   * Returns the entity class managed by this DAO.
   *
   * @return the entity class managed by this DAO
   */
  @SuppressWarnings("unchecked")
  public Class<E> getEntityClass() {
    return (Class<E>) entityClass;
  }

  /**
   * Convenience method to return a single instance that matches the criteria, or null if the
   * criteria returns no results.
   *
   * @param criteria the {@link Criteria} query to run
   * @return the single result or {@code null}
   * @throws HibernateException if there is more than one matching result
   * @see Criteria#uniqueResult()
   */
  @SuppressWarnings("unchecked")
  protected E uniqueResult(Criteria criteria) {
    return (E) requireNonNull(criteria).uniqueResult();
  }

  /**
   * Get the results of a {@link Criteria} query.
   *
   * @param criteria the {@link Criteria} query to run
   * @return the list of matched query results
   * @see Criteria#list()
   */
  @SuppressWarnings("unchecked")
  protected List<E> list(Criteria criteria) {
    return requireNonNull(criteria).list();
  }

  /**
   * Returns the current {@link Session}.
   *
   * @return the current session
   */
  protected Session currentSession() {
    return sessionFactory.getCurrentSession();
  }

  /**
   * Either save or update the given instance, depending upon resolution of the unsaved-value
   * checks (see the manual for discussion of unsaved-value checking).
   * <p/>
   * This operation cascades to associated instances if the association is mapped with
   * <tt>cascade="save-update"</tt>.
   *
   * @param entity a transient or detached instance containing new or updated state
   * @see Session#saveOrUpdate(Object)
   */
  protected E persist(E entity) {
    if (currentSession().contains(entity)) {
      currentSession().refresh(entity);
    }
    currentSession().saveOrUpdate(requireNonNull(entity));
    return entity;
  }
}
