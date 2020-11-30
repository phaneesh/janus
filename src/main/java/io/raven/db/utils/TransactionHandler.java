package io.raven.db.utils;

import org.hibernate.CacheMode;
import org.hibernate.FlushMode;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.context.internal.ManagedSessionContext;
import org.hibernate.exception.ConstraintViolationException;
import org.hibernate.resource.transaction.spi.TransactionStatus;

/**
 * A transaction handler utility class.
 */
public class TransactionHandler {


  private final SessionFactory sessionFactory;
  // Context variables
  private Session session;
  private final boolean readOnly;

  public TransactionHandler(SessionFactory sessionFactory, boolean readOnly) {
    this.sessionFactory = sessionFactory;
    this.readOnly = readOnly;
  }

  public void beforeStart() {
    session = sessionFactory.openSession();
    try {
      configureSession();
      ManagedSessionContext.bind(session);
      beginTransaction();
    } catch (Throwable th) {
      session.close();
      session = null;
      ManagedSessionContext.unbind(sessionFactory);
      throw th;
    }
  }

  private void configureSession() {
    session.setDefaultReadOnly(readOnly);
    session.setCacheMode(CacheMode.NORMAL);
    session.setHibernateFlushMode(FlushMode.AUTO);
  }

  private void beginTransaction() {
    session.beginTransaction();
  }

  public void afterEnd() {
    if (session == null) {
      return;
    }

    try {
      commitTransaction();
    } catch (Exception e) {
      rollbackTransaction();
      throw e;
    } finally {
      session.close();
      session = null;
      ManagedSessionContext.unbind(sessionFactory);
    }

  }

  private void commitTransaction() {
    final Transaction txn = session.getTransaction();
    if (txn != null && txn.getStatus() == TransactionStatus.ACTIVE) {
      txn.commit();
    }
  }

  private void rollbackTransaction() {
    final Transaction txn = session.getTransaction();
    if (txn != null && txn.getStatus() == TransactionStatus.ACTIVE) {
      txn.rollback();
    }
  }

  public void onError(Exception e) {
    if (session == null) {
      return;
    }
    try {
      if (!(e instanceof ConstraintViolationException)) {
        rollbackTransaction();
      }
    } finally {
      session.close();
      session = null;
      ManagedSessionContext.unbind(sessionFactory);
    }
  }
}
