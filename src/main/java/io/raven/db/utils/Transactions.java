package io.raven.db.utils;

import org.hibernate.SessionFactory;

import java.util.function.Function;


/**
 * Utility functional class for running transactions.
 */
public interface Transactions {

  static <T, U> T execute(SessionFactory sessionFactory, boolean readOnly, Function<U, T> function, U arg) {
    return execute(sessionFactory, readOnly, function, arg, t -> t);
  }

  static <T, U, V> V execute(SessionFactory sessionFactory, boolean readOnly,
                             Function<U, T> function, U arg, Function<T, V> handler) {
    return execute(sessionFactory, readOnly, function, arg, handler, true);
  }

  static <T, U, V> V execute(SessionFactory sessionFactory, boolean readOnly,
                             Function<U, T> function, U arg, Function<T, V> handler,
                             boolean completeTransaction) {
    TransactionHandler transactionHandler = new TransactionHandler(sessionFactory, readOnly);
    if (completeTransaction) {
      transactionHandler.beforeStart();
    }
    try {
      T result = function.apply(arg);
      V returnValue = handler.apply(result);
      if (completeTransaction) {
        transactionHandler.afterEnd();
      }
      return returnValue;
    } catch (Exception e) {
      if (completeTransaction) {
        transactionHandler.onError(e);
      }
      throw e;
    }
  }
}
