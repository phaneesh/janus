package io.raven.db.dao;

public class DaoException extends RuntimeException {

  public DaoException(final Throwable cause) {
    super(cause);
  }

  public DaoException(final String message) {
    super(message);
  }

  public DaoException(final String message, final Throwable cause) {
    super(message, cause);
  }
}
