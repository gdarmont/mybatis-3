package org.apache.ibatis.executor.resultset;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.ibatis.mapping.ResultMap;
import org.apache.ibatis.session.ResultContext;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;

/**
 * @author GDA / GD06186S
 */
public class LazyList<E> extends AbstractList<E> {

	// ResultSetHandler stuff
	private final FastResultSetHandler resultSetHandler;
	private final ResultSet rs;
	private final ResultMap resultMap;
	private final FastResultSetHandler.ResultColumnCache resultColumnCache;
	private final ObjectWrapperResultHandler<E> objectWrapperResultHandler = new ObjectWrapperResultHandler<E>();

	private final ArrayList<E> storage = new ArrayList<E>();

  private boolean resultSetExhausted = false;

  public LazyList(FastResultSetHandler resultSetHandler, ResultSet rs, ResultMap resultMap,
          FastResultSetHandler.ResultColumnCache resultColumnCache) {
    this.resultSetHandler = resultSetHandler;
    this.rs = rs;
		this.resultMap = resultMap;
		this.resultColumnCache = resultColumnCache;
	}

	@Override
	public E get(int index) {
		return storage.get(index);
	}

	@Override
	public int size() {
    if (resultSetExhausted) {
      return storage.size();
    }
    throw new UnsupportedOperationException("Cannot retrieve size of a partially fetched lazy list");
  }

  private void closeResultSetAndStatement() {
    try {
      if (rs != null) {
        Statement statement = rs.getStatement();

        rs.close();
        if (statement != null) {
          statement.close();
        }
      }
    } catch (SQLException e) {
      // ignore
    }
  }

  @Override
  public Iterator<E> iterator() {
    return new LazyIterator();
  }

  private static class ObjectWrapperResultHandler<E> implements ResultHandler {

		private E result;

		@Override
		public void handleResult(ResultContext context) {
			this.result = (E) context.getResultObject();
    }
  }

  private class LazyIterator implements Iterator<E> {

    /**
     * Index of next element to be returned...
     */
    int cursor = 0;

    E object;

    @Override
    public boolean hasNext() {
      boolean hasNext = false;

      boolean isAvailableInStorage = storage.size() > cursor;
      if (isAvailableInStorage) {
        object = storage.get(cursor);
        hasNext = true;
      } else if (!resultSetExhausted) {
        // We need to fetch from database
        try {
          resultSetHandler.handleRowValues(rs, resultMap, objectWrapperResultHandler, RowBounds.DEFAULT,
                  resultColumnCache);
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }

        object = objectWrapperResultHandler.result;
        if (object != null) {
          storage.add(object);
          objectWrapperResultHandler.result = null;
          hasNext = true;
        } else {
          closeResultSetAndStatement();
          resultSetExhausted = true;
        }
      }

      return hasNext;
    }

    @Override
    public E next() {
      E next = object;
      if (next != null) {
        object = null;
        cursor++;
        return next;
      }
      throw new NoSuchElementException();
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Cannot remove element from lazily loaded list");
    }
  }
}
