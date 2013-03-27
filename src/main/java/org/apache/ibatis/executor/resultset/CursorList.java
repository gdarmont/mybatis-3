package org.apache.ibatis.executor.resultset;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.AbstractList;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.ibatis.mapping.ResultMap;
import org.apache.ibatis.session.ResultContext;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;

/**
 * @author GDA / GD06186S
 */
public class CursorList<E> extends AbstractList<E> {

	// ResultSetHandler stuff
	private final FastResultSetHandler resultSetHandler;
	private final ResultSet rs;
	private final ResultMap resultMap;
	private final FastResultSetHandler.ResultColumnCache resultColumnCache;
	private final ObjectWrapperResultHandler<E> objectWrapperResultHandler = new ObjectWrapperResultHandler<E>();

	private boolean iteratorAlreadyOpened = false;

	private boolean fetchStarted = false;

	private boolean resultSetExhausted = false;

	public CursorList(FastResultSetHandler resultSetHandler, ResultSet rs, ResultMap resultMap,
			FastResultSetHandler.ResultColumnCache resultColumnCache) {
		this.resultSetHandler = resultSetHandler;
		this.rs = rs;
		this.resultMap = resultMap;
		this.resultColumnCache = resultColumnCache;
	}

	@Override
	public E get(int index) {
		throw new UnsupportedOperationException("Cannot retrieve object at a specific index in CursorList. "
				+ "Consider using Iterator to browse the list.");
	}

	@Override
	public int size() {
		throw new UnsupportedOperationException("Cannot retrieve size of a CursorList. "
				+ "Consider using Iterator to browse the list.");
	}

	public boolean isResultSetExhausted() {
		return resultSetExhausted;
	}

	public boolean isFetchStarted() {
		return fetchStarted;
	}

	@Override
	public Iterator<E> iterator() {
		return new CursorIterator();
	}

	protected E fetchNextObjectFromDatabase() {
		if (resultSetExhausted) return null;

		try {
			fetchStarted = true;
			resultSetHandler.handleRowValues(rs, resultMap, objectWrapperResultHandler, RowBounds.DEFAULT,
					resultColumnCache);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}

		E next = objectWrapperResultHandler.result;
		if (next == null) {
			closeResultSetAndStatement();
			resultSetExhausted = true;
		}
		objectWrapperResultHandler.result = null;

		return next;
	}

	public void closeResultSetAndStatement() {
		try {
			if (rs != null) {
				Statement statement = rs.getStatement();

				if (!rs.isClosed()) {
					rs.close();
				}
				if (statement != null && !statement.isClosed()) {
					statement.close();
				}
			}
		} catch (SQLException e) {
			// ignore
		}
	}

	/**
	 * This toString returns Object's toString default implementation since we don't want AbstractCollection#toString()
	 * to iterate on collection.
	 * 
	 * @return a string representation of the object.
	 */
	@Override
	public String toString() {
		return getClass().getName() + "@" + Integer.toHexString(System.identityHashCode(this));
	}

	private static class ObjectWrapperResultHandler<E> implements ResultHandler {

		private E result;

		@Override
		public void handleResult(ResultContext context) {
			this.result = (E) context.getResultObject();
		}
	}

	private class CursorIterator implements Iterator<E> {

		/**
		 * Holder for the next objet to be returned
		 */
		E object;

		public CursorIterator() {
			if (iteratorAlreadyOpened) {
				throw new IllegalStateException("Cannot open more than one iterator on a CursorList. "
						+ "Use LazyList if you want to iterate more than one time.");
			}
			iteratorAlreadyOpened = true;
		}

		@Override
		public boolean hasNext() {
			object = fetchNextObjectFromDatabase();
			return object != null;
		}

		@Override
		public E next() {
			// Fill next with object fetched from hasNext()
			E next = object;

			if (next == null) {
				next = fetchNextObjectFromDatabase();
			}

			if (next != null) {
				object = null;
				return next;
			}
			throw new NoSuchElementException();
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("Cannot remove element from CursorList");
		}
	}
}
