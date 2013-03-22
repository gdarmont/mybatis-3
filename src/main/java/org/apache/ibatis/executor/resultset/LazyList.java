package org.apache.ibatis.executor.resultset;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.AbstractList;
import java.util.ArrayList;

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

	public LazyList(FastResultSetHandler resultSetHandler, ResultSet rs, ResultMap resultMap,
			FastResultSetHandler.ResultColumnCache resultColumnCache) {
		this.resultSetHandler = resultSetHandler;
		this.rs = rs;
		this.resultMap = resultMap;
		this.resultColumnCache = resultColumnCache;
	}

	@Override
	public E get(int index) {
		while (storage.size() <= index) {
			try {
				resultSetHandler.handleRowValues(rs, resultMap, objectWrapperResultHandler, RowBounds.DEFAULT,
						resultColumnCache);
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}

			Object result = objectWrapperResultHandler.result;
			if (result != null) {
				storage.add(objectWrapperResultHandler.result);
				objectWrapperResultHandler.result = null;
			} else {
				closeResultSetAndStatement();
				throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + storage.size());
			}
		}
		return storage.get(index);
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
	public int size() {
		return storage.size();
	}

	private static class ObjectWrapperResultHandler<E> implements ResultHandler {

		private E result;

		@Override
		public void handleResult(ResultContext context) {
			this.result = (E) context.getResultObject();
		}

	}

}
