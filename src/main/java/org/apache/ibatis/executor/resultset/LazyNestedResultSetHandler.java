package org.apache.ibatis.executor.resultset;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.apache.ibatis.cache.CacheKey;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.executor.parameter.ParameterHandler;
import org.apache.ibatis.executor.result.DefaultResultContext;
import org.apache.ibatis.logging.Log;
import org.apache.ibatis.logging.LogFactory;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.ResultMap;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;

/**
 * @author GDA / GD06186S
 */
public class LazyNestedResultSetHandler extends NestedResultSetHandler {

	private static final Log log = LogFactory.getLog(LazyNestedResultSetHandler.class);

  private Object previousRowValue;

	public LazyNestedResultSetHandler(Executor executor, MappedStatement mappedStatement,
			ParameterHandler parameterHandler, ResultHandler resultHandler, BoundSql boundSql, RowBounds rowBounds) {
		super(executor, mappedStatement, parameterHandler, resultHandler, boundSql, rowBounds);
	}

	@Override
	protected void handleResultSet(ResultSet rs, ResultMap resultMap, List<Object> multipleResults,
			ResultColumnCache resultColumnCache) throws SQLException {
		if (resultHandler == null) {
			LazyList lazyList = new LazyList(this, rs, resultMap, resultColumnCache);
			multipleResults.add(lazyList);
		} else {
			throw new IllegalStateException("LazyNestedResultSetHandler cannot be used with external ResultHandler");
		}
	}

	@Override
	protected void handleRowValues(ResultSet rs, ResultMap resultMap, ResultHandler resultHandler, RowBounds rowBounds,
			ResultColumnCache resultColumnCache) throws SQLException {
		final DefaultResultContext resultContext = new DefaultResultContext();
		skipRows(rs, rowBounds);
		Object rowValue = previousRowValue;
		while (shouldProcessMoreRows(rs, resultContext, rowBounds)) {
			final ResultMap discriminatedResultMap = resolveDiscriminatedResultMap(rs, resultMap, null);
			final CacheKey rowKey = createRowKey(discriminatedResultMap, rs, null, resultColumnCache);
			Object partialObject = objectCache.get(rowKey);
			if (partialObject == null && rowValue != null) { // issue #542 delay calling ResultHandler until object ends
				if (mappedStatement.isResultOrdered()) objectCache.clear(); // issue #577 clear memory if ordered
				callResultHandler(resultHandler, resultContext, rowValue);
			}
			  rowValue = getRowValue(rs, discriminatedResultMap, rowKey, rowKey, null, resultColumnCache, partialObject);

		}
    // If we have a value and we didn't exit from while because of a stopped context
		if (rowValue != null && !resultContext.isStopped()) {
      callResultHandler(resultHandler, resultContext, rowValue);
      previousRowValue = null;
    } else if (rowValue != null) {
      previousRowValue = rowValue;
    }
	}

	@Override
	protected void callResultHandler(ResultHandler resultHandler, DefaultResultContext resultContext, Object rowValue) {
		super.callResultHandler(resultHandler, resultContext, rowValue);
		resultContext.stop();
	}
}
