package org.apache.ibatis.executor.resultset;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

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
public class LazyFastResultSetHandler extends FastResultSetHandler {

	private static final Log log = LogFactory.getLog(LazyFastResultSetHandler.class);

	public LazyFastResultSetHandler(Executor executor, MappedStatement mappedStatement,
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
	protected void callResultHandler(ResultHandler resultHandler, DefaultResultContext resultContext, Object rowValue) {
		super.callResultHandler(resultHandler, resultContext, rowValue);
		resultContext.stop();
	}
}
