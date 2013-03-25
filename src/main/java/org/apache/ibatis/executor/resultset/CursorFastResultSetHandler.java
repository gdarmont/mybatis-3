package org.apache.ibatis.executor.resultset;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.executor.parameter.ParameterHandler;
import org.apache.ibatis.executor.result.DefaultResultContext;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.FetchType;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.ResultMap;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;

/**
 * @author GDA / GD06186S
 */
public class CursorFastResultSetHandler extends FastResultSetHandler {

	private final FetchType fetchType;

	public CursorFastResultSetHandler(Executor executor, MappedStatement mappedStatement,
			ParameterHandler parameterHandler, ResultHandler resultHandler, BoundSql boundSql, RowBounds rowBounds,
			FetchType fetchType) {
		super(executor, mappedStatement, parameterHandler, resultHandler, boundSql, rowBounds);
		this.fetchType = fetchType;
	}

	@Override
	protected void handleResultSet(ResultSet rs, ResultMap resultMap, List<Object> multipleResults,
			ResultColumnCache resultColumnCache) throws SQLException {
		if (resultHandler == null) {
			List cursorList = getResultList(rs, resultMap, resultColumnCache);
			multipleResults.add(cursorList);
		} else {
			throw new IllegalStateException("CursorFastResultSetHandler cannot be used with external ResultHandler");
		}
	}

	private List getResultList(ResultSet rs, ResultMap resultMap, ResultColumnCache resultColumnCache) {
		if (fetchType == FetchType.CURSOR) {
			return new CursorList(this, rs, resultMap, resultColumnCache);
		} else if (fetchType == FetchType.LAZY) {
			return new LazyList(this, rs, resultMap, resultColumnCache);
		} else {
			throw new IllegalArgumentException("FetchType " + fetchType
					+ " is not supported by CursorNestedResultSetHandler");
		}
	}

	@Override
	protected void callResultHandler(ResultHandler resultHandler, DefaultResultContext resultContext, Object rowValue) {
		super.callResultHandler(resultHandler, resultContext, rowValue);
		resultContext.stop();
	}
}
