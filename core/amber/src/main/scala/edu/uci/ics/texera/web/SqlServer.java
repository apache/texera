package edu.uci.ics.texera.web;

import com.mysql.cj.jdbc.MysqlDataSource;
import edu.uci.ics.amber.core.storage.StorageConfig;
import edu.uci.ics.amber.engine.common.AmberConfig;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;

@Deprecated(since = "use sql server in core.dao instead")
public final class SqlServer {
    public static final SQLDialect SQL_DIALECT = SQLDialect.MYSQL;
    private static final MysqlDataSource dataSource;
    public static DSLContext context;

    static {
        dataSource = new MysqlDataSource();
        dataSource.setUrl(StorageConfig.jdbcUrl());
        dataSource.setUser(StorageConfig.jdbcUsername());
        dataSource.setPassword(StorageConfig.jdbcPassword());
        context = DSL.using(dataSource, SQL_DIALECT);
    }

    public static DSLContext createDSLContext() {
        return context;
    }

    public static void replaceDSLContext(DSLContext newContext) {
        context = newContext;
    }
}
