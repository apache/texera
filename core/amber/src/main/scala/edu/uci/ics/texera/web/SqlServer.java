package edu.uci.ics.texera.web;

import com.mysql.cj.jdbc.MysqlDataSource;
import edu.uci.ics.amber.engine.common.AmberConfig;
import edu.uci.ics.amber.engine.common.AmberUtils;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;

public final class SqlServer {
    public static final SQLDialect SQL_DIALECT = SQLDialect.MYSQL;
    private static final MysqlDataSource dataSource;
    public static DSLContext context;

    static {
        dataSource = new MysqlDataSource();
        dataSource.setUrl(AmberConfig.jdbcConfig().getString("url"));
        dataSource.setUser(AmberConfig.jdbcConfig().getString("username"));
        dataSource.setPassword(AmberConfig.jdbcConfig().getString("password"));
        context = DSL.using(dataSource, SQL_DIALECT);
    }

    public static DSLContext createDSLContext() {
        return context;
    }

    public static void replaceDSLContext(DSLContext newContext){
        context = newContext;
    }
}
