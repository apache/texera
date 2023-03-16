package edu.uci.ics.texera.web;

import com.mysql.cj.jdbc.MysqlDataSource;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;

import java.nio.file.Path;
import java.nio.file.Paths;

public final class SqlServer {

    public static final SQLDialect SQL_DIALECT = SQLDialect.MYSQL;
    private static final MysqlDataSource dataSource;
    public static Config jdbcConfig;
    public static DSLContext context;

    static {
        // TODO: do not use hardcoded value
        Path jdbcConfPath = Paths.get("..").resolve("conf").resolve("jdbc.conf").toAbsolutePath();

        jdbcConfig = ConfigFactory.parseFile(jdbcConfPath.toFile());

        dataSource = new MysqlDataSource();
        dataSource.setUrl(jdbcConfig.getString("jdbc.url"));
        dataSource.setUser(jdbcConfig.getString("jdbc.username"));
        dataSource.setPassword(jdbcConfig.getString("jdbc.password"));
        context = DSL.using(dataSource, SQL_DIALECT);
    }

    public static DSLContext createDSLContext() {
        return context;
    }

    public static void replaceDSLContext(DSLContext newContext){
        context = newContext;
    }
}
