package edu.uci.ics.texera.dataflow.sqlServerInfo;

import com.mysql.cj.jdbc.MysqlDataSource;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import edu.uci.ics.texera.api.utils.Utils;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;

import java.nio.file.Path;

public final class SqlServer {

    public static Config jdbcConfig;
    private static final MysqlDataSource dataSource;

    static {
        Path jdbcConfPath = Utils.getTexeraHomePath().resolve("conf").resolve("jdbc.conf");
        jdbcConfig = ConfigFactory.parseFile(jdbcConfPath.toFile());

        dataSource = new MysqlDataSource();
        dataSource.setUrl(jdbcConfig.getString("jdbc.url"));
        dataSource.setUser(jdbcConfig.getString("jdbc.username"));
        dataSource.setPassword(jdbcConfig.getString("jdbc.password"));
    }
    public static final SQLDialect SQL_DIALECT = SQLDialect.MYSQL;

    public static DSLContext createDSLContext() {
        return DSL.using(dataSource, SQL_DIALECT);
    }
}
