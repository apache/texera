package edu.uci.ics.texera.dataflow.sqlServerInfo;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import edu.uci.ics.texera.api.utils.Utils;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;

public final class UserSqlServer {

    public static Config jdbcConfig;
    static {
        Path jdbcConfPath = Utils.getTexeraHomePath().resolve("conf").resolve("jdbc.conf");
        jdbcConfig = ConfigFactory.parseFile(jdbcConfPath.toFile());
    }
    public static final SQLDialect SQL_DIALECT = SQLDialect.MYSQL;

    public static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(
                jdbcConfig.getString("jdbc.url"),
                jdbcConfig.getString("jdbc.username"),
                jdbcConfig.getString("jdbc.password"));
    }

    public static DSLContext createDSLContext(Connection conn) {
        return DSL.using(conn, SQL_DIALECT);
    }
}
