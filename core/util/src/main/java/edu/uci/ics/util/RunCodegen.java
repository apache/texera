package edu.uci.ics.util;


import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.jooq.codegen.GenerationTool;
import org.jooq.meta.jaxb.Configuration;
import org.jooq.meta.jaxb.Jdbc;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * This class is used to generate java classes representing the sql table in Texera database
 * These auto generated classes are essential for the connection between backend and database when using JOOQ library.
 * <p>
 * Every time the table in the Texera database changes, including creating, dropping and modifying the tables,
 * this class must be run to update the corresponding java classes.
 * <p>
 * Remember to change the username and password to your owns before you run this class.
 * <p>
 * The username, password and connection url is located in texera\core\conf\jdbc.conf
 * The configuration file is located in texera\core\conf\jooq-conf.xml
 */
public class RunCodegen {

    public static void main(String[] args) throws Exception {
        Path jooqXmlPath = Path.of("core").resolve("util").resolve("conf").resolve("jooq-conf.xml");
        Configuration jooqConfig = GenerationTool.load(Files.newInputStream(jooqXmlPath));

        Path jdbcConfPath = Path.of("core").resolve("amber").resolve("src").resolve("main").resolve("resources").resolve("application.conf");
        Config jdbcConfig = ConfigFactory.parseFile(jdbcConfPath.toFile());

        Jdbc jooqJdbcConfig = new Jdbc();
        jooqJdbcConfig.setDriver("com.mysql.cj.jdbc.Driver");
        jooqJdbcConfig.setUrl(jdbcConfig.getString("jdbc.url"));
        jooqJdbcConfig.setUsername(jdbcConfig.getString("jdbc.username"));
        jooqJdbcConfig.setPassword(jdbcConfig.getString("jdbc.password"));
        jooqConfig.setJdbc(jooqJdbcConfig);

        GenerationTool.generate(jooqConfig);
    }

}



