package edu.uci.ics.texera.dataflow.jooq;


import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import edu.uci.ics.texera.api.utils.Utils;
import org.jooq.codegen.GenerationTool;
import org.jooq.meta.jaxb.Configuration;
import org.jooq.meta.jaxb.Jdbc;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * This class is used to generate java classes representing the sql table in Texera database
 * These auto generated classes are essential for the connection between backend and database when using JOOQ library.
 * 
 * Every time the table in the Texera database changes, including creating, dropping and modifying the tables,
 * this class must be run to update the corresponding java classes.
 * 
 * Remember to change the username and password to your owns before you run this class.
 *
 * The username, password and connection url is located in texera\core\conf\jdbc.conf
 * The configuration file is located in texera\core\conf\jooq-conf.xml
 * 
 */
public class RunCodegen {

    public static void main(String[] args) throws Exception {

        Path jooqXmlPath = Utils.getTexeraHomePath().resolve("conf").resolve("jooq-conf.xml");
        Configuration jooqConfig = GenerationTool.load(Files.newInputStream(jooqXmlPath));

        Path jdbcConfPath = Utils.getTexeraHomePath().resolve("conf").resolve("jdbc.conf");
        Config jdbcConfig = ConfigFactory.parseFile(jdbcConfPath.toFile());

        Jdbc jooqJdbcConfig = new Jdbc();
        jooqJdbcConfig.setDriver(jdbcConfig.getString("jdbc.driver"));
        jooqJdbcConfig.setUrl(jdbcConfig.getString("jdbc.url"));
        jooqJdbcConfig.setUsername(jdbcConfig.getString("jdbc.username"));
        jooqJdbcConfig.setPassword(jdbcConfig.getString("jdbc.password"));
        jooqConfig.setJdbc(jooqJdbcConfig);


        GenerationTool.generate(jooqConfig);
    }

}
