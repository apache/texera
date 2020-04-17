package edu.uci.ics.texera.dataflow.jooq;


import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import edu.uci.ics.texera.api.utils.Utils;
import org.jooq.codegen.GenerationTool;
import org.jooq.meta.jaxb.Configuration;
import org.jooq.meta.jaxb.Jdbc;

import java.nio.file.Files;
import java.nio.file.Path;

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
