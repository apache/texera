package edu.uci.ics.util;

import org.jooq.codegen.GenerationTool;
import org.jooq.meta.jaxb.Configuration;
import org.jooq.meta.jaxb.Jdbc;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

/**
 * This class is used to generate Java classes representing the SQL tables in the Texera database.
 * These auto-generated classes are essential for the connection between backend and database when using the JOOQ library.
 * <p>
 * Remember to change the username and password to your own before you run this class.
 * <p>
 * The configuration file is located in texera/core/util/conf/jooq-conf.xml
 * The YAML configuration file is located in texera/core/workflow-core/src/main/resources/storage-config.yaml
 */
public class RunCodegen {

    public static void main(String[] args) throws Exception {
        // Path to jOOQ configuration XML
        Path jooqXmlPath = Path.of("core").resolve("util").resolve("conf").resolve("jooq-conf.xml");
        Configuration jooqConfig = GenerationTool.load(Files.newInputStream(jooqXmlPath));

        // Path to the YAML configuration file
        Path yamlConfPath = Path.of("core").resolve("workflow-core").resolve("src").resolve("main").resolve("resources").resolve("storage-config.yaml");

        // Load YAML configuration
        Yaml yaml = new Yaml();
        try (InputStream inputStream = Files.newInputStream(yamlConfPath)) {
            Map<String, Object> config = yaml.load(inputStream);
            Map<String, Object> jdbcConfigMap = (Map<String, Object>) ((Map<String, Object>) config.get("storage")).get("jdbc");

            // Set JDBC configuration for jOOQ
            Jdbc jooqJdbcConfig = new Jdbc();
            jooqJdbcConfig.setDriver("com.mysql.cj.jdbc.Driver");
            jooqJdbcConfig.setUrl(jdbcConfigMap.get("url").toString());
            jooqJdbcConfig.setUsername(jdbcConfigMap.get("username").toString());
            jooqJdbcConfig.setPassword(jdbcConfigMap.get("password").toString());
            jooqConfig.setJdbc(jooqJdbcConfig);

            // Generate the code
            GenerationTool.generate(jooqConfig);
        }
    }
}