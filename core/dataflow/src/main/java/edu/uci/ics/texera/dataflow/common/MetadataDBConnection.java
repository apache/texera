package edu.uci.ics.texera.dataflow.common;

import edu.uci.ics.texera.api.schema.Attribute;

import java.sql.Connection;
import java.sql.DriverManager;

public class MetadataDBConnection {

    /**
     * get a connection based on the passed driver and database info
     *
     */
    public static Connection getConnection(String driver, String HOST, String PORT, String DATABASE, String USERNAME, String PASSWORD) {
        Connection connection = null;
        String url = null;
        try {
            Class.forName(driver).newInstance();
            url = "jdbc:mysql://" + HOST + ":" + PORT + "/"
                    + DATABASE + "?autoReconnect=true&useSSL=true";
            connection = DriverManager.getConnection(url, USERNAME, PASSWORD);
        } catch (Exception e) {
            System.out.println("Could not get connection with " + url + e.getMessage());
        }
        return connection;
    }

    /**
     *
     * Convert a texera attribute into one line of sql statement. Used in the create table statement.
     *
     * @param attribute
     * @return
     */
    public static String convertAttribute(Attribute attribute) {
        String sqlAttrTypeName = attribute.getType().getName();
        String sqlStatement = "\t" + attribute.getName();
        switch (sqlAttrTypeName) {
            case "integer":
                sqlStatement += " INT";
                break;
            case "double":
                sqlStatement += " DOUBLE";
                break;
            case "date":
                sqlStatement += " DATE";
                break;
            default:
                sqlStatement += " TEXT";
                break; // Including string and text
        }
        return sqlStatement;
    }
}