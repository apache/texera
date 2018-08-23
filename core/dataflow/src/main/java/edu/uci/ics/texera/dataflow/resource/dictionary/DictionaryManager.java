package edu.uci.ics.texera.dataflow.resource.dictionary;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import edu.uci.ics.texera.dataflow.common.MetadataDBConnection;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.exception.StorageException;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.schema.Attribute;
import static edu.uci.ics.texera.storage.constants.MySQLConstants.*;

public class DictionaryManager {

    private static DictionaryManager instance = null;
    private static Connection connection;
    private static Statement statement;
    private static PreparedStatement prepStatement;

    private DictionaryManager() throws StorageException {
        // Establish JDBC connection to Database
        connection = MetadataDBConnection.getConnection("com.mysql.jdbc.Driver", HOST, PORT, DATABASE, USERNAME, PASSWORD);
        try {
            statement = connection.createStatement();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        
    }


    public synchronized static DictionaryManager getInstance() throws StorageException {
        if (instance == null) {
            instance = new DictionaryManager();
            instance.createDictionaryManager();
        }
        return instance;
    }

    /**
     * Creates dictionary table
     *
     */
    public void createDictionaryManager() {
        // create the table
        List<Attribute> attributeList = DictionaryManagerConstants.SCHEMA.getAttributes();
        String createTableStatement = "CREATE TABLE IF NOT EXISTS " + DictionaryManagerConstants.TABLE_NAME + " (\n";
        createTableStatement += attributeList.stream().map(attr -> MetadataDBConnection.convertAttribute(attr))
                .collect(Collectors.joining(",\n"));
        createTableStatement += "\n); ";
        try {
            if (statement == null)
                statement = connection.createStatement();
            statement.executeUpdate(createTableStatement);
        } catch (SQLException e) {
            throw new DataflowException(
                    "DictionaryManager failed to create table " + DictionaryManagerConstants.TABLE_NAME + ". " + e.getMessage());
        }
    }

    /**
     * removes dictionary table
     *
     */
    public void destroyDictionaryManager() throws TexeraException {
        // table should exist
        if (!checkTableExistence(DictionaryManagerConstants.TABLE_NAME)) {
            throw new StorageException(String.format("Table %s does not exist.", DictionaryManagerConstants.TABLE_NAME));
        }
        // delete the table
        String dropTableStatement = "DROP TABLE " + DictionaryManagerConstants.TABLE_NAME + ";";
        try {
            if (statement == null)
                statement = connection.createStatement();
            statement.executeUpdate(dropTableStatement);
        } catch (SQLException e) {
            throw new DataflowException(
                    "DictionaryManager failed to delete table " + DictionaryManagerConstants.TABLE_NAME + ". " + e.getMessage());
        }
    }

    /**
     * add a record into dictionary table
     * @param fileName
     * @param dictionaryContent
     */
    public void addDictionary(String fileName, String dictionaryContent) throws StorageException {
        String insertStatement = "INSERT INTO " + DictionaryManagerConstants.TABLE_NAME + " VALUES(?, ?);";
        try {
            if (prepStatement == null)
                prepStatement = connection.prepareStatement(insertStatement);
            prepStatement.setString(1, fileName);
            prepStatement.setString(2, dictionaryContent);
            prepStatement.executeUpdate();
        } catch (SQLException e) {
            throw new DataflowException(
                    "DictionaryManager failed to insert record " + fileName + " into table " + DictionaryManagerConstants.TABLE_NAME + ". " + e.getMessage());
        }
    }

    /**
     * get the names of all records in dictionary table
     * @return list of all names
     */
    public List<String> getDictionaries() throws StorageException {
        List<String> dictionaries = new ArrayList<>();
        String getAllStatement = "SELECT * FROM " + DictionaryManagerConstants.TABLE_NAME + ";";
        try {
            if (statement == null)
                statement = connection.createStatement();
            ResultSet rs = statement.executeQuery(getAllStatement);
            while (rs.next()) {
                dictionaries.add(rs.getString(DictionaryManagerConstants.NAME));
            }
        } catch (SQLException e) {
            throw new StorageException(
                    "DictionaryManager failed to get all dictionaries. " + e.getMessage());
        }
        return dictionaries;
    }
    /**
     * get the content of a record with specified name
     * @param dictionaryName
     * @return the content of that record
     */
    public String getDictionary(String dictionaryName) throws StorageException {
        String getOneStatement = "SELECT * FROM " + DictionaryManagerConstants.TABLE_NAME + " WHERE "
                + DictionaryManagerConstants.NAME + "='" + dictionaryName + "';";
        try {
            if (statement == null)
                statement = connection.createStatement();
            ResultSet rs = statement.executeQuery(getOneStatement);
            if (rs.next())
                return rs.getString(DictionaryManagerConstants.CONTENT);
        } catch (SQLException e) {
            throw new StorageException(
                    "DictionaryManager failed get : " + dictionaryName + ". " + e.getMessage());
        }
        return null;
    }

    /**
     * helper function to check table's existence
     *
     */
    public boolean checkTableExistence(String tableName) throws StorageException {
        String checkStatement = "SHOW TABLES LIKE '" + tableName + "';";
        try {
            if (statement == null)
                statement = connection.createStatement();
            ResultSet rs = statement.executeQuery(checkStatement);
            return rs.next();
        } catch (SQLException e) {
            throw new StorageException(
                    "DictionaryManager failed to check table existence of: " + tableName + ". " + e.getMessage());
        }
    }

}