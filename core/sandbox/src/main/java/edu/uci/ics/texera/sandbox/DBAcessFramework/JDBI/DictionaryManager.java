package edu.uci.ics.texera.dataflow.resource.dictionary;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import javax.sql.DataSource;
import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;

import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;

import static org.junit.Assert.assertThat;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.exception.StorageException;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.storage.utils.StorageUtils;
import edu.uci.ics.texera.storage.constants.MySQLConstants;

public class DictionaryManager {

    private static DictionaryManager instance = null;
    private static Jdbi jdbi = null;

    private DictionaryManager() throws StorageException {
        // Establish JDBI connection to MySQL
        MysqlDataSource ds = new MysqlDataSource();
        ds.setServerName(MySQLConstants.HOST);
        ds.setPortNumber(Integer.parseInt(MySQLConstants.PORT));
        ds.setDatabaseName(MySQLConstants.DATABASE);
        ds.setUser(MySQLConstants.USERNAME);
        ds.setPassword(MySQLConstants.PASSWORD);

        jdbi = Jdbi.create(ds);
    }
    

    public synchronized static DictionaryManager getInstance() throws StorageException {
        if (instance == null) {
            instance = new DictionaryManager();
            instance.createDictionaryManager();
        }
        return instance;
    }

    /**
     * Creates plan store, both an index and a directory for plan objects.
     *
     * @throws TexeraException
     */
    public void createDictionaryManager() throws TexeraException {
        // table should not exist
        if (checkTableExistence(DictionaryManagerConstants.TABLE_NAME)) {
             throw new StorageException(String.format("Table %s already exists.", DictionaryManagerConstants.TABLE_NAME));
        }
        
        // create the table
        List<Attribute> attributeList = DictionaryManagerConstants.SCHEMA.getAttributes();
        String createTableStatement = "CREATE TABLE " + DictionaryManagerConstants.TABLE_NAME + " (\n";
        createTableStatement += attributeList.stream().map(attr -> convertAttribute(attr))
                .collect(Collectors.joining(",\n"));
        createTableStatement += "\n); ";
        final String queryForJdbi = createTableStatement;
        jdbi.useHandle(handle -> {
            handle.execute(queryForJdbi);
        });
    }

    /**
     * removes plan store, both an index and a directory for dictionary objects.
     *
     * @throws TexeraException
     */
    public void destroyDictionaryManager() throws TexeraException {
        // table should exist
        if (!checkTableExistence(DictionaryManagerConstants.TABLE_NAME)) {
            throw new StorageException(String.format("Table %s does not exist.", DictionaryManagerConstants.TABLE_NAME));
        }
        // delete the table
        String dropTableStatement = "DROP TABLE " + DictionaryManagerConstants.TABLE_NAME + ";";
        final String queryForJdbi = dropTableStatement;
        jdbi.useHandle(handle -> {
            handle.execute(queryForJdbi);
        });
    }
    
    public void addDictionary(String fileName, String dictionaryContent) throws StorageException {
        jdbi.useHandle(handle -> {
            handle.execute("INSERT INTO " + DictionaryManagerConstants.TABLE_NAME + " VALUES (?, ?);", fileName, dictionaryContent); 
        });
    }
    
    public List<String> getDictionaries() throws StorageException {
        List<String> dictionaries = jdbi.withHandle(handle -> 
            handle.createQuery("SELECT " + DictionaryManagerConstants.NAME + " FROM " + DictionaryManagerConstants.TABLE_NAME + ";")
                .mapTo(String.class)
                .list()
            );
        return dictionaries;
    }
    
    public String getDictionary(String dictionaryName) throws StorageException {
        List<String> result = jdbi.withHandle(handle ->
            handle.createQuery("SELECT " + DictionaryManagerConstants.CONTENT +" FROM " + DictionaryManagerConstants.TABLE_NAME + " WHERE " + DictionaryManagerConstants.NAME + " = ?")
                .bind(0, dictionaryName)
                .mapTo(String.class)
                .list()
            );
        if (result.size() != 1)
            throw new StorageException(String.format("Row %s cannot be retrieved. No existence or multiple existence.", dictionaryName));
        return result.get(0);
    }
    
    public boolean checkTableExistence(String tableName) throws StorageException {
        List<String> result = jdbi.withHandle(handle -> 
            handle.createQuery("SHOW TABLES LIKE '" + tableName + "';")
                .mapTo(String.class)
                .list());
        return result.contains(tableName);
    }

    /**
     * 
     * Convert a texera attribute into one line of sql statement. Texera
     * attribute is from outputSchema. Used in the create table statement.
     * 
     * @param attribute
     * @return
     */
    private String convertAttribute(Attribute attribute) {
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
