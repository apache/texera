package edu.uci.ics.textdb.exp.sink.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import edu.uci.ics.textdb.api.constants.SchemaConstants;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.api.dataflow.ISink;
import edu.uci.ics.textdb.api.exception.DataFlowException;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.field.DoubleField;
import edu.uci.ics.textdb.api.field.IField;
import edu.uci.ics.textdb.api.field.IntegerField;
import edu.uci.ics.textdb.api.field.DateField;
import edu.uci.ics.textdb.api.schema.Attribute;
import edu.uci.ics.textdb.api.schema.AttributeType;
import edu.uci.ics.textdb.api.schema.Schema;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.api.utils.Utils;

public class MysqlSink implements ISink {
    private final MysqlSinkPredicate predicate;
    private IOperator inputOperator;
    private int cursor = CLOSED;
    private Schema outputSchema;
    private Connection connection;
    private Statement statement;
    private PreparedStatement prepStatement;

    public MysqlSink(MysqlSinkPredicate predicate) {
        this.predicate = predicate;
    }

    public void setInputOperator(IOperator inputOperator) {
        this.inputOperator = inputOperator;
    }

    @Override
    public Schema getOutputSchema() {
        return outputSchema;
    }

    /**
     * Filter the input tuples to removie _id and list fields Setup JDBC
     * connection. Drop previous testTable and create new testTable based on
     * output schema
     */
    @Override
    public void open() throws TextDBException {
        if (cursor == OPENED) {
            return;
        }
        inputOperator.open();
        Schema inputSchema = inputOperator.getOutputSchema();
        outputSchema = new Schema(inputSchema.getAttributes().stream()
                .filter(attr -> !attr.getAttributeName().equalsIgnoreCase(SchemaConstants._ID))
                .filter(attr -> !attr.getAttributeName().equalsIgnoreCase(SchemaConstants.PAYLOAD))
                .filter(attr -> !attr.getAttributeType().equals(AttributeType.LIST)).toArray(Attribute[]::new));
        // JDBC connection
        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            String url = "jdbc:mysql://" + predicate.getHost() + ":" + predicate.getPort() + "/"
                    + predicate.getDatabase() + "?autoReconnect=true&useSSL=true";
            this.connection = DriverManager.getConnection(url, predicate.getUsername(), predicate.getPassword());
            statement = connection.createStatement();
            mysqlDropTable();
            mysqlCreateTable();
            cursor = OPENED;
        } catch (SQLException | InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new DataFlowException("MysqlSink failed to connect to mysql database." + e.getMessage());
        }
    }

    @Override
    public Tuple getNextTuple() throws TextDBException {
        if (cursor == CLOSED) {
            return null;
        }
        if (cursor >= predicate.getLimit() + predicate.getOffset()) {
            return null;
        }
        Tuple inputTuple = null;
        while (true) {
            inputTuple = inputOperator.getNextTuple();
            if (inputTuple == null) {
                return null;
            }
            cursor++;
            if (cursor > predicate.getOffset()) {
                break;
            }
        }
        Tuple resultTuple = Utils.removeFields(inputTuple, SchemaConstants._ID, SchemaConstants.PAYLOAD);
        return resultTuple;
    }

    /**
     * Insert tuples into mysql database using prepared statement. No output
     */
    @Override
    public void processTuples() throws TextDBException {
        String sqlStatemnt = "INSERT INTO " + predicate.getTable() + " VALUES(" + Stream.generate(() -> "?")
                .limit(outputSchema.getAttributeNames().size()).collect(Collectors.joining(",")) + ");";
        try {
            prepStatement = connection.prepareStatement(sqlStatemnt);
            Tuple tuple;
            while ((tuple = this.getNextTuple()) != null) {
                List<IField> fieldList = new ArrayList<>();
                for (int i = 0; i < outputSchema.getAttributeNames().size(); i++) {
                    fieldList.add(tuple.getField(outputSchema.getAttributeNames().get(i)));
                }
                for (int i = 0; i < fieldList.size(); i++) {
                    prepareField(i, fieldList.get(i));
                }
                prepStatement.executeUpdate();
            }
        } catch (SQLException e) {
            throw new DataFlowException(
                    "MysqlSink processTuples fails to execute prepared statement. " + e.getMessage());
        }
    }

    @Override
    public void close() throws TextDBException {
        if (cursor == CLOSED) {
            return;
        }
        inputOperator.close();
        try {
            if (statement != null)
                statement.close();
            if (prepStatement != null)
                prepStatement.close();
            connection.close();
            cursor = CLOSED;
        } catch (SQLException e) {
            throw new DataFlowException("MysqlSink fail to close. " + e.getMessage());
        }
    }

    /**
     * This method is not private as we use it to clean up in MysqlSinkTest.java
     * 
     * @return
     */
    protected int mysqlDropTable() {
        String dropTableStatement = "DROP TABLE IF EXISTS " + predicate.getTable() + ";";
        try {
            if (statement == null)
                statement = connection.createStatement();
            return statement.executeUpdate(dropTableStatement);
        } catch (SQLException e) {
            throw new DataFlowException(
                    "MysqlSink failed to drop table " + predicate.getTable() + ". " + e.getMessage());
        }
    }

    /**
     * Get the table schema from inputOperator, create table in mysql database
     */
    private int mysqlCreateTable() {
        List<Attribute> attributeList = outputSchema.getAttributes();
        String createTableStatement = "CREATE TABLE " + predicate.getTable() + " (\n";
        createTableStatement += attributeList.stream().map(attr -> convertAttribute(attr))
                .collect(Collectors.joining(",\n"));
        createTableStatement += "\n); ";
        try {
            if (statement == null)
                statement = connection.createStatement();
            return statement.executeUpdate(createTableStatement);
        } catch (SQLException e) {
            throw new DataFlowException(
                    "MysqlSink failed to create table " + predicate.getTable() + ". " + e.getMessage());
        }
    }

    /**
     * 
     * Convert a textDB attribute into one line of sql statement. TextDB
     * attribute is from outputSchema. Used in the create table statement.
     * 
     * @param attribute
     * @return
     */
    private String convertAttribute(Attribute attribute) {
        String sqlAttrTypeName = attribute.getAttributeType().getName();
        String sqlStatement = "\t" + attribute.getAttributeName();
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

    private void prepareField(int idx, IField field) throws SQLException {
        if (field == null) {
            return;
        } else if (field instanceof DoubleField) {
            prepStatement.setDouble(idx + 1, (double) field.getValue());
        } else if (field instanceof IntegerField) {
            prepStatement.setInt(idx + 1, (int) field.getValue());
        } else if (field instanceof DateField) { 
            // Notice it's java.sql.Date not java.util.Date
            java.util.Date utilDate = (java.util.Date) field.getValue();
            java.sql.Date sqlDate = new java.sql.Date(utilDate.getTime());
            prepStatement.setDate(idx + 1, sqlDate);
        } else {
            /*
             * textDB STRING, TEXT The attribute in mysql in TEXT.
             * preparedStatement.setString() works for TEXT attribute based the
             * reference below.
             * https://stackoverflow.com/questions/6772594/what-is-the-java-sql-
             * types-equivalent-for-the-mysql-text
             */
            prepStatement.setString(idx + 1, field.getValue().toString());
        }
    }

}
