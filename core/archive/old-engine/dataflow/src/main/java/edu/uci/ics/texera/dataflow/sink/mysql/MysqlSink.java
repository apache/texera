package edu.uci.ics.texera.dataflow.sink.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import edu.uci.ics.texera.api.constants.ErrorMessages;
import edu.uci.ics.texera.api.constants.SchemaConstants;
import edu.uci.ics.texera.api.dataflow.IOperator;
import edu.uci.ics.texera.api.dataflow.ISink;
import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.field.DoubleField;
import edu.uci.ics.texera.api.field.IField;
import edu.uci.ics.texera.api.field.IntegerField;
import edu.uci.ics.texera.api.field.DateField;
import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.Tuple;

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
    public void open() throws TexeraException {
        if (cursor == OPENED) {
            return;
        }
        inputOperator.open();
        Schema inputSchema = inputOperator.getOutputSchema();
        outputSchema = new Schema(inputSchema.getAttributes().stream()
                .filter(attr -> !attr.getName().equalsIgnoreCase(SchemaConstants._ID))
                .filter(attr -> !attr.getName().equalsIgnoreCase(SchemaConstants.PAYLOAD))
                .filter(attr -> !attr.getType().equals(AttributeType.LIST)).toArray(Attribute[]::new));
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
            throw new DataflowException("MysqlSink failed to connect to mysql database." + e.getMessage());
        }
    }

    @Override
    public Tuple getNextTuple() throws TexeraException {
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
        Tuple resultTuple = new Tuple.Builder(inputTuple).removeIfExists(SchemaConstants._ID, SchemaConstants.PAYLOAD).build();

        return resultTuple;
    }

    /**
     * Insert tuples into mysql database using prepared statement. No output
     */
    @Override
    public void processTuples() throws TexeraException {
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
            throw new DataflowException(
                    "MysqlSink processTuples fails to execute prepared statement. " + e.getMessage());
        }
    }

    @Override
    public void close() throws TexeraException {
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
            throw new DataflowException("MysqlSink fail to close. " + e.getMessage());
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
            throw new DataflowException(
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
            throw new DataflowException(
                    "MysqlSink failed to create table " + predicate.getTable() + ". " + e.getMessage());
        }
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
             * texera STRING, TEXT The attribute in mysql in TEXT.
             * preparedStatement.setString() works for TEXT attribute based the
             * reference below.
             * https://stackoverflow.com/questions/6772594/what-is-the-java-sql-
             * types-equivalent-for-the-mysql-text
             */
            prepStatement.setString(idx + 1, field.getValue().toString());
        }
    }

    public Schema transformToOutputSchema(Schema... inputSchema) throws DataflowException {
        throw new TexeraException(ErrorMessages.INVALID_OUTPUT_SCHEMA_FOR_SINK);
    }
}
