package edu.uci.ics.texera.workflow.operators.source.mysql;

import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorExecutor;
import edu.uci.ics.texera.workflow.common.tuple.Tuple;
import edu.uci.ics.texera.workflow.common.tuple.schema.Attribute;
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeType;
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema;
import scala.collection.Iterator;

import java.sql.*;
import java.util.HashSet;

public class MysqlSourceOpExec implements SourceOperatorExecutor {

    // source configs
    private final Schema schema;
    private final String host;
    private final String port;
    private final String database;
    private final String table;
    private final String username;
    private final String password;

    // search column related
    private final String column;
    private final String keywords;

    // progressiveness related
    private final Boolean progressive;
    private final Long interval;
    private final HashSet<String> tableNames;
    private Attribute batchByAttribute = null;

    // connection and query related
    private Connection connection;
    private PreparedStatement curQuery;
    private ResultSet curResultSet;
    private Long curLimit;
    private Long curOffset;
    private Number curLowerBound = 0;
    private Number upperBound = 0;
    private Tuple cachedTuple = null;

    MysqlSourceOpExec(Schema schema, String host, String port, String database, String table, String username,
                      String password, Long limit, Long offset, String column, String keywords, Boolean progressive,
                      String batchByColumn, Long interval) {
        this.schema = schema;
        this.host = host.trim();
        this.port = port.trim();
        this.database = database.trim();
        this.table = table.trim();
        this.username = username.trim();
        this.password = password;
        this.curLimit = limit;
        this.curOffset = offset;
        this.column = column == null ? null : column.trim();
        this.keywords = keywords == null ? null : keywords.trim();
        this.progressive = progressive;
        this.tableNames = new HashSet<>();

        if (!progressive) {
            this.batchByAttribute = null;
            this.interval = Long.MAX_VALUE;
        } else {
            if (interval == 0) {
                throw new RuntimeException("the interval cannot be 0");
            }
            this.interval = interval;
            if (batchByColumn != null && !batchByColumn.isEmpty()) {
                this.batchByAttribute = schema.getAttribute(batchByColumn);
            }
        }
    }

    @Override
    public Iterator<Tuple> produceTexeraTuple() {
        return new Iterator<Tuple>() {

            @Override
            public boolean hasNext() {

                // if existing Tuple in cache, means there exist next Tuple.
                if (cachedTuple != null) {
                    return true;
                }

                // cache the next Tuple
                cachedTuple = next();

                return cachedTuple != null;
            }

            /**
             * Fetch the next row from resultSet, parse it into Texera.Tuple and return.
             * - If resultSet is exhausted, send the next query until no more queries are available.
             * - If no more queries, return null.
             *
             * @return Texera.Tuple
             */
            @Override
            public Tuple next() {

                // if has the next Tuple in cache, return it and clear the cache
                if (cachedTuple != null) {
                    Tuple tuple = cachedTuple;
                    cachedTuple = null;
                    return tuple;
                }

                // otherwise, send query to fetch for the next Tuple
                try {
                    if (curResultSet != null && curResultSet.next()) {

                        // manually skip until the offset position in order to adapt to progressive batches
                        if (curOffset != null && curOffset > 0) {
                            curOffset--;
                            return next();
                        }

                        // construct Texera.Tuple from the next result.
                        Tuple tuple = buildTupleFromRow();

                        // update the limit in order to adapt to progressive batches
                        if (curLimit != null) curLimit--;

                        return tuple;
                    } else {

                        // close the current resultSet and query
                        if (curResultSet != null) curResultSet.close();
                        if (curQuery != null) curQuery.close();

                        curQuery = getNextQuery();
                        if (curQuery != null) {
                            curResultSet = curQuery.executeQuery();
                            return next();
                        } else return null;

                    }

                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    /**
     * Build a Texera.Tuple from a row of curResultSet
     *
     * @return the new Texera.Tuple
     * @throws SQLException
     */
    private Tuple buildTupleFromRow() throws SQLException {

        Tuple.Builder tupleBuilder = Tuple.newBuilder();
        for (Attribute attr : schema.getAttributes()) {
            String columnName = attr.getName();
            AttributeType columnType = attr.getType();
            String value = curResultSet.getString(columnName);
            if (value == null) {
                tupleBuilder.add(attr, null);
                continue;
            }
            switch (columnType) {
                case INTEGER:
                    tupleBuilder.add(attr, Integer.valueOf(value));
                    break;
                case LONG:
                    tupleBuilder.add(attr, Long.valueOf(value));
                    break;
                case DOUBLE:
                    tupleBuilder.add(attr, Double.valueOf(value));
                    break;
                case BOOLEAN:
                    tupleBuilder.add(attr, !value.equals("0"));
                    break;
                case STRING:
                    tupleBuilder.add(attr, value);
                    break;
                case TIMESTAMP:
                    tupleBuilder.add(attr, Timestamp.valueOf(value));
                    break;
                case ANY:
                default:
                    throw new RuntimeException("MySQL Source: unhandled attribute type: " + columnType);
            }
        }
        return tupleBuilder.build();
    }

    /**
     * Establish a connection to the MySQL server and load statistics for constructing future queries.
     * - tableNames, to check if the input tableName exists on the MySQL server, to prevent SQL injection.
     * - batchColumnBoundaries, to be used to split mini queries, if progressive mode is enabled.
     */
    @Override
    public void open() {
        try {
            connection = establishConn();

            // load user table names from the given database
            loadTableNames();

            // validates the input table name
            if (!tableNames.contains(table))
                throw new RuntimeException("MysqlSource can't find the given table `" + table + "`.");

            // load for batch column value boundaries used to split mini queries
            if (progressive) loadBatchColumnBoundaries();

        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("MysqlSource failed to connect to mysql database. " + e.getMessage());
        }


    }

    private Connection establishConn() throws SQLException {
        String url = "jdbc:mysql://" + host + ":" + port + "/" + database + "?autoReconnect=true&useSSL=true";
        Connection connection = DriverManager.getConnection(url, username, password);
        // set to readonly to improve efficiency
        connection.setReadOnly(true);
        return connection;
    }

    private PreparedStatement getNextQuery() throws SQLException {
        boolean hasNextQuery;

        // if the curLowerBound is still smaller than or equal to the upperBound, send one more query
        if (batchByAttribute == null) hasNextQuery = curLowerBound.longValue() <= upperBound.longValue();
        else switch (batchByAttribute.getType()) {
            case INTEGER:
            case LONG:
            case TIMESTAMP:
                hasNextQuery = curLowerBound.longValue() <= upperBound.longValue();
                break;
            case DOUBLE:
                hasNextQuery = curLowerBound.doubleValue() <= upperBound.doubleValue();
                break;
            case STRING:
            case ANY:
            case BOOLEAN:
            default:
                throw new IllegalStateException("Unexpected value: " + batchByAttribute.getType());
        }

        // no more queries to be sent.
        if (!hasNextQuery) return null;

        PreparedStatement preparedStatement = connection.prepareStatement(generateSqlQuery());
        int curIndex = 1;
        if (column != null && keywords != null) {
            preparedStatement.setString(curIndex, keywords);
            curIndex += 1;
        }
        if (curLimit != null) preparedStatement.setLong(curIndex, curLimit);
        return preparedStatement;

    }

    private void loadBatchColumnBoundaries() throws SQLException {
        if (batchByAttribute != null && !batchByAttribute.getName().isEmpty()) {
            upperBound = getBatchByBoundary("MAX");
            curLowerBound = getBatchByBoundary("MIN");
        }
    }


    private Number getBatchByBoundary(String side) throws SQLException {
        Number result;
        PreparedStatement preparedStatement = connection.prepareStatement(
                "SELECT " + side + "(" + batchByAttribute.getName() + ") FROM " + table + ";");
        ResultSet resultSet = preparedStatement.executeQuery();
        resultSet.next();
        switch (schema.getAttribute(batchByAttribute.getName()).getType()) {
            case INTEGER:
                result = resultSet.getInt(1);
                break;
            case LONG:
                result = resultSet.getLong(1);
                break;
            case TIMESTAMP:
                result = resultSet.getTimestamp(1).getTime();
                break;
            case DOUBLE:
                result = resultSet.getDouble(1);
                break;
            case BOOLEAN:
            case STRING:
            case ANY:
            default:
                throw new IllegalStateException("Unexpected value: " + batchByAttribute.getType());
        }

        resultSet.close();
        preparedStatement.close();
        return result;
    }

    private void loadTableNames() throws SQLException {

        PreparedStatement preparedStatement = connection.prepareStatement(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = ?;");

        preparedStatement.setString(1, database);
        ResultSet resultSet = preparedStatement.executeQuery();
        while (resultSet.next()) tableNames.add(resultSet.getString(1));

        resultSet.close();
        preparedStatement.close();

    }

    /**
     * close resultSet, preparedStatement and connection
     */
    @Override
    public void close() {
        try {
            if (curResultSet != null) curResultSet.close();
            if (curQuery != null) curQuery.close();
            if (connection != null) connection.close();
        } catch (SQLException e) {
            throw new RuntimeException("Mysql source fail to close. " + e.getMessage());
        }
    }

    /**
     * generate sql query string using the info provided by user. One of following
     * select * from TableName where 1 = 1 AND MATCH (ColumnName) AGAINST ( ? IN BOOLEAN MODE) LIMIT ?;
     * select * from TableName where 1 = 1 AND MATCH (ColumnName) AGAINST ( ? IN BOOLEAN MODE);
     * select * from TableName where 1 = 1 LIMIT ?;
     * select * from TableName where 1 = 1;
     * <p>
     * with an optional appropriate batchByColumn sliding window,
     * e.g. create_at >= '2017-01-14 03:47:59.0' AND create_at < '2017-01-15 03:47:59.0'
     *
     * @return string of sql query
     */
    private String generateSqlQuery() {
        // in sql prepared statement, table name cannot be inserted using PreparedStatement.setString
        // so it has to be inserted here during sql query generation
        // this.table has to be verified to be existing in the given schema.
        String query = "\n" + "SELECT * FROM " + table + " where 1 = 1";
        // in sql prepared statement, column name cannot be inserted using PreparedStatement.setString either
        if (column != null && keywords != null) query += " AND MATCH(" + column + ") AGAINST (? IN BOOLEAN MODE)";

        Number nextLowerBound;
        boolean isLastBatch;

        if (!progressive) {
            nextLowerBound = curLowerBound.longValue() + interval;
            isLastBatch = nextLowerBound.longValue() >= upperBound.longValue();
        } else switch (batchByAttribute.getType()) {
            case INTEGER:
            case LONG:
            case TIMESTAMP:
                nextLowerBound = curLowerBound.longValue() + interval;
                isLastBatch = nextLowerBound.longValue() >= upperBound.longValue();
                break;
            case DOUBLE:
                nextLowerBound = curLowerBound.doubleValue() + interval;
                isLastBatch = nextLowerBound.doubleValue() >= upperBound.doubleValue();
                break;
            case BOOLEAN:
            case STRING:
            case ANY:
            default:
                throw new IllegalStateException("Unexpected value: " + batchByAttribute.getType());
        }

        if (progressive) query += " AND "
                + batchByAttribute.getName() + " >= '"
                + batchAttributeToString(curLowerBound) + "'"
                + " AND "
                + batchByAttribute.getName() +
                (isLastBatch ?
                        " <= '" + batchAttributeToString(upperBound) :
                        " < '" + batchAttributeToString(nextLowerBound))
                + "'";

        curLowerBound = nextLowerBound;

        if (curLimit != null) {
            if (curLimit < 0) return null;
            else query += " LIMIT ?";
        }
        query += ";";
        return query;
    }


    /**
     * Convert the Number value to a String to be concatenate to SQL.
     *
     * @param value a Number, contains the value to be converted.
     * @return a String of that value
     * @throws IllegalStateException
     */
    private String batchAttributeToString(Number value) throws IllegalStateException {
        switch (batchByAttribute.getType()) {
            case LONG:
            case INTEGER:
            case DOUBLE:
                return String.valueOf(value);
            case TIMESTAMP:
                return new Timestamp(value.longValue()).toString();

            case BOOLEAN:
            case STRING:
            case ANY:
            default:
                throw new IllegalStateException("Unexpected value: " + batchByAttribute.getType());
        }
    }
}

