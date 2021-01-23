package edu.uci.ics.texera.workflow.operators.mysqlsource;

import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorExecutor;
import edu.uci.ics.texera.workflow.common.tuple.Tuple;
import edu.uci.ics.texera.workflow.common.tuple.schema.Attribute;
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeType;
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema;
import scala.collection.Iterator;

import java.sql.*;
import java.util.HashSet;

public class MysqlSourceOpExec implements SourceOperatorExecutor {
    private final Schema schema;
    private final String host;
    private final String port;
    private final String database;
    private final String table;
    private final String username;
    private final String password;

    private final String column;
    private final String keywords;
    private final Boolean progressive;
    private final Long interval;

    private Connection connection;
    private PreparedStatement currentPreparedStatement;
    private ResultSet resultSet;
    private final HashSet<String> tableNames;
    private boolean queriesPrepared = false;

    private Long currentLimit;
    private Long currentOffset;
    private Number currentMin = 0;
    private Number max = 0;
    private Attribute batchByAttribute = null;

    private Tuple cachedTuple = null;

    MysqlSourceOpExec(Schema schema, String host, String port, String database, String table, String username,
                      String password, Long limit, Long offset, String column, String keywords,
                      Boolean progressive, String batchByColumn, Long interval) {
        this.schema = schema;
        this.host = host.trim();
        this.port = port.trim();
        this.database = database.trim();
        this.table = table.trim();
        this.username = username.trim();
        this.password = password;
        this.currentLimit = limit;
        this.currentOffset = offset;
        this.column = column == null ? null : column.trim();
        this.keywords = keywords == null ? null : keywords.trim();
        this.progressive = progressive;
        this.interval = interval;
        this.tableNames = new HashSet<>();
        if (batchByColumn != null) {
            this.batchByAttribute = schema.getAttribute(batchByColumn);
        }

    }

    /**
     * @return A iterator of Texera.Tuple
     */
    @Override
    public Iterator<Tuple> produceTexeraTuple() {
        return new Iterator<Tuple>() {
            /**
             * check if query is sent to mysql server and hasNext flag is true
             * @return bool
             */
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
             * get the next row from the resultSet and parse it into Texera Tuple
             * if there is no more in resultSet, set the hasNext flag to false and return null
             * otherwise, base on the the schema given (which is generated in MysqlSourceOpDesc.querySchema())
             * extract data from resultSet and add to tupleBuilder to construct a Texera.tuple
             *
             * @return Texera.Tuple
             */
            @Override
            public Tuple next() {

                // if has the next Tuple in cache, return it and clear the cache.
                if (cachedTuple != null) {
                    Tuple tuple = cachedTuple;
                    cachedTuple = null;
                    return tuple;
                }

                // otherwise, send query for next Tuple.
                try {
                    if (resultSet != null && resultSet.next()) {
                        if (currentOffset != null && currentOffset > 0) {
                            currentOffset--;
                            return next();
                        }
                        Tuple.Builder tupleBuilder = Tuple.newBuilder();
                        for (Attribute attr : schema.getAttributes()) {
                            String columnName = attr.getName();
                            AttributeType columnType = attr.getType();
                            String value = resultSet.getString(columnName);
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
                        if (currentLimit != null) {
                            currentLimit--;
                        }
                        return tupleBuilder.build();
                    } else {
                        if (resultSet != null) {
                            resultSet.close();
                        }
                        if (currentPreparedStatement != null) {
                            currentPreparedStatement.close();
                        }

                        currentPreparedStatement = getNextQuery();
                        if (currentPreparedStatement != null) {
                            resultSet = currentPreparedStatement.executeQuery();
                            return next();
                        } else {
                            return null;
                        }

                    }

                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    /**
     * during the open process, a connection is established with the remote mysql server
     * then an sql query is generated using the info provided by the user.
     * A prepared statement is used to prevent sql injection attacks
     * Since user might provide info in a combination of column, keywords, limit and offset
     * the prepared statement can have different number of parameters.
     * A variable curIndex is used to keep track of the next parameter should be filled in
     */
    @Override
    public void open() {
        try {
            if (!queriesPrepared) {
                connection = this.establishConn();

                // load user table names from the given database
                loadTableNames();

                if (!tableNames.contains(table)) {
                    throw new RuntimeException("MysqlSource can't find the given table `" + table + "`.");
                }

                if (progressive) {
                    // load for batch statistics used to split mini queries
                    loadBatchColumnStats();
                }
                queriesPrepared = true;

            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("MysqlSource failed to connect to mysql database. " + e.getMessage());
        }

    }

    private Connection establishConn() throws ClassNotFoundException, IllegalAccessException, InstantiationException, SQLException {
        Class.forName("com.mysql.cj.jdbc.Driver").newInstance();
        String url = "jdbc:mysql://" + this.host + ":" + this.port + "/"
                + this.database + "?autoReconnect=true&useSSL=true";
        Connection connection = DriverManager.getConnection(url, this.username, this.password);
        // set to readonly to improve efficiency
        connection.setReadOnly(true);
        return connection;
    }

    private PreparedStatement getNextQuery() throws SQLException {
        boolean hasNextQuery;
        switch (batchByAttribute.getType()) {
            case INTEGER:
            case LONG:
            case TIMESTAMP:
                hasNextQuery = currentMin.longValue() <= max.longValue();
                break;
            case DOUBLE:
                hasNextQuery = currentMin.doubleValue() <= max.doubleValue();
                break;
            case STRING:
            case ANY:
            case BOOLEAN:
            default:
                throw new IllegalStateException("Unexpected value: " + schema.getAttribute(batchByAttribute.getName()).getType());
        }
        if (batchByAttribute.getType() == AttributeType.LONG || batchByAttribute.getType() == AttributeType.TIMESTAMP) {
            hasNextQuery = currentMin.longValue() <= max.longValue();
        }
        if (hasNextQuery) {
            PreparedStatement preparedStatement = this.connection.prepareStatement(generateSqlQuery());
            int curIndex = 1;
            if (column != null && keywords != null) {
                preparedStatement.setString(curIndex, keywords);
                curIndex += 1;
            }
            if (currentLimit != null) {
                preparedStatement.setLong(curIndex, currentLimit);
            }
            return preparedStatement;
        } else return null;
    }

    private void loadBatchColumnStats() {
        try {
            if (batchByAttribute != null && !batchByAttribute.getName().equals("")) {

                max = this.getBatchByBoundary("MAX");
                currentMin = this.getBatchByBoundary("MIN");

            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("MysqlSource fail to load statistics on column `" + batchByAttribute.getName() + "`. " + e.getMessage());
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
                throw new IllegalStateException("Unexpected value: " + schema.getAttribute(batchByAttribute.getName()).getType());

        }

        // MAX is set to be 1 larger than the largest data, so that x < MAX can be outputted

        resultSet.close();
        preparedStatement.close();
        return result;
    }

    private void loadTableNames() throws SQLException {

        PreparedStatement preparedStatement = connection.prepareStatement(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = ?;");

        preparedStatement.setString(1, database);
        ResultSet resultSet = preparedStatement.executeQuery();
        while (resultSet.next()) {
            tableNames.add(resultSet.getString(1));
        }

        resultSet.close();
        preparedStatement.close();

    }

    /**
     * close resultSet, preparedStatement and connection
     */
    @Override
    public void close() {
        try {
            if (resultSet != null) {
                resultSet.close();
            }
            if (currentPreparedStatement != null) {
                currentPreparedStatement.close();
            }
            if (connection != null) {
                connection.close();
            }
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
     *
     * @return string of sql query
     */
    private String generateSqlQuery() {
        // in sql prepared statement, table name cannot be inserted using preparedstatement.setString
        // so it has to be inserted here during sql query generation
        // this.table has to be verified to be existing in the given schema.
        String query = "\n" + "SELECT * FROM " + table + " where 1 = 1";
        // in sql prepared statement, column name cannot be inserted using preparedstatement.setString either
        if (column != null && keywords != null) {
            query += " AND MATCH(" + column + ") AGAINST (? IN BOOLEAN MODE)";
        }

        Number nextMin;
        boolean lastBatch;
        switch (batchByAttribute.getType()) {
            case INTEGER:
            case LONG:
            case TIMESTAMP:
                nextMin = currentMin.longValue() + interval;
                lastBatch = nextMin.longValue() >= max.longValue();
                break;
            case DOUBLE:
                nextMin = currentMin.doubleValue() + interval;
                lastBatch = nextMin.doubleValue() >= max.doubleValue();
                break;
            case BOOLEAN:
            case STRING:
            case ANY:
            default:
                throw new IllegalStateException("Unexpected value: " + schema.getAttribute(batchByAttribute.getName()).getType());
        }

        if (progressive) {
            query += " AND "
                    + batchByAttribute.getName() + " >= '"
                    + batchAttributeToString(currentMin) + "'"
                    + " AND "
                    + batchByAttribute.getName() +
                    (lastBatch ? (" <= '" + batchAttributeToString(max)) : (" < '" + batchAttributeToString(nextMin))) + "'";
        }
        currentMin = nextMin;

        if (currentLimit != null) {
            if (currentLimit < 0) {
                return null;
            }
            query += " LIMIT ?";
        }
        query += ";";
        System.out.println(query);
        return query;
    }


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
                throw new IllegalStateException("Unexpected value: " + schema.getAttribute(batchByAttribute.getName()).getType());
        }
        return null;
    }
}

