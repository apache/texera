package edu.uci.ics.texera.workflow.operators.mysqlsource;

import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorExecutor;
import edu.uci.ics.texera.workflow.common.tuple.Tuple;
import edu.uci.ics.texera.workflow.common.tuple.schema.Attribute;
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeType;
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema;
import scala.collection.Iterator;

import java.sql.*;

public class MysqlSourceOpExec implements SourceOperatorExecutor {
    private final Schema schema;
    private final String host;
    private final String port;
    private final String database;
    private final String table;
    private final String username;
    private final String password;
    private final Integer limit;
    private final Integer offset;
    private final String column;
    private final String keywords;

    private Connection connection;
    private PreparedStatement preparedStatement;
    private ResultSet resultSet;
    private boolean querySent = false;
    private boolean hasNext = true;

    MysqlSourceOpExec(Schema schema, String host, String port, String database, String table, String username,
                      String password, Integer limit, Integer offset, String column, String keywords) {
        this.schema = schema;
        this.host = host.trim();
        this.port = port.trim();
        this.database = database.trim();
        this.table = table.trim();
        this.username = username.trim();
        this.password = password;
        this.limit = limit;
        this.offset = offset;
        if (column!=null) {
            this.column = column.trim();
        } else {
            this.column = null;
        }
        if(keywords!=null) {
            this.keywords = keywords.trim();
        } else {
            this.keywords = null;
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
                return querySent && hasNext;
            }

            /**
             * get the next row from the resultSet and parse it into Texera Tuple
             * if there is no more in resultSet, set the hasNext flag to false and return null
             * otherwise, base on the the schema given (which is generated in MysqlSourceOpDesc.querySchema())
             * extract data from resultSet and add to tupleBuilder to construct a Texera.tuple
             * @return Texera.tuple
             */
            @Override
            public Tuple next() {
                try {
                    if (resultSet.next()) {
                        Tuple.Builder tupleBuilder = Tuple.newBuilder();
                        for (Attribute attr : schema.getAttributes()) {
                            String columnName = attr.getName();
                            AttributeType columnType = attr.getType();
                            String value = resultSet.getString(columnName);
                            if (value == null) {
                                tupleBuilder.add(attr,null);
                                continue;
                            }
                            switch (columnType) {
                                case INTEGER:
                                    tupleBuilder.add(attr, Integer.valueOf(value));
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
                                case ANY:
                                default:
                                    throw new RuntimeException("MySQL Source: unhandled attribute type: " + columnType);
                            }
                        }
                        return tupleBuilder.build();
                    } else {
                        hasNext = false;
                        return null;
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
     * A varaible curIndex is used to keep track of there the next parameter should be filled in
     */
    @Override
    public void open() {
        try {
            if (!querySent) {
                Class.forName("com.mysql.cj.jdbc.Driver").newInstance();
                String url = "jdbc:mysql://" + this.host + ":" + this.port + "/"
                        + this.database + "?autoReconnect=true&useSSL=true";
                this.connection = DriverManager.getConnection(url, this.username, this.password);
                // set to readonly to improve efficiency
                connection.setReadOnly(true);
                preparedStatement = this.connection.prepareStatement(generateSqlQuery());
                int curIndex = 1;
                if (this.column != null && this.keywords != null) {
                    preparedStatement.setString(curIndex, this.keywords);
                    curIndex += 1;
                }
                if (this.limit != null) {
                    preparedStatement.setInt(curIndex, this.limit);
                    curIndex += 1;
                }
                if (this.offset != null) {
                    preparedStatement.setObject(curIndex, this.offset, Types.INTEGER);
                }
                this.resultSet = preparedStatement.executeQuery();
                querySent = true;
            }
        } catch (Exception e) {
            throw new RuntimeException("MysqlSource failed to connect to mysql database." + e.getMessage());
        }

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
            if (preparedStatement != null) {
                preparedStatement.close();
            }
            if (connection != null) {
                connection.close();
            }
        }catch (SQLException e) {
            throw new RuntimeException("Mysql source fail to close. " + e.getMessage());
        }
    }

    /**
     * generate sql query string using the info provided by user. One of following
     * select * from TableName where 1 = 1 AND MATCH (ColumnName) AGAINST ( ? IN BOOLEAN MODE) LIMIT ? OFFSET ?;
     * select * from TableName where 1 = 1 AND MATCH (ColumnName) AGAINST ( ? IN BOOLEAN MODE) LIMIT 999999999999999 OFFSET ?;
     * select * from TableName where 1 = 1 AND MATCH (ColumnName) AGAINST ( ? IN BOOLEAN MODE) LIMIT ?;
     * select * from TableName where 1 = 1 AND MATCH (ColumnName) AGAINST ( ? IN BOOLEAN MODE);
     * select * from TableName where 1 = 1 LIMIT ? OFFSET ?;
     * select * from TableName where 1 = 1 LIMIT 999999999999999 OFFSET ?;
     * select * from TableName where 1 = 1 LIMIT ?;
     * select * from TableName where 1 = 1;
     * @return string of sql query
     */
    private String generateSqlQuery() {
        // in sql prepared statement, table name cannot be inserted using preparedstatement.setString
        // so it has to be inserted here during sql query generation
        String query =  "\n" + "select * from "+ this.table +" where 1 = 1 ";
        // in sql prepared statement, column name cannot be inserted using preparedstatement.setString either
        if(this.column != null && this.keywords != null) {
            query += " AND  MATCH( " + this.column + " )  AGAINST ( ? IN BOOLEAN MODE)";
        }
        if(this.limit != null){
            query += " LIMIT ?";
        }
        if(this.offset != null) {
            if(this.limit == null) {
                // if there is no limit, for OFFSET to work, a arbitrary LARGE number
                // need to be manually provided
                query += "LIMIT 999999999999999";
            }
            query += " OFFSET ?";
        }
        query+=";";
        return query;
    }
}