package edu.uci.ics.texera.workflow.operators.mysqlsource;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import edu.uci.ics.amber.engine.operators.OpExecConfig;
import edu.uci.ics.texera.workflow.common.metadata.OperatorGroupConstants;
import edu.uci.ics.texera.workflow.common.metadata.OperatorInfo;
import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorDescriptor;
import edu.uci.ics.texera.workflow.common.tuple.schema.Attribute;
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeType;
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema;

import java.sql.*;

public class MysqlSourceOpDesc extends SourceOperatorDescriptor {

    @JsonProperty(value = "host", required = true)
    @JsonPropertyDescription("mysql host IP address")
    public String host;

    @JsonProperty(value = "port", required = true, defaultValue = "3306")
    @JsonPropertyDescription("mysql host port")
    public String port;

    @JsonProperty(value = "database", required = true)
    @JsonPropertyDescription("mysql database name")
    public String database;

    @JsonProperty(value = "table", required = true)
    @JsonPropertyDescription("mysql table name")
    public String table;

    @JsonProperty(value = "username", required = true)
    @JsonPropertyDescription("mysql username")
    public String username;

    @JsonProperty(value = "password", required = true)
    @JsonPropertyDescription("mysql user password")
    public String password;

    @JsonProperty(value = "limit")
    @JsonPropertyDescription("query result count upper limit")
    public Integer limit;

    @JsonProperty(value = "offset")
    @JsonPropertyDescription("query offset")
    public Integer offset;

    @JsonProperty(value = "column name")
    @JsonPropertyDescription("the column to be keyword-searched")
    public String column;

    @JsonProperty(value = "keywords")
    @JsonPropertyDescription("search terms in boolean expression")
    public String keywords;

    @Override
    public OpExecConfig operatorExecutor() {
        return new MysqlSourceOpExecConfig(this.operatorIdentifier(), worker -> new MysqlSourceOpExec(
                this.querySchema(),
                host,
                port,
                database,
                table,
                username,
                password,
                limit,
                offset,
                column,
                keywords
        ));
    }

    @Override
    public OperatorInfo operatorInfo() {
        return new OperatorInfo(
                "Mysql Source",
                "Read data from a mysql instance",
                OperatorGroupConstants.SOURCE_GROUP(),
                0, 1);
    }

    /**
     * make sure all the required parameters are not empty,
     * then query the remote Mysql server for the table schema
     *
     * @return Texera.tuple.schama
     */
    @Override
    public Schema sourceSchema() {
        if (this.host == null || this.port == null || this.database == null
                || this.table == null || this.username == null || this.password == null) {
            return null;
        }
        return querySchema();
    }

    /**
     * establish a mysql connection with remote mysql server base on the info provided by the user
     * query the MetaData of the table and generate a Texera.tuple.schema accordingly
     * the "switch" code block shows how mysql datatypes are mapped to Texera AttributeTypes
     *
     * @return
     * @throws Exception
     */
    private Schema querySchema() {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        try {
            Class.forName("com.mysql.cj.jdbc.Driver").newInstance();
            String url = "jdbc:mysql://" + this.host + ":" + this.port + "/"
                    + this.database + "?autoReconnect=true&useSSL=true";
            Connection connection = DriverManager.getConnection(url, this.username, this.password);
            // set to readonly to improve efficiency
            connection.setReadOnly(true);
            DatabaseMetaData databaseMetaData = connection.getMetaData();
            ResultSet columns = databaseMetaData.getColumns(null, null, this.table, null);
            while (columns.next()) {
                String columnName = columns.getString("COLUMN_NAME");
                int datatype = columns.getInt("DATA_TYPE");
                switch (datatype) {
                    case Types.BIT: //-7 Types.BIT
                    case Types.TINYINT: //-6 Types.TINYINT
                    case Types.SMALLINT: //5 Types.SMALLINT
                    case Types.INTEGER: //4 Types.INTEGER
                        schemaBuilder.add(new Attribute(columnName, AttributeType.INTEGER));
                        break;
                    case Types.FLOAT: //6 Types.FLOAT
                    case Types.REAL: //7 Types.REAL
                    case Types.DOUBLE: //8 Types.DOUBLE
                    case Types.NUMERIC: //3 Types.NUMERIC
                        schemaBuilder.add(new Attribute(columnName, AttributeType.DOUBLE));
                        break;
                    case Types.BOOLEAN: //16 Types.BOOLEAN
                        schemaBuilder.add(new Attribute(columnName, AttributeType.BOOLEAN));
                        break;
                    case Types.BINARY: //-2 Types.BINARY
                    case Types.DATE: //91 Types.DATE
                    case Types.TIME: //92 Types.TIME
                    case Types.TIMESTAMP:  //93 Types.TIMESTAMP
                    case Types.LONGVARCHAR: //-1 Types.LONGVARCHAR
                    case Types.BIGINT: //-5 Types.BIGINT
                    case Types.CHAR: //1 Types.CHAR
                    case Types.VARCHAR: //12 Types.VARCHAR
                    case Types.NULL: //0 Types.NULL
                    case Types.OTHER: //1111 Types.OTHER
                        schemaBuilder.add(new Attribute(columnName, AttributeType.STRING));
                        break;
                    default:
                        throw new RuntimeException("MySQL Source: unknown data type: " + datatype);
                }
            }
            connection.close();
            return schemaBuilder.build();
        } catch (SQLException | InstantiationException | IllegalAccessException | ClassNotFoundException | ClassCastException e) {
            e.printStackTrace();
            throw new RuntimeException("Mysql Source failed to connect to mysql database." + e.getMessage());
        }
    }


}
