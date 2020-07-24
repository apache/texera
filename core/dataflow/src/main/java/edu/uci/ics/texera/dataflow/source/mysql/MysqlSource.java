package edu.uci.ics.texera.dataflow.source.mysql;

import edu.uci.ics.texera.api.constants.ErrorMessages;
import edu.uci.ics.texera.api.dataflow.ISourceOperator;
import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.field.*;
import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.Tuple;
import org.jooq.meta.derby.sys.Sys;

import java.sql.*;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class MysqlSource implements ISourceOperator {
	private final MysqlSourcePredicate predicate;
    private int status = CLOSED;
    private Schema outputSchema;
    private final Schema.Builder schemaBuilder;
    private Connection connection;
    private ResultSet rs;
    private boolean querySent = false;

    
    public MysqlSource(MysqlSourcePredicate predicate) {
    	this.predicate = predicate;
        this.schemaBuilder = new Schema.Builder();
    }

    @Override
    public void open() throws TexeraException {
        if (status == OPENED) {
            return;
        }
        // JDBC connection
        try {
            Class.forName("com.mysql.cj.jdbc.Driver").newInstance();
            String url = "jdbc:mysql://" + predicate.getHost() + ":" + predicate.getPort() + "/"
                    + predicate.getDatabase() + "?autoReconnect=true&useSSL=true";
            this.connection = DriverManager.getConnection(url, predicate.getUsername(), predicate.getPassword());
            // set to readonly to improve efficiency
            connection.setReadOnly(true);
            DatabaseMetaData databaseMetaData = connection.getMetaData();
            ResultSet columns = databaseMetaData.getColumns(null,null, predicate.getTable(), null);
            while(columns.next()) {
                String columnName = columns.getString("COLUMN_NAME");
                int datatype = columns.getInt("DATA_TYPE");
                AttributeType attributeType;
                switch (datatype) {
                    case Types.SMALLINT: //5 Types.SMALLINT
                    case Types.INTEGER: //4 Types.INTEGER
                    case Types.BINARY: //-2 Types.BINARY
                        attributeType = AttributeType.INTEGER;
                        break;
                    case Types.FLOAT: //6 Types.FLOAT
                    case Types.REAL: //7 Types.REAL
                    case Types.DOUBLE: //8 Types.DOUBLE
                    case Types.NUMERIC: //3 Types.NUMERIC
                        attributeType = AttributeType.DOUBLE;
                        break;
                    case Types.DATE: //91 Types.DATE
                        attributeType = AttributeType.DATE;
                        break;
                    case Types.TIME: //92 Types.TIME
                    case Types.TIMESTAMP:  //93 Types.TIMESTAMP
                        attributeType = AttributeType.DATETIME;
                        break;
                    case Types.TINYINT: //-6 Types.TINYINT
                    case Types.BOOLEAN: //16 Types.BOOLEAN
                    case Types.BIT: //-7 Types.BIT
                        attributeType = AttributeType.BOOLEAN;
                        break;
                    case Types.LONGVARCHAR: //-1 Types.LONGVARCHAR
                        attributeType = AttributeType.TEXT;
                        break;
                    case Types.BIGINT: //-5 Types.BIGINT
                    case Types.CHAR: //1 Types.CHAR
                    case Types.VARCHAR: //12 Types.VARCHAR
                    case Types.NULL: //0 Types.NULL
                    case Types.OTHER: //1111 Types.OTHER
                    default:
                        attributeType = AttributeType.STRING;
                        break;
                }
                this.schemaBuilder.add(columnName, attributeType);
            }
            this.outputSchema = this.schemaBuilder.build();
            status = OPENED;
        } catch (SQLException | InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new DataflowException("MysqlSink failed to connect to mysql database." + e.getMessage());
        }
    }
    
    @Override
    public Tuple getNextTuple() throws TexeraException {
        if (status == CLOSED) {
            throw new DataflowException(ErrorMessages.OPERATOR_NOT_OPENED);
        }
        try {
            if (!querySent) {
                PreparedStatement ps = this.connection.prepareStatement(generateSqlQuery(predicate));
                int curIndex = 1;
                if (!predicate.getColumn().isEmpty() && !predicate.getKeywords().isEmpty()) {
                    ps.setString(curIndex, predicate.getKeywords());
                    curIndex += 1;
                }
                if (predicate.getLimit() != Integer.MAX_VALUE) {
                    ps.setObject(curIndex, predicate.getLimit(), Types.INTEGER);
                    curIndex += 1;
                }
                if (predicate.getOffset() != 0) {
                    ps.setObject(curIndex, predicate.getOffset(), Types.INTEGER);
                }
                this.rs = ps.executeQuery();
                querySent = true;
            }

            while (rs.next()) {
			    List<IField> row = new ArrayList();
			    for(Attribute a: this.outputSchema.getAttributes()) {
			        AttributeType attrType = a.getType();
                    String value = rs.getString(a.getName());
                    switch (attrType) {
                        case STRING:
                            value = value ==null? "":value;
                            row.add(new StringField(value));
                            break;
                        case TEXT:
                            value = value ==null? "":value;
                            row.add(new TextField(value));
                            break;
                        case INTEGER:
                            // allowing null value Integer to be in the workflow
                            if (value != null) {
                                row.add(new IntegerField(new Integer(value)));
                            } else {
                                row.add(new IntegerField(null));
                            }
                            break;
                        case DOUBLE:
                            if (value != null) {
                                row.add(new DoubleField(new Double(value)));
                            } else {
                                row.add(new DoubleField(null));
                            }
                            break;
                        case DATE:
                            row.add(new DateField(value));
                            break;
                        case DATETIME:
                            // a formatter is needed because
                            // mysql format is    yyyy-MM-dd HH:mm:ss
                            // but java format is yyyy-MM-ddTHH:mm:ss by default
                            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                            row.add(new DateTimeField(value,formatter));
                            break;
                        case BOOLEAN:
                            if (value.equals("0")) {
                                row.add(new StringField("false"));
                            } else {
                                row.add(new StringField("true"));
                            }
                    }
                }
			    IField[] iFieldArray = row.toArray(new IField[0]);
                return new Tuple(this.outputSchema, iFieldArray);
			}
		} catch (SQLException e) {
            throw new DataflowException(e.getMessage());
		}
        return null;
    }
    
    public static String generateSqlQuery(MysqlSourcePredicate predicate) {
        // in sql prepared statement, table name cannot be inserted using preparedstatement.setString
        // so it has to be inserted here during sql query generation
        String query =  "\n" + "select * from "+ predicate.getTable() +" where 1 = 1 ";
        // in sql prepared statement, column name cannot be inserted using preparedstatement.setString either
        if(!predicate.getColumn().isEmpty() && !predicate.getKeywords().isEmpty()) {
            query += " AND  MATCH( " + predicate.getColumn() + " )  AGAINST ( ? IN BOOLEAN MODE)";
        }
        if(predicate.getLimit() != Integer.MAX_VALUE){
            query += " LIMIT ?";
        }
        if(predicate.getOffset() != 0) {
            if(predicate.getLimit() == Integer.MAX_VALUE) {
                // if there is no limit, for OFFSET to work, a arbitrary LARGE number
                // need to be manually provided
                query += "limit 999999999999999";
            }
            query += " OFFSET ?";
        }
        query+=";";
        return query;
    }

    @Override
    public void close() throws TexeraException {
        if (status == CLOSED) {
            return;
        }
        try {
        	connection.close();
            status = CLOSED;
        }catch (SQLException e) {
            throw new DataflowException("Mysql source fail to close. " + e.getMessage());
        }
    }

    @Override
    public Schema getOutputSchema() {
        return this.outputSchema;
    }

    public Schema transformToOutputSchema(Schema... inputSchema) throws DataflowException {
        throw new TexeraException(ErrorMessages.INVALID_INPUT_SCHEMA_FOR_SOURCE);
    }

	
}