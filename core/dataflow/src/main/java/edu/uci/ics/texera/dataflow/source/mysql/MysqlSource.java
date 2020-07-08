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

import java.sql.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class MysqlSource implements ISourceOperator{
	private final MysqlSourcePredicate predicate;
    private int cursor = CLOSED;
    private Schema outputSchema;
    private final Schema.Builder schemaBuilder;
    private Connection connection;
    private ResultSet rs;
    private boolean start = true;

    
    public MysqlSource(MysqlSourcePredicate predicate){
    	this.predicate = predicate;
        this.schemaBuilder = new Schema.Builder();
    }

    @Override
    public void open() throws TexeraException {
        if (cursor == OPENED) {
            return;
        }
        // JDBC connection
        try {
            Class.forName("com.mysql.cj.jdbc.Driver").newInstance();
            String url = "jdbc:mysql://" + predicate.getHost() + ":" + predicate.getPort() + "/"
                    + predicate.getDatabase() + "?autoReconnect=true&useSSL=true";
            this.connection = DriverManager.getConnection(url, predicate.getUsername(), predicate.getPassword());
            connection.setReadOnly(true);
            DatabaseMetaData databaseMetaData = connection.getMetaData();
            ResultSet columns = databaseMetaData.getColumns(null,null, predicate.getTable(), null);
            while(columns.next())
            {
                String columnName = columns.getString("COLUMN_NAME");
                int datatype = columns.getInt("DATA_TYPE");

                AttributeType attributeType;
                switch (datatype) {
                    case Types.BIT: //-7 Types.BIT
                    case Types.TINYINT: //-6 Types.TINYINT
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
                    case Types.BIGINT: //-5 Types.BIGINT
                    case Types.CHAR: //1 Types.CHAR
                    case Types.VARCHAR: //12 Types.VARCHAR
                    case Types.LONGVARCHAR: //-1 Types.LONGVARCHAR
                    case Types.NULL: //0 Types.NULL
                    case Types.OTHER: //1111 Types.OTHER
                    case Types.BOOLEAN: //16 Types.BOOLEAN
                    default:
                        attributeType = AttributeType.STRING;
                        break;
                }
                this.schemaBuilder.add(columnName, attributeType);
            }
            this.outputSchema = this.schemaBuilder.build();
            cursor = OPENED;
        } catch (SQLException | InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new DataflowException("MysqlSink failed to connect to mysql database." + e.getMessage());
        }
    }
    
    @Override
    public Tuple getNextTuple() throws TexeraException {
        if (cursor == CLOSED) {
            throw new DataflowException(ErrorMessages.OPERATOR_NOT_OPENED);
        }
        try {
            if (start) {
                PreparedStatement ps = this.connection.prepareStatement(generateSqlQuery(predicate));
                int nextIndex = 1;
                if (!predicate.getColumn().equals("") && !predicate.getKeywords().isEmpty()) {
                    StringBuilder keywords = new StringBuilder();
                    for (int i = 0; i < predicate.getKeywords().size(); i++) {
                        keywords.append(" (");
                        for (int j = 0; j < predicate.getKeywords().get(i).size(); j++) {
                            keywords.append(" +").append(predicate.getKeywords().get(i).get(j));
                        }
                        keywords.append(" )");
                    }
                    ps.setString(nextIndex, keywords.toString());
                    nextIndex += 1;
                }
                if (predicate.getLimit() != Integer.MAX_VALUE) {
                    ps.setObject(nextIndex, predicate.getLimit(), Types.INTEGER);
                    nextIndex += 1;
                }
                if (predicate.getOffset() != 0) {
                    ps.setObject(nextIndex, predicate.getOffset(), Types.INTEGER);
                }
                System.out.println(ps.toString());
                this.rs = ps.executeQuery();
                start = false;
//                ps.close();
            }

			while (rs.next()) {
			    List<IField> tb = new ArrayList();
			    for(Attribute a: this.outputSchema.getAttributes()){
			        if (a.getType() == AttributeType.STRING){
			            String value = rs.getString(a.getName());
			            value = value ==null? "":value;
			            tb.add(new StringField(value));
                    }else if (a.getType() == AttributeType.INTEGER){
                        String value = rs.getString(a.getName());
                        // allowing null value Integer to be in the workflow
                        if (value != null) {
                            tb.add(new IntegerField(new Integer(value)));
                        } else {
                            tb.add(new IntegerField(null));
                        }
                    }else if (a.getType() == AttributeType.DOUBLE){
                        String value = rs.getString(a.getName());
                        // allowing null value Double to be in the workflow
                        if (value != null) {
                            tb.add(new DoubleField(new Double(value)));
                        } else {
                            tb.add(new DoubleField(null));
                        }
                    }else if (a.getType() == AttributeType.DATE){
                        Date value = rs.getDate(a.getName());
                        // allowing null value DateField to be in the workflow
                        tb.add(new DateField(value));
                    }else if (a.getType() == AttributeType.DATETIME){
                        String value = rs.getString(a.getName());
                        tb.add(new DateTimeField((value)));
                    }
                }
			    IField[] iFieldArray = tb.toArray(new IField[0]);
                Tuple tuple = new Tuple(this.outputSchema, iFieldArray);
                cursor ++;
			    return tuple;
			}
		} catch (SQLException e) {
            throw new DataflowException(e.getMessage());
		}
        return null;
    }
    
    public static String generateSqlQuery(MysqlSourcePredicate predicate) {
        //
        String query =  "\n" +
                "select * from "+ predicate.getTable() +" where 1 = 1 ";
        if(!predicate.getColumn().equals("") && !predicate.getKeywords().isEmpty()) {
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
        System.out.println(query);
        return query;
    }
    
    
    @Override
    public void close() throws TexeraException {
        if (cursor == CLOSED) {
            return;
        }
        try {
        	connection.close();
        	cursor = CLOSED;
        }catch (SQLException e) {
            throw new DataflowException("MysqlSink fail to close. " + e.getMessage());
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