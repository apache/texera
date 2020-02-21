package edu.uci.ics.texera.dataflow.source.mysql;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.sql.DatabaseMetaData;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.ArrayList;
import java.util.Date;
import java.sql.Timestamp;
import java.text.ParseException;


import edu.uci.ics.texera.api.constants.ErrorMessages;
import edu.uci.ics.texera.api.constants.SchemaConstants;
import edu.uci.ics.texera.api.dataflow.IOperator;
import edu.uci.ics.texera.api.dataflow.ISink;
import edu.uci.ics.texera.api.dataflow.ISourceOperator;
import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.field.DoubleField;
import edu.uci.ics.texera.api.field.IDField;
import edu.uci.ics.texera.api.field.IField;
import edu.uci.ics.texera.api.field.IntegerField;
import edu.uci.ics.texera.api.field.StringField;
import edu.uci.ics.texera.api.field.TextField;
import edu.uci.ics.texera.api.field.DateField;
import edu.uci.ics.texera.api.field.DateTimeField;
import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.dataflow.twitterfeed.TwitterUtils;

public class MysqlSource implements ISourceOperator{
	private final MysqlSourcePredicate predicate;
    private int cursor = CLOSED;
    private Schema outputSchema;
    private Schema.Builder schemaBuilder;
    private Connection connection;
    private Statement statement;
    private PreparedStatement prepStatement;
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
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            String url = "jdbc:mysql://" + predicate.getHost() + ":" + predicate.getPort() + "/"
                    + predicate.getDatabase() + "?autoReconnect=true&useSSL=true";
            this.connection = DriverManager.getConnection(url, predicate.getUsername(), predicate.getPassword());

            DatabaseMetaData databaseMetaData = connection.getMetaData();
            ResultSet columns = databaseMetaData.getColumns(null,null, null, null);
            while(columns.next())
            {
                String columnName = columns.getString("COLUMN_NAME");
                Integer datatype = columns.getInt("DATA_TYPE");

                AttributeType attributeType;
                switch (datatype) {
                    case -7:  attributeType = AttributeType.INTEGER; //-7 Types.BIT
                        break;
                    case -6:  attributeType = AttributeType.INTEGER; //-6 Types.TINYINT
                        break;
                    case 5:  attributeType = AttributeType.INTEGER; //5 Types.SMALLINT
                        break;
                    case 4:  attributeType = AttributeType.INTEGER; //4 Types.INTEGER
                        break;
                    case -5:  attributeType = AttributeType.STRING; //-5 Types.BIGINT
                        break;
                    case 6:  attributeType = AttributeType.DOUBLE; //6 Types.FLOAT
                        break;
                    case 7:  attributeType = AttributeType.DOUBLE; //7 Types.REAL
                        break;
                    case 8:  attributeType = AttributeType.DOUBLE; //8 Types.DOUBLE
                        break;
                    case 3:  attributeType = AttributeType.DOUBLE; //3 Types.NUMERIC
                        break;
                    case 1: attributeType = AttributeType.STRING; //1 Types.CHAR
                        break;
                    case 12: attributeType = AttributeType.STRING; //12 Types.VARCHAR
                        break;
                    case -1: attributeType = AttributeType.STRING; //-1 Types.LONGVARCHAR
                        break;
                    case 91: attributeType = AttributeType.DATE; //91 Types.DATE
                        break;
                    case 92: attributeType = AttributeType.DATETIME; //92 Types.TIME
                        break;
                    case 93: attributeType = AttributeType.DATETIME; //93 Types.TIMESTAMP
                        break;
                    case -2: attributeType = AttributeType.INTEGER; //-2 Types.BINARY
                        break;
                    case 0: attributeType = AttributeType.STRING; //0 Types.NULL
                        break;
                    case 1111: attributeType = AttributeType.STRING; //1111 Types.OTHER
                        break;
                    case 16: attributeType = AttributeType.STRING; //16 Types.BOOLEAN
                        break;
                    default: attributeType = AttributeType.STRING;
                        break;
                }

                this.outputSchema = this.schemaBuilder.add(columnName, attributeType).build();

            }
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
            if(start){
                System.out.print(generateSqlQuery(predicate));
                PreparedStatement ps = this.connection.prepareStatement(generateSqlQuery(predicate));
                /*if(!predicate.getColumn().equals("") && !predicate.getKeywords().equals("")){
                    ps.setObject(1, predicate.getColumn(), Types.VARCHAR);
                    ps.setObject(2, predicate.getKeywords(), Types.VARCHAR);
                    if(predicate.getLimit()!=Integer.MAX_VALUE){
                        ps.setObject(3, predicate.getLimit(), Types.INTEGER);
                    }
                }
                else{*/
                if(predicate.getLimit()!=Integer.MAX_VALUE){
                    ps.setObject(1, predicate.getLimit(), Types.INTEGER);
                }
                this.rs = ps.executeQuery();
                start = false;}
            //}
			while (rs.next()) {
			    List<IField> tb = new ArrayList();
			    for(Attribute a: this.outputSchema.getAttributes()){
			        if (a.getType() == AttributeType.STRING){
			            String value = rs.getString(a.getName());
			            value = value ==null? "":value;
			            tb.add(new StringField(value));
                    }else if (a.getType() == AttributeType.INTEGER){
                        String value = rs.getString(a.getName());
                        value = value ==null? "0":value;
                        tb.add(new IntegerField(Integer.valueOf(value)));
                    }else if (a.getType() == AttributeType.DOUBLE){
                        String value = rs.getString(a.getName());
                        value = value ==null? "0.0":value;
                        tb.add(new DoubleField(Double.valueOf(value)));
                    }else if (a.getType() == AttributeType.INTEGER){
                        String value = rs.getString(a.getName());
                        value = value ==null? "0":value;
                        tb.add(new IntegerField(Integer.valueOf(value)));
                    }else if (a.getType() == AttributeType.DATE){
                        Date value = rs.getDate(a.getName());
                        if (value == null) {
                            SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd");
                            try{
                                value = fmt.parse("0000-00-00");
                            }catch (ParseException e){
                                throw new DataflowException(e.getMessage());
                            }
                        }
                        tb.add(new DateField(value));
                    }else if (a.getType() == AttributeType.DATETIME){
                        Timestamp value = rs.getTimestamp(a.getName());

                        if (value == null) {
                            value = Timestamp.valueOf("0000-00-00 00:00:00.0");
                        }
                        tb.add(new DateTimeField(value.toLocalDateTime()));
                    }
                }
                IField[] iFieldArray = tb.toArray(new IField[tb.size()]);
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
        String query =  "\n" +
                "select * from `"+predicate.getTable()+ "` where 1 = 1 ";
        if(!predicate.getColumn().equals("") && !predicate.getKeywords().equals("")) {
            query += " AND  MATCH(`"+predicate.getColumn()+"`)  AGAINST ('"+predicate.getKeywords()+"')";
        }
        if(predicate.getLimit()!=Integer.MAX_VALUE){
            query+="limit ?";
        }
        query+=";";
        return query;
    }
    
    
    @Override
    public void close() throws TexeraException {
        if (cursor == CLOSED) {
            return;
        }
        try {
        	if(statement!=null)
        		statement.close();
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