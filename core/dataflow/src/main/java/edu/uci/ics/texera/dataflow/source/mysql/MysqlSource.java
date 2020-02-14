package edu.uci.ics.texera.dataflow.source.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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
    
    public MysqlSource(MysqlSourcePredicate predicate){
    	this.predicate = predicate;
//    	this.outputSchema = new Schema.Builder().add(SchemaConstants._ID_ATTRIBUTE)
//                .add(TwitterUtils.TwitterSchema.TWITTER_SCHEMA).build();
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
                String columnsize = columns.getString("COLUMN_SIZE");
                String decimaldigits = columns.getString("DECIMAL_DIGITS");
                String isNullable = columns.getString("IS_NULLABLE");
                String is_autoIncrment = columns.getString("IS_AUTOINCREMENT");
                //Printing results
//                System.out.println(columnName + "---" + datatype + "---" + columnsize + "---" + decimaldigits + "---" + isNullable + "---" + is_autoIncrment);

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



//                this.outputSchema.add(columnName, da)
            }
//            System.out.println(this.outputSchema.toString());


            statement = connection.createStatement();
//            System.out.println(generateSqlQuery(predicate));
            rs = statement.executeQuery(generateSqlQuery(predicate));

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
                                System.out.println("parse error");
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

//				String follower = rs.getString("user.followers_count");
//	    		String friend = rs.getString("user.friends_count");
//	    		follower = follower == null ? "0" :follower;
//	    		friend = friend == null ? "0" :friend;
//	    		String coordinate = rs.getString("coordinate");
//	    		coordinate = coordinate == null ? "" :coordinate;
//	    		String language = rs.getString(TwitterUtils.TwitterSchema.LANGUAGE);
//	    		coordinate = language == null ? "" :language;
//			    Tuple tuple =  new Tuple(this.outputSchema,
//			            IDField.newRandomID(), //IntegerField
//			            new TextField(rs.getString("text")),
//			            new StringField(""), new StringField(""), new StringField(""),
//			            new TextField(rs.getString("user.screen_name")),
//			            new TextField(rs.getString("user.name")),
//			            new TextField(rs.getString("user.description")),
//
//			            new IntegerField(Integer.valueOf(follower)),
//			            new IntegerField(Integer.valueOf(friend)),
//			            new TextField(rs.getString("user.location")),
//			            new StringField(rs.getString("user.create_at")),
//			            new TextField(rs.getString("geo_tag.cityName")),
//			            new StringField(coordinate),
//			            new StringField(language)
//			            );
			    		
			            //new StringField(resultJsonArray.getJSONObject(cursor).get("ds").toString()));
			    cursor ++;
			    
			    return tuple;
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        return null;
    }
    
    public static String generateSqlQuery(MysqlSourcePredicate predicate) {
    	StringBuilder sb = new StringBuilder();
    	sb.append("select * from `"+predicate.getTable());
    	if (predicate.getColumn()!="" && predicate.getKeywords()!="") {
    		sb.append("` where  MATCH(`"+predicate.getColumn()+"`) AGAINST ('"+ predicate.getKeywords()+"')");
    	}
    	if (predicate.getLimit()!=Integer.MAX_VALUE)
    		sb.append(" limit "+ predicate.getLimit());
    	sb.append(";");
    	System.out.print(sb.toString());
    	return sb.toString();
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