package edu.uci.ics.texera.dataflow.source.mysql;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.dataflow.twitterfeed.TwitterUtils;

public class MysqlSource implements ISourceOperator{
	private final MysqlSourcePredicate predicate;
    private int cursor = CLOSED;
    private Schema outputSchema;
    private Connection connection;
    private Statement statement;
    private PreparedStatement prepStatement;
    private ResultSet rs;
    private boolean start = true;
    
    public MysqlSource(MysqlSourcePredicate predicate){
    	this.predicate = predicate;
    	//outputSchema = new Schema(inputSchema.getAttributes().stream()
          //      .filter(attr -> !attr.getName().equalsIgnoreCase(SchemaConstants._ID))
            //    .filter(attr -> !attr.getName().equalsIgnoreCase(SchemaConstants.PAYLOAD))
             //   .filter(attr -> !attr.getType().equals(AttributeType.LIST)).toArray(Attribute[]::new));
    	/*this.outputSchema = new Schema(
                SchemaConstants._ID_ATTRIBUTE, 
                new Attribute(this.predicate.getResultAttribute(), AttributeType.STRING));
    */
    	this.outputSchema = new Schema.Builder().add(SchemaConstants._ID_ATTRIBUTE)
                .add(TwitterUtils.TwitterSchema.TWITTER_SCHEMA).build();
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
            
            //statement = connection.createStatement();
            //ResultSetMetaData rs = statement.executeQuery(generateSqlQuery(predicate));
            //rsmd = rs.getMetadata();
            DatabaseMetaData metadata = connection.getMetaData();
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
                PreparedStatement ps = this.connection.prepareStatement(generateSqlQuery(predicate));
                if(!predicate.getColumn().equals("") && !predicate.getKeywords().equals("")){
                    ps.setString(1, predicate.getColumn());
                    ps.setString(2, predicate.getKeywords());
                }
                this.rs = ps.executeQuery();
            }
			while (rs.next()) {
				String follower = rs.getString("user.followers_count");
	    		String friend = rs.getString("user.friends_count");
	    		follower = follower == null ? "0" :follower;
	    		friend = friend == null ? "0" :friend;
	    		String coordinate = rs.getString("coordinate");
	    		coordinate = coordinate == null ? "" :coordinate;
	    		String language = rs.getString(TwitterUtils.TwitterSchema.LANGUAGE);
	    		coordinate = language == null ? "" :language;
			    Tuple tuple =  new Tuple(this.outputSchema, 
			            IDField.newRandomID(), //IntegerField
			            new TextField(rs.getString("text")), 
			            new StringField(""), new StringField(""), new StringField(""), 
			            new TextField(rs.getString("user.screen_name")),
			            new TextField(rs.getString("user.name")),
			            new TextField(rs.getString("user.description")),
			            
			            new IntegerField(Integer.valueOf(follower)),
			            new IntegerField(Integer.valueOf(friend)),
			            new TextField(rs.getString("user.location")),
			            new StringField(rs.getString("user.create_at")),
			            new TextField(rs.getString("geo_tag.cityName")),
			            new StringField(coordinate),
			            new StringField(language)
			            );
			    		
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
        String query =  "\n" +
                "select * from "+predicate.getTable()+ "where 1 = 1 ";
        if(!predicate.getColumn().equals("") && !predicate.getKeywords().equals("")) {
            query += " AND  ? = ?";
        }
        query+=";";
        return query;
//
//        PreparedStatement ps = this.connection.prepareStatement(query);
//
//        ps.setObject(count++, param.getParam(), param.getType());
        //}
//    	StringBuilder sb = new StringBuilder();
//    	sb.append("select * from `"+predicate.getTable());
//    	if (predicate.getColumn()!="" && predicate.getKeywords()!="") {
//    		sb.append("` where  MATCH(`"+predicate.getColumn()+"`) AGAINST ('"+ predicate.getKeywords()+"')");
//    	}
//    	if (predicate.getLimit()!=Integer.MAX_VALUE)
//    		sb.append(" limit "+ predicate.getLimit());
//    	sb.append(";");
//    	System.out.print(sb.toString());
//    	return sb.toString();
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