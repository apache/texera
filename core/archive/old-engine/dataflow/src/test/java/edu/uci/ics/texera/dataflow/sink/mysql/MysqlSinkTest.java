package edu.uci.ics.texera.dataflow.sink.mysql;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import org.mockito.Mockito;
import org.mockito.stubbing.OngoingStubbing;

import edu.uci.ics.texera.api.constants.SchemaConstants;
import edu.uci.ics.texera.api.constants.test.TestConstants;
import edu.uci.ics.texera.api.dataflow.IOperator;
import edu.uci.ics.texera.api.field.DateField;
import edu.uci.ics.texera.api.field.DoubleField;
import edu.uci.ics.texera.api.field.IField;
import edu.uci.ics.texera.api.field.IntegerField;
import edu.uci.ics.texera.api.field.ListField;
import edu.uci.ics.texera.api.field.StringField;
import edu.uci.ics.texera.api.field.TextField;
import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.span.Span;
import edu.uci.ics.texera.api.tuple.Tuple;
import junit.framework.Assert;

/**
 * @Description:
 * 		The purpose of this main function is to test MysqlSink Operator
 * @TODO:
 * 		Before running the test,  setup mysql server and execute the following sql commands.
 * 		-- 1. create mysql user 'testUser' with 'testPassword'
 * 			CREATE USER 'testUser'@'localhost' IDENTIFIED BY 'testPassword';
 * 			GRANT ALL ON *.* TO 'testUser'@'localhost';
 * 		-- 2. create database 'testDB'
 * 			DROP DATABASE IF EXISTS testDB;
 * 			CREATE DATABASE testDB;
 * 
 * @After:
 * 		To check insertion status, comment out cleanUp() and run the following sql commands
 * 		USE testDB;
 * 		SELECT * FROM testDB;
 * 
 * @author Jinggang
 */
public class MysqlSinkTest {
	private MysqlSink mysqlSink;
	private MysqlSinkPredicate predicate;
    private IOperator inputOperator;
    private Schema inputSchema = new Schema(
            SchemaConstants._ID_ATTRIBUTE, new Attribute("content", AttributeType.TEXT), SchemaConstants.PAYLOAD_ATTRIBUTE);

    public static void main(String[] args) throws Exception{
    	MysqlSinkTest test = new MysqlSinkTest();
    	test.setUp();
    	test.testOpen();
    	
    	test.setUp();
    	test.testClose();
    	
    	test.setUp();
    	test.testGetNextTuple();    	
    	
    	test.setUp();
    	test.testProcessTuples();
    	
    	test.setUp();
    	test.test2TupleInsertion();
    	
    	test.setUp();
    	test.testTupleListInsertion();
    	
    	// CleanUp removes the last testTable left in previous test.
    	// Comment out cleanUp to see results in mysql.
    	test.cleanUp();
    }
    
    
    /**
     * To check insertion results, comment out cleanUp() and create database named testDB first.
     */
    public void setUp(){
    	inputOperator = Mockito.mock(IOperator.class);
        Mockito.when(inputOperator.getOutputSchema()).thenReturn(inputSchema);
		predicate = new MysqlSinkPredicate("localhost", 3306, "testDB", "testTable", "testUser", "testPassword", Integer.MAX_VALUE, 0);
		mysqlSink = new MysqlSink(predicate);
		mysqlSink.setInputOperator(inputOperator);
    }
	
    /**
     * Drop testTable from mysql
     */
    public void cleanUp() {
    	mysqlSink.setInputOperator(inputOperator);
    	mysqlSink.open();
    	//Notice only comment out mysqlDropTable() will still left an empty testTable created by mysqlSink.open() in mysql database.
    	mysqlSink.mysqlDropTable();		
        mysqlSink.close();
    }
    
    public void testOpen() throws Exception{
    	mysqlSink.open();
    	Mockito.verify(inputOperator).open();
        Assert.assertEquals(new Schema(new Attribute("content", AttributeType.TEXT)), mysqlSink.getOutputSchema());
        mysqlSink.close();
    }

    public void testProcessTuples(){
    	mysqlSink.open();
    	mysqlSink.processTuples();
    	mysqlSink.close();
    }
    
    public void testClose() throws Exception {
    	mysqlSink.open();
    	mysqlSink.close();
        // verify that inputOperator called close() method
        Mockito.verify(inputOperator).close();   
    }
    
    public void testGetNextTuple() throws Exception {
        Tuple sampleTuple = Mockito.mock(Tuple.class);
        Mockito.when(sampleTuple.toString()).thenReturn("Sample Tuple");
        Mockito.when(sampleTuple.getSchema()).thenReturn(inputSchema);
        // Set the behavior for inputOperator,
        // first it returns some non-null tuple and second time it returns null
        Mockito.when(inputOperator.getNextTuple()).thenReturn(sampleTuple).thenReturn(null);
        
        mysqlSink.open();
        mysqlSink.getNextTuple();
        // Verify that input operator's getNextTuple is called
        Mockito.verify(inputOperator, Mockito.times(1)).getNextTuple();
        mysqlSink.close();
    }
    
    /**
     * Create two tuples, insert into mysql
     * @throws ParseException
     */
    public void test2TupleInsertion() throws Exception {
        ArrayList<String> attributeNames = new ArrayList<>();
        attributeNames.add(TestConstants.FIRST_NAME);
        attributeNames.add(TestConstants.LAST_NAME);
        attributeNames.add(TestConstants.DESCRIPTION);

        // Prepare the expected result list
        List<Span> list = new ArrayList<>();
        Span span1 = new Span("firstName", 0, 5, "bruce", "bruce");
        Span span2 = new Span("lastnName", 0, 5, "jacki", "jacki");
        list.add(span1);
        list.add(span2);
        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];
        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;

        IField[] fields1 = { new StringField("bruce"), new StringField("john Lee"), new IntegerField(46),
                new DoubleField(5.50), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-14-1970")),
                new TextField("Tall Angry"), new ListField<>(list) };
        IField[] fields2 = { new StringField("test"), new StringField("jackie chan"), new IntegerField(0),
                new DoubleField(6.0), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("09-18-1994")),
                new TextField("Angry Bird"), new ListField<>(list) };
        
        Tuple tuple1 = new Tuple(new Schema(schemaAttributes), fields1);
        Tuple tuple2 = new Tuple(new Schema(schemaAttributes), fields2);

        IOperator localInputOperator = Mockito.mock(IOperator.class);
        Mockito.when(localInputOperator.getOutputSchema()).thenReturn(new Schema(schemaAttributes)).thenReturn(null);
        Mockito.when(localInputOperator.getNextTuple()).thenReturn(tuple1).thenReturn(tuple2).thenReturn(null);

        mysqlSink.setInputOperator(localInputOperator);
        mysqlSink.open();
        mysqlSink.processTuples();
        mysqlSink.close();
    }
    
    /**
     * Create 10000 tuples with all regular fields
     * Insert into mysql database
     */
    public void testTupleListInsertion() throws Exception {
        ArrayList<String> attributeNames = new ArrayList<>();
        attributeNames.add(TestConstants.FIRST_NAME);
        attributeNames.add(TestConstants.LAST_NAME);
        attributeNames.add(TestConstants.AGE);
        attributeNames.add(TestConstants.HEIGHT);
        attributeNames.add(TestConstants.DATE_OF_BIRTH);
        attributeNames.add(TestConstants.DESCRIPTION);

        // Prepare Schema
        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length];
        for (int count = 0; count < schemaAttributes.length; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }

        // Prepare 10000 tuples as a tupleList
        int testSize = 10000;
        Random rand = new Random();
        List<Tuple> tupleList = new ArrayList<Tuple>();
        for(int i = 0; i < testSize; i++){
        	IField[] fields = { new StringField(getRandomString()), new StringField(getRandomString()), new IntegerField(rand.nextInt()),
                new DoubleField(rand.nextDouble()*rand.nextInt()), new DateField(getRandomDate()),
                new TextField(getRandomString())};        
        	tupleList.add(new Tuple(new Schema(schemaAttributes), fields));
        }
        assert(tupleList.size() == testSize);
        
        IOperator localInputOperator = Mockito.mock(IOperator.class);
        Mockito.when(localInputOperator.getOutputSchema()).thenReturn(new Schema(schemaAttributes));
        OngoingStubbing<Tuple> stubbing = Mockito.when(localInputOperator.getNextTuple());
        for(Tuple t : tupleList) {
            stubbing = stubbing.thenReturn(t);
        }
        stubbing = stubbing.thenReturn(null);
        
        mysqlSink.setInputOperator(localInputOperator);
        mysqlSink.open();
        mysqlSink.processTuples();
        mysqlSink.close();
    }
    
	public static String getRandomString() {
		String candidateChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ._";
	    StringBuilder sb = new StringBuilder();
	    Random random = new Random();
	    int length = random.nextInt(100);
	    for (int i = 0; i < length; i++) {
	        sb.append(candidateChars.charAt(random.nextInt(candidateChars
	                .length())));
	    }
	    return sb.toString();
	}
    
	public static Date getRandomDate() throws ParseException{
		Random rnd = new Random();
		long ms = -946771200000L + (Math.abs(rnd.nextLong()) % (70L * 365 * 24 * 60 * 60 * 1000));
		Date date = new Date(ms);
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM-dd-yyyy");
		return dateFormat.parse(dateFormat.format(date));
	}
    
}
