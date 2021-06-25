package edu.uci.ics.texera.dataflow.sink.excel;

import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
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

public class ExcelSinkTest {
    
    private ExcelSink excelSink;
    private IOperator inputOperator;
    private Schema inputSchema = new Schema(
            SchemaConstants._ID_ATTRIBUTE, new Attribute("content", AttributeType.TEXT), SchemaConstants.PAYLOAD_ATTRIBUTE);
    
    @Before
    public void setUp() throws FileNotFoundException {
        inputOperator = Mockito.mock(IOperator.class);
        Mockito.when(inputOperator.getOutputSchema()).thenReturn(inputSchema);
        excelSink = new ExcelSink(new ExcelSinkPredicate());
        excelSink.setInputOperator(inputOperator);
    }

    @After
    public void cleanUp() {
    }

    @Test
    public void testOpen() throws Exception {
    	excelSink.open();
//        // verify that inputOperator called open() method
        Mockito.verify(inputOperator).open();
//        // assert that the tuple stream sink removes the _ID and PAYLOAD attribute
        Assert.assertEquals(new Schema(new Attribute("content", AttributeType.TEXT)), excelSink.getOutputSchema());
    
    	excelSink.close();
        Files.deleteIfExists(excelSink.getFilePath());
    }

    @Test
    public void testClose() throws Exception {
    	excelSink.open();
    	excelSink.close();
        // verify that inputOperator called close() method
        Mockito.verify(inputOperator).close();
        Files.deleteIfExists(excelSink.getFilePath());
    }
    
    /**
     * Create two tuples, write into a excel file. Need to manually delete the generated file.
     * @throws ParseException
     */
    @Test
    public void writeSampleExcelFile() throws Exception {
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

        IOperator inputOperator = Mockito.mock(IOperator.class);
        Mockito.when(inputOperator.getOutputSchema()).thenReturn(new Schema(schemaAttributes)).thenReturn(null);
        Mockito.when(inputOperator.getNextTuple()).thenReturn(tuple1).thenReturn(tuple2).thenReturn(null);

        excelSink = new ExcelSink(new ExcelSinkPredicate());
        excelSink.setInputOperator(inputOperator);
        excelSink.open();
        excelSink.collectAllTuples();
        excelSink.close();
        Files.deleteIfExists(excelSink.getFilePath());
    }
     
    
    @Test
    /**
	 * Create 10000 tuples with all field types except LIST field
	 * write into a excel file. Need to manually delete the generated file.
	 * Uncomment excelSink.deleteFile() to check the content of excel file
     */
    // writing 10000 tuples
    public void attributeTypeTest() throws Exception {
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
        
        IOperator inputOperator = Mockito.mock(IOperator.class);
        Mockito.when(inputOperator.getOutputSchema()).thenReturn(new Schema(schemaAttributes));
        OngoingStubbing<Tuple> stubbing = Mockito.when(inputOperator.getNextTuple());
        for(Tuple t : tupleList) {
            stubbing = stubbing.thenReturn(t);
        }
        stubbing = stubbing.thenReturn(null);
        
        // excel writing test
    	excelSink = new ExcelSink(new ExcelSinkPredicate());
        excelSink.setInputOperator(inputOperator);
        excelSink.open();
        excelSink.collectAllTuples();
        excelSink.close();
        Files.deleteIfExists(excelSink.getFilePath());
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
