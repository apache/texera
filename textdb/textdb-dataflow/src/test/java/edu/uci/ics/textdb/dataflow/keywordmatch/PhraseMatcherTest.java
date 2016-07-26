package edu.uci.ics.textdb.dataflow.keywordmatch;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.storage.IDataWriter;
import edu.uci.ics.textdb.common.constants.DataConstants;
import edu.uci.ics.textdb.common.constants.SchemaConstants;
import edu.uci.ics.textdb.common.constants.TestConstants;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.field.DateField;
import edu.uci.ics.textdb.common.field.DoubleField;
import edu.uci.ics.textdb.common.field.IntegerField;
import edu.uci.ics.textdb.common.field.ListField;
import edu.uci.ics.textdb.common.field.Span;
import edu.uci.ics.textdb.common.field.StringField;
import edu.uci.ics.textdb.common.field.TextField;
import edu.uci.ics.textdb.dataflow.common.KeywordPredicate;
import edu.uci.ics.textdb.dataflow.utils.TestUtils;
import edu.uci.ics.textdb.storage.DataStore;
import edu.uci.ics.textdb.storage.writer.DataWriter;

/**
 * @author Prakul
 *
 */

public class PhraseMatcherTest {

    private KeywordMatcher KeywordMatcher;
    private IDataWriter dataWriter;
    private DataStore dataStore;
    private Analyzer luceneAnalyzer;

    @Before
    public void setUp() throws Exception {
        dataStore = new DataStore(DataConstants.INDEX_DIR, TestConstants.SCHEMA_PEOPLE);
        luceneAnalyzer = new StandardAnalyzer();
        dataWriter = new DataWriter(dataStore, luceneAnalyzer);
        dataWriter.clearData();
        dataWriter.writeData(TestConstants.getSamplePeopleTuples());
    }

    @After
    public void cleanUp() throws Exception {
        dataWriter.clearData();
    }

    /**
     * For a given string query & list of attributes it gets a list of results
     * buildMultiQueryOnAttributeList flag decides if the query is formed as a boolean Query on all attribute
     * or all records are scanned
     * @param query
     * @param attributeList
     * @return List<ITuple>
     * @throws DataFlowException
     * @throws ParseException
     */

    public List<ITuple> getPeopleQueryResults(String query, ArrayList<Attribute> attributeList) throws DataFlowException, ParseException {

        IPredicate predicate = new KeywordPredicate(query, attributeList, DataConstants.KeywordMatchingType.PHRASE_INDEXBASED, luceneAnalyzer, dataStore);
        KeywordMatcher = new KeywordMatcher(predicate);
        KeywordMatcher.open();

        List<ITuple> results = new ArrayList<>();
        ITuple nextTuple = null;

        while ((nextTuple = KeywordMatcher.getNextTuple()) != null) {
            results.add(nextTuple);
        }

        return results;
    }

    /**
     * Verifies Phrase Matcher where Query phrase doesn't exist in any document.
     * @throws Exception
     */
    @Test
    public void testKeywordMatcher() throws Exception {
        //Prepare Query
        String query = "lin clooney is short and angry";
        ArrayList<Attribute> attributeList = new ArrayList<>();
        attributeList.add(TestConstants.FIRST_NAME_ATTR);
        attributeList.add(TestConstants.LAST_NAME_ATTR);
        attributeList.add(TestConstants.DESCRIPTION_ATTR);

        //Perform Query
        List<ITuple> results = getPeopleQueryResults(query, attributeList);

        //Perform Check
        Assert.assertEquals(0,results.size());
    }

    /**
     * Verifies List<ITuple> returned by Phrase Matcher on multiple
     * word query on a String Field
     * @throws Exception
     */
    @Test
    public void testPhraseSearchForStringField() throws Exception {
        //Prepare Query
        String query = "george lin lin";
        ArrayList<Attribute> attributeList = new ArrayList<>();
        attributeList.add(TestConstants.FIRST_NAME_ATTR);
        attributeList.add(TestConstants.LAST_NAME_ATTR);
        attributeList.add(TestConstants.DESCRIPTION_ATTR);

        //Prepare expected result list
        List<Span> list = new ArrayList<Span>();
        Span span1 = new Span("firstName", 0, 14, "george lin lin", "george lin lin");
        list.add(span1);

        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];
        for(int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;

        IField[] fields1 = { new StringField("george lin lin"), new StringField("lin clooney"), new IntegerField(43),
                new DoubleField(6.06), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1973")),
                new TextField("Lin Clooney is Short and lin clooney is Angry"), new ListField<>(list) };

        ITuple tuple1 = new DataTuple(new Schema(schemaAttributes), fields1);
        List<ITuple> expectedResultList = new ArrayList<>();
        expectedResultList.add(tuple1);

        //Perform Query
        List<ITuple> resultList = getPeopleQueryResults(query, attributeList);

        //Perform Check
        boolean contains = TestUtils.containsAllResults(expectedResultList, resultList);
        Assert.assertTrue(contains);
    }


    /**
     * Verifies: getNextTuple should return Combined Span info for the phrase
     * @throws Exception
     */
    @Test
    public void testCombinedSpanInMultipleFieldsQuery() throws Exception {
        //Prepare Query
        String query = "lin clooney";
        ArrayList<Attribute> attributeList = new ArrayList<>();
        attributeList.add(TestConstants.FIRST_NAME_ATTR);
        attributeList.add(TestConstants.LAST_NAME_ATTR);
        attributeList.add(TestConstants.DESCRIPTION_ATTR);

        //Prepare expected result list
        List<Span> list = new ArrayList<>();
        Span span1 = new Span("lastName", 0, 11, "lin clooney", "lin clooney");
        Span span2 = new Span("description", 0, 11, "lin clooney", "Lin Clooney");
        Span span3 = new Span("description", 25, 36, "lin clooney", "lin clooney");

        list.add(span1);
        list.add(span2);
        list.add(span3);


        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];
        for(int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;

        IField[] fields1 = { new StringField("george lin lin"), new StringField("lin clooney"), new IntegerField(43),
                new DoubleField(6.06), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1973")),
                new TextField("Lin Clooney is Short and lin clooney is Angry"), new ListField<>(list) };

        ITuple tuple1 = new DataTuple(new Schema(schemaAttributes), fields1);
        List<ITuple> expectedResultList = new ArrayList<>();
        expectedResultList.add(tuple1);

        //Perform Query
        List<ITuple> resultList = getPeopleQueryResults(query, attributeList);

        //Perform Check
        boolean contains = TestUtils.containsAllResults(expectedResultList, resultList);
        Assert.assertTrue(contains);
    }

    /**
     * Verifies: Query with Stop Words match corresponding phrases in the document
     * @throws Exception
     */
    @Test
    public void testWordInMultipleFieldsQueryWithStopWords1() throws Exception {
        //Prepare Query
        String query = "lin and and angry";
        ArrayList<Attribute> attributeList = new ArrayList<>();
        attributeList.add(TestConstants.FIRST_NAME_ATTR);
        attributeList.add(TestConstants.LAST_NAME_ATTR);
        attributeList.add(TestConstants.DESCRIPTION_ATTR);

        //Prepare expected result list
        List<Span> list = new ArrayList<>();
        Span span1 = new Span("description", 25, 45, "lin and and angry", "lin clooney is Angry");

        list.add(span1);

        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];
        for(int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;

        IField[] fields1 = { new StringField("george lin lin"), new StringField("lin clooney"), new IntegerField(43),
                new DoubleField(6.06), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1973")),
                new TextField("Lin Clooney is Short and lin clooney is Angry"), new ListField<>(list) };

        ITuple tuple1 = new DataTuple(new Schema(schemaAttributes), fields1);
        List<ITuple> expectedResultList = new ArrayList<>();
        expectedResultList.add(tuple1);

        //Perform Query
        List<ITuple> resultList = getPeopleQueryResults(query, attributeList);

        //Perform Check
        boolean contains = TestUtils.containsAllResults(expectedResultList, resultList);
        Assert.assertTrue(contains);
    }

    /**
     * Verifies: Query with Stop Words match corresponding phrases in the document
     * @throws Exception
     */
    @Test
    public void testWordInMultipleFieldsQueryWithStopWords2() throws Exception {
        //Prepare Query
        String query = "lin clooney and angry";
        ArrayList<Attribute> attributeList = new ArrayList<>();
        attributeList.add(TestConstants.FIRST_NAME_ATTR);
        attributeList.add(TestConstants.LAST_NAME_ATTR);
        attributeList.add(TestConstants.DESCRIPTION_ATTR);

        //Prepare expected result list
        List<Span> list = new ArrayList<>();
        Span span1 = new Span("description", 25, 45, "lin clooney and angry", "lin clooney is Angry");

        list.add(span1);

        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];
        for(int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;

        IField[] fields1 = { new StringField("george lin lin"), new StringField("lin clooney"), new IntegerField(43),
                new DoubleField(6.06), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1973")),
                new TextField("Lin Clooney is Short and lin clooney is Angry"), new ListField<>(list) };

        ITuple tuple1 = new DataTuple(new Schema(schemaAttributes), fields1);
        List<ITuple> expectedResultList = new ArrayList<>();
        expectedResultList.add(tuple1);

        //Perform Query
        List<ITuple> resultList = getPeopleQueryResults(query, attributeList);

        //Perform Check
        boolean contains = TestUtils.containsAllResults(expectedResultList, resultList);
        Assert.assertTrue(contains);
    }

    /**
     * Verifies: Query with Stop Words match corresponding phrases with Medline data
     * @throws Exception with Medline data
     */
    @Test
    public void testWordInMultipleFieldsQueryWithStopWords3() throws Exception {
    	DataStore MedDataStore = new DataStore("../index/test", keywordTestConstants.SCHEMA_MEDLINE);
        Analyzer MedAnalyzer = new StandardAnalyzer();
        DataWriter MedDataWriter = new DataWriter(MedDataStore, MedAnalyzer);
        MedDataWriter.clearData();
    	MedDataWriter.writeData(keywordTestConstants.getSampleMedlineRecord());
        //Prepare Query
        String query = "skin rash";
        ArrayList<Attribute> attributeList = new ArrayList<>();
        attributeList.add(keywordTestConstants.ABSTRACT_ATTR);

        //Prepare expected result list
        List<Span> list = new ArrayList<>();
        Span span1 = new Span(keywordTestConstants.ABSTRACT, 192, 201, "skin rash", "skin rash");

        list.add(span1);

        Attribute[] schemaAttributes = new Attribute[keywordTestConstants.ATTRIBUTES_MEDLINE.length + 1];
        for(int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = keywordTestConstants.ATTRIBUTES_MEDLINE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;

        IField [] fields1 = {new IntegerField(14347980), new TextField(""), new TextField("CHRONIC MENINGOCOCCEMIA; EPIDEMIOLOGY, DIAGNOSIS AND TREATMENT."),
    			new TextField("D S BLOOM"), new StringField("103 Aug, 1965"), new TextField("California medicine"), new TextField("DRUG THERAPY, MENINGOCOCCAL INFECTIONS, PENICILLIN G, SULFONAMIDES"),
    			new TextField("Drug Therapy, Meningococcal Infections, Penicillin G, Sulfonamides"), 
    			new TextField("This report describes four cases of chronic meningococcemia with the characteristic manifestations of recurrent episodes of "
    					+ "fever, chills, night sweats, headache and anorexia, associated with skin rash and arthralgias. The diagnosis was established in all instances by blood culture. Administration "
    					+ "of sulfonamides in three cases and penicillin in the fourth resulted in prompt recovery. The recent finding of a strain of sulfonamide-resistant meningococci, however, indicates "
    					+ "that antibiotic-sensitivity tests should be carried out in all cases of meningococcal disease. While waiting for the results of such tests to be reported, the clinician should "
    					+ "initiate treatment with large doses of a sulfonamide and penicillin in combination."), new DoubleField(0.664347980), new ListField<>(list)};

        ITuple tuple1 = new DataTuple(new Schema(schemaAttributes), fields1);
        List<ITuple> expectedResultList = new ArrayList<>();
        expectedResultList.add(tuple1);

        //Perform Query
        IPredicate predicate = new KeywordPredicate(query, attributeList, DataConstants.KeywordMatchingType.PHRASE_INDEXBASED, MedAnalyzer, MedDataStore);
        KeywordMatcher = new KeywordMatcher(predicate);
        KeywordMatcher.open();

        List<ITuple> results = new ArrayList<>();
        ITuple nextTuple = null;

        while ((nextTuple = KeywordMatcher.getNextTuple()) != null) {
            results.add(nextTuple);
        }
        //Perform Check
        boolean contains = TestUtils.containsAllResults(expectedResultList, results);
        Assert.assertTrue(contains);
    }
    
    /**
     * Verifies: Query with Stop Words match corresponding phrases in the document
     * Used to cause exception if there is a special symbol with the words, ex: big(
     * @throws Exception with Medline data
     */
    @Test
    public void testWordInMultipleFieldsQueryWithStopWords4() throws Exception {
    	DataStore MedDataStore = new DataStore("../index/test", keywordTestConstants.SCHEMA_MEDLINE);
        Analyzer MedAnalyzer = new StandardAnalyzer();
        DataWriter MedDataWriter = new DataWriter(MedDataStore, MedAnalyzer);
        MedDataWriter.clearData();
    	MedDataWriter.writeData(keywordTestConstants.getSampleMedlineRecord());
        //Prepare Query
        String query = "x-ray";
        ArrayList<Attribute> attributeList = new ArrayList<>();
        attributeList.add(keywordTestConstants.ABSTRACT_ATTR);

        //Prepare expected result list
        List<Span> list = new ArrayList<>();
        Span span1 = new Span(keywordTestConstants.ABSTRACT, 226, 231, "x-ray", "x-ray");

        list.add(span1);

        Attribute[] schemaAttributes = new Attribute[keywordTestConstants.ATTRIBUTES_MEDLINE.length + 1];
        for(int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = keywordTestConstants.ATTRIBUTES_MEDLINE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;

	    IField [] fields = {new IntegerField(17832788), new TextField(""), new TextField("Cosmic X-ray Sources."),
		new TextField("S Bowyer, E T Byram, T A Chubb, H Friedman"), new StringField("147-3656 Jan 22, 1965"), new TextField("Science (New York, N.Y.)"), new TextField(""),
		new TextField(""), new TextField("Eight new sources of cosmic x-rays were detected by two Aerobee surveys in 1964. One source, from Sagittarius, is close to the galactic center, and the other, "
				+ "from Ophiuchus, may coincide with Kepler's 1604 supernova. All the x-ray sources are fairly close to the galactic plane."), new DoubleField(0.667832788), new ListField<>(list)};

        ITuple tuple1 = new DataTuple(new Schema(schemaAttributes), fields);
        List<ITuple> expectedResultList = new ArrayList<>();
        expectedResultList.add(tuple1);

        //Perform Query
        IPredicate predicate = new KeywordPredicate(query, attributeList, DataConstants.KeywordMatchingType.PHRASE_INDEXBASED, MedAnalyzer, MedDataStore);
        KeywordMatcher = new KeywordMatcher(predicate);
        KeywordMatcher.open();

        List<ITuple> results = new ArrayList<>();
        ITuple nextTuple = null;

        while ((nextTuple = KeywordMatcher.getNextTuple()) != null) {
            results.add(nextTuple);
        }
        //Perform Check
        boolean contains = TestUtils.containsAllResults(expectedResultList, results);
        Assert.assertTrue(contains);
    }

    /**
     * Verifies: Query with Stop Words match corresponding phrases in the document
     * Used to cause exception sometimes if there is a space between words
     * @throws Exception with Medline data
     */
    @Test
    public void testWordInMultipleFieldsQueryWithStopWords5() throws Exception {
    	DataStore MedDataStore = new DataStore("../index/test", keywordTestConstants.SCHEMA_MEDLINE);
        Analyzer MedAnalyzer = new StandardAnalyzer();
        DataWriter MedDataWriter = new DataWriter(MedDataStore, MedAnalyzer);
        MedDataWriter.clearData();
    	MedDataWriter.writeData(keywordTestConstants.getSampleMedlineRecord());
        //Prepare Query
        String query = "gain weight";
        ArrayList<Attribute> attributeList = new ArrayList<>();
        attributeList.add(keywordTestConstants.ABSTRACT_ATTR);

        //Prepare expected result list
        List<Span> list = new ArrayList<>();
        Span span1 = new Span(keywordTestConstants.ABSTRACT, 26, 37, "gain weight", "gain weight");

        list.add(span1);

        Attribute[] schemaAttributes = new Attribute[keywordTestConstants.ATTRIBUTES_MEDLINE.length + 1];
        for(int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = keywordTestConstants.ATTRIBUTES_MEDLINE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;

	    IField [] fields = {new IntegerField(4566015), new TextField(""), new TextField("Significance of milk pH in newborn infants."), 
	    		new TextField("V C Harrison, G Peat"), new StringField("4-5839 Dec 2, 1972"), new TextField("British medical journal"), 
	    		new TextField(""), new TextField("Infant Nutritional Physiological Phenomena, Infant, Newborn, Milk"), 
	    		new TextField("Bottle-fed infants do not gain weight as rapidly as breast-fed babies during the first week of life. This "
	    				+ "weight lag can be corrected by the addition of a small amount of alkali (sodium bicarbonate or trometamol) to "
	    				+ "the feeds. The alkali corrects the acidity of cow's milk which now assumes some of the properties of human breast "
	    				+ "milk. It has a bacteriostatic effect on specific Escherichia coli in vitro, and in infants it produces a stool with"
	    				+ " a preponderance of lactobacilli over E. coli organisms. When alkali is removed from the milk there is a decrease in"
	    				+ " the weight of an infant and the stools contain excessive numbers of E. coli bacteria.A pH-corrected milk appears to"
	    				+ " be more physiological than unaltered cow's milk and may provide some protection against gastroenteritis in early "
	    				+ "life. Its bacteriostatic effect on specific E. coli may be of practical significance in feed preparations where "
	    				+ "terminal sterilization and refrigeration are not available. The study was conducted during the week after birth, and "
	    				+ "no conclusions are derived for older infants. The long-term effects of trometamol are unknown. No recommendation can "
	    				+ "be given for the addition of sodium bicarbonate to milks containing a higher content of sodium."), new DoubleField(0.667832788), new ListField<>(list)};
	    

        ITuple tuple1 = new DataTuple(new Schema(schemaAttributes), fields);
        List<ITuple> expectedResultList = new ArrayList<>();
        expectedResultList.add(tuple1);

        //Perform Query
        IPredicate predicate = new KeywordPredicate(query, attributeList, DataConstants.KeywordMatchingType.PHRASE_INDEXBASED, MedAnalyzer, MedDataStore);
        KeywordMatcher = new KeywordMatcher(predicate);
        KeywordMatcher.open();

        List<ITuple> results = new ArrayList<>();
        ITuple nextTuple = null;

        while ((nextTuple = KeywordMatcher.getNextTuple()) != null) {
            results.add(nextTuple);
        }
        //Perform Check
        boolean contains = TestUtils.containsAllResults(expectedResultList, results);
        Assert.assertTrue(contains);
    }

}
