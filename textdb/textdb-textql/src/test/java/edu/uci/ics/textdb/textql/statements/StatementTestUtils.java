package edu.uci.ics.textdb.textql.statements;

import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.stream.Collectors;

import org.junit.Assert;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.field.DoubleField;
import edu.uci.ics.textdb.common.field.IDField;
import edu.uci.ics.textdb.common.field.IntegerField;
import edu.uci.ics.textdb.common.field.ListField;
import edu.uci.ics.textdb.common.field.StringField;
import edu.uci.ics.textdb.common.field.TextField;
import edu.uci.ics.textdb.common.field.DateField;
import edu.uci.ics.textdb.textql.planbuilder.beans.PassThroughBean;
import edu.uci.ics.textdb.textql.statements.Statement;
import edu.uci.ics.textdb.web.request.beans.OperatorBean;
import edu.uci.ics.textdb.web.request.beans.OperatorLinkBean;

/**
 * This class contain helper methods for the purpose of testing Statement class and its implementations.
 * 
 * @author Flavio Bayer
 *
 */
public class StatementTestUtils {
    
    /**
     * Sample attributes.
     * Each attribute have a different values of FieldType.
     */
    public final static Attribute ID_ATTRIBUTE = new Attribute("fieldId", FieldType._ID_TYPE);
    public final static Attribute INTEGER_ATTRIBUTE = new Attribute("fieldInteger", FieldType.INTEGER);
    public final static Attribute DOUBLE_ATTRIBUTE = new Attribute("fieldDouble", FieldType.DOUBLE);
    public final static Attribute STRING_ATTRIBUTE = new Attribute("fieldString", FieldType.STRING);
    public final static Attribute TEXT_ATTRIBUTE = new Attribute("fieldText", FieldType.TEXT);
    public final static Attribute DATE_ATTRIBUTE = new Attribute("fieldDate", FieldType.DATE);
    public final static Attribute LIST_ATTRIBUTE = new Attribute("fieldList", FieldType.LIST);
    
    /**
     * A sample schema containing attributes with all the values of FieldType.
     */
    public final static Schema ALL_FIELD_TYPES_SCHEMA = new Schema(
            ID_ATTRIBUTE,
            INTEGER_ATTRIBUTE,
            DOUBLE_ATTRIBUTE,
            STRING_ATTRIBUTE,
            TEXT_ATTRIBUTE,
            DATE_ATTRIBUTE,
            LIST_ATTRIBUTE
        );
    
    /**
     * Assert the generated beans by a statement are a valid direct acyclic graph (no cycles nor unreached nodes/links 
     * are present) and that the given list of operators are present in the right order in the path from the initial 
     * node to the final node (while ignoring the PassThroughBean and the value of the ID).
     * The following checks are performed:
     * -Check whether all the build operators have unique IDs (between them).
     * -Check whether the generated links are unique (no duplicate links).
     * -Check whether a path from the initial to the final node exists.
     * -Check whether all the operators in the path between the initial and the final node is are present in
     *     expectedOperators in the same order (ignoring the PassThroughBean)
     * -Check whether all the links connect existing operators.
     * -Check whether all the operator beans are visited once at most (no cycles).
     * -Check whether all the link beans are visited once at most (no cycles).
     * -Check whether all the operators, except for the final operator, have output arity equals to one.
     * -Check whether all the operators beans generated are reachable.
     * -Check whether all the link beans generated are reachable.
     * 
     * @param statement The statement to build the beans to be checked.
     * @param expectedOperators The list of the expected OperatorBeans to be build by the statement.
     */
    public static void assertGeneratedBeans(Statement statement, List<OperatorBean> expectedOperators){        
        // Get operators and links from statement
        List<OperatorBean> operators = statement.getInternalOperatorBeans();
        List<OperatorLinkBean> links = statement.getInternalLinkBeans();
        // Assert all statements have an unique id (check whether two operators have the same ID)
        boolean uniqueIds = operators.stream()
                                     .collect(Collectors.groupingBy(op -> op.getOperatorID(), Collectors.counting()))
                                     .values()
                                     .stream()
                                     .allMatch( count -> (count==1) );
        Assert.assertTrue(uniqueIds);

        // Iterate the graph (string of nodes) to look for the expected beans
        HashSet<OperatorBean> visitedOperators = new HashSet<>();
        HashSet<OperatorLinkBean> visitedLinks = new HashSet<>();
        
        String initialNode = statement.getInputNodeID();
        String finalNode = statement.getOutputNodeID();
                
        Iterator<OperatorBean> expectedOperatorsIterator = expectedOperators.iterator();
        OperatorBean nextExpectedOperator = null;

        // Start from the initial node and stop when the final node is reached (or an Assert fail)
        String currentBeanId = initialNode;
        while(true){
            // Get the next expected operator to find (if needed)
            if(nextExpectedOperator==null && expectedOperatorsIterator.hasNext()){
                nextExpectedOperator = expectedOperatorsIterator.next();
            }
            // Get the current bean by ID
            String currentLookingBeanId = currentBeanId;
            OperatorBean currentOperatorBean = operators.stream()
                                                        .filter( op -> op.getOperatorID().equals(currentLookingBeanId) )
                                                        .findAny()
                                                        .orElse(null);
            Assert.assertNotNull(currentOperatorBean);
            // Add the current visited bean to the set of visited beans and assert it hasn't been visited yet (cycle check)
            Assert.assertTrue(visitedOperators.add(currentOperatorBean));
            // Compare the current bean with the next expected bean
            if(nextExpectedOperator!=null && currentOperatorBean.getClass()==nextExpectedOperator.getClass()){
                // Copy the id of the current bean to the bean we are looking for and assert they are equal
                nextExpectedOperator.setOperatorID(currentOperatorBean.getOperatorID());
                Assert.assertEquals(nextExpectedOperator, currentOperatorBean);
                nextExpectedOperator = null;
            }else if(!(currentOperatorBean instanceof PassThroughBean)){
                // Found a bean that is not PassThrough and is not the expected operator!
                Assert.fail();
            }
            // Break once the final node is visited
            if(currentBeanId.equals(finalNode)){
                break;
            }
            // Get outgoing links for the current bean
            List<OperatorLinkBean> currentOperatorBeanOutgoingLinks = links.stream()
                                    .filter(link -> link.getFromOperatorID().equals(currentOperatorBean.getOperatorID()) )
                                    .collect(Collectors.toList());
            // Assert there is only one outgoing link
            Assert.assertEquals(currentOperatorBeanOutgoingLinks.size(), 1);
            OperatorLinkBean currentOperatorBeanOutgoingLink = currentOperatorBeanOutgoingLinks.get(0);
            // Add the outgoing link to the set of visited links and assert it hasn't been visited yet (cycle and duplicate check)
            Assert.assertTrue(visitedLinks.add(currentOperatorBeanOutgoingLink));
            // Set the current bean id to the next bean
            currentBeanId = currentOperatorBeanOutgoingLink.getToOperatorID();
        }

        // Assert there are no more expected operators to look for
        Assert.assertFalse(expectedOperatorsIterator.hasNext());
        // Assert all the operators generated by the statement are visited (no unreachable operators)
        Assert.assertTrue(visitedOperators.containsAll(operators));
        // Assert all the links generated by the statement are visited (no unreachable links)
        Assert.assertTrue(visitedLinks.containsAll(links));
    }
    
    /**
     * Create an IOperator with the given Schema that contains the 
     * given list of tuples. This is an anonymous operator that works
     * like an actual IOperator, it must be opened, closed and the 
     * tuples are accessible through the getNextTuple method. 
     * @param schema The output schema of the created IOperator.
     * @param tuples The tuples to be returned by the IOperator.
     * @return An IOperator containing the given tuples and schema.
     */
    public static IOperator generateSampleIOperator(Schema schema, List<ITuple> tuples) {
        return new IOperator() {
            
            ListIterator<ITuple> tupleIterator;
            
            @Override
            public void open() throws TextDBException {
                if (tupleIterator != null){
                    return;
                }
                tupleIterator = tuples.listIterator();
            }
            
            @Override
            public ITuple getNextTuple() throws TextDBException {
                if(!tupleIterator.hasNext()){
                    return null;
                }
                return tupleIterator.next();
            }
            
            @Override
            public void close() throws TextDBException {
                tupleIterator = null;
            }
            
            @Override
            public Schema getOutputSchema() {
                return schema;
            }
        };
    }
    
    /**
     * Generate a sample IOperator that contain a Schema with all the
     * field types and two sample tuples in which the fields are empty.
     * @return The generated sample IOperator.
     */
    public static IOperator generatePopulatedSampleOperator() {
        Schema schema = StatementTestUtils.ALL_FIELD_TYPES_SCHEMA;
        IField[] emptyTupleFields = new IField[]{ 
                new IDField(""),
                new IntegerField(0),
                new DoubleField(0.),
                new StringField(""),
                new TextField(""),
                new DateField(new Date()),
                new ListField<>(Collections.emptyList())
            };
        ITuple tuple0 = new DataTuple(schema, emptyTupleFields);
        List<ITuple> tuples = Collections.nCopies(2, tuple0);
        return generateSampleIOperator(schema, tuples);        
    }
    
}
