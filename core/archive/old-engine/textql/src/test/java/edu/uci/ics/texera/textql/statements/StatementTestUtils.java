package edu.uci.ics.texera.textql.statements;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Assert;

import edu.uci.ics.texera.dataflow.common.PredicateBase;
import edu.uci.ics.texera.dataflow.plangen.OperatorLink;
import edu.uci.ics.texera.textql.planbuilder.beans.PassThroughPredicate;
import edu.uci.ics.texera.textql.statements.Statement;

/**
 * This class contain helper methods for the purpose of testing Statement class and its implementations.
 * 
 * @author Flavio Bayer
 *
 */
public class StatementTestUtils {
    
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
    public static void assertGeneratedBeans(Statement statement, List<PredicateBase> expectedOperators){        
        // Get operators and links from statement
        List<PredicateBase> operators = statement.getInternalOperatorBeans();
        List<OperatorLink> links = statement.getInternalLinkBeans();
        // Assert all statements have an unique id (check whether two operators have the same ID)
        boolean uniqueIds = operators.stream()
                                     .collect(Collectors.groupingBy(op -> op.getID(), Collectors.counting()))
                                     .values()
                                     .stream()
                                     .allMatch( count -> (count==1) );
        Assert.assertTrue(uniqueIds);

        // Iterate the graph (string of nodes) to look for the expected beans
        HashSet<PredicateBase> visitedOperators = new HashSet<>();
        HashSet<OperatorLink> visitedLinks = new HashSet<>();
        
        String initialNode = statement.getInputNodeID();
        String finalNode = statement.getOutputNodeID();
                
        Iterator<PredicateBase> expectedOperatorsIterator = expectedOperators.iterator();
        PredicateBase nextExpectedOperator = null;

        // Start from the initial node and stop when the final node is reached (or an Assert fail)
        String currentBeanId = initialNode;
        while(true){
            // Get the next expected operator to find (if needed)
            if(nextExpectedOperator==null && expectedOperatorsIterator.hasNext()){
                nextExpectedOperator = expectedOperatorsIterator.next();
            }
            // Get the current bean by ID
            String currentLookingBeanId = currentBeanId;
            PredicateBase currentOperatorBean = operators.stream()
                                                        .filter( op -> op.getID().equals(currentLookingBeanId) )
                                                        .findAny()
                                                        .orElse(null);
            Assert.assertNotNull(currentOperatorBean);
            // Add the current visited bean to the set of visited beans and assert it hasn't been visited yet (cycle check)
            Assert.assertTrue(visitedOperators.add(currentOperatorBean));
            // Compare the current bean with the next expected bean
            if(nextExpectedOperator!=null && currentOperatorBean.getClass()==nextExpectedOperator.getClass()){
                // Copy the id of the current bean to the bean we are looking for and assert they are equal
                nextExpectedOperator.setID(currentOperatorBean.getID());
                Assert.assertEquals(nextExpectedOperator, currentOperatorBean);
                nextExpectedOperator = null;
            }else if(!(currentOperatorBean instanceof PassThroughPredicate)){
                // Found a bean that is not PassThrough and is not the expected operator!
                Assert.fail();
            }
            // Break once the final node is visited
            if(currentBeanId.equals(finalNode)){
                break;
            }
            // Get outgoing links for the current bean
            List<OperatorLink> currentOperatorBeanOutgoingLinks = links.stream()
                                    .filter(link -> link.getOrigin().equals(currentOperatorBean.getID()) )
                                    .collect(Collectors.toList());
            // Assert there is only one outgoing link
            Assert.assertEquals(currentOperatorBeanOutgoingLinks.size(), 1);
            OperatorLink currentOperatorBeanOutgoingLink = currentOperatorBeanOutgoingLinks.get(0);
            // Add the outgoing link to the set of visited links and assert it hasn't been visited yet (cycle and duplicate check)
            Assert.assertTrue(visitedLinks.add(currentOperatorBeanOutgoingLink));
            // Set the current bean id to the next bean
            currentBeanId = currentOperatorBeanOutgoingLink.getDestination();
        }

        // Assert there are no more expected operators to look for
        Assert.assertFalse(expectedOperatorsIterator.hasNext());
        // Assert all the operators generated by the statement are visited (no unreachable operators)
        Assert.assertTrue(visitedOperators.containsAll(operators));
        // Assert all the links generated by the statement are visited (no unreachable links)
        Assert.assertTrue(visitedLinks.containsAll(links));
    }
    
}
