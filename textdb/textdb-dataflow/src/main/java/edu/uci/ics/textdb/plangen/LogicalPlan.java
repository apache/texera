package edu.uci.ics.textdb.plangen;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.api.dataflow.ISink;
import edu.uci.ics.textdb.api.plan.Plan;
import edu.uci.ics.textdb.common.exception.PlanGenException;
import edu.uci.ics.textdb.dataflow.connector.OneToNBroadcastConnector;
import edu.uci.ics.textdb.dataflow.join.Join;

/**
 * A graph of operators representing a query plan.
 * 
 * @author Zuozhi Wang
 */
public class LogicalPlan implements Serializable {
    
    private static final long serialVersionUID = -4473743060478893198L;
    
    // a map of an operator ID to the operator's type
    HashMap<String, String> operatorTypeMap;
    // a map of an operator ID to the operator's properties
    HashMap<String, Map<String, String>> operatorPropertyMap;
    // a map of an operator ID to operator's outputs (a set of operator IDs)
    HashMap<String, HashSet<String>> adjacencyList;

    
    public LogicalPlan() {
        operatorTypeMap = new HashMap<>();
        operatorPropertyMap = new HashMap<>();
        adjacencyList = new HashMap<>();
    }
    
    /**
     * Adds an operator to the operator graph by adding it to the set of operators.
     * 
     * @param operatorID, a unique ID of the operator
     * @param operatorType, the type of the operator
     * @param operatorProperties, a key-value pair map of the properties of the operator
     * @throws PlanGenException 
     */
    public void addOperator(String operatorID, String operatorType, Map<String, String> operatorProperties) throws PlanGenException {
        PlanGenUtils.planGenAssert(operatorID != null, "operatorID is null");
        PlanGenUtils.planGenAssert(operatorType != null, "operatorType is null");
        PlanGenUtils.planGenAssert(operatorProperties != null, "operatorProperties is null");
        
        PlanGenUtils.planGenAssert(! operatorID.trim().isEmpty(), "operatorID is empty");
        PlanGenUtils.planGenAssert(! operatorType.trim().isEmpty(), "operatorType is empty");
        
        PlanGenUtils.planGenAssert(! hasOperator(operatorID), "duplicate operatorID: "+operatorID);
        PlanGenUtils.planGenAssert(PlanGenUtils.isValidOperator(operatorType), 
                String.format("%s is an invalid operator type, it must be one of %s.", 
                        operatorType, PlanGenUtils.operatorBuilderMap.keySet().toString()));
  
        operatorTypeMap.put(operatorID, operatorType);
        operatorPropertyMap.put(operatorID, operatorProperties);
        adjacencyList.put(operatorID, new HashSet<>());
        
    }
    
    /**
     * Adds a link from "src" operator to "dest" operator in the graph.
     * 
     * @param src, the operator ID of src operator
     * @param dest, the operator ID of dest operator
     * @throws PlanGenException, if the operator is null, is empty, or doesn't exist.
     */
    public void addLink(String src, String dest) throws PlanGenException {
        PlanGenUtils.planGenAssert(src != null, "src operator is null");
        PlanGenUtils.planGenAssert(dest != null, "dest operator is null");
        
        PlanGenUtils.planGenAssert(! src.trim().isEmpty(), "src operator is empty");
        PlanGenUtils.planGenAssert(! dest.trim().isEmpty(), "dest operator is empty");
        
        PlanGenUtils.planGenAssert(hasOperator(src), String.format("operator %s doesn't exist", src));
        PlanGenUtils.planGenAssert(hasOperator(dest), String.format("operator %s doesn't exist", dest));
        
        adjacencyList.get(src).add(dest);
    }
    
    /**
     * Returns true if the operator graph contains the operatorID.
     * 
     * @param operatorID
     * @return
     */
    public boolean hasOperator(String operatorID) {
        return adjacencyList.containsKey(operatorID);
    }
    
    /**
     * Builds and returns the query plan from the operator graph.
     * 
     * @return the plan generated from the operator graph
     * @throws PlanGenException, if the operator graph is invalid.
     */
    public Plan buildQueryPlan() throws PlanGenException {
        HashMap<String, IOperator> operatorObjectMap = buildOperators();
        validateOperatorGraph();
        connectOperators(operatorObjectMap);
        ISink sink = findSinkOperator(operatorObjectMap);
        
        Plan queryPlan = new Plan(sink);
        return queryPlan;
    }
    
    /*
     * Build the operator objects from operator properties.
     */
    private HashMap<String, IOperator> buildOperators() throws PlanGenException {
        HashMap<String, IOperator> operatorObjectMap = new HashMap<>();
        for (String operatorID : operatorTypeMap.keySet()) {
            IOperator operator = PlanGenUtils.buildOperator(
                    operatorTypeMap.get(operatorID), operatorPropertyMap.get(operatorID));
            operatorObjectMap.put(operatorID, operator);
        }
        return operatorObjectMap;
    }


    /*
     * Validates the operator graph.
     * The operator graph must meet all of the following requirements:
     * 
     *   the graph is a DAG (directed acyclic graph)
     *     this DAG is weakly connected (no unreachable vertices).
     *     there's no cycles in this DAG.
     *   each operator must meet its input and output arity constraints.
     *   the operator graph has at least one source operator.
     *   the operator graph has exactly one sink.
     * 
     * Throws PlanGenException if the operator graph is invalid.
     */
    private void validateOperatorGraph() throws PlanGenException {
        checkGraphConnectivity();
        checkGraphCyclicity();
        checkOperatorInputArity();
        checkOperatorOutputArity();
        checkSourceOperator();
        checkSinkOperator();
    }
  
    /*
     * Detects if the graph can be partitioned disjoint subgraphs.
     * 
     * This function builds an undirected version of the operator graph, and then 
     *   uses a Depth First Search (DFS) algorithm to traverse the graph from any vertex.
     * If the graph is weakly connected, then every vertex should be reached after the traversal.
     * 
     * PlanGenException is thrown if there is an operator not connected to the operator graph.
     * 
     */
    private void checkGraphConnectivity() throws PlanGenException {
        HashMap<String, HashSet<String>> undirectedAdjacencyList = new HashMap<>();
        for (String vertexOrigin : adjacencyList.keySet()) {
            undirectedAdjacencyList.put(vertexOrigin, new HashSet<>(adjacencyList.get(vertexOrigin)));
        }
        for (String vertexOrigin : adjacencyList.keySet()) {
            for (String vertexDestination : adjacencyList.get(vertexOrigin)) {
                undirectedAdjacencyList.get(vertexDestination).add(vertexOrigin);
            }
        }
        
        String vertex = undirectedAdjacencyList.keySet().iterator().next();
        HashSet<String> unvisitedVertices = new HashSet<>(undirectedAdjacencyList.keySet());
        
        connectivityDfsVisit(vertex, undirectedAdjacencyList, unvisitedVertices);
        
        if (! unvisitedVertices.isEmpty()) {
            throw new PlanGenException("Operators: " + unvisitedVertices + " are not connected to the operator graph.");
        }   
    }
    
    /*
     * This is a helper function for checking connectivity by traversing the graph using DFS algorithm. 
     */
    private void connectivityDfsVisit(String vertex, HashMap<String, HashSet<String>> undirectedAdjacencyList, 
            HashSet<String> unvisitedVertices) {
        unvisitedVertices.remove(vertex);       
        for (String adjacentVertex : undirectedAdjacencyList.get(vertex)) {
            if (unvisitedVertices.contains(adjacentVertex)) {
                connectivityDfsVisit(adjacentVertex, undirectedAdjacencyList, unvisitedVertices);
            }
        }
    }
    
    /*
     * Detects if there are any cycles in the operator graph.
     * 
     * This function uses a Depth First Search (DFS) algorithm to traverse the graph.
     * It detects a cycle by maintaining a list of visited vertices, and a list of DFS's path.
     * during the traversal, if it reaches an vertex that is in the DFS path, then there's a cycle.
     * 
     * PlanGenException is thrown if a cycle is detected in the graph.
     * 
     */
    private void checkGraphCyclicity() throws PlanGenException {
        HashSet<String> unvisitedVertices = new HashSet<>(adjacencyList.keySet());
        HashSet<String> dfsPath = new HashSet<>();
        
        for (String vertex : adjacencyList.keySet()) {
            if (unvisitedVertices.contains(vertex)) {
                checkCyclicityDfsVisit(vertex, unvisitedVertices, dfsPath);
            }
        }
    }
    
    /*
     * This is a helper function for detecting cycles by traversing the graph using a DFS algorithm. 
     */
    private void checkCyclicityDfsVisit(String vertex, HashSet<String> unvisitedVertices, 
            HashSet<String> dfsPath) throws PlanGenException {
        unvisitedVertices.remove(vertex);
        dfsPath.add(vertex);
        
        for (String adjacentVertex : adjacencyList.get(vertex)) {
            if (dfsPath.contains(adjacentVertex)) {
                throw new PlanGenException("The following operators form a cycle in operator graph: " + dfsPath);
            }
            if (unvisitedVertices.contains(adjacentVertex)) {
                checkCyclicityDfsVisit(adjacentVertex, unvisitedVertices, dfsPath);
            }
        }
        
        dfsPath.remove(vertex);
    }
    
    /*
     * Checks if the input arities of all operators match the expected input arities.
     */
    private void checkOperatorInputArity() throws PlanGenException {
        HashMap<String, Integer> inputArityMap = new HashMap<>();
        for (String vertex : adjacencyList.keySet()) {
            inputArityMap.put(vertex, 0);
        }
        for (String vertexOrigin : adjacencyList.keySet()) {
            for (String vertexDestination : adjacencyList.get(vertexOrigin)) {
                int newCount = inputArityMap.get(vertexDestination) + 1;
                inputArityMap.put(vertexDestination, newCount);
            }
        }
        
        for (String vertex : inputArityMap.keySet()) {
            int actualInputArity = inputArityMap.get(vertex);
            int expectedInputArity = OperatorArityConstants.getFixedInputArity(operatorTypeMap.get(vertex));
            PlanGenUtils.planGenAssert(
                    actualInputArity == expectedInputArity,
                    String.format("Operator %s should have %d inputs, got %d.", vertex, expectedInputArity, actualInputArity));
        }
    }
    
    /*
     * Checks if the output arity of the operators matches.
     * 
     * All operators (except sink) should have at least 1 output.
     * 
     * The linking operator phase will automatically add a One to N Connector to
     * an operator with multiple outputs, so the output arities are not checked.
     * 
     */
    private void checkOperatorOutputArity() throws PlanGenException {
        for (String vertex : adjacencyList.keySet()) {
            int actualOutputArity = adjacencyList.get(vertex).size();
            int expectedOutputArity = OperatorArityConstants.getFixedOutputArity(operatorTypeMap.get(vertex));

            if (vertex.toLowerCase().contains("sink")) {
                PlanGenUtils.planGenAssert(
                        actualOutputArity == expectedOutputArity,
                        String.format("Sink %s should have %d output links, got %d.", vertex, expectedOutputArity, actualOutputArity));
            } else {
                PlanGenUtils.planGenAssert(
                        actualOutputArity != 0,
                        String.format("Operator %s should have at least %d output links, got 0.", vertex, expectedOutputArity)); 
            }
        }
    }
    
    /*
     * Checks that the operator graph has at least one source operator
     */
    private void checkSourceOperator() throws PlanGenException {
        boolean sourceExist = adjacencyList.keySet().stream()
                .map(operator -> operatorTypeMap.get(operator))
                .anyMatch(type -> type.toLowerCase().contains("source"));
        
        PlanGenUtils.planGenAssert(sourceExist, "There must be at least one source operator.");
    }
    
    /*
     * Checks that the operator graph has exactly one sink operator.
     */
    private void checkSinkOperator() throws PlanGenException {
        long sinkOperatorNumber = adjacencyList.keySet().stream()
                .map(operator -> operatorTypeMap.get(operator))
                .filter(operatorType -> operatorType.toLowerCase().contains("sink"))
                .count();
        
        PlanGenUtils.planGenAssert(sinkOperatorNumber == 1, 
                String.format("There must be exaxtly one sink operator, got %d.", sinkOperatorNumber));
    }
    
    /*
     * Connects IOperator objects together according to the operator graph.
     * 
     * This function assumes that the operator graph is valid.
     * It goes through every link, and invokes
     * the corresponding "setInputOperator" function to connect operators.
     */
    private void connectOperators(HashMap<String, IOperator> operatorObjectMap) throws PlanGenException { 
        for (String vertex : adjacencyList.keySet()) {
            IOperator currentOperator = operatorObjectMap.get(vertex);
            int outputArity = adjacencyList.get(vertex).size();
            
            // automatically adds a OneToNBroadcastConnector if the output arity > 1
            if (outputArity > 1) {
                OneToNBroadcastConnector oneToNConnector = new OneToNBroadcastConnector(outputArity);
                oneToNConnector.setInputOperator(currentOperator);
                int counter = 0;
                for (String adjacentVertex : adjacencyList.get(vertex)) {
                    IOperator adjacentOperator = operatorObjectMap.get(adjacentVertex);
                    handleSetInputOperator(oneToNConnector.getOutputOperator(counter), adjacentOperator);
                    counter++;
                }
            } else {
                for (String adjacentVertex : adjacencyList.get(vertex)) {
                    IOperator adjacentOperator = operatorObjectMap.get(adjacentVertex);
                    handleSetInputOperator(currentOperator, adjacentOperator);
                }
            }         
        }     
    }

    /*
     * Invoke the corresponding "setInputOperator" method of the dest operator.
     */
    private void handleSetInputOperator(IOperator src, IOperator dest) throws PlanGenException {
        // handles Join operator differently
        if (dest instanceof Join) {
            Join join = (Join) dest;
            if (join.getInnerOperator() == null) {
                join.setInnerInputOperator(src);
            } else {
                join.setOuterInputOperator(src);
            }
        // invokes "setInputOperator" for all other operators
        } else {
            try {
                dest.getClass().getMethod("setInputOperator", IOperator.class).invoke(dest, src);
            } catch (NoSuchMethodException | SecurityException | IllegalAccessException 
                    | IllegalArgumentException | InvocationTargetException e) {
                throw new PlanGenException(e.getMessage(), e);
            }  
        }
    }
     
    /*
     * Finds the sink operator in the operator graph.
     * 
     * This function assumes that the graph is valid and there is only one sink in the graph.
     */
    private ISink findSinkOperator(HashMap<String, IOperator> operatorObjectMap) throws PlanGenException {
        IOperator sinkOperator = adjacencyList.keySet().stream()
                .filter(operator -> operatorTypeMap.get(operator).toLowerCase().contains("sink"))
                .map(operator -> operatorObjectMap.get(operator))
                .findFirst().orElse(null);
        
        PlanGenUtils.planGenAssert(sinkOperator != null, "Error: sink operator doesn't exist.");
        PlanGenUtils.planGenAssert(sinkOperator instanceof ISink, "Error: sink operator's type doesn't match.");
        
        return (ISink) sinkOperator;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LogicalPlan that = (LogicalPlan) o;

        if (operatorTypeMap != null ? !operatorTypeMap.equals(that.operatorTypeMap) : that.operatorTypeMap != null)
            return false;
        if (operatorPropertyMap != null ? !operatorPropertyMap.equals(that.operatorPropertyMap) : that.operatorPropertyMap != null)
            return false;
        return adjacencyList != null ? adjacencyList.equals(that.adjacencyList) : that.adjacencyList == null;

    }

    @Override
    public int hashCode() {
        int result = operatorTypeMap != null ? operatorTypeMap.hashCode() : 0;
        result = 31 * result + (operatorPropertyMap != null ? operatorPropertyMap.hashCode() : 0);
        result = 31 * result + (adjacencyList != null ? adjacencyList.hashCode() : 0);
        return result;
    }
}
