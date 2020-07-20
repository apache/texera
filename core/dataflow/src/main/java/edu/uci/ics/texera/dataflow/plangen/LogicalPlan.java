package edu.uci.ics.texera.dataflow.plangen;


import java.lang.reflect.InvocationTargetException;
import java.util.*;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uci.ics.texera.api.dataflow.IOperator;
import edu.uci.ics.texera.api.dataflow.ISink;
import edu.uci.ics.texera.api.dataflow.ISourceOperator;
import edu.uci.ics.texera.api.engine.Plan;
import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.exception.PlanGenException;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.dataflow.common.PredicateBase;
import edu.uci.ics.texera.dataflow.common.PropertyNameConstants;
import edu.uci.ics.texera.dataflow.connector.OneToNBroadcastConnector;
import edu.uci.ics.texera.dataflow.join.Join;
import edu.uci.ics.texera.api.schema.Schema;


/**
 * A graph of operators representing a query plan.
 * 
 * @author Zuozhi Wang
 */
public class LogicalPlan {

    private QueryContext context;

    // a map from operatorID to its operator
    private HashMap<String, IOperator> operatorObjectMap;
    // use LinkedHashMap to retain insertion order
    // a map from operatorID to its predicate
    private LinkedHashMap<String, PredicateBase> operatorPredicateMap;
    // a map of an operator ID to operator's outputs (a set of operator IDs)
    private LinkedHashMap<String, LinkedHashSet<String>> adjacencyList;
    /**
     * Create an empty logical plan.
     * 
     * This class is not a JSON entry point. It is for internal use only.
     */
    public LogicalPlan() {
        operatorPredicateMap = new LinkedHashMap<>();
        adjacencyList = new LinkedHashMap<>();

    }
    
    /**
     * Create a LogicalPlan from an existing plan (represented by a list of operators and a list of links)
     * 
     * @param predicateList, a list of operator predicates
     * @param operatorLinkList, a list of operator links
     */
    @JsonCreator
    public LogicalPlan(
            @JsonProperty(value = PropertyNameConstants.OPERATOR_LIST, required = true)
            List<PredicateBase> predicateList,
            @JsonProperty(value = PropertyNameConstants.OPERATOR_LINK_LIST, required = true)
            List<OperatorLink> operatorLinkList
            ) {
        // initialize private variables
        this();
        // add predicates and links
        for (PredicateBase predicate : predicateList) {
            addOperator(predicate);
        }
        for (OperatorLink link : operatorLinkList) {
            addLink(link);
        }
    }

    @JsonIgnore
    public QueryContext getContext() {
        return context;
    }

    @JsonIgnore
    public void setContext(QueryContext context) {
        this.context = context;
    }

    /**
     * Gets the list of operator predicates.
     * Order is NOT guaranteed to be the same as insertion order.
     * @return a list of operator predicates
     */
    @JsonProperty(value = PropertyNameConstants.OPERATOR_LIST)
    public List<PredicateBase> getPredicateList() {
        return new ArrayList<>(operatorPredicateMap.values());
    }
    
    /**
     * Gets the list of operator links.
     * Order is NOT guaranteed to be the same as insertion order.
     * @return a list of operator links
     */
    @JsonProperty(value = PropertyNameConstants.OPERATOR_LINK_LIST)
    public List<OperatorLink> getOperatorLinkList() {
        ArrayList<OperatorLink> linkList = new ArrayList<>();
        for (String origin : adjacencyList.keySet()) {
            for (String destination : adjacencyList.get(origin)) {
                linkList.add(new OperatorLink(origin, destination));
            }
        }
        return linkList;
    }

    /**
     * Updates the current plan and fetch the schema from an operator
     * @param operatorID, the ID of an operator
     * @return Schema, which includes the attributes setting of the operator
     */
    public Schema getOperatorOutputSchema(String operatorID) throws PlanGenException, DataflowException {

        buildOperators();
        checkGraphCyclicity();
        connectOperators(operatorObjectMap);

        IOperator currentOperator = operatorObjectMap.get(operatorID);
        currentOperator.open();
        Schema operatorSchema = currentOperator.getOutputSchema();
        currentOperator.close();

        return operatorSchema;
    }

    /**
     * Updates the current plan and fetch the schema from an operator
     * @param operatorID, the ID of an operator
     * @param operatorInputSchemaMap Map of operators to their input schemas
     * @return Schema, which includes the attributes setting of the operator
     */
    public Optional<Schema> getOperatorOutputSchema(String operatorID, Map<String, List<Schema>> operatorInputSchemaMap)
            throws PlanGenException, DataflowException {

        IOperator currentOperator = operatorObjectMap.get(operatorID);
        Optional<Schema> outputSchema = Optional.empty();
        if (currentOperator instanceof ISourceOperator) {
            outputSchema = Optional.ofNullable(currentOperator.transformToOutputSchema());
        } else if (operatorInputSchemaMap.containsKey(operatorID)) {
            List<Schema> inputSchemaList = operatorInputSchemaMap.get(operatorID);
            try {
                outputSchema = Optional.ofNullable(currentOperator.transformToOutputSchema(
                        inputSchemaList.toArray(new Schema[inputSchemaList.size()])));
            } catch (TexeraException e) {
                System.out.println(e.getMessage());
            }
        }
        return outputSchema;
    }

    /**
     * For each operator, get its input schema based on the topological order of the graph
     * @return Map where id of the operator as the key and input schema as the value
     * @throws PlanGenException
     */
    public Map<String, List<Schema>> retrieveAllOperatorInputSchema() throws PlanGenException {

        buildOperators();
        checkGraphCyclicity();

        // Calculate the in-edge count of each operator
        Map<String, Integer> inEdgeCount = new HashMap<>();
        for (Map.Entry<String, LinkedHashSet<String>> entry: adjacencyList.entrySet()) {
            inEdgeCount.putIfAbsent(entry.getKey(), 0);
            for (String to: entry.getValue()) {
                inEdgeCount.put(to, inEdgeCount.getOrDefault(to, 0)+1);
            }
        }

        // This queue will contain all operators for which input schemas have been completely found. At start, it has all the source operator
        Queue<String> operatorQueue = new LinkedList<>();
        for (Map.Entry<String, IOperator> entry: operatorObjectMap.entrySet()) {
            if (entry.getValue() instanceof ISourceOperator) {
                operatorQueue.add(entry.getKey());
            }
        }

        // Retrieve input schema based on topological order. Initially, the queue contains only source operators and we find their output schemas.
        // The output schema of an operator becomes the input schema of another. When, we find output schema of one operator, we record that for the
        // next operator, we have found one of its input schema and decrease its in-edge count by one (in-edge count represents the inputs for which schema hasn't yet been determined).
        // When in-edge count reaches 0, all input schemas of the operator has been found. So, the operator is put into queue (as an operator for which we can find output schema).
        Map<String, List<Schema>> inputSchemas = new HashMap<>();
        while (!operatorQueue.isEmpty()) {
            String origin = operatorQueue.poll();
            Optional<Schema> currentOutputSchema = getOperatorOutputSchema(origin, inputSchemas);

            if(!currentOutputSchema.isPresent()) {
                continue;
            }

            for (String destination: adjacencyList.get(origin)) {
                if (inputSchemas.containsKey(destination)) {
                    inputSchemas.get(destination).add(currentOutputSchema.get());
                } else {
                    inputSchemas.put(destination, new ArrayList<>(Arrays.asList(currentOutputSchema.get())));
                }

                inEdgeCount.put(destination, inEdgeCount.get(destination) - 1);
                if (inEdgeCount.get(destination) == 0) {
                    inEdgeCount.remove(destination);
                    operatorQueue.offer(destination);
                }
            }

        }
        return inputSchemas;
    }

    /**
     * Adds a new operator to the logical plan.
     * @param operatorPredicate, the predicate of the operator
     */
    public void addOperator(PredicateBase operatorPredicate) {

        String operatorID = operatorPredicate.getID();
        PlanGenUtils.planGenAssert(! hasOperator(operatorID), 
                String.format("duplicate operator id: %s is found", operatorID));
        operatorPredicateMap.put(operatorID, operatorPredicate);
        adjacencyList.put(operatorID, new LinkedHashSet<>());
    }

    /**
     * Adds a new link to the logical plan
     * @param operatorLink, a link of two operators
     */
    public void addLink(OperatorLink operatorLink) {

        String origin = operatorLink.getOrigin();
        String destination = operatorLink.getDestination();
        PlanGenUtils.planGenAssert(hasOperator(origin), 
                String.format("origin operator id: %s is not found", origin));
        PlanGenUtils.planGenAssert(hasOperator(destination), 
                String.format("destination operator id: %s is not found", destination));
        adjacencyList.get(origin).add(destination);
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

        buildOperators();
        validateOperatorGraph();
        connectOperators(operatorObjectMap);
        HashMap<String, ISink> sinkMap = findSinkOperators(operatorObjectMap);

        return new Plan(sinkMap);
    }
    
    /*
     * Build the operator objects from operator properties.
     */
    private void buildOperators() throws PlanGenException {
        operatorObjectMap = new HashMap<>();
        for (String operatorID : operatorPredicateMap.keySet()) {
            IOperator operator = operatorPredicateMap.get(operatorID).newOperator(context);
            operatorObjectMap.put(operatorID, operator);
        }
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
     *   the operator graph has at least one sink.
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
            int expectedInputArity = OperatorArityConstants.getFixedInputArity(
                    operatorPredicateMap.get(vertex).getClass());
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
            Class<? extends PredicateBase> predicateClass = operatorPredicateMap.get(vertex).getClass();
            
            int actualOutputArity = adjacencyList.get(vertex).size();
            int expectedOutputArity = OperatorArityConstants.getFixedOutputArity(predicateClass);
            
            if (predicateClass.toString().toLowerCase().contains("sink")) {
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
                .map(operator -> operatorPredicateMap.get(operator).getClass().toString())
                .anyMatch(predicateClass -> predicateClass.toLowerCase().contains("source"));
        
        PlanGenUtils.planGenAssert(sourceExist, "There must be at least one source operator.");
    }
    
    /*
     * Checks that the operator graph has exactly one sink operator.
     */
    private void checkSinkOperator() throws PlanGenException {
        long sinkOperatorNumber = adjacencyList.keySet().stream()
                .map(operator -> operatorPredicateMap.get(operator).getClass().toString())
                .filter(predicateClass -> predicateClass.toLowerCase().contains("sink"))
                .count();
        
        PlanGenUtils.planGenAssert(sinkOperatorNumber > 0,
                String.format("There must be at least one sink operator, got %d.", sinkOperatorNumber));


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
            if (join.getInnerInputOperator() == null) {
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
     * Finds all sink operators in the operator graph.
     * his function assumes that the graph is valid and there is at least one sink in the graph.
     */
    private HashMap<String, ISink> findSinkOperators(HashMap<String, IOperator> operatorObjectMap) throws PlanGenException {

        HashMap<String, IOperator> operatorMap = new HashMap<>();
        for (HashMap.Entry<String, IOperator> entry: operatorObjectMap.entrySet()) {
            if (entry.getValue() instanceof ISink) {
                operatorMap.put(entry.getKey(), entry.getValue());
            }
        }

        PlanGenUtils.planGenAssert(operatorMap.size() > 0, "Error: sink operator doesn't exist.");
        HashMap<String, ISink> sinkMap = new HashMap<>();
        for (HashMap.Entry<String, IOperator> entry: operatorMap.entrySet()) {
            PlanGenUtils.planGenAssert(entry.getValue() instanceof ISink, "Error: sink operator's type doesn't match.");
            sinkMap.put(entry.getKey(), (ISink)entry.getValue());

        }
        return sinkMap;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LogicalPlan that = (LogicalPlan) o;

        if (operatorPredicateMap != null ? !operatorPredicateMap.equals(that.operatorPredicateMap) : that.operatorPredicateMap != null)
            return false;
        return adjacencyList != null ? adjacencyList.equals(that.adjacencyList) : that.adjacencyList == null;

    }

    @Override
    public int hashCode() {
        int result = operatorPredicateMap != null ? operatorPredicateMap.hashCode() : 0;
        result = 31 * result + (adjacencyList != null ? adjacencyList.hashCode() : 0);
        return result;
    }
}
