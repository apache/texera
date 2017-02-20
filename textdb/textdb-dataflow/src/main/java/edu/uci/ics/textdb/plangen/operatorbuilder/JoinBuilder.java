package edu.uci.ics.textdb.plangen.operatorbuilder;

import java.util.HashMap;
import java.util.Map;

import edu.uci.ics.textdb.common.exception.PlanGenException;
import edu.uci.ics.textdb.dataflow.common.IJoinPredicate;
import edu.uci.ics.textdb.dataflow.common.JoinDistancePredicate;
import edu.uci.ics.textdb.dataflow.join.Join;
import edu.uci.ics.textdb.dataflow.join.SimilarityJoinPredicate;
import edu.uci.ics.textdb.plangen.PlanGenUtils;


/**
 * JoinBuilder provides a static function that builds a Join operator.
 * 
 * JoinBuilder currently needs the following properties:
 * 
 *   predicateType (required)
 *   
 *   innerAttributeName,
 *   outerAttributeName
 *   
 *   requirements for different join predicates:
 *   
 *   predicatType CharacterDistance:
 *      threshold (required), the character distance threshold of this join predicate.
 *   
 *   predicateType SimilarityJoin:
 *      threshold (required), the similarity threshold of this join predicate
 *   
 *   Sample JSON representation:
 *   {
 *      "predicateType" : "CharacterDistance",
 *      "threshold" : "100",
 *      
 *      "innerAttributeName" : "content",
 *      "outerAttributeName" : "content"
 *   }
 *   
 * 
 * @author Zuozhi Wang
 *
 */
public class JoinBuilder {
    
    public static final String JOIN_PREDICATE = "predicateType";
        
    public static final String JOIN_CHARACTER_DISTANCE = "CharacterDistance";
    public static final String JOIN_SIMILARITY = "SimilarityJoin";
    
    public static final String JOIN_THRESHOLD = "threshold";
    public static final String JOIN_INNER_ATTR_NAME = "inner_attribute";
    public static final String JOIN_OUTER_ATTR_NAME = "outer_attribute";
    
    
    public static Join buildOperator(Map<String, String> operatorProperties) throws PlanGenException {        
        String joinPredicateType = OperatorBuilderUtils.getRequiredProperty(JOIN_PREDICATE, operatorProperties);
        PlanGenUtils.planGenAssert(! joinPredicateType.trim().isEmpty(), "Join predicate type is empty");
        
        IJoinPredicate joinPredicate = generateJoinPredicate(joinPredicateType, operatorProperties);
        Join joinOperator = new Join(joinPredicate);
        
        return joinOperator;
    }
    
    
    /*
     * This is an interface to builds a Join Predicate from the given operator properties
     */
    @FunctionalInterface
    private interface GetJoinPredicate {
        IJoinPredicate getJoinPredicate(Map<String, String> operatorProperties) throws PlanGenException;
    }
    
    /*
     * This is a map of join predicates' names to functions that build the corresponding join predicate.
     */
    private static HashMap<String, GetJoinPredicate> joinPredicateHandlerMap = new HashMap<>();
    static {
        joinPredicateHandlerMap.put(JOIN_CHARACTER_DISTANCE.toLowerCase(), JoinBuilder::getJoinCharDistancePredicate);
        joinPredicateHandlerMap.put(JOIN_SIMILARITY.toLowerCase(), JoinBuilder::getSimilarityJoinPredicate);
    }
    
    /*
     * This function returns a Join Predicate from the given join predicate type.
     */
    private static IJoinPredicate generateJoinPredicate(String joinPredicateType, Map<String, String> operatorProperties) throws PlanGenException {
        PlanGenUtils.planGenAssert(joinPredicateHandlerMap.containsKey(joinPredicateType.toLowerCase()), 
                "Join predicate type is invalid, it must be one of " + joinPredicateHandlerMap.keySet());
        return joinPredicateHandlerMap.get(joinPredicateType.toLowerCase()).getJoinPredicate(operatorProperties);
    }
    
    /*
     * This functions builds a JoinDistancePredicate, which is a join predicate of character distance.
     */
    private static JoinDistancePredicate getJoinCharDistancePredicate(
            Map<String, String> operatorProperties) throws PlanGenException{
        String distanceStr = OperatorBuilderUtils.getRequiredProperty(JOIN_THRESHOLD, operatorProperties);
        String innerAttrName = OperatorBuilderUtils.getRequiredProperty(JOIN_INNER_ATTR_NAME, operatorProperties);
        String outerAttrName = OperatorBuilderUtils.getRequiredProperty(JOIN_OUTER_ATTR_NAME, operatorProperties);
        
        if (! innerAttrName.equals(outerAttrName)) {
            throw new PlanGenException("inner attr name and outer attr name must be the same for join distance");
        }

        int distance;
        try {
            distance = Integer.parseInt(distanceStr);
        } catch (NumberFormatException e) {
            throw new PlanGenException(e.getMessage(), e);
        }
        if (distance <= 0) {
            throw new PlanGenException("Join character distance predicate: distance must be greater than 0.");
        }
        
        return new JoinDistancePredicate(outerAttrName, distance);
    }
    
    /*
     * This functions builds a SimilarityJoinPredicate, which is a join predicate of similar span values.
     */
    private static SimilarityJoinPredicate getSimilarityJoinPredicate(
            Map<String, String> operatorProperties) throws PlanGenException {
        
        String thresholdStr = OperatorBuilderUtils.getRequiredProperty(JOIN_THRESHOLD, operatorProperties);
        String outerAttrName = OperatorBuilderUtils.getRequiredProperty(JOIN_OUTER_ATTR_NAME, operatorProperties);
        String innerAttrName = OperatorBuilderUtils.getRequiredProperty(JOIN_INNER_ATTR_NAME, operatorProperties);

        Double threshold;
        try {
            threshold = Double.parseDouble(thresholdStr);
        } catch (NumberFormatException e) {
            throw new PlanGenException(e);
        }
        return new SimilarityJoinPredicate(outerAttrName, innerAttrName, threshold);
    }

}
