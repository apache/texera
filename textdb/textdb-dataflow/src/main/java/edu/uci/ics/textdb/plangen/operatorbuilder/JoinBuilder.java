package edu.uci.ics.textdb.plangen.operatorbuilder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.uci.ics.textdb.common.exception.PlanGenException;
import edu.uci.ics.textdb.dataflow.common.IJoinPredicate;
import edu.uci.ics.textdb.dataflow.common.JoinDistancePredicate;
import edu.uci.ics.textdb.dataflow.join.Join;
import edu.uci.ics.textdb.plangen.PlanGenUtils;


/**
 * JoinBuilder provides a static function that builds a Join operator.
 * 
 * JoinBuilder currently needs the following properties:
 * 
 *   predicateType (required)
 *   
 *   properties required for constructing attributeList, see OperatorBuilderUtils.constructAttributeList
 *   (As of now, join must have one attribute in the attribute list.)
 *   
 *   idAttributeName (required): the name of the "ID" attribute
 *   
 *   requirements for differnt join predicates:
 *   
 *   predicatType CharacterDistance:
 *      distance (required), the character distance threshold of this join predicate.
 *   
 *   Sample JSON representation:
 *   {
 *      "predicateType" : "CharacterDistance",
 *      "distance" : "100",
 *      
 *      "attributeName" : "content",
 *      "idAttributeName" : "id",
 *   }
 * 
 * @author Zuozhi Wang
 *
 */
public class JoinBuilder {
    
    public static final String JOIN_PREDICATE = "predicateType";
    
    public static final String JOIN_ID_ATTRIBUTE_NAME = "idAttributeName";
    
    public static final String JOIN_CHARACTER_DISTANCE = "CharacterDistance";
    public static final String JOIN_DISTANCE = "distance";
    
    
    public static Join buildOperator(Map<String, String> operatorProperties) throws PlanGenException {        
        String joinPredicateType = OperatorBuilderUtils.getRequiredProperty(JOIN_PREDICATE, operatorProperties);
        PlanGenUtils.planGenAssert(! joinPredicateType.trim().isEmpty(), "Join predicate type is empty");
        
        IJoinPredicate joinPredicate = generateJoinPredicate(joinPredicateType, operatorProperties);
        Join joinOperator = new Join(null, null, joinPredicate);
        
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
        String distanceStr = OperatorBuilderUtils.getRequiredProperty(JOIN_DISTANCE, operatorProperties);
        String joinIDAttributeName = OperatorBuilderUtils.getRequiredProperty(JOIN_ID_ATTRIBUTE_NAME, operatorProperties);
        
        PlanGenUtils.planGenAssert(! joinIDAttributeName.trim().isEmpty(), 
                "Join character distance predicate: ID attribute name is empty.");
        
        List<String> attributeNames = OperatorBuilderUtils.constructAttributeNames(operatorProperties);
        PlanGenUtils.planGenAssert(attributeNames.size() == 1, 
                "Join character distance predicate allows only 1 attribute, got " + attributeNames.size()+  " attributes.");
        String joinAttributeName = attributeNames.get(0);
        
        int distance;
        try {
            distance = Integer.parseInt(distanceStr);
        } catch (NumberFormatException e) {
            throw new PlanGenException(e.getMessage(), e);
        }
        if (distance <= 0) {
            throw new PlanGenException("Join character distance predicate: distance must be greater than 0.");
        }
        
        return new JoinDistancePredicate(joinIDAttributeName, joinAttributeName, distance);
    }

}
