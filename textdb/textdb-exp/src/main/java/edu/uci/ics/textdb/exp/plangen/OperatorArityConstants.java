package edu.uci.ics.textdb.exp.plangen;

import java.util.HashMap;
import java.util.Map;

import edu.uci.ics.textdb.api.exception.PlanGenException;
import edu.uci.ics.textdb.exp.common.PredicateBase;
import edu.uci.ics.textdb.exp.dictionarymatcher.DictionaryPredicate;
import edu.uci.ics.textdb.exp.dictionarymatcher.DictionarySourcePredicate;
import edu.uci.ics.textdb.exp.fuzzytokenmatcher.FuzzyTokenPredicate;
import edu.uci.ics.textdb.exp.fuzzytokenmatcher.FuzzyTokenSourcePredicate;
import edu.uci.ics.textdb.exp.join.JoinDistancePredicate;
import edu.uci.ics.textdb.exp.join.SimilarityJoinPredicate;
import edu.uci.ics.textdb.exp.keywordmatcher.KeywordPredicate;
import edu.uci.ics.textdb.exp.keywordmatcher.KeywordSourcePredicate;
import edu.uci.ics.textdb.exp.nlp.entity.NlpEntityPredicate;
import edu.uci.ics.textdb.exp.nlp.sentiment.NlpSentimentPredicate;
import edu.uci.ics.textdb.exp.projection.ProjectionPredicate;
import edu.uci.ics.textdb.exp.regexmatcher.RegexPredicate;
import edu.uci.ics.textdb.exp.regexmatcher.RegexSourcePredicate;
import edu.uci.ics.textdb.exp.regexsplit.RegexSplitPredicate;
import edu.uci.ics.textdb.exp.sampler.SamplerPredicate;
import edu.uci.ics.textdb.exp.sink.tuple.TupleSinkPredicate;
import edu.uci.ics.textdb.exp.source.file.FileSourcePredicate;
import edu.uci.ics.textdb.exp.source.scan.ScanSourcePredicate;

/**
 * OperatorArityConstants class includes the input and output arity constraints of each operator.
 * 
 * @author Zuozhi Wang
 *
 */
public class OperatorArityConstants {
    
    public static Map<Class<? extends PredicateBase>, Integer> fixedInputArityMap = new HashMap<>();
    static {
        fixedInputArityMap.put(DictionaryPredicate.class, 1); 
        fixedInputArityMap.put(DictionarySourcePredicate.class, 0); 
        fixedInputArityMap.put(FuzzyTokenPredicate.class, 1); 
        fixedInputArityMap.put(FuzzyTokenSourcePredicate.class, 0); 
        fixedInputArityMap.put(KeywordPredicate.class, 1); 
        fixedInputArityMap.put(KeywordSourcePredicate.class, 0); 
        fixedInputArityMap.put(RegexPredicate.class, 1); 
        fixedInputArityMap.put(RegexSourcePredicate.class, 0); 

        fixedInputArityMap.put(JoinDistancePredicate.class, 2);
        fixedInputArityMap.put(SimilarityJoinPredicate.class, 2);

        fixedInputArityMap.put(NlpEntityPredicate.class, 1);
        fixedInputArityMap.put(NlpSentimentPredicate.class, 1);
        fixedInputArityMap.put(ProjectionPredicate.class, 1);
        fixedInputArityMap.put(RegexSplitPredicate.class, 1);
        fixedInputArityMap.put(SamplerPredicate.class, 1);

        fixedInputArityMap.put(ScanSourcePredicate.class, 0);
        fixedInputArityMap.put(FileSourcePredicate.class, 0);
        
        fixedInputArityMap.put(TupleSinkPredicate.class, 1);
        
    }
    
    public static Map<Class<? extends PredicateBase>, Integer> fixedOutputArityMap = new HashMap<>();
    static {
        fixedOutputArityMap.put(DictionaryPredicate.class, 1); 
        fixedOutputArityMap.put(DictionarySourcePredicate.class, 1); 
        fixedOutputArityMap.put(FuzzyTokenPredicate.class, 1); 
        fixedOutputArityMap.put(FuzzyTokenSourcePredicate.class, 1); 
        fixedOutputArityMap.put(KeywordPredicate.class, 1); 
        fixedOutputArityMap.put(KeywordSourcePredicate.class, 1); 
        fixedOutputArityMap.put(RegexPredicate.class, 1); 
        fixedOutputArityMap.put(RegexSourcePredicate.class, 1); 

        fixedOutputArityMap.put(JoinDistancePredicate.class, 1);
        fixedOutputArityMap.put(SimilarityJoinPredicate.class, 1);

        fixedOutputArityMap.put(NlpEntityPredicate.class, 1);
        fixedOutputArityMap.put(NlpSentimentPredicate.class, 1);
        fixedOutputArityMap.put(ProjectionPredicate.class, 1);
        fixedOutputArityMap.put(RegexSplitPredicate.class, 1);
        fixedOutputArityMap.put(SamplerPredicate.class, 1);

        fixedOutputArityMap.put(ScanSourcePredicate.class, 1);
        fixedOutputArityMap.put(FileSourcePredicate.class, 1);
        
        fixedOutputArityMap.put(TupleSinkPredicate.class, 0);
        
    }
    
    /**
     * Gets the input arity of an operator type.
     * 
     * @param operatorType
     * @return
     * @throws PlanGenException, if the oeprator's input arity is not specified.
     */
    public static int getFixedInputArity(Class<? extends PredicateBase> predicateClass) {
        PlanGenUtils.planGenAssert(fixedInputArityMap.containsKey(predicateClass), 
                String.format("input arity of %s is not specified.", predicateClass));
        return fixedInputArityMap.get(predicateClass);
    }
    
    /**
     * Gets the output arity of an operator type.
     * 
     * @param operatorType
     * @return
     * @throws PlanGenException, if the oeprator's output arity is not specified.
     */
    public static int getFixedOutputArity(Class<? extends PredicateBase> predicateClass) {
        PlanGenUtils.planGenAssert(fixedOutputArityMap.containsKey(predicateClass), 
                String.format("output arity of %s is not specified.", predicateClass));
        return fixedOutputArityMap.get(predicateClass);
    }
    
}
