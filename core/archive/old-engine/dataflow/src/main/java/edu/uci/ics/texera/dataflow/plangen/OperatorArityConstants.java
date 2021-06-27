package edu.uci.ics.texera.dataflow.plangen;

import edu.uci.ics.texera.dataflow.sink.barchart.BarChartSinkPredicate;
import edu.uci.ics.texera.dataflow.sink.linechart.LineChartSinkPredicate;
import edu.uci.ics.texera.dataflow.sink.piechart.PieChartSinkPredicate;
import edu.uci.ics.texera.dataflow.sink.wordcloud.WordCloudSink;
import edu.uci.ics.texera.dataflow.sink.wordcloud.WordCloudSinkPredicate;
import java.util.HashMap;
import java.util.Map;

import edu.uci.ics.texera.api.exception.PlanGenException;
import edu.uci.ics.texera.dataflow.aggregator.AggregatorPredicate;
import edu.uci.ics.texera.dataflow.nlp.sentiment.arrow.NltkSentimentPredicate;
import edu.uci.ics.texera.dataflow.common.PredicateBase;
import edu.uci.ics.texera.dataflow.comparablematcher.ComparablePredicate;
import edu.uci.ics.texera.dataflow.dictionarymatcher.DictionaryPredicate;
import edu.uci.ics.texera.dataflow.dictionarymatcher.DictionarySourcePredicate;
import edu.uci.ics.texera.dataflow.fuzzytokenmatcher.FuzzyTokenPredicate;
import edu.uci.ics.texera.dataflow.fuzzytokenmatcher.FuzzyTokenSourcePredicate;
import edu.uci.ics.texera.dataflow.join.JoinDistancePredicate;
import edu.uci.ics.texera.dataflow.join.SimilarityJoinPredicate;
import edu.uci.ics.texera.dataflow.keywordmatcher.KeywordPredicate;
import edu.uci.ics.texera.dataflow.keywordmatcher.KeywordSourcePredicate;
import edu.uci.ics.texera.dataflow.nlp.entity.NlpEntityPredicate;
import edu.uci.ics.texera.dataflow.nlp.sentiment.EmojiSentimentPredicate;
import edu.uci.ics.texera.dataflow.nlp.sentiment.NlpSentimentPredicate;
import edu.uci.ics.texera.dataflow.nlp.splitter.NlpSplitPredicate;
import edu.uci.ics.texera.dataflow.projection.ProjectionPredicate;
import edu.uci.ics.texera.dataflow.regexmatcher.RegexPredicate;
import edu.uci.ics.texera.dataflow.regexmatcher.RegexSourcePredicate;
import edu.uci.ics.texera.dataflow.regexsplit.RegexSplitPredicate;
import edu.uci.ics.texera.dataflow.sampler.SamplerPredicate;
import edu.uci.ics.texera.dataflow.sink.excel.ExcelSinkPredicate;
import edu.uci.ics.texera.dataflow.sink.mysql.MysqlSinkPredicate;
import edu.uci.ics.texera.dataflow.sink.tuple.TupleSinkPredicate;
import edu.uci.ics.texera.dataflow.source.asterix.AsterixSourcePredicate;
import edu.uci.ics.texera.dataflow.source.file.FileSourcePredicate;
import edu.uci.ics.texera.dataflow.source.mysql.MysqlSourcePredicate;
import edu.uci.ics.texera.dataflow.source.scan.ScanSourcePredicate;
import edu.uci.ics.texera.dataflow.twitter.TwitterJsonConverterPredicate;
import edu.uci.ics.texera.dataflow.twitterfeed.TwitterFeedSourcePredicate;
import edu.uci.ics.texera.dataflow.wordcount.WordCountIndexSourcePredicate;
import edu.uci.ics.texera.dataflow.wordcount.WordCountOperatorPredicate;

/**
 * OperatorArityConstants class includes the input and output arity constraints of each operator.
 * 
 * @author Zuozhi Wang
 *
 */
public class OperatorArityConstants {
    
    public static Map<Class<? extends PredicateBase>, Integer> fixedInputArityMap = new HashMap<>();
    static {

        fixedInputArityMap.put(MysqlSourcePredicate.class, 0);
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
        fixedInputArityMap.put(EmojiSentimentPredicate.class, 1);
        fixedInputArityMap.put(ProjectionPredicate.class, 1);
        fixedInputArityMap.put(RegexSplitPredicate.class, 1);
        fixedInputArityMap.put(NlpSplitPredicate.class, 1);
        fixedInputArityMap.put(SamplerPredicate.class, 1);
        fixedInputArityMap.put(WordCountIndexSourcePredicate.class, 0);
        fixedInputArityMap.put(WordCountOperatorPredicate.class, 1);
        fixedInputArityMap.put(ComparablePredicate.class, 1);
        fixedInputArityMap.put(AggregatorPredicate.class, 1);

        fixedInputArityMap.put(AsterixSourcePredicate.class, 0);
        
        fixedInputArityMap.put(TwitterJsonConverterPredicate.class, 1);

        fixedInputArityMap.put(ScanSourcePredicate.class, 0);
        fixedInputArityMap.put(FileSourcePredicate.class, 0);
        fixedInputArityMap.put(TwitterFeedSourcePredicate.class, 0);
        
        fixedInputArityMap.put(TupleSinkPredicate.class, 1);
        fixedInputArityMap.put(ExcelSinkPredicate.class, 1);
        fixedInputArityMap.put(MysqlSinkPredicate.class, 1);

        fixedInputArityMap.put(WordCloudSinkPredicate.class , 1);
        fixedInputArityMap.put(BarChartSinkPredicate.class, 1);
        fixedInputArityMap.put(PieChartSinkPredicate.class, 1);
        fixedInputArityMap.put(LineChartSinkPredicate.class, 1);

        fixedInputArityMap.put(NltkSentimentPredicate.class, 1);

        
    }
    
    public static Map<Class<? extends PredicateBase>, Integer> fixedOutputArityMap = new HashMap<>();
    static {

        fixedOutputArityMap.put(MysqlSourcePredicate.class, 1);
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
        fixedOutputArityMap.put(EmojiSentimentPredicate.class, 1);
        fixedOutputArityMap.put(ProjectionPredicate.class, 1);
        fixedOutputArityMap.put(RegexSplitPredicate.class, 1);
        fixedOutputArityMap.put(NlpSplitPredicate.class, 1);
        fixedOutputArityMap.put(SamplerPredicate.class, 1);
        fixedOutputArityMap.put(WordCountIndexSourcePredicate.class, 1);
        fixedOutputArityMap.put(WordCountOperatorPredicate.class, 1);
        fixedOutputArityMap.put(ComparablePredicate.class, 1);
        fixedOutputArityMap.put(AggregatorPredicate.class, 1);

        fixedOutputArityMap.put(AsterixSourcePredicate.class, 1);
        
        fixedOutputArityMap.put(TwitterJsonConverterPredicate.class, 1);

        fixedOutputArityMap.put(ScanSourcePredicate.class, 1);
        fixedOutputArityMap.put(FileSourcePredicate.class, 1);
        fixedOutputArityMap.put(TwitterFeedSourcePredicate.class, 1);
        
        fixedOutputArityMap.put(TupleSinkPredicate.class, 0);
        fixedOutputArityMap.put(ExcelSinkPredicate.class, 0);
        fixedOutputArityMap.put(MysqlSinkPredicate.class, 0);

        fixedOutputArityMap.put(WordCloudSinkPredicate.class , 0);
        fixedOutputArityMap.put(BarChartSinkPredicate.class, 0);
        fixedOutputArityMap.put(PieChartSinkPredicate.class, 0);
        fixedOutputArityMap.put(LineChartSinkPredicate.class, 0);

        fixedOutputArityMap.put(NltkSentimentPredicate.class, 1);

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
