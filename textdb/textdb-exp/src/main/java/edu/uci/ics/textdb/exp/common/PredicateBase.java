package edu.uci.ics.textdb.exp.common;

import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.api.dataflow.IPredicate;
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
import edu.uci.ics.textdb.exp.sink.excel.ExcelSinkPredicate;
import edu.uci.ics.textdb.exp.sink.tuple.TupleSinkPredicate;
import edu.uci.ics.textdb.exp.source.file.FileSourcePredicate;
import edu.uci.ics.textdb.exp.source.scan.ScanSourcePredicate;


/**
 * PredicateBase is the base for all predicates which follow the 
 *   Predicate Bean pattern.
 * 
 * Every predicate needs to register itself in the JsonSubTypes annotation
 *   so that the Jackson Library can map each JSON string to the correct type
 * 
 * @author Zuozhi Wang
 *
 */
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME, // logical user-defined type names are used (rather than Java class names)
        include = JsonTypeInfo.As.PROPERTY, // make the type info as a property in the JSON representation
        property = PropertyNameConstants.OPERATOR_TYPE // the name of the JSON property indicating the type
)
@JsonSubTypes({ 
        @Type(value = DictionaryPredicate.class, name = "DictionaryMatcher"), 
        @Type(value = DictionarySourcePredicate.class, name = "DictionarySource"), 
        @Type(value = FuzzyTokenPredicate.class, name = "FuzzyTokenMatcher"), 
        @Type(value = FuzzyTokenSourcePredicate.class, name = "FuzzyTokenSource"), 
        @Type(value = KeywordPredicate.class, name = "KeywordMatcher"), 
        @Type(value = KeywordSourcePredicate.class, name = "KeywordSource"), 
        @Type(value = RegexPredicate.class, name = "RegexMatcher"), 
        @Type(value = RegexSourcePredicate.class, name = "RegexSource"), 
        
        @Type(value = JoinDistancePredicate.class, name = "JoinDistance"),
        @Type(value = SimilarityJoinPredicate.class, name = "SimilarityJoin"),
        
        @Type(value = NlpEntityPredicate.class, name = "NlpEntity"),
        @Type(value = NlpSentimentPredicate.class, name = "NlpSentiment"),
        @Type(value = ProjectionPredicate.class, name = "Projection"),
        @Type(value = RegexSplitPredicate.class, name = "RegexSplit"),
        @Type(value = SamplerPredicate.class, name = "Sampler"),
        
        @Type(value = ScanSourcePredicate.class, name = "ScanSource"),
        @Type(value = FileSourcePredicate.class, name = "FileSink"),        
        @Type(value = TupleSinkPredicate.class, name = "ViewResults"),
        @Type(value = ExcelSinkPredicate.class, name = "ExcelSink"),
        
})
public abstract class PredicateBase implements IPredicate {
    
    // default id is random uuid (internal code doesn't care about id)
    private String id = UUID.randomUUID().toString();
    
    @JsonProperty(PropertyNameConstants.OPERATOR_ID)
    public void setID(String id) {
        this.id = id;
    }
    
    @JsonProperty(PropertyNameConstants.OPERATOR_ID)
    public String getID() {
        return id;
    }
    
    @JsonIgnore
    public abstract IOperator newOperator();
    
}
