package edu.uci.ics.texera.dataflow.common;

import edu.uci.ics.texera.dataflow.sink.barchart.BarChartSinkPredicate;
import edu.uci.ics.texera.dataflow.sink.linechart.LineChartSinkPredicate;
import edu.uci.ics.texera.dataflow.sink.piechart.PieChartSinkPredicate;
import edu.uci.ics.texera.dataflow.sink.wordcloud.WordCloudSinkPredicate;
import java.util.UUID;

import edu.uci.ics.texera.dataflow.aggregator.AggregatorPredicate;
import edu.uci.ics.texera.dataflow.plangen.QueryContext;
import edu.uci.ics.texera.dataflow.twitterfeed.TwitterFeedSourcePredicate;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import edu.uci.ics.texera.api.dataflow.IOperator;
import edu.uci.ics.texera.api.dataflow.IPredicate;
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
import edu.uci.ics.texera.dataflow.sink.mysql.MysqlSinkPredicate;
import edu.uci.ics.texera.dataflow.sink.tuple.TupleSinkPredicate;
import edu.uci.ics.texera.dataflow.source.asterix.AsterixSourcePredicate;
import edu.uci.ics.texera.dataflow.source.file.FileSourcePredicate;
import edu.uci.ics.texera.dataflow.source.mysql.MysqlSourcePredicate;
import edu.uci.ics.texera.dataflow.source.scan.ScanSourcePredicate;
import edu.uci.ics.texera.dataflow.twitter.TwitterJsonConverterPredicate;
import edu.uci.ics.texera.dataflow.wordcount.WordCountIndexSourcePredicate;
import edu.uci.ics.texera.dataflow.wordcount.WordCountOperatorPredicate;
import edu.uci.ics.texera.dataflow.nlp.sentiment.arrow.NltkSentimentPredicate;

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
        @Type(value = EmojiSentimentPredicate.class, name = "EmojiSentiment"),

        @Type(value = ProjectionPredicate.class, name = "Projection"),
        @Type(value = RegexSplitPredicate.class, name = "RegexSplit"),
        @Type(value = NlpSplitPredicate.class, name = "NlpSplit"),
        @Type(value = SamplerPredicate.class, name = "Sampler"),

        // remove comparable matcher because of the json schema "any" issue
        // TODO: fix the problem and add Comparable matcher back later
         @Type(value = ComparablePredicate.class, name = "Comparison"),
        
        @Type(value = AsterixSourcePredicate.class, name = "AsterixSource"),
        @Type(value = MysqlSourcePredicate.class, name = "MysqlSource"),
        @Type(value = TwitterJsonConverterPredicate.class, name = "TwitterJsonConverter"),
        
        @Type(value = ScanSourcePredicate.class, name = "ScanSource"),
        @Type(value = FileSourcePredicate.class, name = "FileSource"),        
        @Type(value = TupleSinkPredicate.class, name = "ViewResults"),
        @Type(value = MysqlSinkPredicate.class, name = "MysqlSink"),
        @Type(value = TwitterFeedSourcePredicate.class, name = "TwitterFeed"),
        
        @Type(value = WordCountIndexSourcePredicate.class, name = "WordCountIndexSource"),
        @Type(value = WordCountOperatorPredicate.class, name = "WordCount"),
        @Type(value = AggregatorPredicate.class, name = "Aggregation"),

        @Type(value = BarChartSinkPredicate.class, name = "BarChart"),
        @Type(value = PieChartSinkPredicate.class, name = "PieChart"),
        @Type(value = LineChartSinkPredicate.class, name = "LineChart"),
        @Type(value = NltkSentimentPredicate.class, name = "NltkSentiment"),

        @Type(value = WordCloudSinkPredicate.class, name = "WordCloud"),

        @Type(value = NltkSentimentPredicate.class, name = "NltkSentiment")
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
    public IOperator newOperator() {
        throw new UnsupportedOperationException("not implemented");
    }

    @JsonIgnore
    public IOperator newOperator(QueryContext ctx) {
        return newOperator();
    }
    
    @Override
    public int hashCode() {
        // TODO: evaluate performance impact using reflection
        return HashCodeBuilder.reflectionHashCode(this);
    }
    
    @Override
    public boolean equals(Object that) {
        // TODO: evaluate performance impact using reflection
        return EqualsBuilder.reflectionEquals(this, that);
    }
    
    @Override
    public String toString() {
        // TODO: evaluate performance impact using reflection
        return ToStringBuilder.reflectionToString(this);
    }
    
}
