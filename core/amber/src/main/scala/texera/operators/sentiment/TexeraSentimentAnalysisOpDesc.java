package texera.operators.sentiment;

import Engine.Common.tuple.texera.TexeraTuple;
import Engine.Common.tuple.texera.schema.AttributeType;
import Engine.Common.tuple.texera.schema.Schema;
import texera.operators.Common.Map.MapOpExecConfig;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.google.common.base.Preconditions;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;
import scala.collection.Seq;
import texera.common.schema.OperatorGroupConstants;
import texera.common.schema.TexeraOperatorDescription;
import texera.common.workflow.common.MapOpDesc;

import java.util.Properties;

public class TexeraSentimentAnalysisOpDesc extends MapOpDesc {

    @JsonProperty("attribute")
    @JsonPropertyDescription("column to perform sentiment analysis on")
    public String attribute;

    @JsonProperty("result attribute")
    @JsonPropertyDescription("column name of the sentiment analysis result")
    public String resultAttribute;

    private StanfordCoreNLPWrapper coreNlp;

    @Override
    public MapOpExecConfig amberOperator() {
        if (attribute == null) {
            throw new RuntimeException("sentiment analysis: attribute is null");
        }
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
        coreNlp = new StanfordCoreNLPWrapper(props);
        return new MapOpExecConfig(amberOperatorTag(), this::processTuple);
    }

    public TexeraTuple processTuple(TexeraTuple t) {
        String text = t.getField(attribute).toString();
        Annotation documentAnnotation = new Annotation(text);
        coreNlp.get().annotate(documentAnnotation);
        // mainSentiment is calculated by the sentiment class of the longest sentence
        int mainSentiment = 0;
        int longestSentenceLength = 0;
        for (CoreMap sentence : documentAnnotation.get(CoreAnnotations.SentencesAnnotation.class)) {
            Tree tree = sentence.get(SentimentCoreAnnotations.SentimentAnnotatedTree.class);
            int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
            String sentenceText = sentence.toString();
            if (sentenceText.length() > longestSentenceLength) {
                mainSentiment = sentiment;
                longestSentenceLength = sentenceText.length();
            }
        }
        String sentiment = "";
        if (mainSentiment > 2) {
            sentiment = "positive";
        } else if (mainSentiment == 2) {
            sentiment = "neutral";
        } else {
            sentiment = "negative";
        }

        return TexeraTuple.newBuilder().add(t).add(resultAttribute, AttributeType.STRING, sentiment).build();
    }

    @Override
    public TexeraOperatorDescription texeraOperatorDescription() {
        return new TexeraOperatorDescription(
                "Sentiment Analysis",
                "analysis the sentiment of a text using machine learning",
                OperatorGroupConstants.ANALYTICS_GROUP(),
                1, 1
        );
    }

    @Override
    public Schema transformSchema(Seq<Schema> schemas) {
        Preconditions.checkArgument(schemas.length() == 1);
        return Schema.newBuilder().add(schemas.apply(0)).add(resultAttribute, AttributeType.STRING).build();
    }
}
