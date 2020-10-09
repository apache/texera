package texera.operators.sentiment;

import Engine.Common.tuple.amber.AmberTuple;
import Engine.Common.tuple.Tuple;
import Engine.Common.Constants;
import Engine.Operators.Common.Map.MapMetadata;
import Engine.Operators.OperatorMetadata;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;
import org.apache.commons.lang3.ArrayUtils;
import scala.Function1;
import scala.Serializable;
import texera.common.schema.OperatorGroupConstants;
import texera.common.schema.TexeraOperatorDescription;
import texera.common.workflow.TexeraOperator;

import java.util.Properties;

public class TexeraSentimentAnalysis extends TexeraOperator {

    @JsonProperty("attribute")
    @JsonPropertyDescription("column to perform sentiment analysis on")
    public String attribute;

    @Override
    public OperatorMetadata amberOperator() {
        if (attribute == null) {
            throw new RuntimeException("sentiment analysis: attribute is null");
        }
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
        StanfordCoreNLPWrapper coreNlp = new StanfordCoreNLPWrapper(props);
        int column = this.context().fieldIndexMapping(attribute);
        return new MapMetadata(this.amberOperatorTag(), Constants.defaultNumWorkers(),
            (Function1<Tuple, Tuple> & Serializable) t -> {
                String text = t.get(column).toString();
                Annotation documentAnnotation = new Annotation(text);
                coreNlp.get().annotate(documentAnnotation);
                // mainSentiment is calculated by the sentiment class of the longest sentence
                Integer mainSentiment = 0;
                Integer longestSentenceLength = 0;
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

                return new AmberTuple(ArrayUtils.add(t.toArray(), sentiment));
            });
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
}
