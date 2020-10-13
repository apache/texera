package texera.operators.sentiment;


import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;
import scala.Function1;
import scala.Serializable;
import texera.common.operators.map.TexeraMapOpExec;
import texera.common.tuple.TexeraTuple;
import texera.common.tuple.schema.AttributeType;

import java.util.Properties;

public class SentimentAnalysisOpExec extends TexeraMapOpExec {

    private final SentimentAnalysisOpDesc opDesc;
    private final StanfordCoreNLPWrapper coreNlp;

    public SentimentAnalysisOpExec(SentimentAnalysisOpDesc opDesc) {
        this.opDesc = opDesc;
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
        coreNlp = new StanfordCoreNLPWrapper(props);
        this.setMapFunc(
                // must cast the lambda function to "(Function & Serializable)" in Java
                (Function1<TexeraTuple, TexeraTuple> & Serializable) this::processTuple);
    }

    public TexeraTuple processTuple(TexeraTuple t) {
        String text = t.getField(opDesc.attribute).toString();
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

        return TexeraTuple.newBuilder().add(t).add(opDesc.resultAttribute, AttributeType.STRING, sentiment).build();
    }


}
