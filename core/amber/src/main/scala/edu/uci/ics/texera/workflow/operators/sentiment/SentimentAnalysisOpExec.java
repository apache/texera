package edu.uci.ics.texera.workflow.operators.sentiment;


import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;
import edu.uci.ics.texera.workflow.common.operators.map.MapOpExec;
import edu.uci.ics.texera.workflow.common.tuple.Tuple;
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeType;
import edu.uci.ics.texera.workflow.common.tuple.schema.OperatorSchemaInfo;
import scala.Function1;
import scala.Serializable;

import java.util.Properties;

public class SentimentAnalysisOpExec extends MapOpExec {

    private final SentimentAnalysisOpDesc opDesc;
    private final StanfordCoreNLPWrapper coreNlp;
    private final OperatorSchemaInfo operatorSchemaInfo;

    public SentimentAnalysisOpExec(SentimentAnalysisOpDesc opDesc, OperatorSchemaInfo operatorSchemaInfo) {
        this.opDesc = opDesc;
        this.operatorSchemaInfo = operatorSchemaInfo;
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
        coreNlp = new StanfordCoreNLPWrapper(props);
        this.setMapFunc(
                // must cast the lambda function to "(Function & Serializable)" in Java
                (Function1<Tuple, Tuple> & Serializable) this::processTuple);
    }

    public Tuple processTuple(Tuple t) {
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
        Integer sentiment;
        if (mainSentiment > 2) {
            sentiment = 1;
        } else if (mainSentiment == 2) {
            sentiment = 0;
        } else {
            sentiment = -1;
        }

        return Tuple.newBuilder(operatorSchemaInfo.outputSchema()).add(t).add(opDesc.resultAttribute, AttributeType.INTEGER, sentiment).build();
    }


}
