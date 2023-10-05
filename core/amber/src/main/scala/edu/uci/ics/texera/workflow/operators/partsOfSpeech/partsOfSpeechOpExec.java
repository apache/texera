package edu.uci.ics.texera.workflow.operators.partsOfSpeech;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.ling.*;
import edu.stanford.nlp.util.CoreMap;
import edu.uci.ics.texera.workflow.common.operators.map.MapOpExec;
import edu.uci.ics.texera.workflow.common.tuple.Tuple;
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeType;
import edu.uci.ics.texera.workflow.common.tuple.schema.OperatorSchemaInfo;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.PartOfSpeechAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;


import scala.Function1;
import scala.Serializable;
import java.util.List;
import java.util.Properties;


public class partsOfSpeechOpExec extends MapOpExec {

    private final partsOfSpeechOpDesc opDesc;
    private final StanfordCoreNLPWrapper coreNlp;
    private final OperatorSchemaInfo operatorSchemaInfo;

    public partsOfSpeechOpExec(partsOfSpeechOpDesc opDesc, OperatorSchemaInfo operatorSchemaInfo) {
        this.opDesc = opDesc;
        this.operatorSchemaInfo = operatorSchemaInfo;
        Properties props = new Properties();

        props.setProperty("annotators", "tokenize,ssplit,pos");




        coreNlp = new edu.uci.ics.texera.workflow.operators.partsOfSpeech.StanfordCoreNLPWrapper(props);
        this.setMapFunc(
                // must cast the lambda function to "(Function & Serializable)" in Java
                (Function1<Tuple, Tuple> & Serializable) this::processTuple);
    }

    public Tuple processTuple(Tuple t) {
        String text = t.getField(opDesc.attribute()).toString();
        Annotation document = new Annotation(text);
        coreNlp.get().annotate(document);
        List<CoreMap> sentences = document.get(SentencesAnnotation.class);
        String Fullpos="";
        for (CoreMap sentence: sentences) {
            for (CoreLabel token : sentence.get(TokensAnnotation.class)) {
                String pos = token.get(PartOfSpeechAnnotation.class);
                Fullpos = Fullpos + ' ' + token + '=' + pos+',';
            }
        }

        return Tuple.newBuilder(operatorSchemaInfo.outputSchemas()[0]).add(t).add(opDesc.resultAttribute(), AttributeType.STRING,Fullpos).build();
    }
}
