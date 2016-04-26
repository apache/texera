package edu.uci.ics.textdb.dataflow.nameentityextractor;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.common.exception.DataFlowException;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Properties;

import edu.stanford.nlp.ling.CoreAnnotations.NamedEntityTagAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.PartOfSpeechAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;

/**
 * Created by FengHong and YangJiao.
 */
public class NEExtractor implements IOperator {


    private IOperator operator;
    private String document;
    private int fieldIndex;
    private ITuple neetuple;
    private String fieldName;
    private ITuple sourceTuple;
    private List<IField> fields;
    private List<Attribute> schema;

    private List<ITuple> tupleList;

    public NEExtractor(IOperator operator) {
        this.operator = operator;
    }


    /**
     * @about Opens name entities matcher.
     *
     */
    @Override
    public void open() throws Exception {
        try {
            operator.open();
            sourceTuple = operator.getNextTuple();
            fields = sourceTuple.getFields();
            schema = sourceTuple.getSchema();
            processSourceTuple(sourceTuple);



        } catch (Exception e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }

    }

    /**
     * @about Gets next Name Entities tuple. Returns a new span tuple including the
     *        span results.
     *
     * @overview  First get next tuple form the lower level operator, then process it
     * with Stanford NLP package  and return the next Name Entities Tuple
     *
     */
    @Override
    public ITuple getNextTuple() throws Exception {

        if(tupleList.size()!=0){
            ITuple tuple = tupleList.remove(0);
            return tuple;
        }else{

            sourceTuple= operator.getNextTuple();
            processSourceTuple(sourceTuple);
            return getNextTuple();
        }
    }

    private void processSourceTuple(ITuple souceTuple){
        //TODO: Fixed with schema

        String sourceText = sourceTuple.toString();

        Properties props = new Properties();
        props.put("annotators", "tokenize, ssplit, pos, lemma, ner");
        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);

        Annotation document = new Annotation(sourceText);
        pipeline.annotate(document);

        List<CoreMap> sentences = document.get(SentencesAnnotation.class);
        int i=0;
        for(CoreMap sentence: sentences) {
            for (CoreLabel token : sentence.get(TokensAnnotation.class)) {
                String ne = token.get(NamedEntityTagAnnotation.class);
                if(ne != null){
                    String word=token.get(TextAnnotation.class);
                    tupleList.add(new NameEntity(word, ne));
                    int span=i;
                }
                i++;
            }
        }
    }




        /**
         * @about Modifies schema, fields and creates a new span tuple for return
         */

        // TODO: implement
    private ITuple getSpanTuple() {
        return null;
    }

    /**
     * @about Closes the operator
     */
    @Override
    public void close() throws DataFlowException {
        try {
            operator.close();
        } catch (Exception e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }
    }



}
