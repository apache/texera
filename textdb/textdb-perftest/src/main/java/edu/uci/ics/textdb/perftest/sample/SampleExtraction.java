package edu.uci.ics.textdb.perftest.sample;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Scanner;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import edu.uci.ics.textdb.api.plan.Plan;
import edu.uci.ics.textdb.common.constants.DataConstants.KeywordMatchingType;
import edu.uci.ics.textdb.common.constants.LuceneAnalyzerConstants;
import edu.uci.ics.textdb.common.constants.SchemaConstants;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.field.StringField;
import edu.uci.ics.textdb.common.field.TextField;
import edu.uci.ics.textdb.common.utils.Utils;
import edu.uci.ics.textdb.dataflow.common.IJoinPredicate;
import edu.uci.ics.textdb.dataflow.common.JoinDistancePredicate;
import edu.uci.ics.textdb.dataflow.common.KeywordPredicate;
import edu.uci.ics.textdb.dataflow.common.RegexPredicate;
import edu.uci.ics.textdb.dataflow.join.Join;
import edu.uci.ics.textdb.dataflow.keywordmatch.KeywordMatcherSourceOperator;
import edu.uci.ics.textdb.dataflow.nlpextrator.NlpExtractor;
import edu.uci.ics.textdb.dataflow.nlpextrator.NlpPredicate;
import edu.uci.ics.textdb.dataflow.projection.ProjectionOperator;
import edu.uci.ics.textdb.dataflow.projection.ProjectionPredicate;
import edu.uci.ics.textdb.dataflow.regexmatch.RegexMatcher;
import edu.uci.ics.textdb.dataflow.sink.FileSink;
import edu.uci.ics.textdb.dataflow.sink.IndexSink;
import edu.uci.ics.textdb.dataflow.source.TupleStreamSourceOperator;
import edu.uci.ics.textdb.engine.Engine;
import edu.uci.ics.textdb.perftest.promed.PromedSchema;
import edu.uci.ics.textdb.storage.DataStore;
import edu.uci.ics.textdb.storage.writer.DataWriter;

public class SampleExtraction {
        
    public static final String promedFilesDirectory = "./sample-data-files/promed/";
    public static final String promedIndexDirectory = "./index/standard/promed/"; 
    public static final DataStore promedDataStore = new DataStore(promedIndexDirectory, PromedSchema.PROMED_SCHEMA);
    
    
    public static void main(String[] args) throws Exception {
        
        // write the index of data files
        // index only needs to be written once, after the first run, this function can be commented out
        writeSampleIndex();
        
        // perform the extraction task
        extractPersonLocation();
    }
    
    public static ITuple parsePromedHTML(String fileName, String content) {
        try {
            Document parsedDocument = Jsoup.parse(content);
            String mainText = parsedDocument.getElementById("preview").text();
            ITuple tuple = new DataTuple(PromedSchema.PROMED_SCHEMA, new StringField(fileName), new TextField(mainText));
            return tuple;
        } catch (Exception e) {
            return null;
        }
    }

    
    public static void writeSampleIndex() throws Exception {
        // clear the directory
        new DataWriter(promedDataStore, LuceneAnalyzerConstants.getStandardAnalyzer()).clearData();
        
        File sourceFileFolder = new File(promedFilesDirectory);
        ArrayList<ITuple> fileTuples = new ArrayList<>();
        for (File htmlFile : sourceFileFolder.listFiles()) {
            StringBuilder sb = new StringBuilder();
            Scanner scanner = new Scanner(htmlFile);
            while (scanner.hasNext()) {
                sb.append(scanner.nextLine());
            }
            scanner.close();
            ITuple tuple = parsePromedHTML(htmlFile.getName(), sb.toString());
            if (tuple != null) {
                fileTuples.add(tuple);
            }
        }

        ISourceOperator fileSource = new TupleStreamSourceOperator(fileTuples,
                PromedSchema.PROMED_SCHEMA);

        IndexSink standardIndexSink = new IndexSink(promedIndexDirectory, PromedSchema.PROMED_SCHEMA,
                new StandardAnalyzer(), false);
        standardIndexSink.setInputOperator(fileSource);
        Plan standardIndexPlan = new Plan(standardIndexSink);

        Engine.getEngine().evaluate(standardIndexPlan);
    }

    /*
     * This is the DAG of this extraction plan.
     * 
     * 
     *              KeywordSource (zika)
     *                       ↓
     *              Projection (content)
     *                  ↓          ↓
     *       regex (a...man)      NLP (location)
     *                  ↓          ↓     
     *             Join (distance < 100)
     *                       ↓
     *              Projection (spanList)
     *                       ↓
     *                    FileSink
     *                    
     */
    public static void extractPersonLocation() throws Exception {
                
        String keywordZika = "zika";
        KeywordPredicate keywordPredicateZika = new KeywordPredicate(keywordZika, Arrays.asList(PromedSchema.CONTENT),
                new StandardAnalyzer(), KeywordMatchingType.CONJUNCTION_INDEXBASED);
        
        KeywordMatcherSourceOperator keywordSource = new KeywordMatcherSourceOperator(
                keywordPredicateZika, promedDataStore);
        
        ProjectionPredicate projectionPredicateIdAndContent = new ProjectionPredicate(
                Arrays.asList(PromedSchema.ID, PromedSchema.CONTENT));
        
        ProjectionOperator projectionOperatorIdAndContent1 = new ProjectionOperator(projectionPredicateIdAndContent);
        ProjectionOperator projectionOperatorIdAndContent2 = new ProjectionOperator(projectionPredicateIdAndContent);

        String regexPerson = "\\b(A|a|(an)|(An)) .{1,40} ((woman)|(man))\\b";
        RegexPredicate regexPredicatePerson = new RegexPredicate(regexPerson, Arrays.asList(PromedSchema.CONTENT),
                LuceneAnalyzerConstants.getNGramAnalyzer(3));
        RegexMatcher regexMatcherPerson = new RegexMatcher(regexPredicatePerson);
        
        NlpPredicate nlpPredicateLocation = new NlpPredicate(NlpPredicate.NlpTokenType.Location, Arrays.asList(PromedSchema.CONTENT_ATTR));
        NlpExtractor nlpExtractorLocation = new NlpExtractor(nlpPredicateLocation);

        IJoinPredicate joinPredicatePersonLocation = new JoinDistancePredicate(PromedSchema.ID, PromedSchema.CONTENT, 100);
        Join joinPersonLocation = new Join(null, null, joinPredicatePersonLocation);
        
        ProjectionPredicate projectionPredicateIdAndSpan = new ProjectionPredicate(
                Arrays.asList(PromedSchema.ID, SchemaConstants.SPAN_LIST));
        ProjectionOperator projectionOperatorIdAndSpan = new ProjectionOperator(projectionPredicateIdAndSpan);
         
        SimpleDateFormat sdf = new SimpleDateFormat("MM-dd-yyyy-HH-mm-ss");
        FileSink fileSink = new FileSink( 
                new File("./sample-data-files/person-location-result-" 
                		+ sdf.format(new Date(System.currentTimeMillis())).toString() + ".txt"));

        fileSink.setToStringFunction((tuple -> Utils.getTupleString(tuple)));
        
        
        projectionOperatorIdAndContent1.setInputOperator(keywordSource);
        
        regexMatcherPerson.setInputOperator(projectionOperatorIdAndContent1);
        
        projectionOperatorIdAndContent2.setInputOperator(regexMatcherPerson);
        nlpExtractorLocation.setInputOperator(projectionOperatorIdAndContent2);
        
        joinPersonLocation.setInnerInputOperator(regexMatcherPerson);
        joinPersonLocation.setOuterInputOperator(nlpExtractorLocation);
                      
        projectionOperatorIdAndSpan.setInputOperator(joinPersonLocation);
        fileSink.setInputOperator(projectionOperatorIdAndSpan);
        
        Plan extractPersonPlan = new Plan(fileSink);
        Engine.getEngine().evaluate(extractPersonPlan);
    }

}
