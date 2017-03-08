package edu.uci.ics.textdb.perftest.nlpextractor;

import java.io.File;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

import edu.uci.ics.textdb.api.common.Tuple;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import edu.uci.ics.textdb.common.constants.SchemaConstants;
import edu.uci.ics.textdb.common.field.ListField;
import edu.uci.ics.textdb.common.field.Span;
import edu.uci.ics.textdb.dataflow.nlpextrator.NlpExtractor;
import edu.uci.ics.textdb.dataflow.nlpextrator.NlpPredicate;
import edu.uci.ics.textdb.dataflow.source.ScanBasedSourceOperator;
import edu.uci.ics.textdb.perftest.medline.MedlineIndexWriter;
import edu.uci.ics.textdb.perftest.utils.*;

/**
 * @author Hailey Pan
 * @author Zhenfeng Qi
 * 
 *         This is the performance test of NLP extractor matcher
 */
public class NlpExtractorPerformanceTest {

    private static String HEADER = "Date,Record #, Avg Time, Avg Result #, Commit #";
    private static String delimiter = ",";
    private static String newLine = "\n";
    private static int numOfNlpType = 5;
    private static String csvFile = "nlp.csv";
    private static double totalMatchingTime = 0.0;
    private static int totalResults = 0;
    
    
    /*
     * 
     * 
     * This function will match the queries against all indices in
     * ./index/standard/
     * 
     * Test results includes the average runtime of all nlp token types, the
     * average number of results. These results are recorded in
     * ./perftest-files/results/nlp.csv
     * 
     * CSV file example:
     * Date,                Record #,     Avg Time, Avg Result #, Commit #
     * 09-09-2016 00:54:40, abstract_100, 29.5494,  252.36
     * 
     * Commit number is designed for performance dashboard. It will be appended
     * to the result file only when the performance test is run by
     * /textdb-scripts/dashboard/build.py
     * 
     */
    public static void runTest() throws Exception {
    	
    	// Gets the current time  
        String currentTime = PerfTestUtils.formatTime(System.currentTimeMillis());
        
        File indexFiles = new File(PerfTestUtils.standardIndexFolder);
        
        for (File file : indexFiles.listFiles()) {
            if (file.getName().startsWith(".")) {
                continue;
            }
            String tableName = file.getName().replace(".txt", "");

            PerfTestUtils.createFile(PerfTestUtils.getResultPath(csvFile), HEADER);
            FileWriter fileWriter = new FileWriter(PerfTestUtils.getResultPath(csvFile), true);
            fileWriter.append(newLine);
            fileWriter.append(currentTime + delimiter);
            fileWriter.append(file.getName() + delimiter);
            matchNLP(tableName, NlpPredicate.NlpTokenType.NE_ALL, new StandardAnalyzer());
            matchNLP(tableName, NlpPredicate.NlpTokenType.Adjective, new StandardAnalyzer());
            matchNLP(tableName, NlpPredicate.NlpTokenType.Adverb, new StandardAnalyzer());
            matchNLP(tableName, NlpPredicate.NlpTokenType.Noun, new StandardAnalyzer());
            matchNLP(tableName, NlpPredicate.NlpTokenType.Verb, new StandardAnalyzer());
            fileWriter.append(String.format("%.4f", totalMatchingTime / numOfNlpType));
            fileWriter.append(delimiter);
            fileWriter.append(String.format("%.2f", totalResults * 0.1 / numOfNlpType ));
            fileWriter.flush();
            fileWriter.close();
        }
    
 
    }

    /*
     * This function does match based on tokenType
     */
    public static void matchNLP(String tableName, NlpPredicate.NlpTokenType tokenType, Analyzer analyzer) throws Exception {

        List<String> attributeNames = Arrays.asList(MedlineIndexWriter.ABSTRACT);

        ISourceOperator sourceOperator = new ScanBasedSourceOperator(tableName);

        NlpPredicate nlpPredicate = new NlpPredicate(tokenType, attributeNames);
        NlpExtractor nlpExtractor = new NlpExtractor(nlpPredicate);
        nlpExtractor.setInputOperator(sourceOperator);

        long startMatchTime = System.currentTimeMillis();
        nlpExtractor.open();
        Tuple nextTuple = null;
        int counter = 0;
        while ((nextTuple = nlpExtractor.getNextTuple()) != null) {
            ListField<Span> spanListField = nextTuple.getField(SchemaConstants.SPAN_LIST);
            List<Span> spanList = spanListField.getValue();
            counter += spanList.size();

        }
        nlpExtractor.close();
        long endMatchTime = System.currentTimeMillis();
        double matchTime = (endMatchTime - startMatchTime) / 1000.0;

        totalMatchingTime += matchTime;
        totalResults += counter;

    }

}
