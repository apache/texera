package edu.uci.ics.textdb.perftest.sample;

import edu.uci.ics.textdb.api.field.StringField;
import edu.uci.ics.textdb.api.field.TextField;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.exp.nlp.entity.NlpEntityOperator;
import edu.uci.ics.textdb.exp.nlp.entity.NlpEntityPredicate;
import edu.uci.ics.textdb.exp.nlp.entity.NlpEntityType;
import edu.uci.ics.textdb.exp.sink.FileSink;
import edu.uci.ics.textdb.exp.sink.tuple.TupleSink;
import edu.uci.ics.textdb.exp.source.scan.ScanBasedSourceOperator;
import edu.uci.ics.textdb.exp.source.scan.ScanSourcePredicate;
import edu.uci.ics.textdb.exp.utils.DataflowUtils;
import edu.uci.ics.textdb.perftest.medline.MedlineIndexWriter;
import edu.uci.ics.textdb.perftest.promed.PromedSchema;
import edu.uci.ics.textdb.perftest.utils.PerfTestUtils;
import edu.uci.ics.textdb.storage.DataWriter;
import edu.uci.ics.textdb.storage.RelationManager;
import edu.uci.ics.textdb.storage.constants.LuceneAnalyzerConstants;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.File;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.*;

public class SampleExtraction {
    
    public static final String PROMED_SAMPLE_TABLE = "promed";
        
    public static String promedFilesDirectory = PerfTestUtils.getResourcePath("/sample-data-files/promed");
    public static String promedIndexDirectory = PerfTestUtils.getResourcePath("/index/standard/promed");
    public static String sampleDataFilesDirectory = PerfTestUtils.getResourcePath("sample-data-files");        
    
    
    public static void main(String[] args) throws Exception {
        // write the index of data files
        // index only needs to be written once, after the first run, this function can be commented out
        writeSampleIndex();

        // perform the extraction task
        extractPersonLocation();
    }

    public static Tuple parsePromedHTML(String fileName, String content) {
        try {
            Document parsedDocument = Jsoup.parse(content);
            String mainText = parsedDocument.getElementById("preview").text();
            Tuple tuple = new Tuple(PromedSchema.PROMED_SCHEMA, new StringField(fileName), new TextField(mainText));
            return tuple;
        } catch (Exception e) {
            return null;
        }
    }

    public static void writeSampleIndex() throws Exception {
        // parse the original file
        File sourceFileFolder = new File(promedFilesDirectory);
        ArrayList<Tuple> fileTuples = new ArrayList<>();
        for (File htmlFile : sourceFileFolder.listFiles()) {
            StringBuilder sb = new StringBuilder();
            Scanner scanner = new Scanner(htmlFile);
            while (scanner.hasNext()) {
                sb.append(scanner.nextLine());
            }
            scanner.close();
            Tuple tuple = parsePromedHTML(htmlFile.getName(), sb.toString());
            if (tuple != null) {
                fileTuples.add(tuple);
            }
        }
        
        // write tuples into the table
        RelationManager relationManager = RelationManager.getRelationManager();
        
        relationManager.deleteTable(PROMED_SAMPLE_TABLE);
        relationManager.createTable(PROMED_SAMPLE_TABLE, promedIndexDirectory, 
                PromedSchema.PROMED_SCHEMA, LuceneAnalyzerConstants.standardAnalyzerString());
        
        DataWriter dataWriter = relationManager.getTableDataWriter(PROMED_SAMPLE_TABLE);
        dataWriter.open();
        for (Tuple tuple : fileTuples) {
            dataWriter.insertTuple(tuple);
        }
        dataWriter.close();
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
        // this sample extraction won't work because the CharacterDistanceJoin hasn't been updated yet
        // TODO: after Join is fixed, add this sample extraction back.
        ScanSourcePredicate scanSourcePredicate = new ScanSourcePredicate(PROMED_SAMPLE_TABLE);
        ScanBasedSourceOperator scanBasedSourceOperator = new ScanBasedSourceOperator(scanSourcePredicate);
        String s = "locationspanresult";
        NlpEntityPredicate nlpEntityPredicate = new NlpEntityPredicate(NlpEntityType.ORGANIZATION, Arrays.asList(PromedSchema.CONTENT), s);
        NlpEntityOperator nlpEntityOperator = new NlpEntityOperator(nlpEntityPredicate);
        SimpleDateFormat sdf = new SimpleDateFormat("MM-dd-yyyy-HH-mm-ss");
        FileSink fileSink = new FileSink(
                new File(sampleDataFilesDirectory + "/person-location-result-"
                        + sdf.format(new Date(System.currentTimeMillis())).toString() + ".txt"));

        fileSink.setToStringFunction((tuple -> DataflowUtils.getTupleString(tuple)));
        nlpEntityOperator.setInputOperator(scanBasedSourceOperator);
        // fileSink.setInputOperator(nlpEntityOperator);
        // Plan extractPersonPlan = new Plan(fileSink);
        // Engine.getEngine().evaluate(extractPersonPlan);
        TupleSink tupleSink = new TupleSink();
        tupleSink.setInputOperator(nlpEntityOperator);
        tupleSink.open();
        List<String> results = tupleSink.collectAttributes(s);
        FileWriter writer = new FileWriter("output.txt");
        for(String str: results) {
            writer.write(str);
            writer.write(", ");
        }
        writer.close();
        System.out.print("done");
        //System.out.println("result "+ DataflowUtils.getTupleListString(results));
    }

}
