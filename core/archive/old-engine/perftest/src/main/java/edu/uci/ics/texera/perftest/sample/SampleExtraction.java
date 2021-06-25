package edu.uci.ics.texera.perftest.sample;

import edu.uci.ics.texera.api.field.StringField;
import edu.uci.ics.texera.api.field.TextField;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.perftest.promed.PromedSchema;
import edu.uci.ics.texera.perftest.utils.PerfTestUtils;
import edu.uci.ics.texera.storage.DataWriter;
import edu.uci.ics.texera.storage.RelationManager;
import edu.uci.ics.texera.storage.constants.LuceneAnalyzerConstants;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Scanner;

public class SampleExtraction {
    
    public static final String PROMED_SAMPLE_TABLE = "promed";
        
    public static String promedFilesDirectory = PerfTestUtils.getResourcePath("/sample-data-files/promed").toString();
    public static String promedIndexDirectory = PerfTestUtils.getResourcePath("/index/standard/promed").toString();
    public static String sampleDataFilesDirectory = PerfTestUtils.getResourcePath("sample-data-files").toString();        
    
    
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
        RelationManager relationManager = RelationManager.getInstance();
        
        relationManager.deleteTable(PROMED_SAMPLE_TABLE);
        relationManager.createTable(PROMED_SAMPLE_TABLE, Paths.get(promedIndexDirectory), 
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
    }

}
