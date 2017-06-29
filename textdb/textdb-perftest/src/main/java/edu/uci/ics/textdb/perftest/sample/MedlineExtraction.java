package edu.uci.ics.textdb.perftest.sample;

/**
 * Created by Chang on 6/26/17.
 */

import edu.uci.ics.textdb.api.constants.TestConstantsChinese;
import edu.uci.ics.textdb.api.engine.Engine;
import edu.uci.ics.textdb.api.engine.Plan;
import edu.uci.ics.textdb.api.field.StringField;
import edu.uci.ics.textdb.api.field.TextField;
import edu.uci.ics.textdb.api.span.Span;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.exp.dictionarymatcher.*;
import edu.uci.ics.textdb.exp.keywordmatcher.KeywordMatchingType;
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
import sun.misc.Perf;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.net.URISyntaxException;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Dictionary;


public class MedlineExtraction {

    public static final String MEDLINE_SAMPLE_TABLE = "medline";
    public static int number = 0;

    public static String medlineFilesDirectory = PerfTestUtils.getResourcePath("/sample-data-files/medline");
    public static String medlineIndexDirectory = PerfTestUtils.getResourcePath("/index/standard/medline");
    public static String sampleDataFilesDirectory = PerfTestUtils.getResourcePath("/sample-data-files");
//
//    static {
//        try {
//            // Finding the absolute path to the sample data files directory and index directory
//
//            // Checking if the resource is in a jar
//            String referencePath = SampleExtraction.class.getResource("").toURI().toString();
//            if(referencePath.substring(0, 3).equals("jar")) {
//                medlineFilesDirectory = "/textdb-perftest/src/main/resources/sample-data-files/medline/";
//                medlineIndexDirectory = "/textdb-perftest/src/main/resources/index/standard/medline/";
//                sampleDataFilesDirectory = "/textdb-perftest/src/main/resources/sample-data-files/";
//            }
//            else {
//                medlineFilesDirectory = Paths.get(SampleExtraction.class.getResource("/sample-data-files/medline")
//                        .toURI())
//                        .toString();
//                medlineIndexDirectory = Paths.get(SampleExtraction.class.getResource("/index/standard")
//                        .toURI())
//                        .toString() + "/medline";
//                System.out.println(medlineIndexDirectory);
//                sampleDataFilesDirectory = Paths.get(SampleExtraction.class.getResource("/sample-data-files")
//                        .toURI())
//                        .toString();
//            }
//        }
//        catch(URISyntaxException | FileSystemNotFoundException e) {
//            e.printStackTrace();
//        }
//    }
    public static void main(String[] args) throws Exception {
        // write the index of data files
        // index only needs to be written once, after the first run, this function can be commented out
       writeSampleIndex();
       System.out.println(number);
       extractPersonLocation();
        // perform the extraction task
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
        File sourceFileFolder = new File(medlineFilesDirectory);
        ArrayList<Tuple> fileTuples = new ArrayList<>();
        for (File htmlFile : sourceFileFolder.listFiles()) {
            StringBuilder sb = new StringBuilder();
            Scanner scanner = new Scanner(htmlFile);
            while (scanner.hasNext()) {
                Tuple tuple = MedlineIndexWriter.recordToTuple(scanner.nextLine());
                if (tuple != null) {
                    fileTuples.add(tuple);
                }
            }
            scanner.close();
        }

        // write tuples into the table
        RelationManager relationManager = RelationManager.getRelationManager();

        relationManager.deleteTable(MEDLINE_SAMPLE_TABLE);
        relationManager.createTable(MEDLINE_SAMPLE_TABLE, medlineIndexDirectory,
                MedlineIndexWriter.SCHEMA_MEDLINE, LuceneAnalyzerConstants.standardAnalyzerString());

        DataWriter dataWriter = relationManager.getTableDataWriter(MEDLINE_SAMPLE_TABLE);
        dataWriter.open();
        for (Tuple tuple : fileTuples) {
            dataWriter.insertTuple(tuple);
            number += 1;
        }
        dataWriter.close();
    }
    public static void extractPersonLocation() throws Exception {
        ScanSourcePredicate scanSourcePredicate = new ScanSourcePredicate(MEDLINE_SAMPLE_TABLE);
        ScanBasedSourceOperator scanBasedSourceOperator = new ScanBasedSourceOperator(scanSourcePredicate);
        List<String> attributeNames = Arrays.asList(MedlineIndexWriter.ABSTRACT);
        String org = "organization";
        String per = "person";
        RelationManager relationManager = RelationManager.getRelationManager();
        String luceneAnalyzerStr = relationManager.getTableAnalyzerString(MEDLINE_SAMPLE_TABLE);
        String dic_org_path = PerfTestUtils.getResourcePath("/dictionary/disease_dict.txt");
        List<String> list_org = tokenizeFile(dic_org_path);
       // List<String> list_org = new ArrayList<>();
        String dic_per_path = PerfTestUtils.getResourcePath("/dictionary/drugs_dict.txt");
        List<String> list_per = tokenizeFile(dic_per_path);
        edu.uci.ics.textdb.exp.dictionarymatcher.Dictionary dic_per = new edu.uci.ics.textdb.exp.dictionarymatcher.Dictionary(list_per);
        edu.uci.ics.textdb.exp.dictionarymatcher.Dictionary dic_org = new edu.uci.ics.textdb.exp.dictionarymatcher.Dictionary(list_org);
        System.out.println("size of organization" + dic_org.getDictionaryEntries().size());
        System.out.println(" size of person" + dic_per.getDictionaryEntries().size());
        DictionaryPredicate dictionaryPredicate = new DictionaryPredicate(dic_org, attributeNames, luceneAnalyzerStr, KeywordMatchingType.SUBSTRING_SCANBASED, org);
        DictionaryMatcher dictionaryMatcher = new DictionaryMatcher(dictionaryPredicate);
        dictionaryMatcher.setInputOperator(scanBasedSourceOperator);
        DictionaryPredicate dictionaryPredicate1 = new DictionaryPredicate(dic_per, attributeNames, luceneAnalyzerStr, KeywordMatchingType.SUBSTRING_SCANBASED, per);
        DictionaryMatcher dictionaryMatcher1 = new DictionaryMatcher(dictionaryPredicate1);
        dictionaryMatcher1.setInputOperator(dictionaryMatcher);
        String name = "locationspanresult";
     //   NlpEntityPredicate nlpEntityPredicate = new NlpEntityPredicate(NlpEntityType.ADJECTIVE, Arrays.asList(MedlineIndexWriter.ABSTRACT), name);
      //  NlpEntityOperator nlpEntityOperator = new NlpEntityOperator(nlpEntityPredicate);
        //SimpleDateFormat sdf = new SimpleDateFormat("MM-dd-yyyy-HH-mm-ss");
        //FileSink fileSink = new FileSink(
          //      new File(sampleDataFilesDirectory + "/person-location-result-"
            //            + sdf.format(new Date(System.currentTimeMillis())).toString() + ".txt"));

        //fileSink.setToStringFunction((tuple -> DataflowUtils.getTupleString(tuple)));
    //    nlpEntityOperator.setInputOperator(scanBasedSourceOperator);
       // fileSink.setInputOperator(nlpEntityOperator);
       // Plan extractPersonPlan = new Plan(fileSink);
       // Engine.getEngine().evaluate(extractPersonPlan);
        TupleSink tupleSink = new TupleSink();
        tupleSink.setInputOperator(dictionaryMatcher1);
        tupleSink.open();
        List<Tuple> result = tupleSink.collectAllTuples();
     //   for(Tuple t: result){
     //       System.out.println(t.getField("abstract").toString());
    //        for(Span span: (List<Span>) t.getField(org).getValue()){
    //            System.out.println(span.getAttributeName() + " " + span.getStart() + " " + span.getEnd() + " " + span.getValue());
    //        }
    //        System.out.println("This is for another dictionary matcher");
    //        for(Span span: (List<Span>) t.getField(per).getValue()){
    //            System.out.println(span.getAttributeName() + " " + span.getStart() + " " + span.getEnd() + " " + span.getValue());
    //        }
    //    }
        int count = result.size();
//        List<String> results = tupleSink.collectAttributes(name);
//        FileWriter writer = new FileWriter("adj_dic.txt");
//        int cnt = 0;
//        for(String str: results) {
//            writer.write(str);
//            cnt += 1;
//            writer.write(", ");
//        }
//        writer.close();
        System.out.println("Done_number of tuples" + count);
     //   System.out.print("done_number of dictionary element" + cnt);

    }
    public static List<String> tokenizeFile(String inputfile) {
        List<String> tokenlist = new ArrayList<String>();
        try {
            File file = new File(inputfile);
            FileReader in = new FileReader(file);
            BufferedReader reader = new BufferedReader(in);
            String tmp = null;
            tmp = reader.readLine();
            int count = 1000;
            while (tmp !=null) {
                String tokenstring = tmp.trim();
                if (tokenstring.contains("[^a-zA-Z0-9 ]")){
                    continue;
                }
                //String change = tokenstring.replaceAll(",", " ").toLowerCase();
                String[] a =tokenstring.trim().split("\n");
                List<String> temp = Arrays.asList(a);
                temp.stream().forEach(s -> tokenlist.add(s.trim()));
                if (tokenlist.size() >= count) break;
                //tokenlist.addAll(temp);
                tmp = reader.readLine();
            }
            reader.close();
            in.close();

        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return tokenlist;


    }




}
