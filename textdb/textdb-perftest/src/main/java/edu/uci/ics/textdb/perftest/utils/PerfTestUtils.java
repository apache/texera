package edu.uci.ics.textdb.perftest.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Scanner;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

import edu.uci.ics.textdb.common.constants.LuceneAnalyzerConstants;
import edu.uci.ics.textdb.engine.Engine;
import edu.uci.ics.textdb.perftest.medline.MedlineIndexWriter;
import edu.uci.ics.textdb.storage.DataStore;
import edu.uci.ics.textdb.storage.relation.RelationManager;

/**
 * @author Hailey Pan
 *
 *         Performance test helper functions
 **/

public class PerfTestUtils {

    /**
     * These default paths work only when the program is run from the directory,
     * textdb-perftest
     */
    public static String fileFolder;
    public static String standardIndexFolder;
    public static String trigramIndexFolder;
    public static String resultFolder;
    public static String queryFolder;


    static {
        try {
            fileFolder = Paths.get(PerfTestUtils.class.getResource("/sample-data-files").toURI()).toString();
            standardIndexFolder = Paths.get(PerfTestUtils.class.getResource("/index/standard").toURI()).toString();
            trigramIndexFolder = Paths.get(PerfTestUtils.class.getResource("/index/trigram").toURI()).toString();
            resultFolder = Paths.get(PerfTestUtils.class.getResource("/perftest-files/results").toURI()).toString();
            queryFolder = Paths.get(PerfTestUtils.class.getResource("/perftest-files/queries").toURI()).toString();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }


    /**
     * 
     * Checks whether the given file exists. If not, create such a file with a
     * header written into it.
     */
    public static void createFile(String filePath, String header) throws IOException {
        File file = new File(filePath);
        if (!file.exists()) {
            file.createNewFile();
            FileWriter fileWriter = new FileWriter(filePath, true);
            fileWriter.write(header);
            fileWriter.close();
        }

    }

    /*
     * The purpose for below setters:
     * 
     * When the program is not run from the directory, textdb-perftest, all path
     * need to be reset so that the program can recognize the paths.
     * 
     * For examplem, the default ./data-files/ works well when the program is
     * run from textdb-perftest, but the program is run from the outermost
     * folder of the project, the directory should be
     * ./textdb-perftest/data-files/
     */
    public static void setFileFolder(String filefolder) {
        if (!filefolder.trim().isEmpty()) {
            fileFolder = filefolder;
        }
    }

    public static void setStandardIndexFolder(String indexFolder) {
        if (!indexFolder.trim().isEmpty()) {
            standardIndexFolder = indexFolder;
        }

    }

    public static void setTrigramIndexFolder(String indexFolder) {
        if (!indexFolder.trim().isEmpty()) {
            trigramIndexFolder = indexFolder;
        }

    }

    public static void setQueryFolder(String queryfolder) {
        if (!queryfolder.trim().isEmpty()) {
            queryFolder = queryfolder;
        }
    }

    public static void setResultFolder(String resultfolder) {
        if (!resultfolder.trim().isEmpty()) {
            resultFolder = resultfolder;
        }
    }

    /**
     * 
     * @param testResults,
     *            a list of doubles
     * @return the average of the values in testResults
     */
    public static double calculateAverage(List<Double> testResults) {
        double totalTime = 0;
        for (Double result : testResults) {
            totalTime += result;
        }

        return Double.parseDouble(String.format("%.4f", totalTime / testResults.size()));
    }

    /**
     * @param testResults,
     *            a list of doubles
     * @param average,
     *            the average of the values in testResults
     * @return standard deviation of the values in testResults
     */
    public static double calculateSTD(List<Double> testResults, Double average) {
        double numerator = 0;
        for (Double result : testResults) {
            numerator += Math.pow(result - average, 2);
        }

        return Double.parseDouble(String.format("%.4f", Math.sqrt(numerator / testResults.size())));
    }
    
    /**
     * Delete all files recursively in a directory
     * 
     * @param indexDirectory
     * @throws Exception
     */
    public static void deleteDirectory(File indexDirectory) throws Exception {
        boolean deleteTopDir= false; 
        for (File file : indexDirectory.listFiles()) {
            if (file.getName().equals(".gitignore")) {
                continue;
            }
            if (file.isDirectory()) {
                deleteDirectory(file);
            } else {
                deleteTopDir = file.delete();
            }
        }
        if(deleteTopDir){
            indexDirectory.delete();
        }
        
    }

    /**
     * Writes all files in ./data-files/ into indices
     * 
     * @throws Exception
     */
    public static void writeStandardAnalyzerIndices() throws Exception {
        File files = new File(fileFolder);
        for (File file : files.listFiles()) {
            if (file.getName().startsWith(".")) {
                continue;
            }
            if (file.isDirectory()) {
                continue;
            }
            writeIndex(file.getName(), new StandardAnalyzer(), "standard");
        }
    }

    /**
     * Writes all files in ./data-files/ into trigram indices
     * 
     * @throws Exception
     */
    public static void writeTrigramIndices() throws Exception {
        File files = new File(fileFolder);
        for (File file : files.listFiles()) {
            if (file.getName().startsWith(".")) {
                continue;
            }
            if (file.isDirectory()) {
                continue;
            }
            writeIndex(file.getName(), LuceneAnalyzerConstants.getNGramAnalyzer(3), "trigram");
        }

    }

    /**
     * Writes a data file into an index
     * 
     * @param fileName,
     *            data file
     * @param luceneAnalyzer
     * @param indexType,
     *            indicates the types of index, trigram or standard
     * @throws Exception
     */
    public static void writeIndex(String fileName, Analyzer luceneAnalyzer, String indexType) throws Exception {
        RelationManager relationManager = RelationManager.getRelationManager();
        
        String tableName = fileName.replace(".txt", "");
        
        relationManager.deleteTable(tableName);
        
        if (indexType.equalsIgnoreCase("trigram")) {
            relationManager.createTable(tableName, getTrigramIndexPath(tableName), 
                    MedlineIndexWriter.SCHEMA_MEDLINE, LuceneAnalyzerConstants.nGramAnalyzerString(3));
        } else if (indexType.equalsIgnoreCase("standard")) {
            relationManager.createTable(tableName, getIndexPath(tableName), 
                    MedlineIndexWriter.SCHEMA_MEDLINE, LuceneAnalyzerConstants.standardAnalyzerString());
        } else {
            System.out.println("Index is not successfully written.");
            System.out.println("IndexType has to be either \"standard\" or \"trigram\"  ");
        }
    }

    /**
     * Reads lines in a file into a list
     * 
     * @param filePath
     * @return a list of strings
     * @throws FileNotFoundException
     */
    public static ArrayList<String> readQueries(String filePath) throws FileNotFoundException {
        ArrayList<String> queries = new ArrayList<String>();
        Scanner scanner = new Scanner(new File(filePath));
        while (scanner.hasNextLine()) {
            queries.add(scanner.nextLine().trim());
        }
        scanner.close();
        return queries;
    }

    /**
     * 
     * @param indexName
     * @return a path of an index in ./index/standard/
     */
    public static String getIndexPath(String indexName) {
        return standardIndexFolder + indexName;
    }

    /**
     * 
     * @param indexName
     * @return a path of a trigram index in ./index/trigram/
     */
    public static String getTrigramIndexPath(String indexName) {
        return trigramIndexFolder + indexName;
    }

    /**
     * 
     * @param resultFileName
     * @return a path of a result file in ./data-files/results/
     */
    public static String getResultPath(String resultFileName) {
        return resultFolder + resultFileName;
    }

    /**
     * 
     * @param queryFileName
     * @return a path of a data file in ./data-files/queries/
     */
    public static String getQueryPath(String queryFileName) {
        return queryFolder + queryFileName;

    }

    /**
     * Formats a time to string
     * 
     * @param time
     *            (the milliseconds since January 1, 1970, 00:00:00 GMT)
     * @return string representation of the time
     */
    public static String formatTime(long time) {
        Date date = new Date(time);

        SimpleDateFormat sdf = new SimpleDateFormat("MM-dd-yyyy HH:mm:ss");

        return sdf.format(date).toString();
    }

}
