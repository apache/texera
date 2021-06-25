package edu.uci.ics.texera.perftest.utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

import edu.uci.ics.texera.api.constants.DataConstants.TexeraProject;
import edu.uci.ics.texera.api.utils.Utils;
import edu.uci.ics.texera.perftest.medline.MedlineIndexWriter;
import edu.uci.ics.texera.storage.RelationManager;
import edu.uci.ics.texera.storage.constants.LuceneAnalyzerConstants;

/**
 * @author Hailey Pan
 *
 *         Performance test helper functions
 **/

public class PerfTestUtils {

    /**
     * These default paths work only when the program is run from the directory,
     * perftest
     */
    public static String fileFolder = getResourcePath("/sample-data-files").toString();
    public static String standardIndexFolder = getResourcePath("/index/standard").toString();
    public static String trigramIndexFolder = getResourcePath("/index/trigram").toString();
    public static String resultFolder = getResourcePath("/perftest-files/results").toString();
    public static String queryFolder = getResourcePath("/perftest-files/queries").toString();
    
    public static Path getResourcePath(String resourcePath) {
        return Utils.getResourcePath(resourcePath, TexeraProject.TEXERA_PERFTEST);
    }

    /**
     * 
     * Checks whether the given file exists. If not, create such a file with a
     * header written into it.
     */
    public static void createFile(Path filePath, String header) throws IOException {
        if (Files.notExists(filePath)) {
            Files.createFile(filePath);
            Files.write(filePath, header.getBytes());
        }
    }

    /*
     * The purpose for below setters:
     * 
     * When the program is not run from the directory, perftest, all path
     * need to be reset so that the program can recognize the paths.
     * 
     * For examplem, the default ./data-files/ works well when the program is
     * run from perftest, but the program is run from the outermost
     * folder of the project, the directory should be
     * ./perftest/data-files/
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
            if (file.isDirectory()) {
                continue;
            }
            if (file.getName().startsWith("abstract")) {
                writeIndex(file.getName(), new StandardAnalyzer(), "standard");
            }
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
        RelationManager relationManager = RelationManager.getInstance();
        
        String tableName = fileName.replace(".txt", "");
                
        if (indexType.equalsIgnoreCase("trigram")) {
            tableName = tableName + "_trigram";
            relationManager.deleteTable(tableName);
            relationManager.createTable(tableName, getTrigramIndexPath(tableName), 
                    MedlineIndexWriter.SCHEMA_MEDLINE, LuceneAnalyzerConstants.nGramAnalyzerString(3));
            
            MedlineIndexWriter.writeMedlineIndex(Paths.get(fileFolder, fileName), tableName);
            
        } else if (indexType.equalsIgnoreCase("standard")) {
            relationManager.deleteTable(tableName);
            relationManager.createTable(tableName, getIndexPath(tableName), 
                    MedlineIndexWriter.SCHEMA_MEDLINE, LuceneAnalyzerConstants.standardAnalyzerString());
            MedlineIndexWriter.writeMedlineIndex(Paths.get(fileFolder, fileName), tableName);
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
     * @throws IOException 
     */
    public static ArrayList<String> readQueries(Path queryFilePath) throws IOException {
        return Files.readAllLines(queryFilePath).stream()
                .map(str -> str.trim())
                .collect(Collectors.toCollection(ArrayList::new));
    }

    /**
     * 
     * @param indexName
     * @return a path of an index in ./index/standard/
     */
    public static Path getIndexPath(String indexName) {
        return Paths.get(standardIndexFolder, indexName);
    }

    /**
     * 
     * @param indexName
     * @return a path of a trigram index in ./index/trigram/
     */
    public static Path getTrigramIndexPath(String indexName) {
        return Paths.get(trigramIndexFolder, indexName);
    }

    /**
     * 
     * @param resultFileName
     * @return a path of a result file in ./data-files/results/
     */
    public static Path getResultPath(String resultFileName) {
        return Paths.get(resultFolder, resultFileName);
    }

    /**
     * 
     * @param queryFileName
     * @return a path of a data file in ./data-files/queries/
     */
    public static Path getQueryPath(String queryFileName) {
        return Paths.get(queryFolder, queryFileName);

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
