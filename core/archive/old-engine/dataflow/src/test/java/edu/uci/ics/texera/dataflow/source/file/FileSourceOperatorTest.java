package edu.uci.ics.texera.dataflow.source.file;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.uci.ics.texera.api.exception.StorageException;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.field.TextField;
import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.api.utils.TestUtils;
import edu.uci.ics.texera.storage.utils.StorageUtils;
import junit.framework.Assert;

public class FileSourceOperatorTest {
    
    public static Path tempFolderPath = Paths.get("./index/test_tables/filesource/tempfolder/");
    
    public static Path tempFile1Path = tempFolderPath.resolve("test1.txt");
    public static String tempFile1String = "File Source Operator Test File 1. This file is directly under test folder.";
    
    public static Path tempFile2Path = tempFolderPath.resolve("test2.txt");
    public static String tempFile2String = "File Source Operator Test File 2. This file is directly under test folder.";
    
    public static Path nestedFolderPath = tempFolderPath.resolve("nested/");
    
    public static Path tempFile4Path = nestedFolderPath.resolve("test4.txt");
    public static String tempFile4String = "File Source Operator Test File 4. This file is in depth 2 nested folder.";
    
    public static Path nested2FolderPath = nestedFolderPath.resolve("nested2/");
    public static Path tempFile5Path = nested2FolderPath.resolve("test5.txt");
    public static String tempFile5String = "File Source Operator Test File 5. This file is in depth 3 nested folder.";
    
    public static Path emptyFolderPath = tempFolderPath.resolve("empty/");

    
    /*
     * Organization of FileSource Test files:
     * tempfolder/
     *   test1.txt
     *   test2.txt
     * 
     */
    @BeforeClass
    public static void setup() throws IOException, StorageException {
        // make sure the files are deleted
        cleanUp();
                
        // create a temp folder
        Files.createDirectories(tempFolderPath);
        // create multiple files under this directory
        Files.createFile(tempFile1Path);
        Files.write(tempFile1Path, tempFile1String.getBytes());
        
        Files.createFile(tempFile2Path);
        Files.write(tempFile2Path, tempFile2String.getBytes());

    }
    
    @AfterClass
    public static void cleanUp() throws StorageException {
        //delete temp folder
        if (Files.exists(tempFolderPath)) {
            StorageUtils.deleteDirectory(tempFolderPath.toString());
        }
    }
    
    /*
     * Test FileSourceOperator with a single "txt" file.
     * Optional parameters are all set to default.
     */
    @Test
    public void test1() throws Exception {
        String attrName = "c1";
        Schema schema = new Schema(new Attribute(attrName, AttributeType.TEXT));
        
        FileSourcePredicate predicate = FileSourcePredicate.createWithFilePath(
                tempFile1Path.toString());
        FileSourceOperator fileSource = new FileSourceOperator(predicate, null);
        
        Tuple tuple;
        ArrayList<Tuple> exactResults = new ArrayList<>();
        fileSource.open();
        while ((tuple = fileSource.getNextTuple()) != null) {
            exactResults.add(tuple);
        }
        fileSource.close();
        
        List<Tuple> expectedResults = Arrays.asList(
                new Tuple(schema, new TextField(tempFile1String)));
        
        Assert.assertTrue(TestUtils.equals(expectedResults, exactResults));
    }
    
    /*
     * Test FileSourceOperator with a file that does not exist
     */
    @Test(expected = TexeraException.class)
    public void test2() throws Exception {
        FileSourcePredicate predicate = FileSourcePredicate.createWithFilePath(
                tempFolderPath.resolve("notexist.txt").toString());
        new FileSourceOperator(predicate, null);
    }
    
}
