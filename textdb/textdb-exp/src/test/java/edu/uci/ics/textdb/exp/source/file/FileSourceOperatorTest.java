package edu.uci.ics.textdb.exp.source.file;

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

import edu.uci.ics.textdb.api.exception.StorageException;
import edu.uci.ics.textdb.api.field.TextField;
import edu.uci.ics.textdb.api.schema.Attribute;
import edu.uci.ics.textdb.api.schema.AttributeType;
import edu.uci.ics.textdb.api.schema.Schema;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.api.utils.TestUtils;
import edu.uci.ics.textdb.storage.utils.StorageUtils;
import junit.framework.Assert;

public class FileSourceOperatorTest {
    
    public static Path tempFolderPath = Paths.get("./index/test_tables/filesource/tempfolder/");
    
    public static Path tempFile1Path = tempFolderPath.resolve("test1.txt");
    public static String tempFile1String = "File Source Operator Test File 1. This file is directly under test folder.";
    
    public static Path tempFile2Path = tempFolderPath.resolve("test2.txt");
    public static String tempFile2String = "File Source Operator Test File 2. This file is directly under test folder.";
    
    public static Path tempFile3Path = tempFolderPath.resolve("test3.tmp");
    public static String tempFile3String = "File Source Operator Test File 3. The extension (tmp) is not allowed by default.";
    
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
     *   test3.tmp
     *   nested/
     *     test4.txt
     *     nested2/
     *       test5.txt
     *   empty/
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
        
        Files.createFile(tempFile3Path);
        Files.write(tempFile3Path, tempFile3String.getBytes());
        
        Files.createDirectories(nestedFolderPath);
        Files.createFile(tempFile4Path);
        Files.write(tempFile4Path, tempFile4String.getBytes());
        
        Files.createDirectories(nested2FolderPath);
        Files.createFile(tempFile5Path);
        Files.write(tempFile5Path, tempFile5String.getBytes());
        
        Files.createDirectories(emptyFolderPath);

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
        String attrName = "content";
        Schema schema = new Schema(new Attribute(attrName, AttributeType.TEXT));
        
        FileSourcePredicate predicate = new FileSourcePredicate(
                tempFile1Path.toString(), attrName);
        FileSourceOperator fileSource = new FileSourceOperator(predicate);
        
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
     * Test FileSourceOperator with a Directory.
     * Optional parameters are all set to default. (only list files directly in this folder)
     * 
     * Only the files directly under this directory will be used.
     *     
     * expected results: test1.txt and test2.txt will be included.
     */
    @Test
    public void test2() throws Exception {
        String attrName = "content";
        Schema schema = new Schema(new Attribute(attrName, AttributeType.TEXT));
        
        FileSourcePredicate predicate = new FileSourcePredicate(
                tempFolderPath.toString(), attrName);
        FileSourceOperator fileSource = new FileSourceOperator(predicate);
        
        Tuple tuple;
        ArrayList<Tuple> exactResults = new ArrayList<>();
        fileSource.open();
        while ((tuple = fileSource.getNextTuple()) != null) {
            exactResults.add(tuple);
        }
        fileSource.close();
                
        List<Tuple> expectedResults = Arrays.asList(
                new Tuple(schema, new TextField(tempFile1String)), 
                new Tuple(schema, new TextField(tempFile2String)));
        
        Assert.assertTrue(TestUtils.equals(expectedResults, exactResults));
    }
    
    /*
     * Test FileSourceOperator with a Directory with recursive = true and maxDepth = null.
     * 
     * All the files under the recursive sub-directories will be read.
     *     
     * expected results: test1.txt, test2.txt, test4.txt and test5.txt will be included
     */
    @Test
    public void test3() throws Exception {
        String attrName = "content";
        Schema schema = new Schema(new Attribute(attrName, AttributeType.TEXT));
        
        FileSourcePredicate predicate = new FileSourcePredicate(
                tempFolderPath.toString(), attrName, true, null, null);
        FileSourceOperator fileSource = new FileSourceOperator(predicate);
        
        Tuple tuple;
        ArrayList<Tuple> exactResults = new ArrayList<>();
        fileSource.open();
        while ((tuple = fileSource.getNextTuple()) != null) {
            exactResults.add(tuple);
        }
        fileSource.close();
                
        List<Tuple> expectedResults = Arrays.asList(
                new Tuple(schema, new TextField(tempFile1String)), 
                new Tuple(schema, new TextField(tempFile2String)),
                new Tuple(schema, new TextField(tempFile4String)),
                new Tuple(schema, new TextField(tempFile5String)));
                
        Assert.assertTrue(TestUtils.equals(expectedResults, exactResults));
    }
    
    /*
     * Test FileSourceOperator with a Directory with recursive = true and maxDepth = 2.
     * 
     * The files under the recursive sub-directories with recursive depth 2 will be read.
     *     
     * expected results: test1.txt, test2.txt and test4.txt will be included
     */
    @Test
    public void test4() throws Exception {
        String attrName = "content";
        Schema schema = new Schema(new Attribute(attrName, AttributeType.TEXT));
        
        FileSourcePredicate predicate = new FileSourcePredicate(
                tempFolderPath.toString(), attrName, true, 2, null);
        FileSourceOperator fileSource = new FileSourceOperator(predicate);
        
        Tuple tuple;
        ArrayList<Tuple> exactResults = new ArrayList<>();
        fileSource.open();
        while ((tuple = fileSource.getNextTuple()) != null) {
            exactResults.add(tuple);
        }
        fileSource.close();
                
        List<Tuple> expectedResults = Arrays.asList(
                new Tuple(schema, new TextField(tempFile1String)), 
                new Tuple(schema, new TextField(tempFile2String)),
                new Tuple(schema, new TextField(tempFile4String)));
                
        Assert.assertTrue(TestUtils.equals(expectedResults, exactResults));
    }
    
    /*
     * Test FileSourceOperator with a single file with an extension (.tmp) not allowed by default.
     */
    @Test(expected = RuntimeException.class)
    public void test5() throws Exception {
        String attrName = "content";
        
        FileSourcePredicate predicate = new FileSourcePredicate(
                tempFile3Path.toString(), attrName);
        new FileSourceOperator(predicate);
    }
    
    /*
     * Test FileSourceOperator with custom extensions.
     * 
     * Specify "tmp" as q valid extension. Only test3.tmp should be read.
     */
    @Test
    public void test6() throws Exception {
        String attrName = "content";
        Schema schema = new Schema(new Attribute(attrName, AttributeType.TEXT));
        
        FileSourcePredicate predicate = new FileSourcePredicate(
                tempFile3Path.toString(), attrName, null, null, Arrays.asList("tmp"));
        FileSourceOperator fileSource = new FileSourceOperator(predicate);
        
        Tuple tuple;
        ArrayList<Tuple> exactResults = new ArrayList<>();
        fileSource.open();
        while ((tuple = fileSource.getNextTuple()) != null) {
            exactResults.add(tuple);
        }
        fileSource.close();
                
        List<Tuple> expectedResults = Arrays.asList(
                new Tuple(schema, new TextField(tempFile3String)));
                
        Assert.assertTrue(TestUtils.equals(expectedResults, exactResults));
    }
    
    /*
     * Test FileSourceOperator with custom extensions that won't match any file.
     */
    @Test(expected = RuntimeException.class)
    public void test7() throws Exception {
        String attrName = "content";
        
        FileSourcePredicate predicate = new FileSourcePredicate(
                tempFile3Path.toString(), attrName, null, null, Arrays.asList("invalid"));
        new FileSourceOperator(predicate);
    }
    
    /*
     * Test FileSourceOperator with a single directory that does not contain any valid files
     */
    @Test(expected = RuntimeException.class)
    public void test8() throws Exception {
        String attrName = "content";
        
        FileSourcePredicate predicate = new FileSourcePredicate(
                emptyFolderPath.toString(), attrName);
        new FileSourceOperator(predicate);     
    }
    
    /*
     * Test FileSourceOperator with a file that does not exist
     */
    @Test(expected = RuntimeException.class)
    public void test9() throws Exception {
        String attrName = "content";
        
        FileSourcePredicate predicate = new FileSourcePredicate(
                tempFolderPath.resolve("notexist.txt").toString(), attrName);
        new FileSourceOperator(predicate);
    }
    
    /*
     * Test FileSourceOperator with a directory that does not exist
     */
    @Test(expected = RuntimeException.class)
    public void test10() throws Exception {
        String attrName = "content";
        
        FileSourcePredicate predicate = new FileSourcePredicate(
                tempFolderPath.resolve("notexist").toString(), attrName);
        new FileSourceOperator(predicate);
    }
    
    /*
     * Test FileSourceOperator with a directory, 
     * recursive set to true, depth set to 10, using default extensions.
     * 
     * expected result: 4 tuples should be returned.
     */
    @Test
    public void test11() throws Exception {
        String attrName = "content";

        FileSourcePredicate predicate = new FileSourcePredicate(tempFolderPath.toString(), attrName, true, 10, null);
        FileSourceOperator fileSource = new FileSourceOperator(predicate);

        Tuple tuple;
        ArrayList<Tuple> exactResults = new ArrayList<>();
        fileSource.open();
        while ((tuple = fileSource.getNextTuple()) != null) {
            exactResults.add(tuple);
        }
        fileSource.close();

        Assert.assertEquals(exactResults.size(), 4);
    }
    
}
