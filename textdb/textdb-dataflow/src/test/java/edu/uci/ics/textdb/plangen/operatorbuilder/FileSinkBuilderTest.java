package edu.uci.ics.textdb.plangen.operatorbuilder;

import java.io.File;
import java.util.HashMap;

import org.junit.Assert;
import org.junit.Test;

import edu.uci.ics.textdb.api.exception.PlanGenException;
import edu.uci.ics.textdb.dataflow.sink.FileSink;
import edu.uci.ics.textdb.plangen.operatorbuilder.FileSinkBuilder;

public class FileSinkBuilderTest {
    
    @Test
    public void testFileSinkBuilder1() throws Exception {
        String filePath = "./result_file.txt";
        
        HashMap<String, String> operatorProperties = new HashMap<>();
        operatorProperties.put(FileSinkBuilder.FILE_PATH, filePath);
        
        FileSink fileSink = FileSinkBuilder.buildSink(operatorProperties);
        
        Assert.assertEquals(fileSink.getFile().getAbsolutePath(), new File(filePath).getAbsolutePath());   
    }
    
    /*
     * Test building an invalid file sink with an empty path.
     */
    @Test(expected = PlanGenException.class)
    public void testInvalidFileSinkBuilder1() throws Exception {
        String filePath = "";
        
        HashMap<String, String> operatorProperties = new HashMap<>();
        operatorProperties.put(FileSinkBuilder.FILE_PATH, filePath);
        
        FileSinkBuilder.buildSink(operatorProperties);
    }
    
    /*
     * Test building an invalid file sink with an empty path.
     */
    @Test(expected = PlanGenException.class)
    public void testInvalidFileSinkBuilder2() throws Exception {
        String filePath = "     ";
        
        HashMap<String, String> operatorProperties = new HashMap<>();
        operatorProperties.put(FileSinkBuilder.FILE_PATH, filePath);
        
        FileSinkBuilder.buildSink(operatorProperties);
    }

}
