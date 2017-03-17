package edu.uci.ics.textdb.plangen.operatorbuilder;

import java.io.File;
import java.util.Map;

import edu.uci.ics.textdb.api.exception.PlanGenException;
import edu.uci.ics.textdb.dataflow.sink.FileSink;
import edu.uci.ics.textdb.plangen.PlanGenUtils;

/**
 * FileSinkBuilder provides a static function that builds a FileSink.
 * 
 * FileSinkBuilder currently needs the following properties:
 *   file (required)
 * 
 * @author Zuozhi Wang
 *
 */
public class FileSinkBuilder {
    
    public static final String FILE_PATH = "file";
    
    public static FileSink buildSink(Map<String, String> operatorProperties) throws PlanGenException {
        String filePath = OperatorBuilderUtils.getRequiredProperty(FILE_PATH, operatorProperties);
        
        PlanGenUtils.planGenAssert(! filePath.trim().isEmpty(), "file path can't be empty");
        
        File file = new File(filePath);
        FileSink fileSink = new FileSink(file);
        
        return fileSink;
    }

}
