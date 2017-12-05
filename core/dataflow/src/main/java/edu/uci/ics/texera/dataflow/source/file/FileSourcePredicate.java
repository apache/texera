package edu.uci.ics.texera.dataflow.source.file;

import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uci.ics.texera.api.dataflow.IOperator;
import edu.uci.ics.texera.dataflow.common.PredicateBase;
import edu.uci.ics.texera.dataflow.common.PropertyNameConstants;

public class FileSourcePredicate extends PredicateBase {
    
    private final String filePath;
    private final String attributeName;
    private final Integer maxDepth;
    private final Boolean recursive;
    
    /**
     * FileSourcePredicate is used by FileSource Operator.
     * 
     * @param filePath, the path to a file or a directory
     * @param attributeName, the name of the attribute that the file content will be put in
     * @param recursive, optional, if recursively list files or not (in the case of a directory), default False
     * @param maxDepth, optional, specify the max recursive depth (if recursive is True), default Integer.MAX_VALUE
     * @param allowedExtensions, optional, specify a list of allowed extensions, default {@code defaultSupportedExtensions}
     */
    public FileSourcePredicate(
            @JsonProperty(value = PropertyNameConstants.FILE_PATH, required = true)
            String filePath, 
            @JsonProperty(value = PropertyNameConstants.RESULT_ATTRIBUTE_NAME, required = true)
            String attributeName,
            @JsonProperty(value = PropertyNameConstants.FILE_RECURSIVE, required = false)
            Boolean recursive,
            @JsonProperty(value = PropertyNameConstants.FILE_MAX_DEPTH, required = false)
            Integer maxDepth) {
        this.filePath = filePath;
        this.attributeName = attributeName;
        
        if (recursive == null) {
            this.recursive = false;
        } else {
            this.recursive = recursive;
        }
        if (maxDepth == null) {
            this.maxDepth = Integer.MAX_VALUE;
        } else {
            this.maxDepth = maxDepth;
        }
    }
    
    /**
     * Constructs a FileSourcePredicate with minimal required parameters, 
     *   with all optional parameters are set to null.
     *   
     * This is only for internal use, it's NOT an entry point for JSON data. 
     * 
     * @param filePath
     * @param attributeName
     */
    public FileSourcePredicate(String filePath, String attributeName) {
        this(filePath, attributeName, null, null);
    }
    
    @JsonProperty(PropertyNameConstants.FILE_PATH)
    public String getFilePath() {
        return this.filePath;
    }
    
    @JsonProperty(PropertyNameConstants.RESULT_ATTRIBUTE_NAME)
    public String getAttributeName() {
        return this.attributeName;
    }
    
    @JsonProperty(PropertyNameConstants.FILE_RECURSIVE)
    public Boolean isRecursive() {
        return this.recursive;
    }
    
    @JsonProperty(PropertyNameConstants.FILE_MAX_DEPTH)
    public Integer getMaxDepth() {
        return this.maxDepth;
    }
    
    @Override
    public FileSourceOperator newOperator() {
        return new FileSourceOperator(this);
    }

}
