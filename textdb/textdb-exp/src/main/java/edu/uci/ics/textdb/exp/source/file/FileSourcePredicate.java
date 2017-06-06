package edu.uci.ics.textdb.exp.source.file;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.exp.common.PredicateBase;
import edu.uci.ics.textdb.exp.common.PropertyNameConstants;

public class FileSourcePredicate extends PredicateBase {

    public static final List<String> defaultAllowedExtensions = Arrays.asList(
            "txt", "json", "xml", "csv", "html", "md", "pdf", "ppt", "pptx");

    private final String filePath;
    private final String attributeName;
    private final Integer maxDepth;
    private final Boolean recursive;
    private final List<String> allowedExtensions;

    /**
     * FileSourcePredicate is used by FileSource Operator.
     *
     * @param filePath,          the path to a file or a directory
     * @param attributeName,     the name of the attribute that the file content will be put in
     * @param recursive,         optional, if recursively list files or not (in the case of a directory), default False
     * @param maxDepth,          optional, specify the max recursive depth (if recursive is True), default Integer.MAX_VALUE
     * @param allowedExtensions, optional, specify a list of allowed extensions, default {@code defaultSupportedExtensions}
     */
    public FileSourcePredicate(
            @JsonProperty(value = PropertyNameConstants.FILE_PATH, required = true)
                    String filePath,
            @JsonProperty(value = PropertyNameConstants.ATTRIBUTE_NAME, required = true)
                    String attributeName,
            @JsonProperty(value = PropertyNameConstants.FILE_RECURSIVE, required = false)
                    Boolean recursive,
            @JsonProperty(value = PropertyNameConstants.FILE_MAX_DEPTH, required = false)
                    Integer maxDepth,
            @JsonProperty(value = PropertyNameConstants.FILE_ALLOWED_EXTENSIONS, required = false)
                    List<String> allowedExtensions) {
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
        if (allowedExtensions == null || allowedExtensions.isEmpty()) {
            this.allowedExtensions = unifyExtensions(defaultAllowedExtensions);
        } else {
            this.allowedExtensions = unifyExtensions(allowedExtensions);
        }
    }

    /**
     * Constructs a FileSourcePredicate with minimal required parameters,
     * with all optional parameters are set to null.
     * <p>
     * This is only for internal use, it's NOT an entry point for JSON data.
     *
     * @param filePath
     * @param attributeName
     */
    public FileSourcePredicate(String filePath, String attributeName) {
        this(filePath, attributeName, null, null, null);
    }

    /*
     * A helper function to remove the dot(".") in the beginning of the extensions.
     * For example, ".txt" and "txt" will be treated as the same extension: "txt".
     */
    private static List<String> unifyExtensions(List<String> extensions) {
        return extensions.stream()
                .map(ext -> ext.toLowerCase())
                .map(ext -> ext.charAt(0) == '.' ? ext.substring(1) : ext)
                .collect(Collectors.toList());
    }

    @JsonProperty(PropertyNameConstants.FILE_PATH)
    public String getFilePath() {
        return this.filePath;
    }

    @JsonProperty(PropertyNameConstants.ATTRIBUTE_NAME)
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

    @JsonProperty(PropertyNameConstants.FILE_ALLOWED_EXTENSIONS)
    public List<String> getAllowedExtensions() {
        return Collections.unmodifiableList(this.allowedExtensions);
    }

    @Override
    public IOperator newOperator() {
        return new FileSourceOperator(this);
    }

}
