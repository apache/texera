package edu.uci.ics.texera.dataflow.source.file;

import java.util.Map;
import java.util.regex.Pattern;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.ImmutableMap;

import edu.uci.ics.texera.dataflow.common.OperatorGroupConstants;
import edu.uci.ics.texera.dataflow.common.PredicateBase;
import edu.uci.ics.texera.dataflow.common.PropertyNameConstants;
import edu.uci.ics.texera.dataflow.plangen.QueryContext;

public class FileSourcePredicate extends PredicateBase {

    public static final String ROW_DELIMITER = "rowDelimiter";
    public static final String COLUMN_DELIMITER = "columnDelimiter";

    private final String fileName;
    private FileFormat fileFormat;
    private String rowDelimiter;
    private String columnDelimiter;

    private String filePath;

    public enum FileFormat {
        CSV("csv", "A csv file without header."),
        CSV_WITH_HEADER("csv (with header)", "a csv file with header"),
        PLAIN_TEXT("plain text", "treat the file as a plain text file");

        private final String name;
        private final String description;

        FileFormat(String name, String description) {
            this.name = name;
            this.description = description;
        }

        @JsonValue
        public String getName() {
            return name;
        }

        @JsonIgnore
        public String getDescription() {
            return description;
        }

    }

    /**
     * FileSourcePredicate is used by FileSource Operator.
     *
     * @param fileName, the path to a file or a directory
     */
    @JsonCreator
    public FileSourcePredicate(
            @JsonProperty(value = PropertyNameConstants.FILE_NAME, required = true)
            String fileName,
            @JsonProperty(value = PropertyNameConstants.FILE_FORMAT)
            FileFormat fileFormat,
            @JsonProperty(value = ROW_DELIMITER)
            String rowDelimiter,
            @JsonProperty(value = COLUMN_DELIMITER)
            String columnDelimiter
            ) {
        this.fileName = fileName;
        this.fileFormat = fileFormat == null ? FileFormat.PLAIN_TEXT : fileFormat;
        this.rowDelimiter = rowDelimiter == null || rowDelimiter.isEmpty() ? System.lineSeparator() : rowDelimiter;
        this.columnDelimiter = columnDelimiter;
        if (this.columnDelimiter == null &&
                (this.fileFormat == FileFormat.CSV || this.fileFormat == FileFormat.CSV_WITH_HEADER)) {
            this.columnDelimiter = ",";
        }
        if (this.columnDelimiter != null && this.columnDelimiter.isEmpty()) {
            this.columnDelimiter = null;
        }
    }

    // for internal testing purpose only, directly specify file path
    public static FileSourcePredicate createWithFilePath(String filePath) {
        FileSourcePredicate predicate = new FileSourcePredicate(null, null, null, null);
        predicate.filePath = filePath;
        return predicate;
    }

    @JsonProperty(PropertyNameConstants.FILE_NAME)
    public String getFileName() {
        return this.fileName;
    }

    @JsonIgnore
    public String getFilePath() {
        return this.filePath;
    }

    @JsonProperty(PropertyNameConstants.FILE_FORMAT)
    public FileFormat getFileFormat() {
        return fileFormat;
    }

    @JsonProperty(ROW_DELIMITER)
    public String getRowDelimiter() {
        return rowDelimiter;
    }

    @JsonProperty(COLUMN_DELIMITER)
    public String getColumnDelimiter() {
        return columnDelimiter;
    }

    @Override
    public FileSourceOperator newOperator(QueryContext ctx) {
        return new FileSourceOperator(this, ctx);
    }

    public static Map<String, Object> getOperatorMetadata() {
        return ImmutableMap.<String, Object>builder()
            .put(PropertyNameConstants.USER_FRIENDLY_NAME, "Source: File")
            .put(PropertyNameConstants.OPERATOR_DESCRIPTION, "Read the content of one file or multiple files")
            .put(PropertyNameConstants.OPERATOR_GROUP_NAME, OperatorGroupConstants.SOURCE_GROUP)
            .build();
    }

}
