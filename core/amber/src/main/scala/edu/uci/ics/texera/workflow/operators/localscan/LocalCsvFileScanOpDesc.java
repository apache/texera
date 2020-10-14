package edu.uci.ics.texera.workflow.operators.localscan;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.google.common.io.Files;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaDefault;
import edu.uci.ics.amber.engine.common.Constants;
import edu.uci.ics.texera.workflow.common.metadata.OperatorGroupConstants;
import edu.uci.ics.texera.workflow.common.metadata.OperatorInfo;
import edu.uci.ics.texera.workflow.common.operators.source.SourceOpExecConfig;
import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorDescriptor;
import edu.uci.ics.texera.workflow.common.tuple.schema.Attribute;
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeType;
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


public class LocalCsvFileScanOpDesc extends SourceOperatorDescriptor {

    @JsonProperty("file path")
    @JsonPropertyDescription("local file path")
    public String filePath;

    @JsonProperty("delimiter")
    @JsonPropertyDescription("delimiter to separate each line into fields")
    @JsonSchemaDefault(",")
    public String delimiter;

    @JsonProperty("header")
    @JsonPropertyDescription("whether the CSV file contains a header line")
    @JsonSchemaDefault("true")
    public Boolean header;

    @Override
    public SourceOpExecConfig operatorExecutor() {
        try {
            String headerLine = Files.asCharSource(new File(filePath), Charset.defaultCharset()).readFirstLine();
            return new LocalCsvFileScanOpExecConfig(this.operatorIdentifier(), Constants.defaultNumWorkers(),
                    filePath, delimiter.charAt(0), this.inferSchema(headerLine), header != null && header);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public OperatorInfo operatorInfo() {
        return new OperatorInfo(
                "CSV File Scan",
                "Scan data from a local CSV file",
                OperatorGroupConstants.SOURCE_GROUP(),
                0, 1);
    }

    @Override
    public Schema sourceSchema() {
        if (this.filePath == null) {
            return null;
        }
        try {
            String headerLine = Files.asCharSource(new File(filePath), Charset.defaultCharset()).readFirstLine();
            if (header == null) {
                return null;
            }
            return inferSchema(headerLine);
        } catch (IOException e) {
            return null;
        }
    }

    private Schema inferSchema(String headerLine) {
        if (delimiter == null) {
            return null;
        }
        if (header != null && header) {
            return Schema.newBuilder().add(Arrays.stream(headerLine.split(delimiter)).map(c -> c.trim())
                    .map(c -> new Attribute(c, AttributeType.STRING)).collect(Collectors.toList())).build();
        } else {
            return Schema.newBuilder().add(IntStream.range(0, headerLine.split(delimiter).length).
                    mapToObj(i -> new Attribute("column" + i, AttributeType.STRING))
                    .collect(Collectors.toList())).build();
        }
    }

}
