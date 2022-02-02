package edu.uci.ics.texera.workflow.operators.hdfsscan;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import edu.uci.ics.amber.engine.common.Constants;
import edu.uci.ics.amber.engine.operators.OpExecConfig;
import edu.uci.ics.texera.workflow.common.metadata.OperatorGroupConstants;
import edu.uci.ics.texera.workflow.common.metadata.OperatorInfo;
import edu.uci.ics.texera.workflow.common.metadata.OutputPort;
import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorDescriptor;
import edu.uci.ics.texera.workflow.common.scanner.BufferedBlockReaderOld;
import edu.uci.ics.texera.workflow.common.tuple.schema.Attribute;
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeType;
import edu.uci.ics.texera.workflow.common.tuple.schema.OperatorSchemaInfo;
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema;
import scala.collection.immutable.List;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Collections.singletonList;
import static scala.collection.JavaConverters.asScalaBuffer;


public class HdfsScanOpDesc extends SourceOperatorDescriptor {

    @JsonProperty(value = "host", required = true)
    @JsonPropertyDescription("host")
    public String host;

    @JsonProperty(value = "hdfs port", required = true)
    @JsonPropertyDescription("hdfs port used with hdfs://")
    public String hdfsPort;

    @JsonProperty(value = "hdfs restapi port", required = true)
    @JsonPropertyDescription("hdfs port used with http://")
    public String hdfsRestApiPort;

    @JsonProperty(value = "file path", required = true)
    @JsonPropertyDescription("local file path")
    public String filePath;

    @JsonProperty(value = "delimiter", defaultValue = ",")
    @JsonPropertyDescription("delimiter to separate each line into fields")
    public String delimiter;

    @JsonProperty(value = "indices to keep")
    @JsonPropertyDescription("Indices to keep")
    public String indicesToKeep;

    @JsonProperty(value = "header", defaultValue = "true")
    @JsonPropertyDescription("whether the CSV file contains a header line")
    public Boolean header;

    @Override
    public OpExecConfig operatorExecutor(OperatorSchemaInfo operatorSchemaInfo) {
        // fill in default values
        if (this.delimiter == null) {
            this.delimiter = ",";
        }
        ArrayList<Object> idxToKeep = new ArrayList<Object>();
        if (indicesToKeep != null && indicesToKeep.trim().length() > 0) {
            Arrays.stream(indicesToKeep.split(",")).forEach(idx -> idxToKeep.add(Integer.parseInt(idx)));
        }
        try {
            URL url = new URL("http://" + host + ":" + hdfsRestApiPort + "/webhdfs/v1" + filePath + "?op=OPEN&offset=0");
            InputStream stream = url.openStream();
            BufferedBlockReaderOld reader = new BufferedBlockReaderOld(stream, 10000, delimiter.charAt(0), idxToKeep);
            String[] headerLine = reader.readLine();
            return new HdfsScanOpExecConfig(this.operatorIdentifier(), Constants.currentWorkerNum() - 6, host, hdfsPort, hdfsRestApiPort,
                    filePath, delimiter.charAt(0), idxToKeep, this.inferSchema(headerLine), header != null && header);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public OperatorInfo operatorInfo() {
        return new OperatorInfo(
                "Hdfs File Scan",
                "Scan data from HDFS file",
                OperatorGroupConstants.SOURCE_GROUP(),
                List.empty(),
                asScalaBuffer(singletonList(new OutputPort(""))).toList());
    }

    @Override
    public Schema sourceSchema() {
        if (this.filePath == null) {
            return null;
        }
        try {
            ArrayList<Object> idxToKeep = new ArrayList<Object>();
            if (indicesToKeep != null && indicesToKeep.trim().length() > 0) {
                Arrays.stream(indicesToKeep.split(",")).forEach(idx -> idxToKeep.add(Integer.parseInt(idx)));
            }
            URL url = new URL("http://" + host + ":" + hdfsRestApiPort + "/webhdfs/v1" + filePath + "?op=OPEN&offset=0");
            InputStream stream = url.openStream();
            BufferedBlockReaderOld reader = new BufferedBlockReaderOld(stream, 10000, delimiter.charAt(0), idxToKeep);
            String[] headerLine = reader.readLine();
            if (header == null) {
                return null;
            }
            return inferSchema(headerLine);
        } catch (IOException e) {
            return null;
        }
    }

    private Schema inferSchema(String[] headerLine) {
        if (delimiter == null) {
            return null;
        }
        if (header != null && header) {
            return Schema.newBuilder().add(Arrays.stream(headerLine).map(c -> c.trim())
                    .map(c -> new Attribute(c, AttributeType.STRING)).collect(Collectors.toList())).build();
        } else {
            String[] p = filePath.trim().split("/");
            if (indicesToKeep != null && indicesToKeep.trim().length() > 0) {
                if (!Constants.sortExperiment()) {
                    Schema.Builder builder = Schema.newBuilder();
                    Arrays.stream(indicesToKeep.split(",")).forEach(idx -> {
                        builder.add(new Attribute(p[p.length - 1] + " column" + idx.trim(), AttributeType.STRING));
                    });
                    return builder.build();
                } else {
                    Schema.Builder builder = Schema.newBuilder();
                    for (int i = 0; i < headerLine.length; i++) {
                        try {
                            Double.parseDouble(headerLine[i]);
                            builder.add(new Attribute("column" + indicesToKeep.split(",")[i].trim(), AttributeType.DOUBLE));
                        } catch (NumberFormatException e) {
                            builder.add(new Attribute("column" + indicesToKeep.split(",")[i].trim(), AttributeType.STRING));
                        }
                    }
                    return builder.build();
                }
            } else {
                return Schema.newBuilder().add(IntStream.range(0, headerLine.length).
                        mapToObj(i -> new Attribute(p[p.length - 1] + " column" + i, AttributeType.STRING))
                        .collect(Collectors.toList())).build();
            }
        }
    }

}