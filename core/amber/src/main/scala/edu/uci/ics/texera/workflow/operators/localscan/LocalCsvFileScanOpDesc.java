package edu.uci.ics.texera.workflow.operators.localscan;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.google.common.io.Files;
import edu.uci.ics.amber.engine.common.Constants;
import edu.uci.ics.amber.engine.operators.OpExecConfig;
import edu.uci.ics.texera.workflow.common.metadata.OperatorGroupConstants;
import edu.uci.ics.texera.workflow.common.metadata.OperatorInfo;
import edu.uci.ics.texera.workflow.common.metadata.OutputPort;
import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorDescriptor;
import edu.uci.ics.texera.workflow.common.tuple.schema.Attribute;
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeType;
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema;
import scala.collection.immutable.List;

import java.io.*;
import java.nio.charset.Charset;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Collections.singletonList;
import static scala.collection.JavaConverters.asScalaBuffer;


public class LocalCsvFileScanOpDesc extends SourceOperatorDescriptor {

    @JsonProperty(value = "file path", required = true)
    @JsonPropertyDescription("local file path")
    public String filePath;

    @JsonProperty(value = "delimiter", defaultValue = ",")
    @JsonPropertyDescription("delimiter to separate each line into fields")
    public String delimiter;

    @JsonProperty(value = "header", defaultValue = "true")
    @JsonPropertyDescription("whether the CSV file contains a header line")
    public Boolean header;




    final int INFERROWS = 100;

    @Override
    public OpExecConfig operatorExecutor() {
        // fill in default values
        if (this.delimiter == null) {
            this.delimiter = ",";
        }
        if (this.header == null) {
            this.header = true;
        }

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
                List.empty(),
                asScalaBuffer(singletonList(new OutputPort(""))).toList());
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

        java.util.List<AttributeType> attributeTypeList = IntStream
                .range(0, headerLine.split(delimiter).length).mapToObj(i->AttributeType.INTEGER).collect(Collectors.toList());


        try(BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            if (header != null && header)
                reader.readLine();
            int i = 0;
            while((line = reader.readLine())!=null && i<=INFERROWS) {
                attributeTypeList = inferLine(attributeTypeList, line.split(delimiter));
                i++;
            }
        } catch (RuntimeException | IOException e) {
            e.printStackTrace();
        }
        final java.util.List<AttributeType> finalattributeTypeList = attributeTypeList;


        if (header != null && header) {


            return Schema.newBuilder().add(IntStream.range(0, headerLine.split(delimiter).length).
                    mapToObj(i -> new Attribute(headerLine.split(delimiter)[i], finalattributeTypeList.get(i)))
                    .collect(Collectors.toList())).build();
        } else {
            return Schema.newBuilder().add(IntStream.range(0, headerLine.split(delimiter).length).
                    mapToObj(i -> new Attribute("column" + i, finalattributeTypeList.get(i)))
                    .collect(Collectors.toList())).build();
        }
    }
    private java.util.List<AttributeType> inferLine (java.util.List<AttributeType> attributeTypeList, String[] tokens) {
        return IntStream.range(0, tokens.length).
                mapToObj(i->inferToken(attributeTypeList.get(i),tokens[i].toLowerCase())).collect(Collectors.toList());
    }

    private AttributeType inferToken (AttributeType attributeType, String token) {
        if (attributeType.getName().equals("string")) {
            return AttributeType.STRING;
        } else if (attributeType.getName().equals("boolean")) {
            return this.tryParseBoolean(token);
        } else if (attributeType.getName().equals("double")) {
            return this.tryParseDouble(token);
        } else if (attributeType.getName().equals("long")) {
            return this.tryParseLong(token);
        } else if (attributeType.getName().equals("integer")) {
            return this.tryParseInteger(token);
        }
        return AttributeType.STRING;

    }
    private AttributeType tryParseInteger (String token) {
        try {
            Integer.parseInt(token);
            return AttributeType.INTEGER;
        } catch (Exception e) {
            return tryParseLong(token);
        }
    }
    private AttributeType tryParseLong (String token) {
        try {
            Long.parseLong(token);
            return AttributeType.LONG;
        } catch (Exception e) {
            return tryParseDouble(token);
        }
    }
    private AttributeType tryParseDouble (String token) {
        try {
            Double.parseDouble(token);
            return AttributeType.DOUBLE;
        } catch (Exception e) {
            return tryParseBoolean(token);
        }
    }
    private AttributeType tryParseBoolean (String token) {
        if (token.equals("true") || token.equals("false") ||token.equals("1")||token.equals("0") ) {
            return AttributeType.BOOLEAN;
        } else {
            return AttributeType.STRING;
        }
    }

}
