package edu.uci.ics.texera.workflow.operators.localscan;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.google.common.io.Files;
import edu.uci.ics.amber.engine.common.Constants;
import edu.uci.ics.amber.engine.operators.OpExecConfig;
import edu.uci.ics.texera.workflow.common.metadata.OperatorGroupConstants;
import edu.uci.ics.texera.workflow.common.metadata.OperatorInfo;
import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorDescriptor;
import edu.uci.ics.texera.workflow.common.tuple.schema.Attribute;
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeType;
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema;

import java.io.*;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


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
                0, 1, false);
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
        List<AttributeType> attributeType = IntStream
                .range(0, headerLine.split(delimiter).length).mapToObj(i->AttributeType.INTEGER).collect(Collectors.toList());


        try(BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            if (header != null && header)
                reader.readLine();
            int i = 0;
            while((line = reader.readLine())!=null && i<=100) {
                String[] records = line.split((delimiter));
                attributeType = inferline(attributeType, records);
                i++;
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        List<AttributeType> finalAttributeType = attributeType;
        System.out.println("a->"+finalAttributeType);
//        System.out.println(Schema.newBuilder().add(IntStream.range(0, headerLine.split(delimiter).length).
//                mapToObj(i -> new Attribute(l.get(i), finalAttributeType.get(i)))
//                .collect(Collectors.toList())).build());

        if (delimiter == null) {
            return null;
        }
        if (header != null && header) {


            return Schema.newBuilder().add(IntStream.range(0, headerLine.split(delimiter).length).
                    mapToObj(i -> new Attribute(headerLine.split(delimiter)[i], finalAttributeType.get(i)))
                    .collect(Collectors.toList())).build();
//            return Schema.newBuilder().add(Arrays.stream(headerLine.split(delimiter)).map(c -> c.trim())
//                    .map(c -> new Attribute(c, AttributeType.STRING)).collect(Collectors.toList())).build();
        } else {
            return Schema.newBuilder().add(IntStream.range(0, headerLine.split(delimiter).length).
                    mapToObj(i -> new Attribute("column" + i, finalAttributeType.get(i)))
                    .collect(Collectors.toList())).build();
        }
    }
    private List<AttributeType> inferline (List<AttributeType> attributeType, String[] line) {
        return IntStream.range(0, line.length).
                mapToObj(i->inferChar(attributeType.get(i),line[i])).collect(Collectors.toList());
    }

    private AttributeType inferChar (AttributeType attributeType, String c) {
        if (attributeType.getName().equals("string")) {
            return AttributeType.STRING;
        } else if (attributeType.getName().equals("boolean")) {
            try {
                Integer.parseInt(c);
                return AttributeType.BOOLEAN;
            } catch (Exception e1) {
                try {
                    Double.parseDouble(c);
                    return AttributeType.BOOLEAN;
                } catch (Exception e2) {
                    if (c.equals("TRUE") || c.equals("FALSE"))  {
                        return AttributeType.BOOLEAN;
                    } else {
                        return AttributeType.STRING;
                    }
                }
            }
        } else if (attributeType.getName().equals("double")) {
            try {
                Double.parseDouble(c);
                return AttributeType.DOUBLE;
            } catch (Exception e2) {
                if (c.equals("TRUE") || c.equals("FALSE"))  {
                    return AttributeType.BOOLEAN;
                } else {
                    return AttributeType.STRING;
                }
            }
        } else if (attributeType.getName().equals("integer")) {
            try {
                Integer.parseInt(c);
                return AttributeType.INTEGER;
            } catch (Exception e1) {
                try {
                    Double.parseDouble(c);
                    return AttributeType.DOUBLE;
                } catch (Exception e2) {
                    if (c.equals("TRUE") || c.equals("FALSE"))  {
                        return AttributeType.BOOLEAN;
                    } else {
                        return AttributeType.STRING;
                    }
                }
            }
        }
        return AttributeType.STRING;

    }

}
