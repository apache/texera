package texera.operators.localscan;

import Engine.Common.Constants;
import Engine.Common.tuple.texera.schema.Attribute;
import Engine.Common.tuple.texera.schema.AttributeType;
import Engine.Common.tuple.texera.schema.Schema;
import Engine.Operators.OpExecConfig;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import org.apache.curator.shaded.com.google.common.io.Files;
import scala.collection.Seq;
import texera.common.metadata.OperatorGroupConstants;
import texera.common.metadata.TexeraOperatorInfo;
import texera.common.operators.TexeraOperatorDescriptor;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.stream.Collectors;


public class LocalCsvFileScanOpDesc extends TexeraOperatorDescriptor {

    @JsonProperty("file path")
    @JsonPropertyDescription("local file path")
    public String filePath;

    @JsonProperty("delimiter")
    @JsonPropertyDescription("delimiter to separate each line into fields")
    public String delimiter;

    @JsonProperty("header")
    @JsonPropertyDescription("whether the CSV file contains a header line")
    public Boolean header;

    @Override
    public OpExecConfig texeraOpExec() {
        try {
            Schema schema = null;
            if (header != null && header) {
                String header = Files.readFirstLine(new File(filePath), Charset.defaultCharset());
                schema = Schema.newBuilder().add(
                        Arrays.stream(header.split(delimiter)).map(c -> c.trim())
                        .map(c -> new Attribute(c, AttributeType.STRING)).collect(Collectors.toList())
                ).build();
            }
            return new LocalCsvFileScanOpExecConfig(this.amberOperatorTag(), Constants.defaultNumWorkers(),
                    filePath, delimiter.charAt(0), schema, header != null && header);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public TexeraOperatorInfo texeraOperatorInfo() {
        return new TexeraOperatorInfo(
                "File Scan",
                "Scan data from a local file",
                OperatorGroupConstants.SOURCE_GROUP(),
                0, 1);
    }

    @Override
    public Schema transformSchema(Seq<Schema> schemas) {
        return null;
    }

}
