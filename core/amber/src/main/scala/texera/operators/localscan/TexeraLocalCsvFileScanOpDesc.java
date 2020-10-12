package texera.operators.localscan;

import Engine.Common.Constants;
import Engine.Common.tuple.texera.schema.Schema;
import Engine.Operators.OpExecConfig;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import scala.collection.Seq;
import texera.common.schema.OperatorGroupConstants;
import texera.common.schema.TexeraOperatorDescription;
import texera.common.workflow.TexeraOperatorDescriptor;


public class TexeraLocalCsvFileScanOpDesc extends TexeraOperatorDescriptor {

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
    public OpExecConfig amberOperator() {
        return new TexeraLocalFileScanOpExecConfig(this.amberOperatorTag(), Constants.defaultNumWorkers(),
                filePath, delimiter.charAt(0), null, header != null && header);
    }

    @Override
    public TexeraOperatorDescription texeraOperatorDescription() {
        return new TexeraOperatorDescription(
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
