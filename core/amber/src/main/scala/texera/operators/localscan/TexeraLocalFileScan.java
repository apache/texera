package texera.operators.localscan;

import Engine.Common.Constants;
import Engine.Operators.OperatorMetadata;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import texera.common.schema.OperatorGroupConstants;
import texera.common.schema.TexeraOperatorDescription;
import texera.common.workflow.TexeraOperator;


public class TexeraLocalFileScan extends TexeraOperator {

    @JsonProperty("file path")
    @JsonPropertyDescription("local file path")
    public String filePath;

    @JsonProperty("delimiter")
    @JsonPropertyDescription("delimiter to separate each line into fields")
    public String delimiter;

    @Override
    public OperatorMetadata amberOperator() {
        return new TexeraLocalFileScanMetadata(this.amberOperatorTag(), Constants.defaultNumWorkers(),
                filePath, delimiter.charAt(0), null, null);
    }

    @Override
    public TexeraOperatorDescription texeraOperatorDescription() {
        return new TexeraOperatorDescription(
                "File Scan",
                "Scan data from a local file",
                OperatorGroupConstants.SOURCE_GROUP(),
                0, 1);
    }

}
