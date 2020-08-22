package texera.operators.hdfsscan;

import Engine.Common.Constants;
import Engine.Operators.OperatorMetadata;
import Engine.Operators.Scan.HDFSFileScan.HDFSFileScanMetadata;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import texera.common.schema.OperatorGroupConstants;
import texera.common.schema.TexeraOperatorDescription;
import texera.common.workflow.TexeraOperator;

public class TexeraHdfsFileScan extends TexeraOperator {

    @JsonProperty("HDFS host")
    @JsonPropertyDescription("HDFS host URL and port")
    public String hdfsHost;

    @JsonProperty("file path")
    @JsonPropertyDescription("HDFS file path")
    public String filePath;

    @JsonProperty("delimiter")
    @JsonPropertyDescription("delimiter to separate each line into fields")
    public String delimiter;

    @Override
    public OperatorMetadata amberOperator() {
        return new HDFSFileScanMetadata(this.amberOperatorTag(), Constants.defaultNumWorkers(),
                hdfsHost, filePath, delimiter.charAt(0), null, null);
    }

    @Override
    public TexeraOperatorDescription texeraOperatorDescription() {
        return new TexeraOperatorDescription(
                "HDFS File Scan",
                "Scan data from a remote HDFS cluster",
                OperatorGroupConstants.SOURCE_GROUP(),
                0, 1);
    }
}
