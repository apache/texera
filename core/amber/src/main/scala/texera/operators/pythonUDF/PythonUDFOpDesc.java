package texera.operators.pythonUDF;

import Engine.Common.Constants;
import Engine.Common.tuple.texera.schema.Schema;
import Engine.Operators.OpExecConfig;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import texera.common.metadata.OperatorGroupConstants;
import texera.common.metadata.TexeraOperatorInfo;
import texera.common.workflow.TexeraOperatorDescriptor;

import java.util.List;


public class PythonUDFOpDesc extends TexeraOperatorDescriptor {

    @JsonProperty("Python script file")
    @JsonPropertyDescription("name of the UDF script file")
    public String pythonScriptFile;

    @JsonProperty("input column(s)")
    @JsonPropertyDescription("name of the input column(s) that the UDF will use, if any")
    public List<String> inputColumns;

    @JsonProperty("output column(s)")
    @JsonPropertyDescription("name of the newly added output columns that the UDF will produce, if any")
    public List<String> outputColumns;

    @JsonProperty("outer file(s)")
    @JsonPropertyDescription("name(s) of outer file(s) to be used, if any")
    public List<String> outerFiles;

    @JsonProperty("batch size")
    @JsonPropertyDescription("size of every batch of tuples to pass to python")
    public int batchSize;

    @Override
    public OpExecConfig texeraOpExec() {
        return new PythonUDFMetadata(this.amberOperatorTag(), Constants.defaultNumWorkers(),
                this.pythonScriptFile,
                JavaConverters.asScalaIteratorConverter(this.inputColumns.iterator()).asScala().toBuffer(),
                JavaConverters.asScalaIteratorConverter(this.outputColumns.iterator()).asScala().toBuffer(),
                JavaConverters.asScalaIteratorConverter(this.outerFiles.iterator()).asScala().toBuffer(),
                this.batchSize);
    }

    @Override
    public TexeraOperatorInfo texeraOperatorInfo() {
        return new TexeraOperatorInfo(
                "Python UDF",
                "User-defined function operator in Python script",
                OperatorGroupConstants.UDF_GROUP(),
                1, 1);
    }

    @Override
    public Schema transformSchema(Seq<Schema> schemas) {
        return null;
    }
}
