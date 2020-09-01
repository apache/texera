package texera.operators.pythonUDF;

import Engine.Common.Constants;
import Engine.Operators.OperatorMetadata;
import Engine.Operators.PythonUDF.PythonUDFMetadata;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import scala.collection.JavaConverters;
import texera.common.schema.OperatorGroupConstants;
import texera.common.schema.TexeraOperatorDescription;
import texera.common.workflow.TexeraOperator;

import java.util.List;


public class TexeraPythonUDF extends TexeraOperator {

    @JsonProperty("Python script")
    @JsonPropertyDescription("input your code here")
    public String pythonScriptText;

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
    public OperatorMetadata amberOperator() {
        return new PythonUDFMetadata(this.amberOperatorTag(), Constants.defaultNumWorkers(),
                this.pythonScriptText,
                this.pythonScriptFile,
                JavaConverters.asScalaIteratorConverter(this.inputColumns.iterator()).asScala().toBuffer(),
                JavaConverters.asScalaIteratorConverter(this.outputColumns.iterator()).asScala().toBuffer(),
                JavaConverters.asScalaIteratorConverter(this.outerFiles.iterator()).asScala().toBuffer(),
                this.batchSize);
    }

    @Override
    public TexeraOperatorDescription texeraOperatorDescription() {
        return new TexeraOperatorDescription(
                "Python UDF",
                "User-defined function operator in Python script",
                OperatorGroupConstants.UDF_GROUP(),
                1, 1);
    }
}
