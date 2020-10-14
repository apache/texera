package edu.uci.ics.texera.workflow.operators.pythonUDF;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import edu.uci.ics.amber.engine.common.Constants;
import edu.uci.ics.amber.engine.operators.OpExecConfig;
import edu.uci.ics.texera.workflow.common.metadata.OperatorGroupConstants;
import edu.uci.ics.texera.workflow.common.metadata.OperatorInfo;
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor;
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.List;


public class PythonUDFOpDesc extends OperatorDescriptor {

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
    public OpExecConfig operatorExecutor() {
        return new PythonUDFMetadata(this.operatorIdentifier(), Constants.defaultNumWorkers(),
                this.pythonScriptFile,
                JavaConverters.asScalaIteratorConverter(this.inputColumns.iterator()).asScala().toBuffer(),
                JavaConverters.asScalaIteratorConverter(this.outputColumns.iterator()).asScala().toBuffer(),
                JavaConverters.asScalaIteratorConverter(this.outerFiles.iterator()).asScala().toBuffer(),
                this.batchSize);
    }

    @Override
    public OperatorInfo operatorInfo() {
        return new OperatorInfo(
                "Python UDF",
                "User-defined function operator in Python script",
                OperatorGroupConstants.UDF_GROUP(),
                1, 1);
    }

    @Override
    public Schema getOutputSchema(Schema[] schemas) {
        return null;
    }

}
