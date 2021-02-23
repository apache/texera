package edu.uci.ics.texera.workflow.operators.pythonUDF;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import edu.uci.ics.amber.engine.common.Constants;
import edu.uci.ics.amber.engine.operators.OpExecConfig;
import edu.uci.ics.texera.workflow.common.metadata.InputPort;
import edu.uci.ics.texera.workflow.common.metadata.OperatorGroupConstants;
import edu.uci.ics.texera.workflow.common.metadata.OperatorInfo;
import edu.uci.ics.texera.workflow.common.metadata.OutputPort;
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor;
import edu.uci.ics.texera.workflow.common.tuple.schema.Attribute;
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema;
import scala.collection.JavaConverters;

import java.util.List;

import static java.util.Collections.singletonList;
import static scala.collection.JavaConverters.asScalaBuffer;


public class PythonUDFOpDesc extends OperatorDescriptor {

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
    public List<Attribute> outputColumns;

    @JsonProperty("outer file(s)")
    @JsonPropertyDescription("name(s) of outer file(s) to be used, if any")
    public List<String> outerFiles;

    @JsonProperty(value = "batch size", required = true, defaultValue = "100")
    @JsonPropertyDescription("size of every batch of tuples to pass to python")
    public int batchSize;

    @Override
    public OpExecConfig operatorExecutor() {
        return new PythonUDFOpExecConfig(this.operatorIdentifier(), Constants.defaultNumWorkers(),
                this.pythonScriptText,
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
                asScalaBuffer(singletonList(new InputPort("", false))).toList(),
                asScalaBuffer(singletonList(new OutputPort(""))).toList());
    }

    @Override
    public Schema getOutputSchema(Schema[] schemas) {
        Schema inputSchema = schemas[0];
        if (inputColumns != null && !inputColumns.isEmpty()) {
            for (String s : inputColumns) {
                if (!inputSchema.containsAttribute(s)) throw new RuntimeException("No such column:" + s);
            }
        }

        Schema outputSchema = inputSchema;
        if (outputColumns != null && !outputColumns.isEmpty()) {
            for (Attribute a : outputColumns) {
                if (inputSchema.containsAttribute(a.getName())) throw new RuntimeException("Column name " + a.getName()
                + "already exists!");
            }
            outputSchema = Schema.newBuilder().add(inputSchema).add(outputColumns).build();
        }
        return outputSchema;
    }

}
