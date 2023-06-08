package edu.uci.ics.texera.workflow.operators.udf.python.source;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.google.common.base.Preconditions;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle;
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecConfig;
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecFunc;
import edu.uci.ics.texera.workflow.common.metadata.InputPort;
import edu.uci.ics.texera.workflow.common.metadata.OperatorGroupConstants;
import edu.uci.ics.texera.workflow.common.metadata.OperatorInfo;
import edu.uci.ics.texera.workflow.common.metadata.OutputPort;
import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorDescriptor;
import edu.uci.ics.texera.workflow.common.tuple.schema.Attribute;
import edu.uci.ics.texera.workflow.common.tuple.schema.OperatorSchemaInfo;
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema;

import java.io.Serializable;
import java.util.List;

import static java.util.Collections.singletonList;
import static scala.collection.JavaConverters.asScalaBuffer;


public class PythonUDFSourceOpDescV2 extends SourceOperatorDescriptor {

    @JsonProperty(required = true, defaultValue =
            "# Choose from the following templates:\n" +
                    "# \n" +
                    "# from pytexera import *\n" +
                    "# \n" +
                    "# class GenerateOperator(UDFSourceOperator):\n" +
                    "# \n" +
                    "#     @overrides\n" +
                    "#     def produce(self) -> Iterator[Union[TupleLike, TableLike, None]]:\n" +
                    "#         yield\n")
    @JsonSchemaTitle("Python script")
    @JsonPropertyDescription("Input your code here")
    public String code;

    @JsonProperty(required = true)
    @JsonSchemaTitle("Worker count")
    @JsonPropertyDescription("Specify how many parallel workers to lunch")
    public Integer workers = 1;

    @JsonProperty()
    @JsonSchemaTitle("Columns")
    @JsonPropertyDescription("The columns of the source")
    public List<Attribute> columns;

    @Override
    public OpExecConfig operatorExecutor(OperatorSchemaInfo operatorSchemaInfo) {
        OpExecFunc exec = (OpExecFunc & Serializable) (i) ->
                new PythonUDFSourceOpExecV2(code, operatorSchemaInfo.outputSchemas()[0]);
        Preconditions.checkArgument(workers >= 1, "Need at least 1 worker.");
        if (workers > 1) {
            return OpExecConfig.oneToOneLayer(operatorIdentifier(), exec).withNumWorkers(workers)
                    .withIsOneToManyOp(true);
        } else {
            return OpExecConfig.manyToOneLayer(operatorIdentifier(), exec).withIsOneToManyOp(true);
        }

    }

    @Override
    public OperatorInfo operatorInfo() {
        return new OperatorInfo(
                "1-out Python UDF",
                "User-defined function operator in Python script",
                OperatorGroupConstants.UDF_GROUP(),
                scala.collection.immutable.List.<InputPort>empty(),
                asScalaBuffer(singletonList(new OutputPort(""))).toList(),
                false,
                false,
                true
        );
    }

    @Override
    public Schema sourceSchema() {
        Schema.Builder outputSchemaBuilder = Schema.newBuilder();

        // for any pythonUDFType, it can add custom output columns (attributes).
        if (columns != null) {
            outputSchemaBuilder.add(columns).build();
        }
        return outputSchemaBuilder.build();
    }
}
