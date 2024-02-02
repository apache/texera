package edu.uci.ics.texera.workflow.operators.visualization.wordCloud;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaInject;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaInt;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle;
import edu.uci.ics.amber.engine.architecture.deploysemantics.PhysicalOp;
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecInitInfo;
import edu.uci.ics.amber.engine.architecture.scheduling.config.OperatorConfig;
import edu.uci.ics.amber.engine.common.IOperatorExecutor;
import edu.uci.ics.amber.engine.common.virtualidentity.ExecutionIdentity;
import edu.uci.ics.amber.engine.common.virtualidentity.PhysicalOpIdentity;
import edu.uci.ics.amber.engine.common.virtualidentity.WorkflowIdentity;
import edu.uci.ics.amber.engine.common.workflow.PhysicalLink;
import edu.uci.ics.texera.workflow.common.ProgressiveUtils;
import edu.uci.ics.texera.workflow.common.metadata.OperatorGroupConstants;
import edu.uci.ics.texera.workflow.common.metadata.OperatorInfo;
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeName;
import edu.uci.ics.texera.workflow.common.tuple.schema.Attribute;
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeType;
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema;
import edu.uci.ics.texera.workflow.common.workflow.PhysicalPlan;
import edu.uci.ics.amber.engine.common.workflow.InputPort;
import edu.uci.ics.amber.engine.common.workflow.OutputPort;
import edu.uci.ics.amber.engine.common.workflow.PortIdentity;
import edu.uci.ics.texera.workflow.operators.visualization.VisualizationConstants;
import edu.uci.ics.texera.workflow.operators.visualization.VisualizationOperator;
import scala.Tuple3;
import scala.collection.immutable.List;

import java.util.HashMap;
import java.util.function.Function;

import static java.util.Collections.singletonList;
import static scala.collection.JavaConverters.asScalaBuffer;
import static scala.collection.JavaConverters.mapAsScalaMap;

/**
 * WordCloud is a visualization operator that can be used by the caller to generate data for wordcloud.js in frontend.
 * WordCloud returns tuples with word (String) and its font size (Integer) for frontend.
 */

public class WordCloudOpDesc extends VisualizationOperator {
    @JsonProperty(required = true)
    @JsonSchemaTitle("Text column")
    @AutofillAttributeName
    public String textColumn;

    @JsonProperty(defaultValue = "100")
    @JsonSchemaTitle("Number of most frequent words")
    @JsonSchemaInject(ints = {@JsonSchemaInt(path = "exclusiveMinimum", value = 0)})
    public Integer topN;

    @Override
    public String chartType() {
        return VisualizationConstants.WORD_CLOUD;
    }

    public static final Schema partialAggregateSchema = Schema.newBuilder().add(
            new Attribute("word", AttributeType.STRING),
            new Attribute("count", AttributeType.INTEGER)).build();

    public static final Schema finalInsertRetractSchema = Schema.newBuilder()
            .add(ProgressiveUtils.insertRetractFlagAttr())
            .add(partialAggregateSchema).build();

    @Override
    public PhysicalOp getPhysicalOp(WorkflowIdentity workflowId, ExecutionIdentity executionId) {
        throw new UnsupportedOperationException("opExec implemented in getPhysicalPlan");
    }

    @Override
    public PhysicalPlan getPhysicalPlan(WorkflowIdentity workflowId, ExecutionIdentity executionId) {
        if (topN == null) {
            topN = 100;
        }

        PhysicalOpIdentity partialOpId = new PhysicalOpIdentity(operatorIdentifier(), "partial");
        OutputPort partialOpOutputPort = new OutputPort(new PortIdentity(0, true), "");
        HashMap<PortIdentity, Schema> outputPortToSchemaMapping = new HashMap<>();
        outputPortToSchemaMapping.put(partialOpOutputPort.id(), outputPortToSchemaMapping().values().head());
        PhysicalOp partialPhysicalOp = PhysicalOp.oneToOnePhysicalOp(
                        workflowId,
                        executionId,
                        this.operatorIdentifier(),
                        OpExecInitInfo.apply(
                                (Function<Tuple3<Object, PhysicalOp, OperatorConfig>, IOperatorExecutor> & java.io.Serializable)
                                        worker -> new WordCloudOpPartialExec(textColumn)
                        )
                )
                .withId(partialOpId)
                .withIsOneToManyOp(true)
                .withParallelizable(false)
                .withInputPorts(operatorInfo().inputPorts(), inputPortToSchemaMapping())
                // assume partial op's output is the same as global op's
                .withOutputPorts(asScalaBuffer(singletonList(partialOpOutputPort)).toList(), mapAsScalaMap(outputPortToSchemaMapping));


        PhysicalOpIdentity globalOpId = new PhysicalOpIdentity(operatorIdentifier(), "global");
        InputPort globalOpInputPort = new InputPort(new PortIdentity(0, true), "", false, List.empty());
        HashMap<PortIdentity, Schema> inputPortToSchemaMapping = new HashMap<>();
        inputPortToSchemaMapping.put(globalOpInputPort.id(), outputPortToSchemaMapping().values().head());
        PhysicalOp globalPhysicalOp = PhysicalOp.manyToOnePhysicalOp(
                workflowId,
                executionId,
                this.operatorIdentifier(),
                OpExecInitInfo.apply(
                        (Function<Tuple3<Object, PhysicalOp, OperatorConfig>, IOperatorExecutor> & java.io.Serializable)
                                worker -> new WordCloudOpFinalExec(topN)
                )
        )
            .withId(globalOpId).withIsOneToManyOp(true)
            // assume partial op's output is the same as global op's
            .withInputPorts(asScalaBuffer(singletonList(globalOpInputPort)).toList(), mapAsScalaMap(inputPortToSchemaMapping))
            .withOutputPorts(operatorInfo().outputPorts(), outputPortToSchemaMapping());

        PhysicalOp[] physicalOps = {partialPhysicalOp, globalPhysicalOp};
        PhysicalLink[] links = {new PhysicalLink(partialPhysicalOp.id(), partialOpOutputPort.id(), globalPhysicalOp.id(), globalOpInputPort.id())};

        return PhysicalPlan.apply(physicalOps, links);
    }

    @Override
    public OperatorInfo operatorInfo() {
        return new OperatorInfo("Word Cloud",
                "Generate word cloud for result texts",
                OperatorGroupConstants.VISUALIZATION_GROUP(),
                asScalaBuffer(singletonList(new InputPort(new PortIdentity(0, false), "", false, List.empty()))).toList(),
                asScalaBuffer(singletonList(new OutputPort(new PortIdentity(0, false), ""))).toList(),
                false,
                false,
                false,
                false
        );
    }

    @Override
    public Schema getOutputSchema(Schema[] schemas) {
        return finalInsertRetractSchema;
    }
}
