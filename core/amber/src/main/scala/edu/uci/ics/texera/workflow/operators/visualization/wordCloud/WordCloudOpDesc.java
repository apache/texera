package edu.uci.ics.texera.workflow.operators.visualization.wordCloud;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaInject;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaInt;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle;
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecConfig;
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecInitInfo;
import edu.uci.ics.amber.engine.common.IOperatorExecutor;
import edu.uci.ics.amber.engine.common.virtualidentity.LayerIdentity;
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity;
import edu.uci.ics.amber.engine.common.virtualidentity.util;
import edu.uci.ics.texera.workflow.common.ProgressiveUtils;
import edu.uci.ics.texera.workflow.common.metadata.InputPort;
import edu.uci.ics.texera.workflow.common.metadata.OperatorGroupConstants;
import edu.uci.ics.texera.workflow.common.metadata.OperatorInfo;
import edu.uci.ics.texera.workflow.common.metadata.OutputPort;
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeName;
import edu.uci.ics.texera.workflow.common.tuple.schema.Attribute;
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeType;
import edu.uci.ics.texera.workflow.common.tuple.schema.OperatorSchemaInfo;
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema;
import edu.uci.ics.texera.workflow.common.workflow.PhysicalPlan;
import edu.uci.ics.texera.workflow.operators.visualization.VisualizationConstants;
import edu.uci.ics.texera.workflow.operators.visualization.VisualizationOperator;
import scala.Tuple2;
import scala.util.Left;

import java.io.Serializable;
import java.util.function.Function;

import static java.util.Collections.singletonList;
import static scala.collection.JavaConverters.asScalaBuffer;

/**
 * WordCloud is a visualization operator that can be used by the caller to generate data for wordcloud.js in frontend.
 * WordCloud returns tuples with word (String) and its font size (Integer) for frontend.
 *
 * @author Mingji Han, Xiaozhen Liu
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
    public OpExecConfig operatorExecutor(OperatorSchemaInfo operatorSchemaInfo) {
        throw new UnsupportedOperationException("opExec implemented in operatorExecutorMultiLayer");
    }

    @Override
    public PhysicalPlan operatorExecutorMultiLayer(OperatorSchemaInfo operatorSchemaInfo) {
        if (topN == null) {
            topN = 100;
        }

        LayerIdentity partialId = util.makeLayer(operatorIdentifier(), "partial");
        OpExecConfig partialLayer = OpExecConfig.oneToOneLayer(
                this.operatorIdentifier(),
                OpExecInitInfo.apply((Function<Tuple2<Object, OpExecConfig>, IOperatorExecutor> & java.io.Serializable)worker -> new WordCloudOpPartialExec(textColumn))
        ).withId(partialId).withIsOneToManyOp(true).withNumWorkers(1).withOutputPorts(
                asScalaBuffer(singletonList(new OutputPort("internal-output"))).toList());


        LayerIdentity finalId = util.makeLayer(operatorIdentifier(), "global");
        OpExecConfig finalLayer = OpExecConfig.manyToOneLayer(
                this.operatorIdentifier(),
                        OpExecInitInfo.apply((Function<Tuple2<Object, OpExecConfig>, IOperatorExecutor> & java.io.Serializable)worker -> new WordCloudOpFinalExec(topN))
        ).withId(finalId).withIsOneToManyOp(true)
                .withInputPorts(asScalaBuffer(singletonList(new InputPort("internal-input", false))).toList());

        OpExecConfig[] layers = {partialLayer, finalLayer};
        LinkIdentity[] links = {new LinkIdentity(partialLayer.id(), 0, finalLayer.id(), 0)};

        return PhysicalPlan.apply(layers, links);
    }

    @Override
    public OperatorInfo operatorInfo() {
        return new OperatorInfo("Word Cloud",
                "Generate word cloud for result texts",
                OperatorGroupConstants.VISUALIZATION_GROUP(),
                asScalaBuffer(singletonList(new InputPort("", false))).toList(),
                asScalaBuffer(singletonList(new OutputPort(""))).toList(),
                false, false, false, false);
    }

    @Override
    public Schema getOutputSchema(Schema[] schemas) {
        return finalInsertRetractSchema;
    }
}
