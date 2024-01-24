package edu.uci.ics.texera.workflow.operators.sink.managed;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Preconditions;
import edu.uci.ics.amber.engine.architecture.deploysemantics.PhysicalOp;
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecInitInfo;
import edu.uci.ics.amber.engine.architecture.scheduling.config.OperatorConfig;
import edu.uci.ics.amber.engine.common.IOperatorExecutor;
import edu.uci.ics.amber.engine.common.virtualidentity.ExecutionIdentity;
import edu.uci.ics.amber.engine.common.virtualidentity.OperatorIdentity;
import edu.uci.ics.amber.engine.common.virtualidentity.WorkflowIdentity;
import edu.uci.ics.amber.engine.common.workflow.InputPort;
import edu.uci.ics.amber.engine.common.workflow.OutputPort;
import edu.uci.ics.amber.engine.common.workflow.PortIdentity;
import edu.uci.ics.texera.workflow.common.IncrementalOutputMode;
import edu.uci.ics.texera.workflow.common.ProgressiveUtils;
import edu.uci.ics.texera.workflow.common.metadata.OperatorGroupConstants;
import edu.uci.ics.texera.workflow.common.metadata.OperatorInfo;
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema;
import edu.uci.ics.texera.workflow.operators.sink.SinkOpDesc;
import edu.uci.ics.texera.workflow.operators.sink.storage.SinkStorageReader;
import scala.Option;
import scala.Tuple3;
import scala.collection.immutable.List;

import java.util.function.Function;

import static edu.uci.ics.texera.workflow.common.IncrementalOutputMode.SET_SNAPSHOT;
import static java.util.Collections.singletonList;
import static scala.collection.JavaConverters.asScalaBuffer;

public class ProgressiveSinkOpDesc extends SinkOpDesc {

    // use SET_SNAPSHOT as the default output mode
    // this will be set internally by the workflow compiler
    @JsonIgnore
    private IncrementalOutputMode outputMode = SET_SNAPSHOT;

    // whether this sink corresponds to a visualization result, default is no
    @JsonIgnore
    private Option<String> chartType = Option.empty();

    @JsonIgnore
    private SinkStorageReader storage = null;

    // corresponding upstream operator ID and output port, will be set by workflow compiler
    @JsonIgnore
    private Option<OperatorIdentity> upstreamId = Option.empty();

    @JsonIgnore
    private Option<Integer> upstreamPort = Option.empty();

    @Override
    public PhysicalOp getPhysicalOp(WorkflowIdentity workflowId, ExecutionIdentity executionId) {
        Schema inputSchema = this.inputPortToSchemaMapping().get(this.operatorInfo().inputPorts().head().id()).get();
        return PhysicalOp.localPhysicalOp(
                workflowId,
                executionId,
                operatorIdentifier(),
                OpExecInitInfo.apply(
                        (Function<Tuple3<Object, PhysicalOp, OperatorConfig>, IOperatorExecutor> & java.io.Serializable)
                                worker -> new ProgressiveSinkOpExec(outputMode, storage.getStorageWriter(), inputSchema)
                )
        )
                .withInputPorts(this.operatorInfo().inputPorts(), inputPortToSchemaMapping())
                .withOutputPorts(this.operatorInfo().outputPorts(), outputPortToSchemaMapping());
    }

    @Override
    public OperatorInfo operatorInfo() {
        return new OperatorInfo(
                "View Results",
                "View the results",
                OperatorGroupConstants.UTILITY_GROUP(),
                asScalaBuffer(singletonList(new InputPort(new PortIdentity(0, false), "", false, List.empty()))).toList(),
                asScalaBuffer(singletonList(new OutputPort(new PortIdentity(0, false), ""))).toList(),
                false,
                false,
                false,
                false);
    }

    @Override
    public Schema getOutputSchema(Schema[] schemas) {
        Preconditions.checkArgument(schemas.length == 1);
        Schema inputSchema = schemas[0];

        // SET_SNAPSHOT:
        if (this.outputMode.equals(SET_SNAPSHOT)) {
            if (inputSchema.containsAttribute(ProgressiveUtils.insertRetractFlagAttr().getName())) {
                // input is insert/retract delta: the flag column is removed in output
                return Schema.newBuilder().add(inputSchema)
                        .remove(ProgressiveUtils.insertRetractFlagAttr().getName()).build();
            } else {
                // input is insert-only delta: output schema is the same as input schema
                return inputSchema;
            }
        } else {
            // SET_DELTA: output schema is always the same as input schema
            return inputSchema;
        }
    }

    @JsonIgnore
    public IncrementalOutputMode getOutputMode() {
        return outputMode;
    }

    @JsonIgnore
    public void setOutputMode(IncrementalOutputMode outputMode) {
        this.outputMode = outputMode;
    }

    @JsonIgnore
    public Option<String> getChartType() {
        return this.chartType;
    }

    @JsonIgnore
    public void setChartType(String chartType) {
        this.chartType = Option.apply(chartType);
    }

    @JsonIgnore
    public void setStorage(SinkStorageReader storage) {
        this.storage = storage;
    }

    @JsonIgnore
    public SinkStorageReader getStorage() {
        return this.storage;
    }

    public Option<OperatorIdentity> getUpstreamId() {
        return upstreamId;
    }

    public void setUpstreamId(OperatorIdentity upstreamId) {
        this.upstreamId = Option.apply(upstreamId);
    }

    public Option<Integer> getUpstreamPort() {
        return upstreamPort;
    }

    public void setUpstreamPort(Integer upstreamPort) {
        this.upstreamPort = Option.apply(upstreamPort);
    }


}
