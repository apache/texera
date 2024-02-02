package edu.uci.ics.texera.workflow.operators.filter;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import edu.uci.ics.amber.engine.architecture.deploysemantics.PhysicalOp;
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecInitInfo;
import edu.uci.ics.amber.engine.architecture.scheduling.config.OperatorConfig;
import edu.uci.ics.amber.engine.common.IOperatorExecutor;
import edu.uci.ics.amber.engine.common.virtualidentity.ExecutionIdentity;
import edu.uci.ics.amber.engine.common.virtualidentity.WorkflowIdentity;
import edu.uci.ics.amber.engine.common.workflow.InputPort;
import edu.uci.ics.amber.engine.common.workflow.OutputPort;
import edu.uci.ics.amber.engine.common.workflow.PortIdentity;
import edu.uci.ics.texera.workflow.common.metadata.OperatorGroupConstants;
import edu.uci.ics.texera.workflow.common.metadata.OperatorInfo;
import edu.uci.ics.texera.workflow.common.operators.filter.FilterOpDesc;

import scala.Tuple3;
import scala.collection.immutable.Seq;


import java.util.ArrayList;
import java.util.function.Function;

import static java.util.Collections.singletonList;
import static scala.collection.JavaConverters.*;
import static scala.jdk.javaapi.CollectionConverters.asScala;

public class SpecializedFilterOpDesc extends FilterOpDesc {

    @JsonProperty(value = "predicates", required = true)
    @JsonPropertyDescription("multiple predicates in OR")
    public java.util.List<FilterPredicate> predicates;

    @Override
    public PhysicalOp getPhysicalOp(WorkflowIdentity workflowId, ExecutionIdentity executionId) {
        return PhysicalOp.oneToOnePhysicalOp(
                        workflowId,
                        executionId,
                        operatorIdentifier(),
                        OpExecInitInfo.apply(
                                (Function<Tuple3<Object, PhysicalOp, OperatorConfig>, IOperatorExecutor> & java.io.Serializable)
                                        x -> new SpecializedFilterOpExec(this)
                        )
                )
                .withInputPorts(operatorInfo().inputPorts(), inputPortToSchemaMapping())
                .withOutputPorts(operatorInfo().outputPorts(), outputPortToSchemaMapping());
    }

    @Override
    public OperatorInfo operatorInfo() {
        return new OperatorInfo(
                "Filter",
                "Performs a filter operation",
                OperatorGroupConstants.SEARCH_GROUP(),
                asScala(singletonList(new InputPort(new PortIdentity(0, false), "", false, asScala(new ArrayList<PortIdentity>()).toSeq()))).toList(),
                asScala(singletonList(new OutputPort(new PortIdentity(0, false), ""))).toList(),
                false,
                false,
                true,
                false
        );
    }
}
