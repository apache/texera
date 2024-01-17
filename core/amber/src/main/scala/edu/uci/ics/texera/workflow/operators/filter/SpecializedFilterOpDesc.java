package edu.uci.ics.texera.workflow.operators.filter;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import edu.uci.ics.amber.engine.architecture.deploysemantics.PhysicalOp;
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecInitInfo;
import edu.uci.ics.amber.engine.architecture.scheduling.config.OperatorConfig;
import edu.uci.ics.amber.engine.common.IOperatorExecutor;
import edu.uci.ics.amber.engine.common.virtualidentity.ExecutionIdentity;
import edu.uci.ics.amber.engine.common.virtualidentity.WorkflowIdentity;
import edu.uci.ics.texera.workflow.common.metadata.InputPort;
import edu.uci.ics.texera.workflow.common.metadata.OperatorGroupConstants;
import edu.uci.ics.texera.workflow.common.metadata.OperatorInfo;
import edu.uci.ics.texera.workflow.common.metadata.OutputPort;
import edu.uci.ics.texera.workflow.common.operators.filter.FilterOpDesc;
import edu.uci.ics.texera.workflow.common.tuple.schema.OperatorSchemaInfo;
import scala.Tuple3;

import java.util.List;
import java.util.function.Function;

import static java.util.Collections.singletonList;
import static scala.collection.JavaConverters.asScalaBuffer;

public class SpecializedFilterOpDesc extends FilterOpDesc {

    @JsonProperty(value = "predicates", required = true)
    @JsonPropertyDescription("multiple predicates in OR")
    public List<FilterPredicate> predicates;

    @Override
    public PhysicalOp getPhysicalOp(WorkflowIdentity workflowId, ExecutionIdentity executionId, OperatorSchemaInfo operatorSchemaInfo) {
        return PhysicalOp.oneToOnePhysicalOp(
                workflowId,
                executionId,
                operatorIdentifier(),
                OpExecInitInfo.apply(
                        (Function<Tuple3<Object, PhysicalOp, OperatorConfig>, IOperatorExecutor> & java.io.Serializable)
                                x -> new SpecializedFilterOpExec(this)
                )
        );
    }

    @Override
    public OperatorInfo operatorInfo() {
        return new OperatorInfo(
                "Filter",
                "Performs a filter operation",
                OperatorGroupConstants.SEARCH_GROUP(),
                asScalaBuffer(singletonList(new InputPort("", false))).toList(),
                asScalaBuffer(singletonList(new OutputPort(""))).toList(),
                false, false, true, false);
    }
}
