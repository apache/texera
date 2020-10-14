package edu.uci.ics.texera.workflow.operators.sink;

import com.google.common.base.Preconditions;
import edu.uci.ics.amber.engine.operators.OpExecConfig;
import edu.uci.ics.texera.workflow.common.metadata.OperatorGroupConstants;
import edu.uci.ics.texera.workflow.common.metadata.OperatorInfo;
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor;
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema;
import scala.collection.Seq;

public class SimpleSinkOpDesc extends OperatorDescriptor {

    @Override
    public OpExecConfig operatorExecutor() {
        return new SimpleSinkOpExecConfig(this.operatorIdentifier());
    }

    @Override
    public OperatorInfo operatorInfo() {
        return new OperatorInfo(
                "View Results",
                "View the edu.uci.ics.texera.workflow results",
                OperatorGroupConstants.RESULT_GROUP(),
                1, 0);
    }

    @Override
    public Schema getOutputSchema(Schema[] schemas) {
        Preconditions.checkArgument(schemas.length == 1);
        return schemas[0];
    }

}
