package edu.uci.ics.texera.workflow.operators.dictionary

import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{AttributeType, OperatorSchemaInfo}

import scala.collection.Iterator
import scala.util.Either

class DictionaryMatcherOpExec(opDesc: DictionaryMatcherOpDesc, operatorSchemaInfo: OperatorSchemaInfo)
  extends OperatorExecutor {

  var dictionaryValues: List[String] = _

  override def open(): Unit = {
    dictionaryValues = this.opDesc.dictionary.split(",").toList
  }

  override def close(): Unit = {}

  override def processTexeraTuple(
                                   tuple: Either[Tuple, InputExhausted],
                                   input: LinkIdentity
                                 ): Iterator[Tuple] =
    tuple match {
      case Left(t) =>
          val result = Tuple
            .newBuilder(operatorSchemaInfo.outputSchema)
            .add(t).add(opDesc.resultAttribute, AttributeType.BOOLEAN, dictionaryValues.contains(t.getField(this.opDesc.attribute)))
            .build()
          Iterator(result)

      case Right(_) => Iterator()
    }
}


