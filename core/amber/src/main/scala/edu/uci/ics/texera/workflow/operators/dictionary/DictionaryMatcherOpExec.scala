package edu.uci.ics.texera.workflow.operators.dictionary


import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{OperatorSchemaInfo}

import scala.collection.{Iterator}
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
        if (dictionaryValues.contains(t.getField(this.opDesc.attribute))) {
          Iterator(t)
        } else {
          Iterator()
        }

      case Right(_) => Iterator()
    }
}


