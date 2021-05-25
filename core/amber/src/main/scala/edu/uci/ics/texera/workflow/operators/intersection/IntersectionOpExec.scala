package edu.uci.ics.texera.workflow.operators.intersection

import com.google.common.base.Preconditions
import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple

import scala.collection.convert.ImplicitConversions.`mutableSet AsJavaSet`
import scala.collection.mutable

class IntersectionOpExec extends OperatorExecutor {
  private val hashMap: mutable.HashMap[LinkIdentity, mutable.LinkedHashSet[Tuple]] =
    new mutable.HashMap()
  override def processTexeraTuple(
      tuple: Either[Tuple, InputExhausted],
      input: LinkIdentity
  ): Iterator[Tuple] = {
    tuple match {
      case Left(t) =>
        if (!hashMap.contains(input)) {
          hashMap.put(input, mutable.LinkedHashSet())
        }
        hashMap(input) += t
        Iterator()

      case Right(_) =>
        Preconditions.checkArgument(hashMap.size > 1)
        hashMap.valuesIterator.reduce((set1, set2) => { set1.retainAll(set2); set1 }).iterator

    }
  }

  override def open(): Unit = {}

  override def close(): Unit = {}
}
