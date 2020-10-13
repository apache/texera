package engine.architecture.controller

import engine.common.ambertag.OperatorIdentifier
import engine.common.AmberUtils
import engine.operators.OpExecConfig

import scala.collection.{JavaConverters, mutable}

//object Workflow {
//  def apply(operators: java.util.Map[OperatorTag,OperatorMetadata],
//            outLinks:java.util.Map[OperatorTag, java.util.Set[OperatorTag]]): Workflow = {
//    val operatorsScala = JavaConverters.mapAsScalaMap(operators);
//    val outLinksScala = JavaConverters.mapAsScalaMap(outLinks).map(kv => (kv._1, JavaConverters.asScalaSet(kv._2).toSet)).toMap
//    new Workflow(operatorsScala, outLinksScala)
//  }
//
//}

class Workflow(
                val operators: mutable.Map[OperatorIdentifier, OpExecConfig],
                val outLinks: Map[OperatorIdentifier, Set[OperatorIdentifier]]
) {
  val inLinks: Map[OperatorIdentifier, Set[OperatorIdentifier]] = AmberUtils.reverseMultimap(outLinks)
  val startOperators: Iterable[OperatorIdentifier] = operators.keys.filter(!inLinks.contains(_))
  val endOperators: Iterable[OperatorIdentifier] = operators.keys.filter(!outLinks.contains(_))

  def getSources(operator: OperatorIdentifier): Set[OperatorIdentifier] = {
    var result = Set[OperatorIdentifier]()
    var current = Set[OperatorIdentifier](operator)
    while (current.nonEmpty) {
      var next = Set[OperatorIdentifier]()
      for (i <- current) {
        if (inLinks.contains(i) && inLinks(i).nonEmpty) {
          next ++= inLinks(i)
        } else {
          result += i
        }
        current = next
      }
    }
    result
  }
}
