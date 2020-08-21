package Engine.Architecture.Controller

import Engine.Common.AmberTag.OperatorTag
import Engine.Common.AmberUtils
import Engine.Operators.OperatorMetadata

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

class Workflow(val operators:mutable.Map[OperatorTag,OperatorMetadata],val outLinks:Map[OperatorTag,Set[OperatorTag]]) {
  val inLinks: Map[OperatorTag, Set[OperatorTag]] = AmberUtils.reverseMultimap(outLinks)
  val startOperators: Iterable[OperatorTag] = operators.keys.filter(!inLinks.contains(_))
  val endOperators: Iterable[OperatorTag] = operators.keys.filter(!outLinks.contains(_))

  def getSources(operator: OperatorTag): Set[OperatorTag] = {
    var result = Set[OperatorTag]()
    var current = Set[OperatorTag](operator)
    while(current.nonEmpty){
      var next = Set[OperatorTag]()
      for(i <- current){
        if(inLinks.contains(i) && inLinks(i).nonEmpty){
          next ++= inLinks(i)
        }else{
          result += i
        }
        current = next
      }
    }
    result
  }
}
