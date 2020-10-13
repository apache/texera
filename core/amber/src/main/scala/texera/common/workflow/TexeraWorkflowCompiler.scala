package texera.common.workflow

import Engine.Architecture.Breakpoint.GlobalBreakpoint.{ConditionalGlobalBreakpoint, CountGlobalBreakpoint}
import Engine.Architecture.Controller.Workflow
import Engine.Common.AmberMessage.ControllerMessage.PassBreakpointTo
import Engine.Common.AmberTag.OperatorIdentifier
import Engine.Operators.OpExecConfig
import akka.actor.ActorRef
import org.jgrapht.graph.{DefaultEdge, DirectedAcyclicGraph}
import texera.common.operators.TexeraOperatorDescriptor
import texera.common.operators.source.TexeraSourceOperatorDescriptor
import texera.common.tuple.TexeraTuple
import texera.common.tuple.schema.Schema
import texera.common.{TexeraConstraintViolation, TexeraContext}

import scala.collection.{JavaConverters, mutable}

class TexeraWorkflowCompiler(val texeraWorkflow: TexeraWorkflow, val context: TexeraContext) {

  def init(): Unit = {
    this.texeraWorkflow.operators.foreach(initOperator)
  }

  def initOperator(operator: TexeraOperatorDescriptor): Unit = {
    operator.context = context
  }

  def validate: Map[String, Set[TexeraConstraintViolation]] =
    this.texeraWorkflow.operators
      .map(o => {
        o.operatorID -> {
          o.validate().toSet
        }
      })
      .toMap
      .filter(pair => pair._2.nonEmpty)

  def amberWorkflow: Workflow = {
    val amberOperators: mutable.Map[OperatorIdentifier, OpExecConfig] = mutable.Map()
    texeraWorkflow.operators.foreach(o => {
      val amberOperator = o.texeraOperatorExecutor
      amberOperators.put(amberOperator.tag, amberOperator)
    })

    val outLinks: mutable.Map[OperatorIdentifier, mutable.Set[OperatorIdentifier]] = mutable.Map()
    texeraWorkflow.links.foreach(link => {
      val origin = OperatorIdentifier(this.context.workflowID, link.origin)
      val dest = OperatorIdentifier(this.context.workflowID, link.destination)
      val destSet = outLinks.getOrElse(origin, mutable.Set())
      destSet.add(dest)
      outLinks.update(origin, destSet)
    })

    val outLinksImmutableValue: mutable.Map[OperatorIdentifier, Set[OperatorIdentifier]] = mutable.Map()
    outLinks.foreach(entry => {
      outLinksImmutableValue.update(entry._1, entry._2.toSet)
    })
    val outLinksImmutable: Map[OperatorIdentifier, Set[OperatorIdentifier]] = outLinksImmutableValue.toMap

    new Workflow(amberOperators, outLinksImmutable)
  }

  def addBreakpoint(
      controller: ActorRef,
      operatorID: String,
      breakpoint: TexeraBreakpoint
  ): Unit = {
    val breakpointID = "breakpoint-" + operatorID
    breakpoint match {
      case conditionBp: TexeraConditionBreakpoint =>
        val column = conditionBp.column
        val predicate: TexeraTuple => Boolean = conditionBp.condition match {
          case TexeraBreakpointCondition.EQ =>
            tuple => {
              tuple.getField(column).toString.trim == conditionBp.value
            }
          case TexeraBreakpointCondition.LT =>
            tuple => tuple.getField(column).toString.trim < conditionBp.value
          case TexeraBreakpointCondition.LE =>
            tuple => tuple.getField(column).toString.trim <= conditionBp.value
          case TexeraBreakpointCondition.GT =>
            tuple => tuple.getField(column).toString.trim > conditionBp.value
          case TexeraBreakpointCondition.GE =>
            tuple => tuple.getField(column).toString.trim >= conditionBp.value
          case TexeraBreakpointCondition.NE =>
            tuple => tuple.getField(column).toString.trim != conditionBp.value
          case TexeraBreakpointCondition.CONTAINS =>
            tuple => tuple.getField(column).toString.trim.contains(conditionBp.value)
          case TexeraBreakpointCondition.NOT_CONTAINS =>
            tuple => !tuple.getField(column).toString.trim.contains(conditionBp.value)
        }
        controller ! PassBreakpointTo(
          operatorID,
          new ConditionalGlobalBreakpoint(
            breakpointID,
            tuple => {
              val texeraTuple = tuple.asInstanceOf[TexeraTuple]
              predicate.apply(texeraTuple)
            }
          )
        )
      case countBp: TexeraCountBreakpoint =>
        controller ! PassBreakpointTo(
          operatorID,
          new CountGlobalBreakpoint("breakpointID", countBp.count)
        )
    }
  }

  def initializeBreakpoint(controller: ActorRef): Unit = {
    for (pair <- this.texeraWorkflow.breakpoints) {
      addBreakpoint(controller, pair.operatorID, pair.breakpoint)
    }
  }

  def propagateWorkflowSchema(): Map[TexeraOperatorDescriptor, Schema] = {
    // construct the workflow DAG object using jGraphT
    val workflowDag = new DirectedAcyclicGraph[TexeraOperatorDescriptor, DefaultEdge](classOf[DefaultEdge])
    this.texeraWorkflow.operators.foreach(op => workflowDag.addVertex(op))
    this.texeraWorkflow.links.foreach(link => {
      val origin = this.texeraWorkflow.operators.filter(op => op.operatorID == link.origin).head
      val destination = this.texeraWorkflow.operators.filter(op => op.operatorID == link.destination).head
      workflowDag.addEdge(origin, destination)
    })

    // a map from an operator to its output schema
    val outputSchemaMap = new mutable.HashMap[TexeraOperatorDescriptor, Option[Schema]]()
    // a map from an operator to the list of its input schema
    val inputSchemaMap = new mutable.HashMap[TexeraOperatorDescriptor, List[Option[Schema]]]()

    // propagate output schema following topological order
    // TODO: introduce the concept of port in TexeraOperatorDescriptor and propagate schema according to port
    val topologicalOrderIterator = workflowDag.iterator()
    topologicalOrderIterator.forEachRemaining(op => {
      val outputSchema: Option[Schema] =
        if (op.isInstanceOf[TexeraSourceOperatorDescriptor]) {
          Option.apply(op.transformSchema())
        } else if (inputSchemaMap(op).exists(s => s.isEmpty)) {
          Option.empty
        } else {
          try {
            Option.apply(op.transformSchema(inputSchemaMap(op).map(s => s.get).toArray: _*))
          } catch {
            case e: Throwable =>
              e.printStackTrace()
              Option.empty
          }
        }
      outputSchemaMap(op) = outputSchema
      JavaConverters.asScalaSet(workflowDag.outgoingEdgesOf(op))
        .map(e => workflowDag.getEdgeTarget(e)).foreach(downstream => {
          inputSchemaMap.put(downstream, inputSchemaMap.getOrElse(downstream, List()) :+ outputSchema)
      })
    })

    inputSchemaMap
      .filter(e => ! (e._2.exists(s => s.isEmpty) || e._2.isEmpty))
      .map(e => (e._1, e._2.head.get))
      .toMap
  }

}
