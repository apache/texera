package edu.uci.ics.texera.workflow.common.workflow

import akka.actor.ActorRef
import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.{
  ConditionalGlobalBreakpoint,
  CountGlobalBreakpoint
}
import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.amber.engine.common.ambermessage.ControllerMessage.PassBreakpointTo
import edu.uci.ics.amber.engine.common.ambertag.OperatorIdentifier
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema
import edu.uci.ics.texera.workflow.common.{ConstraintViolation, WorkflowContext}
import org.jgrapht.graph.{DefaultEdge, DirectedAcyclicGraph}

import scala.collection.{JavaConverters, mutable}

class WorkflowCompiler(val workflowInfo: WorkflowInfo, val context: WorkflowContext) {

  def init(): Unit = {
    this.workflowInfo.operators.foreach(initOperator)
  }

  def initOperator(operator: OperatorDescriptor): Unit = {
    operator.context = context
  }

  def validate: Map[String, Set[ConstraintViolation]] =
    this.workflowInfo.operators
      .map(o => {
        o.operatorID -> {
          o.validate().toSet
        }
      })
      .toMap
      .filter(pair => pair._2.nonEmpty)

  def amberWorkflow: Workflow = {
    val amberOperators: mutable.Map[OperatorIdentifier, OpExecConfig] = mutable.Map()
    workflowInfo.operators.foreach(o => {
      val amberOperator = o.operatorExecutor
      amberOperators.put(amberOperator.tag, amberOperator)
    })

    val outLinks: mutable.Map[OperatorIdentifier, mutable.Set[OperatorIdentifier]] = mutable.Map()
    workflowInfo.links.foreach(link => {
      val origin = OperatorIdentifier(this.context.workflowID, link.origin)
      val dest = OperatorIdentifier(this.context.workflowID, link.destination)
      val destSet = outLinks.getOrElse(origin, mutable.Set())
      destSet.add(dest)
      outLinks.update(origin, destSet)
    })

    val outLinksImmutableValue: mutable.Map[OperatorIdentifier, Set[OperatorIdentifier]] =
      mutable.Map()
    outLinks.foreach(entry => {
      outLinksImmutableValue.update(entry._1, entry._2.toSet)
    })
    val outLinksImmutable: Map[OperatorIdentifier, Set[OperatorIdentifier]] =
      outLinksImmutableValue.toMap

    new Workflow(amberOperators, outLinksImmutable)
  }

  def addBreakpoint(
      controller: ActorRef,
      operatorID: String,
      breakpoint: Breakpoint
  ): Unit = {
    val breakpointID = "breakpoint-" + operatorID
    breakpoint match {
      case conditionBp: ConditionBreakpoint =>
        val column = conditionBp.column
        val predicate: Tuple => Boolean = conditionBp.condition match {
          case BreakpointCondition.EQ =>
            tuple => {
              tuple.getField(column).toString.trim == conditionBp.value
            }
          case BreakpointCondition.LT =>
            tuple => tuple.getField(column).toString.trim < conditionBp.value
          case BreakpointCondition.LE =>
            tuple => tuple.getField(column).toString.trim <= conditionBp.value
          case BreakpointCondition.GT =>
            tuple => tuple.getField(column).toString.trim > conditionBp.value
          case BreakpointCondition.GE =>
            tuple => tuple.getField(column).toString.trim >= conditionBp.value
          case BreakpointCondition.NE =>
            tuple => tuple.getField(column).toString.trim != conditionBp.value
          case BreakpointCondition.CONTAINS =>
            tuple => tuple.getField(column).toString.trim.contains(conditionBp.value)
          case BreakpointCondition.NOT_CONTAINS =>
            tuple => !tuple.getField(column).toString.trim.contains(conditionBp.value)
        }
        controller ! PassBreakpointTo(
          operatorID,
          new ConditionalGlobalBreakpoint(
            breakpointID,
            tuple => {
              val texeraTuple = tuple.asInstanceOf[Tuple]
              predicate.apply(texeraTuple)
            }
          )
        )
      case countBp: CountBreakpoint =>
        controller ! PassBreakpointTo(
          operatorID,
          new CountGlobalBreakpoint("breakpointID", countBp.count)
        )
    }
  }

  def initializeBreakpoint(controller: ActorRef): Unit = {
    for (pair <- this.workflowInfo.breakpoints) {
      addBreakpoint(controller, pair.operatorID, pair.breakpoint)
    }
  }

  def propagateWorkflowSchema(): Map[OperatorDescriptor, Schema] = {
    // construct the edu.uci.ics.texera.workflow DAG object using jGraphT
    val workflowDag =
      new DirectedAcyclicGraph[OperatorDescriptor, DefaultEdge](classOf[DefaultEdge])
    this.workflowInfo.operators.foreach(op => workflowDag.addVertex(op))
    this.workflowInfo.links.foreach(link => {
      val origin = this.workflowInfo.operators.filter(op => op.operatorID == link.origin).head
      val destination =
        this.workflowInfo.operators.filter(op => op.operatorID == link.destination).head
      workflowDag.addEdge(origin, destination)
    })

    // a map from an operator to the list of its input schema
    val inputSchemaMap = new mutable.HashMap[OperatorDescriptor, List[Option[Schema]]]()

    // propagate output schema following topological order
    // TODO: introduce the concept of port in TexeraOperatorDescriptor and propagate schema according to port
    val topologicalOrderIterator = workflowDag.iterator()
    topologicalOrderIterator.forEachRemaining(op => {
      val outputSchema: Option[Schema] = {
        if (op.isInstanceOf[SourceOperatorDescriptor]) {
          // op is a source operator, ask for it output schema
          Option.apply(op.getOutputSchema(Array()))
        } else if (!inputSchemaMap.contains(op) || inputSchemaMap(op).exists(s => s.isEmpty)) {
          // op does not have input, or any of the op's input's output schema is null
          // then this op's output schema cannot be inferred as well
          Option.empty
        } else {
          // op's input schema is complete, try to infer its output schema
          // if inference failed, print an exception message, but still continue the process
          try {
            Option.apply(op.getOutputSchema(inputSchemaMap(op).map(s => s.get).toArray))
          } catch {
            case e: Throwable =>
              e.printStackTrace()
              Option.empty
          }
        }
      }
      JavaConverters
        .asScalaSet(workflowDag.outgoingEdgesOf(op))
        .map(e => workflowDag.getEdgeTarget(e))
        .foreach(downstream => {
          inputSchemaMap
            .put(downstream, inputSchemaMap.getOrElse(downstream, List()) :+ outputSchema)
        })
    })

    inputSchemaMap
      .filter(e => !(e._2.exists(s => s.isEmpty) || e._2.isEmpty))
      .map(e => (e._1, e._2.head.get))
      .toMap
  }

}
