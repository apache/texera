package edu.uci.ics.texera.workflow.common.workflow

import akka.actor.ActorRef
import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.amber.engine.common.virtualidentity.{LinkIdentity, OperatorIdentity}
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.{ConstraintViolation, WorkflowContext}
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{OperatorSchemaInfo, Schema}
import edu.uci.ics.texera.workflow.operators.sink.SimpleSinkOpDesc
import edu.uci.ics.texera.workflow.operators.visualization.VisualizationOperator
import org.jgrapht.graph.{DefaultEdge, DirectedAcyclicGraph}

import scala.collection.mutable

object WorkflowCompiler {

  def isSink(operatorID: String, workflowCompiler: WorkflowCompiler): Boolean = {
    val outLinks =
      workflowCompiler.workflowInfo.links.filter(link => link.origin.operatorID == operatorID)
    outLinks.isEmpty
  }

  def getUpstreamOperators(
      operatorID: String,
      workflowCompiler: WorkflowCompiler
  ): List[OperatorDescriptor] = {
    workflowCompiler.workflowInfo.links
      .filter(link => link.destination.operatorID == operatorID)
      .flatMap(link =>
        workflowCompiler.workflowInfo.operators.filter(o => o.operatorID == link.origin.operatorID)
      )
      .toList
  }

}

class WorkflowCompiler(val workflowInfo: WorkflowInfo, val context: WorkflowContext) {

  val workflow = new WorkflowDAG(workflowInfo)

  init()

  def init(): Unit = {
    this.workflowInfo.operators.foreach(initOperator)
  }

  def initOperator(operator: OperatorDescriptor): Unit = {
    operator.setContext(context)
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
    // pre-process: set output mode for sink based on the visualization operator before it
    this.workflow.getSinkOperators.foreach(sinkOpId => {
      val sinkOp = this.workflow.getOperator(sinkOpId)
      val upstream = this.workflow.getUpstream(sinkOpId)
      (upstream.head, sinkOp) match {
        // match the combination of a visualization operator followed by a sink operator
        case (viz: VisualizationOperator, sink: SimpleSinkOpDesc) =>
          sink.setOutputMode(viz.outputMode())
          sink.setChartType(viz.chartType())
        case _ =>
      }
    })

    val inputSchemaMap = propagateWorkflowSchema()
    val amberOperators: mutable.Map[OperatorIdentity, OpExecConfig] = mutable.Map()
    workflowInfo.operators.foreach(o => {
      val inputSchemas = inputSchemaMap(o).map(s => s.get).toArray
      val outputSchema =
        if (o.isInstanceOf[SourceOperatorDescriptor]) o.getOutputSchema(Array())
        else o.getOutputSchema(inputSchemas)
      val amberOperator: OpExecConfig =
        o.operatorExecutor(OperatorSchemaInfo(inputSchemas, outputSchema))
      amberOperators.put(amberOperator.id, amberOperator)
    })

    val outLinks: mutable.Map[OperatorIdentity, mutable.Set[OperatorIdentity]] = mutable.Map()
    workflowInfo.links.foreach(link => {
      val origin = OperatorIdentity(this.context.jobID, link.origin.operatorID)
      val dest = OperatorIdentity(this.context.jobID, link.destination.operatorID)
      val destSet = outLinks.getOrElse(origin, mutable.Set())
      destSet.add(dest)
      outLinks.update(origin, destSet)
      val layerLink = LinkIdentity(
        Option(amberOperators(origin).topology.layers.last.id),
        Option(amberOperators(dest).topology.layers.head.id)
      )
      amberOperators(dest).setInputToOrdinalMapping(layerLink, link.destination.portOrdinal)
    })

    val outLinksImmutableValue: mutable.Map[OperatorIdentity, Set[OperatorIdentity]] =
      mutable.Map()
    outLinks.foreach(entry => {
      outLinksImmutableValue.update(entry._1, entry._2.toSet)
    })
    val outLinksImmutable: Map[OperatorIdentity, Set[OperatorIdentity]] =
      outLinksImmutableValue.toMap

    new Workflow(amberOperators, outLinksImmutable)
  }

  def initializeBreakpoint(controller: ActorRef): Unit = {
    for (pair <- this.workflowInfo.breakpoints) {
      addBreakpoint(controller, pair.operatorID, pair.breakpoint)
    }
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
      //TODO: add new handling logic here
//        controller ! PassBreakpointTo(
//          operatorID,
//          new ConditionalGlobalBreakpoint(
//            breakpointID,
//            tuple => {
//              val texeraTuple = tuple.asInstanceOf[Tuple]
//              predicate.apply(texeraTuple)
//            }
//          )
//        )
      case countBp: CountBreakpoint =>
//        controller ! PassBreakpointTo(
//          operatorID,
//          new CountGlobalBreakpoint("breakpointID", countBp.count)
//        )
    }
  }

  def propagateWorkflowSchema(): Map[OperatorDescriptor, List[Option[Schema]]] = {
    // a map from an operator to the list of its input schema
    val inputSchemaMap =
      new mutable.HashMap[OperatorDescriptor, mutable.MutableList[Option[Schema]]]()
        .withDefault(op => mutable.MutableList.fill(op.operatorInfo.inputPorts.size)(Option.empty))

    // propagate output schema following topological order
    val topologicalOrderIterator = workflow.jgraphtDag.iterator()
    topologicalOrderIterator.forEachRemaining(opID => {
      val op = workflow.getOperator(opID)
      // infer output schema of this operator based on its input schema
      val outputSchema: Option[Schema] = {
        // call to "getOutputSchema" might cause exceptions, wrap in try/catch and return empty schema
        try {
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
            Option.apply(op.getOutputSchema(inputSchemaMap(op).map(s => s.get).toArray))
          }
        } catch {
          case e: Throwable =>
            e.printStackTrace()
            Option.empty
        }
      }
      // exception: if op is a source operator, use its output schema as input schema for autocomplete
      if (op.isInstanceOf[SourceOperatorDescriptor]) {
        inputSchemaMap.update(op, mutable.MutableList(outputSchema))
      }

      // update input schema of all outgoing links
      val outLinks = this.workflowInfo.links.filter(link => link.origin.operatorID == op.operatorID)
      outLinks.foreach(link => {
        val dest = workflowInfo.operators.find(o => o.operatorID == link.destination.operatorID).get
        // get the input schema list, should be pre-populated with size equals to num of ports
        val destInputSchemas = inputSchemaMap(dest)
        // put the schema into the ordinal corresponding to the port
        destInputSchemas(link.destination.portOrdinal) = outputSchema
        inputSchemaMap.update(dest, destInputSchemas)
      })
    })

    inputSchemaMap
      .filter(e => !(e._2.exists(s => s.isEmpty) || e._2.isEmpty))
      .map(e => (e._1, e._2.toList))
      .toMap
  }

}
