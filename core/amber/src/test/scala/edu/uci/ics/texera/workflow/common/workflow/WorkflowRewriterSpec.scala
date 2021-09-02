package edu.uci.ics.texera.workflow.common.workflow

import edu.uci.ics.amber.engine.architecture.storage.OpResultStorage
import edu.uci.ics.amber.engine.architecture.storage.mongo.MongoOpResultStorage
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.operators.sink.{CacheSinkOpDesc, SimpleSinkOpDesc}
import edu.uci.ics.texera.workflow.operators.source.cache.CacheSourceOpDesc
import edu.uci.ics.texera.workflow.operators.source.scan.csv.CSVScanSourceOpDesc
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.mutable

class WorkflowRewriterSpec extends AnyFlatSpec with BeforeAndAfter {

  var opResultStorage: OpResultStorage = _

  before {
    opResultStorage = new MongoOpResultStorage()
  }

  var rewriter: WorkflowRewriter = _

  def operatorToString(operator: OperatorDescriptor): OperatorDescriptor = {
    operator
  }

  it should "modify no operator v2" in {
    val operators = mutable.MutableList[OperatorDescriptor]()
    val links = mutable.MutableList[OperatorLink]()
    val breakpoints = mutable.MutableList[BreakpointInfo]()
    val sourceOperator = new CSVScanSourceOpDesc()
    val sinkOperator = new SimpleSinkOpDesc()
    operators += sourceOperator
    operators += sinkOperator
    val origin = OperatorPort(sourceOperator.operatorID, 0)
    val destination = OperatorPort(sinkOperator.operatorID, 0)
    links += OperatorLink(origin, destination)
    val workflowInfo = WorkflowInfo(operators, links, breakpoints)
    workflowInfo.cachedOperatorIDs = mutable.MutableList[String]()
    rewriter = new WorkflowRewriter(
      workflowInfo,
      mutable.HashMap[String, OperatorDescriptor](),
      mutable.HashMap[String, CacheSourceOpDesc](),
      mutable.HashMap[String, CacheSinkOpDesc](),
      mutable.HashMap[String, WorkflowVertex](),
      opResultStorage
    )
    val rewrittenWorkflowInfo = rewriter.rewrite
    rewrittenWorkflowInfo.operators.foreach(operator => {
      assert(operators.contains(operator))
    })
  }

//  it should "replace source with cache v2" in {
//    val operators = mutable.MutableList[OperatorDescriptor]()
//    val links = mutable.MutableList[OperatorLink]()
//    val breakpoints = mutable.MutableList[BreakpointInfo]()
//    val sourceOperator = new CSVScanSourceOpDesc()
//    val sinkOperator = new SimpleSinkOpDesc()
//    val uuid = UUID.randomUUID().toString
//    operators += sourceOperator
//    operators += sinkOperator
//
//    val origin = OperatorPort(sourceOperator.operatorID, 0)
//    val destination = OperatorPort(sinkOperator.operatorID, 0)
//    links += OperatorLink(origin, destination)
//
//    val workflowInfo = WorkflowInfo(operators, links, breakpoints)
//    workflowInfo.cachedOperatorIDs = mutable.MutableList(sourceOperator.operatorID)
//
//    val tuples = mutable.MutableList[Tuple]()
//    val cacheSourceOperator = new CacheSourceOpDescV2(uuid, opResultStorage)
//    val cacheSinkOperator = new CacheSinkOpDescV2(uuid, opResultStorage)
//    val operatorOutputCache = mutable.HashMap[String, mutable.MutableList[Tuple]]()
//    cacheSinkOperator.schema = new Schema()
//    operatorOutputCache += ((sourceOperator.operatorID, tuples))
//    val cachedOperators = mutable.HashMap[String, OperatorDescriptor]()
//    cachedOperators += ((sourceOperator.operatorID, operatorToString(sourceOperator)))
//    val cacheSourceOperators = mutable.HashMap[String, CacheSourceOpDescV2]()
//    cacheSourceOperators += ((sourceOperator.operatorID, cacheSourceOperator))
//    val cacheSinkOperators = mutable.HashMap[String, CacheSinkOpDescV2]()
//    cacheSinkOperators += ((sourceOperator.operatorID, cacheSinkOperator))
//    val breakpointInfo = BreakpointInfo(sourceOperator.operatorID, CountBreakpoint(0))
//    breakpoints += breakpointInfo
//    rewriter = new WorkflowRewriterV2(
//      workflowInfo,
//      operatorOutputCache,
//      cachedOperators,
//      cacheSourceOperators,
//      cacheSinkOperators,
//      mutable.HashMap[String, WorkflowVertex]()
//    )
//
//    rewriter.operatorRecord += (
//      (
//        sourceOperator.operatorID,
//        rewriter.getWorkflowVertex(sourceOperator)
//      )
//    )
//    val rewrittenWorkflowInfo = rewriter.rewrite_v2
//    assert(2.equals(rewrittenWorkflowInfo.operators.size))
//    assert(rewrittenWorkflowInfo.operators.contains(cacheSourceOperator))
//    assert(rewrittenWorkflowInfo.operators.contains(sinkOperator))
//    assert(1.equals(rewrittenWorkflowInfo.links.size))
//    assert(1.equals(rewrittenWorkflowInfo.breakpoints.size))
//  }
//
//  it should "add a CacheSinkOpDescV2 v2" in {
//    val operators = mutable.MutableList[OperatorDescriptor]()
//    val links = mutable.MutableList[OperatorLink]()
//    val breakpoints = mutable.MutableList[BreakpointInfo]()
//    val sourceOperator = new CSVScanSourceOpDesc()
//    val sinkOperator = new SimpleSinkOpDesc()
//    operators += sourceOperator
//    operators += sinkOperator
//
//    val origin = OperatorPort(sourceOperator.operatorID, 0)
//    val destination = OperatorPort(sinkOperator.operatorID, 0)
//    links += OperatorLink(origin, destination)
//
//    val workflowInfo = WorkflowInfo(operators, links, breakpoints)
//    workflowInfo.cachedOperatorIDs = mutable.MutableList(sourceOperator.operatorID)
//
//    val operatorOutputCache = mutable.HashMap[String, mutable.MutableList[Tuple]]()
//    val cachedOperators = mutable.HashMap[String, OperatorDescriptor]()
//    val cacheSourceOperators = mutable.HashMap[String, CacheSourceOpDescV2]()
//    val cacheSinkOperators = mutable.HashMap[String, CacheSinkOpDescV2]()
//
//    rewriter = new WorkflowRewriterV2(
//      workflowInfo,
//      operatorOutputCache,
//      cachedOperators,
//      cacheSourceOperators,
//      cacheSinkOperators,
//      mutable.HashMap[String, WorkflowVertex]()
//    )
//
//    val rewrittenWorkflowInfo = rewriter.rewrite_v2
//    assert(3.equals(rewrittenWorkflowInfo.operators.size))
//    assert(rewrittenWorkflowInfo.operators.contains(sourceOperator))
//    assert(rewrittenWorkflowInfo.operators(1).isInstanceOf[CacheSinkOpDescV2])
//    assert(rewrittenWorkflowInfo.operators.contains(sinkOperator))
//    assert(2.equals(rewrittenWorkflowInfo.links.size))
//    assert(0.equals(rewrittenWorkflowInfo.breakpoints.size))
//  }
//
//  it should "add correct numbers of operators and links v2" in {
//    val operators = mutable.MutableList[OperatorDescriptor]()
//    val links = mutable.MutableList[OperatorLink]()
//    val breakpoints = mutable.MutableList[BreakpointInfo]()
//    val sourceOperator = new CSVScanSourceOpDesc()
//    val sinkOperator = new SimpleSinkOpDesc()
//    val sinkOperator2 = new SimpleSinkOpDesc()
//    operators += sourceOperator
//    operators += sinkOperator
//    operators += sinkOperator2
//
//    val origin = OperatorPort(sourceOperator.operatorID, 0)
//    val destination = OperatorPort(sinkOperator.operatorID, 0)
//    links += OperatorLink(origin, destination)
//
//    val destination2 = OperatorPort(sinkOperator2.operatorID, 0)
//    links += OperatorLink(origin, destination2)
//
//    val workflowInfo = WorkflowInfo(operators, links, breakpoints)
//    workflowInfo.cachedOperatorIDs = mutable.MutableList(sourceOperator.operatorID)
//
//    val operatorOutputCache = mutable.HashMap[String, mutable.MutableList[Tuple]]()
//    val cachedOperators = mutable.HashMap[String, OperatorDescriptor]()
//    val cacheSourceOperators = mutable.HashMap[String, CacheSourceOpDescV2]()
//    val cacheSinkOperators = mutable.HashMap[String, CacheSinkOpDescV2]()
//
//    val breakpointInfo = BreakpointInfo(sourceOperator.operatorID, CountBreakpoint(0))
//    breakpoints += breakpointInfo
//
//    rewriter = new WorkflowRewriterV2(
//      workflowInfo,
//      operatorOutputCache,
//      cachedOperators,
//      cacheSourceOperators,
//      cacheSinkOperators,
//      mutable.HashMap[String, WorkflowVertex]()
//    )
//
//    val rewrittenWorkflowInfo = rewriter.rewrite_v2
//    assert(4.equals(rewrittenWorkflowInfo.operators.size))
//    assert(rewrittenWorkflowInfo.operators.contains(sourceOperator))
//    assert(rewrittenWorkflowInfo.operators.contains(sinkOperator))
//    assert(3.equals(rewrittenWorkflowInfo.links.size))
//    assert(1.equals(rewrittenWorkflowInfo.breakpoints.size))
//  }
//
//  it should "replace source and filter with cache v2" in {
//    val operators = mutable.MutableList[OperatorDescriptor]()
//    val links = mutable.MutableList[OperatorLink]()
//    val breakpoints = mutable.MutableList[BreakpointInfo]()
//    val sourceOperator = new CSVScanSourceOpDesc()
//    val filterOperator = new RegexOpDesc()
//    val sinkOperator = new SimpleSinkOpDesc()
//    operators += sourceOperator
//    operators += filterOperator
//    operators += sinkOperator
//
//    val origin = OperatorPort(sourceOperator.operatorID, 0)
//    val destination = OperatorPort(filterOperator.operatorID, 0)
//    links += OperatorLink(origin, destination)
//
//    val origin2 = OperatorPort(filterOperator.operatorID, 0)
//    val destination2 = OperatorPort(sinkOperator.operatorID, 0)
//    links += OperatorLink(origin2, destination2)
//
//    val workflowInfo = WorkflowInfo(operators, links, breakpoints)
//
//    val tuples = mutable.MutableList[Tuple]()
//    val cacheSourceOperator = new CacheSourceOpDescV2(tuples)
//    val cacheSinkOperator = new CacheSinkOpDescV2(tuples)
//    val operatorOutputCache = mutable.HashMap[String, mutable.MutableList[Tuple]]()
//    cacheSinkOperator.schema = new Schema()
//
//    val cachedOperatorID = filterOperator.operatorID
//
//    workflowInfo.cachedOperatorIDs = mutable.MutableList(cachedOperatorID)
//    operatorOutputCache += ((cachedOperatorID, tuples))
//
//    val cachedOperators = mutable.HashMap[String, OperatorDescriptor]()
//    cachedOperators += ((cachedOperatorID, operatorToString(filterOperator)))
//    val cacheSourceOperators = mutable.HashMap[String, CacheSourceOpDescV2]()
//    cacheSourceOperators += ((cachedOperatorID, cacheSourceOperator))
//    val cacheSinkOperators = mutable.HashMap[String, CacheSinkOpDescV2]()
//    cacheSinkOperators += ((cachedOperatorID, cacheSinkOperator))
//
//    val breakpointInfo = BreakpointInfo(sourceOperator.operatorID, CountBreakpoint(0))
//    breakpoints += breakpointInfo
//    rewriter = new WorkflowRewriterV2(
//      workflowInfo,
//      operatorOutputCache,
//      cachedOperators,
//      cacheSourceOperators,
//      cacheSinkOperators,
//      mutable.HashMap[String, WorkflowVertex]()
//    )
//
//    rewriter.operatorRecord += (
//      (
//        sourceOperator.operatorID,
//        rewriter.getWorkflowVertex(sourceOperator)
//      )
//    )
//    rewriter.operatorRecord += (
//      (
//        filterOperator.operatorID,
//        rewriter.getWorkflowVertex(filterOperator)
//      )
//    )
//
//    val rewrittenWorkflowInfo = rewriter.rewrite_v2
//    assert(2.equals(rewrittenWorkflowInfo.operators.size))
//    assert(rewrittenWorkflowInfo.operators.contains(cacheSourceOperator))
//    assert(rewrittenWorkflowInfo.operators.contains(sinkOperator))
//    assert(1.equals(rewrittenWorkflowInfo.links.size))
//    assert(0.equals(rewrittenWorkflowInfo.breakpoints.size))
//  }
//
//  it should "invalidate cache and replace no operator v2" in {
//    val operators = mutable.MutableList[OperatorDescriptor]()
//    val links = mutable.MutableList[OperatorLink]()
//    val breakpoints = mutable.MutableList[BreakpointInfo]()
//    val sourceOperator = new CSVScanSourceOpDesc()
//    val filterOperator = new RegexOpDesc()
//    val sinkOperator = new SimpleSinkOpDesc()
//    operators += sourceOperator
//    operators += filterOperator
//    operators += sinkOperator
//
//    val origin = OperatorPort(sourceOperator.operatorID, 0)
//    val destination = OperatorPort(filterOperator.operatorID, 0)
//    links += OperatorLink(origin, destination)
//
//    val origin2 = OperatorPort(filterOperator.operatorID, 0)
//    val destination2 = OperatorPort(sinkOperator.operatorID, 0)
//    links += OperatorLink(origin2, destination2)
//
//    val workflowInfo = WorkflowInfo(operators, links, breakpoints)
//
//    val tuples = mutable.MutableList[Tuple]()
//    val cacheSourceOperator = new CacheSourceOpDescV2(tuples)
//    val cacheSinkOperator = new CacheSinkOpDescV2(tuples)
//    val operatorOutputCache = mutable.HashMap[String, mutable.MutableList[Tuple]]()
//    cacheSinkOperator.schema = new Schema()
//
//    val cachedOperatorID = filterOperator.operatorID
//
//    workflowInfo.cachedOperatorIDs = mutable.MutableList(cachedOperatorID)
//    operatorOutputCache += ((cachedOperatorID, tuples))
//
//    val cachedOperators = mutable.HashMap[String, OperatorDescriptor]()
//    cachedOperators += ((cachedOperatorID, operatorToString(filterOperator)))
//    val cacheSourceOperators = mutable.HashMap[String, CacheSourceOpDescV2]()
//    cacheSourceOperators += ((cachedOperatorID, cacheSourceOperator))
//    val cacheSinkOperators = mutable.HashMap[String, CacheSinkOpDescV2]()
//    cacheSinkOperators += ((cachedOperatorID, cacheSinkOperator))
//
//    val breakpointInfo = BreakpointInfo(sourceOperator.operatorID, CountBreakpoint(0))
//    breakpoints += breakpointInfo
//    rewriter = new WorkflowRewriterV2(
//      workflowInfo,
//      operatorOutputCache,
//      cachedOperators,
//      cacheSourceOperators,
//      cacheSinkOperators,
//      mutable.HashMap[String, WorkflowVertex]()
//    )
//
//    val modifiedSourceOperator = new CSVScanSourceOpDesc()
//    modifiedSourceOperator.hasHeader = false
//    modifiedSourceOperator.operatorID = sourceOperator.operatorID
//    rewriter.operatorRecord += (
//      (
//        sourceOperator.operatorID,
//        rewriter.getWorkflowVertex(modifiedSourceOperator)
//      )
//    )
//    rewriter.operatorRecord += (
//      (
//        filterOperator.operatorID,
//        rewriter.getWorkflowVertex(filterOperator)
//      )
//    )
//
//    val rewrittenWorkflowInfo = rewriter.rewrite_v2
//    assert(4.equals(rewrittenWorkflowInfo.operators.size))
//    assert(!rewrittenWorkflowInfo.operators.contains(cacheSourceOperator))
//    assert(rewrittenWorkflowInfo.operators.contains(sourceOperator))
//    assert(rewrittenWorkflowInfo.operators.contains(filterOperator))
//    assert(rewrittenWorkflowInfo.operators.contains(sinkOperator))
//    assert(3.equals(rewrittenWorkflowInfo.links.size))
//    assert(1.equals(rewrittenWorkflowInfo.breakpoints.size))
//    assert(3.equals(rewriter.operatorRecord.size))
//  }
}
